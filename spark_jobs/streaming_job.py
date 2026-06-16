"""
streaming_job.py
Spark Structured Streaming: Kafka → ClickHouse Raw

Установка:
    pip install pyspark clickhouse-connect

Запуск:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_jobs/streaming_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, md5, concat_ws,
    current_timestamp, lit, when
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType
)
import clickhouse_connect

# ---- Настройки ----
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "transactions"
CLICKHOUSE_HOST         = "localhost"
CLICKHOUSE_PORT         = 8123
CLICKHOUSE_USER         = "default"
CLICKHOUSE_PASSWORD     = ""
CLICKHOUSE_DATABASE     = "default"
CHECKPOINT_DIR          = "/tmp/spark_checkpoints/transactions"

# ---- Схема входящего JSON ----
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("customer_id",       StringType(),  True),
    StructField("account_id",        StringType(),  True),
    StructField("merchant_id",       StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("currency",          StringType(),  True),
    StructField("status",            StringType(),  True),
    StructField("merchant_name",     StringType(),  True),
    StructField("category",          StringType(),  True),
    StructField("is_suspicious",     BooleanType(), True),
    StructField("customer_name",     StringType(),  True),
    StructField("customer_email",    StringType(),  True),
    StructField("customer_country",  StringType(),  True),
    StructField("event_timestamp",   StringType(),  True),
    StructField("record_source",     StringType(),  True),
])


# ---- Инициализация ClickHouse ----

def init_clickhouse():
    """Создаёт таблицу raw_transactions если не существует"""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

    client.command("""
        CREATE TABLE IF NOT EXISTS raw_transactions (
            -- Хэши для Data Vault (считаются здесь, в Spark)
            transaction_hash_key  String,
            customer_hash_key     String,
            account_hash_key      String,
            merchant_hash_key     String,

            -- Бизнес-ключи
            transaction_id    String,
            customer_id       String,
            account_id        String,
            merchant_id       String,

            -- Атрибуты транзакции
            amount            Float64,
            currency          String,
            status            String,
            merchant_name     String,
            category          String,
            is_suspicious     Bool,

            -- Атрибуты клиента
            customer_name     String,
            customer_email    String,
            customer_country  String,

            -- Служебные поля Data Vault
            event_timestamp   String,
            load_date         DateTime DEFAULT now(),
            record_source     String
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(load_date)
        ORDER BY (load_date, transaction_hash_key)
    """)

    print("✅ Таблица raw_transactions готова")
    client.close()


# ---- Запись батча в ClickHouse ----

def write_to_clickhouse(batch_df, batch_id):
    """Пишет каждый микробатч в ClickHouse"""
    rows = batch_df.collect()

    if not rows:
        return

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

    data = [
        [
            row.transaction_hash_key,
            row.customer_hash_key,
            row.account_hash_key,
            row.merchant_hash_key,
            row.transaction_id,
            row.customer_id,
            row.account_id,
            row.merchant_id,
            row.amount,
            row.currency,
            row.status,
            row.merchant_name,
            row.category,
            bool(row.is_suspicious),
            row.customer_name,
            row.customer_email,
            row.customer_country,
            row.event_timestamp,
            row.record_source,
        ]
        for row in rows
    ]

    columns = [
        "transaction_hash_key", "customer_hash_key",
        "account_hash_key", "merchant_hash_key",
        "transaction_id", "customer_id", "account_id", "merchant_id",
        "amount", "currency", "status", "merchant_name", "category",
        "is_suspicious", "customer_name", "customer_email", "customer_country",
        "event_timestamp", "record_source",
    ]

    client.insert("raw_transactions", data, column_names=columns)
    print(f"✅ Batch {batch_id}: записано {len(rows)} строк в ClickHouse")
    client.close()


# ---- Main ----

def main():
    print("🚀 Запуск Spark Structured Streaming")

    # Инициализируем таблицу в ClickHouse
    init_clickhouse()

    spark = SparkSession.builder \
        .appName("FintechVault_Streaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Читаем из Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Парсим JSON
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
    ).select("data.*")

    # Фильтруем невалидные записи
    clean_df = parsed_df.filter(
        col("transaction_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("merchant_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

    # Считаем хэши для Data Vault 2.0
    # MD5 от бизнес-ключа — стандарт DV2.0
    hashed_df = clean_df \
        .withColumn("transaction_hash_key", md5(col("transaction_id"))) \
        .withColumn("customer_hash_key",    md5(col("customer_id"))) \
        .withColumn("account_hash_key",     md5(col("account_id"))) \
        .withColumn("merchant_hash_key",    md5(col("merchant_id")))

    # Пишем в ClickHouse через foreachBatch
    query = hashed_df.writeStream \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"⚡ Стриминг запущен. Читаем топик '{KAFKA_TOPIC}'...")
    print(f"   Checkpoint: {CHECKPOINT_DIR}")
    print(f"   Интервал батча: 10 секунд")
    print(f"   Ctrl+C для остановки\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
