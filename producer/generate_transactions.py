"""
generate_transactions.py
Генератор банковских транзакций → Kafka топик 'transactions'

Установка:
    pip install kafka-python faker

Запуск:
    python producer/generate_transactions.py
"""

import json
import random
import time
import uuid
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer

# ---- Настройки ----
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"
MESSAGES_PER_SECOND = 5  # скорость генерации

fake = Faker()

# ---- Справочники ----
MERCHANTS = [
    {"merchant_id": "M001", "name": "Apple Store", "category": "electronics"},
    {"merchant_id": "M002", "name": "Netflix", "category": "entertainment"},
    {"merchant_id": "M003", "name": "Albert Heijn", "category": "groceries"},
    {"merchant_id": "M004", "name": "Shell", "category": "fuel"},
    {"merchant_id": "M005", "name": "Booking.com", "category": "travel"},
    {"merchant_id": "M006", "name": "Uber", "category": "transport"},
    {"merchant_id": "M007", "name": "Zara", "category": "clothing"},
    {"merchant_id": "M008", "name": "McDonald's", "category": "food"},
    {"merchant_id": "M009", "name": "Amazon", "category": "e-commerce"},
    {"merchant_id": "M010", "name": "Spotify", "category": "entertainment"},
]

STATUSES = [
    "completed",
    "completed",
    "completed",
    "pending",
    "failed",
]  # completed чаще

CURRENCIES = ["EUR", "USD", "GBP"]

COUNTRIES = ["NL", "DE", "FR", "GB", "US", "IT", "ES"]


# ---- Генераторы ----


def generate_customer_id() -> str:
    """Генерирует customer_id — небольшой пул чтобы были повторы (реалистично)"""
    return f"C{random.randint(1, 200):04d}"


def generate_account_id(customer_id: str) -> str:
    """У одного клиента может быть несколько счетов"""
    account_num = random.randint(1, 3)
    return f"ACC-{customer_id}-{account_num:02d}"


def is_anomaly() -> bool:
    """1% транзакций — аномальные (очень большая сумма)"""
    return random.random() < 0.01


def generate_transaction() -> dict:
    """Собирает одну транзакцию"""
    customer_id = generate_customer_id()
    account_id = generate_account_id(customer_id)
    merchant = random.choice(MERCHANTS)

    # Аномальные транзакции — сумма x50
    if is_anomaly():
        amount = round(random.uniform(5000, 50000), 2)
        is_suspicious = True
    else:
        amount = round(random.uniform(1, 500), 2)
        is_suspicious = False

    transaction = {
        # --- Бизнес-ключи (будут хэшироваться в Data Vault) ---
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "account_id": account_id,
        "merchant_id": merchant["merchant_id"],
        # --- Атрибуты транзакции (→ sat_transaction_details) ---
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "status": random.choice(STATUSES),
        "merchant_name": merchant["name"],
        "category": merchant["category"],
        "is_suspicious": is_suspicious,
        # --- Атрибуты клиента (→ sat_customer_info) ---
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "customer_country": random.choice(COUNTRIES),
        # --- Служебные поля Data Vault ---
        "event_timestamp": datetime.utcnow().isoformat(),
        "record_source": "kafka.transactions",
    }

    return transaction


# ---- Kafka Producer ----


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        # Надёжность
        acks="all",
        retries=3,
    )


def on_send_success(record_metadata):
    pass  # тихий успех


def on_send_error(exc):
    print(f"❌ Ошибка отправки: {exc}")


# ---- Main ----


def main():
    print(f"🚀 Запуск генератора транзакций")
    print(f"   Топик:   {KAFKA_TOPIC}")
    print(f"   Скорость: {MESSAGES_PER_SECOND} сообщений/сек")
    print(f"   Ctrl+C для остановки\n")

    producer = create_producer()
    sent = 0

    try:
        while True:
            transaction = generate_transaction()

            producer.send(
                topic=KAFKA_TOPIC,
                key=transaction["transaction_id"],
                value=transaction,
            ).add_callback(on_send_success).add_errback(on_send_error)

            sent += 1

            # Логируем каждые 10 сообщений
            if sent % 10 == 0:
                flag = "🚨 АНОМАЛИЯ" if transaction["is_suspicious"] else ""
                print(
                    f"[{sent:>6}] {transaction['event_timestamp']} | "
                    f"customer={transaction['customer_id']} | "
                    f"merchant={transaction['merchant_name']:<15} | "
                    f"amount={transaction['amount']:>8.2f} {transaction['currency']} | "
                    f"status={transaction['status']:<10} {flag}"
                )

            time.sleep(1 / MESSAGES_PER_SECOND)

    except KeyboardInterrupt:
        print(f"\n⏹ Остановлено. Отправлено сообщений: {sent}")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer закрыт")


if __name__ == "__main__":
    main()
