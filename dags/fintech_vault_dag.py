import logging

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

OWNER = "Timmy-DE"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "prod"

args = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def make_dbt_run_task(dag: DAG, task_id: str, select: str) -> BashOperator:
    log.info("Creating dbt run task: task_id=%s, select=%s", task_id, select)
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"echo 'Starting dbt run for layer: {select}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--select {select} "
            f"--target {DBT_TARGET} && "
            f"echo 'Finished dbt run for layer: {select}'"
        ),
        dag=dag,
    )


def make_dbt_test_task(dag: DAG, task_id: str, select: str = None) -> BashOperator:
    log.info("Creating dbt test task: task_id=%s, select=%s", task_id, select)
    select_flag = f"--select {select}" if select else ""
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"echo 'Starting dbt tests' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"{select_flag} "
            f"--target {DBT_TARGET} && "
            f"echo 'dbt tests finished'"
        ),
        dag=dag,
    )


with DAG(
    dag_id="fintech_vault_pipeline",
    description="Data Vault 2.0: Raw → Vault → Gold",
    schedule_interval="*/30 * * * *",
    start_date=pendulum.datetime(2026, 6, 24, tz="Europe/Moscow"),
    catchup=False,
    default_args=args,
    tags=["fintech", "data_vault", "dbt", "clickhouse"],
) as dag:
    raw = make_dbt_run_task(dag, "dbt_run_raw", "raw")
    hubs = make_dbt_run_task(dag, "dbt_run_hubs", "vault.hubs")
    links = make_dbt_run_task(dag, "dbt_run_links", "vault.links")
    satellites = make_dbt_run_task(dag, "dbt_run_satellites", "vault.satellites")
    gold = make_dbt_run_task(dag, "dbt_run_gold", "gold")
    tests = make_dbt_test_task(dag, "dbt_test")

    raw >> hubs >> links >> satellites >> gold >> tests
