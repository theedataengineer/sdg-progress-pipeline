"""
DAG 1: kafka_producer_dag
Polls World Bank API every 6 hours, publishes to Kafka.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys

sys.path.append("/opt/airflow")

default_args = {
    "owner": "sdg-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def run_producer(**context):
    from kafka.producer.wb_producer import run_producer_once
    stats = run_producer_once()
    context["ti"].xcom_push(key="producer_stats", value=stats)
    print(f"Published: {stats.get('records_published')} | Failed: {stats.get('records_failed')}")
    return stats

def log_metrics(**context):
    stats = context["ti"].xcom_pull(task_ids="publish_to_kafka", key="producer_stats")
    print(f"Records published: {stats.get('records_published', 0)}")
    print(f"Records failed:    {stats.get('records_failed', 0)}")
    print(f"Topics written:    {stats.get('topics_written', [])}")

with DAG(
    dag_id="kafka_producer_dag",
    description="Poll World Bank API and publish SDG updates to Kafka",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["sdg", "kafka", "streaming", "world-bank"],
) as dag:

    start      = EmptyOperator(task_id="start")
    publish    = PythonOperator(task_id="publish_to_kafka", python_callable=run_producer, provide_context=True)
    log        = PythonOperator(task_id="log_metrics", python_callable=log_metrics, provide_context=True)
    end        = EmptyOperator(task_id="end")

    start >> publish >> log >> end
