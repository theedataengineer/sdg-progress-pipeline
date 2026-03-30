"""
DAG 2: kafka_consumer_dag
===========================
Consumes messages from Kafka topics, writes batches
to MinIO, then loads into PostgreSQL raw tables.

Schedule: Every 6 hours (offset 30 min from producer)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys

sys.path.append("/opt/airflow")

default_args = {
    "owner":            "sdg-pipeline",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

def run_consumer_batch(**context):
    from kafka.consumer.sdg_consumer import run_consumer
    run_consumer(duration_seconds=300)  # consume for 5 minutes
    print("Consumer batch complete")

def load_minio_to_postgres(**context):
    """
    Load latest MinIO batch files into PostgreSQL raw tables.
    Reads the most recent batch files and inserts into raw tables.
    """
    import boto3
    import json
    import psycopg2
    from botocore.client import Config
    from datetime import datetime, timezone

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="sdg_warehouse",
        user="sdg_user",
        password="sdg_password_2024"
    )
    cur = conn.cursor()

    # Get today's batch files
    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    prefix = f"streaming/kafka-batches/validated/{today}/"

    response = s3.list_objects_v2(
        Bucket="sdg-progress-pipeline",
        Prefix=prefix
    )

    files = [o for o in response.get("Contents", []) if o["Size"] > 0]
    total_loaded = 0

    for file_obj in files:
        obj = s3.get_object(
            Bucket="sdg-progress-pipeline",
            Key=file_obj["Key"]
        )
        batch = json.loads(obj["Body"].read().decode("utf-8"))
        records = batch.get("records", [])

        for record in records:
            cur.execute("""
                INSERT INTO raw_kafka_sdg_stream (
                    record_id, goal, indicator_code, indicator_name,
                    country_code, country_name, year, value,
                    unit, obs_status, source,
                    extracted_at, published_at, batch_file
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (record_id) DO UPDATE SET
                    value = EXCLUDED.value,
                    extracted_at = EXCLUDED.extracted_at
            """, (
                record.get("record_id"),
                record.get("goal"),
                record.get("indicator_code"),
                record.get("indicator_name"),
                record.get("country_code"),
                record.get("country_name"),
                record.get("year"),
                record.get("value"),
                record.get("unit", ""),
                record.get("obs_status", ""),
                record.get("source"),
                record.get("extracted_at"),
                record.get("published_at"),
                file_obj["Key"]
            ))
            total_loaded += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {total_loaded} records into PostgreSQL")
    return total_loaded

with DAG(
    dag_id="kafka_consumer_dag",
    description="Consume Kafka messages and load to MinIO then PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="30 */6 * * *",  # offset 30min from producer
    catchup=False,
    tags=["sdg", "kafka", "streaming", "minio", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    consume = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=run_consumer_batch,
        provide_context=True,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_minio_to_postgres,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> consume >> load_to_postgres >> end
