"""
DAG 3: batch_ingestion_dag
============================
Weekly full refresh of all SDG indicators from
World Bank API. Uploads to MinIO and loads to PostgreSQL.

Schedule: Every Sunday at 11 PM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys

sys.path.append("/opt/airflow")

default_args = {
    "owner":            "sdg-pipeline",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

def extract_world_bank_full(**context):
    from ingestion.extract_world_bank import extract_africa_sdg_data
    summary = extract_africa_sdg_data(
        start_year=2000,
        end_year=2024,
        output_dir="/opt/airflow/data/raw/world_bank"
    )
    context["ti"].xcom_push(key="extraction_summary", value=summary)
    print(f"Extracted {summary['total_records']} records")
    return summary

def upload_to_minio(**context):
    import boto3
    import json
    from pathlib import Path
    from botocore.client import Config
    from datetime import datetime, timezone

    summary = context["ti"].xcom_pull(
        task_ids="extract_world_bank",
        key="extraction_summary"
    )

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    data_dir = Path("/opt/airflow/data/raw/world_bank")
    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    uploaded = 0

    for json_file in data_dir.glob("*.json"):
        s3_key = f"raw/world-bank/{today}/{json_file.name}"
        s3.upload_file(
            str(json_file),
            "sdg-progress-pipeline",
            s3_key
        )
        uploaded += 1
        print(f"Uploaded: {s3_key}")

    print(f"Uploaded {uploaded} files to MinIO")
    return uploaded

def load_batch_to_postgres(**context):
    import psycopg2
    import json
    from pathlib import Path

    conn = psycopg2.connect(
        host="postgres", port=5432,
        dbname="sdg_warehouse",
        user="sdg_user", password="sdg_password_2024"
    )
    cur = conn.cursor()

    data_dir = Path("/opt/airflow/data/raw/world_bank")
    combined = data_dir / "all_indicators_africa.json"

    if not combined.exists():
        print("Combined file not found")
        return 0

    with open(combined) as f:
        records = json.load(f)

    total = 0
    for record in records:
        cur.execute("""
            INSERT INTO raw_wb_sdg_indicators (
                indicator_code, indicator_name, goal,
                country_code, country_name, year, value,
                unit, obs_status, source, extracted_at
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT (country_code, indicator_code, year)
            DO UPDATE SET
                value = EXCLUDED.value,
                extracted_at = EXCLUDED.extracted_at
        """, (
            record.get("indicator_code"),
            record.get("indicator_name"),
            record.get("goal"),
            record.get("country_code"),
            record.get("country_name"),
            record.get("year"),
            record.get("value"),
            record.get("unit", ""),
            record.get("obs_status", ""),
            record.get("source"),
            record.get("extracted_at"),
        ))
        total += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {total} records into raw_wb_sdg_indicators")
    return total

with DAG(
    dag_id="batch_ingestion_dag",
    description="Weekly full refresh of World Bank SDG indicators",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 23 * * 0",  # Sunday 11 PM
    catchup=False,
    tags=["sdg", "batch", "world-bank", "minio", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_world_bank",
        python_callable=extract_world_bank_full,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )

    upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_batch_to_postgres,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> upload >> load >> end
