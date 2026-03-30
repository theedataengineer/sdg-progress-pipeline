"""
DAG 4: dbt_transform_dag
==========================
Runs Great Expectations validation then dbt transformations
every Monday at 2 AM — after the Sunday batch ingestion.

Schedule: Every Monday at 2 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner":            "sdg-pipeline",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

def run_great_expectations(**context):
    """
    Run data quality checks on raw tables before dbt runs.
    If checks fail, dbt transformation is skipped.
    """
    import psycopg2

    conn = psycopg2.connect(
        host="postgres", port=5432,
        dbname="sdg_warehouse",
        user="sdg_user", password="sdg_password_2024"
    )
    cur = conn.cursor()

    checks_passed = 0
    checks_failed = 0
    failures = []

    # Check 1: raw_wb_sdg_indicators has data
    cur.execute("SELECT COUNT(*) FROM raw_wb_sdg_indicators")
    count = cur.fetchone()[0]
    if count >= 100:
        checks_passed += 1
        print(f"✓ raw_wb_sdg_indicators has {count} rows")
    else:
        checks_failed += 1
        failures.append(f"raw_wb_sdg_indicators only has {count} rows (expected 100+)")

    # Check 2: No null country codes
    cur.execute("""
        SELECT COUNT(*) FROM raw_wb_sdg_indicators
        WHERE country_code IS NULL OR country_code = ''
    """)
    null_count = cur.fetchone()[0]
    if null_count == 0:
        checks_passed += 1
        print("✓ No null country codes")
    else:
        checks_failed += 1
        failures.append(f"{null_count} records have null country codes")

    # Check 3: Years are within valid SDG range
    cur.execute("""
        SELECT COUNT(*) FROM raw_wb_sdg_indicators
        WHERE year < 2000 OR year > 2030
    """)
    bad_years = cur.fetchone()[0]
    if bad_years == 0:
        checks_passed += 1
        print("✓ All years within 2000-2030 range")
    else:
        checks_failed += 1
        failures.append(f"{bad_years} records have invalid years")

    # Check 4: Values are not negative for non-growth indicators
    cur.execute("""
        SELECT COUNT(*) FROM raw_wb_sdg_indicators
        WHERE value < 0
        AND indicator_code != 'NY.GDP.MKTP.KD.ZG'
    """)
    neg_values = cur.fetchone()[0]
    if neg_values == 0:
        checks_passed += 1
        print("✓ No unexpected negative values")
    else:
        checks_failed += 1
        failures.append(f"{neg_values} records have unexpected negative values")

    # Check 5: Kafka stream table has recent data
    cur.execute("""
        SELECT COUNT(*) FROM raw_kafka_sdg_stream
        WHERE extracted_at > NOW() - INTERVAL '7 days'
    """)
    recent = cur.fetchone()[0]
    if recent > 0:
        checks_passed += 1
        print(f"✓ {recent} recent streaming records (last 7 days)")
    else:
        checks_failed += 1
        failures.append("No recent streaming records in last 7 days")

    cur.close()
    conn.close()

    print(f"\nGreat Expectations: {checks_passed} passed, {checks_failed} failed")

    if checks_failed > 0:
        raise ValueError(f"Data quality checks failed: {failures}")

    return {"passed": checks_passed, "failed": checks_failed}

with DAG(
    dag_id="dbt_transform_dag",
    description="Run GE validation then dbt transformations",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * 1",  # Monday 2 AM
    catchup=False,
    tags=["sdg", "dbt", "great-expectations", "transform"],
) as dag:

    start = EmptyOperator(task_id="start")

    validate = PythonOperator(
        task_id="great_expectations_validation",
        python_callable=run_great_expectations,
        provide_context=True,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select staging",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select intermediate",
    )

    dbt_marts = BashOperator(
        task_id="dbt_mart_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_tests",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt_project && dbt docs generate",
    )

    end = EmptyOperator(task_id="end")

    start >> validate >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test >> dbt_docs >> end
