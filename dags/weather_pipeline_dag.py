"""Phase 5: Airflow orchestration of the weather pipeline.

DAG topology mirrors the architecture diagram:
    ingest_to_s3  ->  validate_transform  ->  load_postgres
"""

import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/pipeline")   # container mount of the project


def _alert_on_failure(context):
    ti = context["task_instance"]
    print(f"*** PIPELINE ALERT *** task={ti.task_id} dag={ti.dag_id} "
          f"run={context['run_id']} failed. Exception: {context.get('exception')}")


default_args = {
    "owner": "weather-pipeline",
    "retries": 0,
    "on_failure_callback": _alert_on_failure,
}


def _ingest():
    from ingestion.fetch_weather import run_ingestion
    run_ingestion()


def _process():
    from transformation.process_raw import run_processing
    run_processing()


def _load():
    from loading.load_postgres import run_load
    run_load()


with DAG(
    dag_id="weather_pipeline",
    description="Open-Meteo -> S3 -> validate/transform -> Postgres. Runs daily at 6 UTC (8 SAST)",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 9, 20, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["weather", "production"],
) as dag:

    ingest = PythonOperator(task_id="ingest_to_s3", python_callable=_ingest)
    process = PythonOperator(task_id="validate_transform", python_callable=_process)
    load = PythonOperator(task_id="load_postgres", python_callable=_load)

    ingest >> process >> load
