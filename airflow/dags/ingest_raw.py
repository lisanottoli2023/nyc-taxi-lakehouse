import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
RAW_DIR = "/opt/lakehouse/raw/yellow"


def download_yellow_taxi(year_month: str):
    year, month = year_month.split("-")
    dest_dir = os.path.join(RAW_DIR, f"year={year}", f"month={month}")
    dest_file = os.path.join(dest_dir, f"yellow_tripdata_{year_month}.parquet")

    if os.path.exists(dest_file):
        print(f"Already exists, skipping: {dest_file}")
        return

    os.makedirs(dest_dir, exist_ok=True)

    url = TLC_URL.format(year_month=year_month)
    print(f"Downloading {url}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print(f"Saved to {dest_file}")


def on_failure_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    print(f"DAG FAILED: {dag_id} | Task: {task_id} | Date: {execution_date}")


default_args = {
    "owner": "admin",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    "ingest_raw",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={"year_month": "2026-01"},
) as dag:

    download = PythonOperator(
        task_id="download_yellow_taxi",
        python_callable=download_yellow_taxi,
        op_kwargs={"year_month": "{{ params.year_month }}"},
    )
