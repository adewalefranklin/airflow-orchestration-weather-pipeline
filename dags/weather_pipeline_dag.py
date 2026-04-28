from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/project")
sys.path.append("/opt/airflow/project/src")

from weather_pipeline.extract import WeatherExtractor
from weather_pipeline.load import S3Loader


def extract_task():
    import os
    from dotenv import load_dotenv
    from weather_pipeline.extract import WeatherExtractor

    load_dotenv("/opt/airflow/project/.env")

    extractor = WeatherExtractor(
        api_key=os.getenv("API_KEY"),
        base_url=os.getenv("BASE_URL"),
    )

    data = extractor.fetch_weather(
        location="Berlin",
        start_date="2026-04-15",
        end_date="2026-04-15",
    )

    return data

def load_task(ti):
    import os
    from dotenv import load_dotenv
    from weather_pipeline.load import S3Loader

    load_dotenv("/opt/airflow/project/.env")

    data = ti.xcom_pull(task_ids="extract_task")

    loader = S3Loader(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_region=os.getenv("AWS_REGION"),
        bucket_name=os.getenv("AWS_S3_BUCKET_NAME"),
        prefix=os.getenv("AWS_PREFIX"),
    )

    loader.s3_upload(
        data,
        "Berlin",
        "2026-04-15",
        "2026-04-15",
    )

with DAG(
    dag_id="weather_pipeline_extract_load_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task,
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_task,
    )

    extract >> load