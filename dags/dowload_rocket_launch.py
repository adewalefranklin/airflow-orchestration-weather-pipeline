import requests
import json
import pathlib
import airflow 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="download_rocket_launch",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    download_launches = BashOperator(
        task_id="download_launches",
        bash_command='curl -L "https://ll.thespacedevs.com/2.0.0/launch/upcoming" -o /opt/airflow/data/launches.json'
    )

def get_images():
    # if dir exists
    pathlib.Path('/opt/airflow/data/launch_images').mkdir(parents=True, exist_ok=True)
    with open('/opt/airflow/data/launches.json') as f:
        launches = json.load(f)
        image_urls=[launch['image'] for launch in launches['results'] if launch.get('image')]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                response.raise_for_status()  # Check if the request was successful
                image_name = image_url.split("/")[-1]
                with open(f'/opt/airflow/data/launch_images/{image_name}', 'wb') as img_file:
                    img_file.write(response.content)
            except requests.RequestException as e:
                print(f'Error downloading {image_name}: {e}')
        

get_images = PythonOperator(
    task_id='get_images',
    python_callable=get_images,
    dag=dag,
)

notify = PythonOperator(
    task_id='notify',
    python_callable=lambda: print("All images downloaded!"),
    dag=dag,
)

download_launches >> get_images >> notify