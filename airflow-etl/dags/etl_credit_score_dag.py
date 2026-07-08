from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/dags')
from etl_functions import extract, transform, load

default_args = {
    'owner': 'data-girls-finance',
    'retries': 1,
}

with DAG(
    dag_id='etl_credit_score',
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'credit-score'],
) as dag:

    t1_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
    )

    t2_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )

    t3_load = PythonOperator(
        task_id='load_to_s3',
        python_callable=load,
    )

    t1_extract >> t2_transform >> t3_load