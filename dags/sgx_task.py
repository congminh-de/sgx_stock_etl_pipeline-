from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.models import Variable
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts")
from sgx_stock import run_etl_process, seed_daily_job

default_args = {
    'owner': 'minhde',
    'depend_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=4)
}

with DAG(
    'sgx_etl_to_mysql',
    default_args=default_args,
    schedule='0 18 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['mysql', 'finance'],
) as dag:
    seed_task = PythonOperator(
        task_id = 'seed_daily_job',
        python_callable = seed_daily_job,
    )
process_task = PythonOperator(
        task_id='process_pending_files',
        python_callable=run_etl_process
    )

seed_task >> process_task
    