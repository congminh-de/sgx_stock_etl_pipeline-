from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts")
from sgx_stock import run_etl_process

default_args = {
    'owner': 'minhde',
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
}

with DAG(
    'sgx_etl_to_mysql',
    default_args=default_args,
    schedule='0 18 * * 1-5',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mysql', 'finance'],
) as dag:

    def task_wrapper(**kwargs):
        current_id = int(Variable.get("sgx_current_id", default_var=6076))
        run_etl_process(current_id, **kwargs)

    def increase_id():
        current = int(Variable.get("sgx_current_id", default_var=6076))
        Variable.set("sgx_current_id", str(current + 1))
        print(f"Next ID: {current + 1}")

    run_task = PythonOperator(
        task_id='run_etl_job',
        python_callable=task_wrapper,
        provide_context=True 
    )

    update_task = PythonOperator(
        task_id='update_next_id',
        python_callable=increase_id
    )

    run_task >> update_task