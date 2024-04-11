from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jv.munizkali@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG',
)

def print_hello():
    return 'Hello Airflow!'

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

start_task >> hello_task >> end_task
