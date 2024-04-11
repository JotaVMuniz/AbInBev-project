from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

get_api_task = SimpleHttpOperator(
    task_id='api_request',
    method='GET',
    http_conn_id='http_api_con',
    endpoint='',
    headers={'Content-Type': 'application/json'},
    log_response=True,
    dag=dag
)

medallion_stage_task = SparkSubmitOperator(
    task_id='medallion_stage',
    application='/include/spark_script.py',
    conn_id='spark_default',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

start_task >> get_api_task >> medallion_stage_task >> end_task
