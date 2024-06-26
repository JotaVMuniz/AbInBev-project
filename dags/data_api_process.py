from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'data_api_process',
    default_args=default_args,
    description='Data API Process DAG',
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
        application='/opt/airflow/dags/spark_script.py',
        conn_id='spark_local',
        application_args=["{{ task_instance.xcom_pull(task_ids='api_request') }}"],
        packages="org.apache.spark:spark-avro_2.12:3.4.0",
        total_executor_cores=2,
        executor_cores=2,
        executor_memory='1G',
        driver_memory='1G',
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )


end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

start_task >> get_api_task >> medallion_stage_task >> end_task
