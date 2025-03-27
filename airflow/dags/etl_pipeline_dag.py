from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='ecommerce_etl_pipeline',
    default_args=default_args,
    description='Orchestrates real-time e-commerce ETL pipeline',
    schedule_interval=None,  # Run manually for now
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'etl', 'spark']
) as dag:

    generate_data = BashOperator(
        task_id='generate_sample_data',
        bash_command='python /opt/airflow/scripts/generate_all_data.py'
    )

    stream_to_kinesis = BashOperator(
        task_id='stream_to_kinesis',
        bash_command='python /opt/airflow/scripts/stream_to_kinesis.py'
    )

    start_spark_job = BashOperator(
        task_id='start_spark_job',
        bash_command='spark-submit /opt/airflow/scripts/spark_streaming_etl.py'
    )

    generate_data >> stream_to_kinesis >> start_spark_job
