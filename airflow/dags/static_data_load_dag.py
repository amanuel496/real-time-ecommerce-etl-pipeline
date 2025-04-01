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
    dag_id='static_data_load_dag',
    default_args=default_args,
    description='Loads static e-commerce data to S3 and Redshift',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'etl', 'static']
) as dag:

    upload_static_data = BashOperator(
        task_id='upload_static_data',
        bash_command='python /opt/airflow/scripts/static_data_to_s3_redshift.py'
    )

    upload_static_data
    # This DAG only has one task which is to upload static data
    # Hence, no need for task dependencies or branching