from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_streaming_orchestrator',
    default_args=default_args,
    description='Orchestrates the daily start of our real-time data pipeline',
    schedule_interval=None,  # Change this to a CRON schedule later if needed
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'etl', 'spark']
) as dag:

    stream_orders_and_inventory = BashOperator(
        task_id='stream_orders_and_inventory',
        bash_command='python /opt/airflow/scripts/stream_to_kinesis.py'
    )

    start_spark_streaming_job = BashOperator(
        task_id='start_spark_streaming_job',
        bash_command='/opt/spark/bin/spark-submit \
                    --master local[*] \
                    --jars /opt/airflow/jars/target/spark-streaming-sql-kinesis-connector_2.12-1.4.1.jar,/opt/airflow/jars/redshift-jdbc42-2.1.0.32.jar \
                    --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints \
                    /opt/airflow/scripts/spark/spark_streaming_etl.py'
    )


    end = DummyOperator(task_id='end', trigger_rule='all_done')

    # DAG task flow
    stream_orders_and_inventory >> start_spark_streaming_job >> end
