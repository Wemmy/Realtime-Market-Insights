from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

from utils_minio.save_data import MinioDataConsumer

def task_to_run():
    topics = ['historical_data', 'news_data']
    consumer = MinioDataConsumer()
    for t in topics:
        consumer.consume_and_save_topic(t)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    'consumer_yfinance_daily',
    default_args=default_args,
    description='A simple DAG to consume from Kafka and store to MinIO',
    catchup=False,
) as dag:

    start  = DummyOperator(
        task_id='start',
    )

    consume_and_save = PythonOperator(
        task_id='consumer1',
        python_callable=task_to_run
    )

    end = DummyOperator(task_id='end')

    start >> consume_and_save >> end