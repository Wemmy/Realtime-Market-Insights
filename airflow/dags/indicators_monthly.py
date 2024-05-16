from alpha_vantage.get_data import AlphaVantageAPIProducer
from utils_minio.save_data import MinioDataConsumer

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

from airflow.models import Variable
ALPHA_VANTAGE_API_KEY = Variable.get("ALPHA_VANTAGE_API_KEY", default_var=None)

args = {
    'owner': 'airflow',
    'email': ['wenmin961028@gmail.com'],
    'email_on_failure': False,
    'start_date': datetime.today()
}

def produce():
    fetcher = AlphaVantageAPIProducer(ALPHA_VANTAGE_API_KEY)
    fetcher.get_and_send_indicators()

def consume():
    topic = 'indicator'
    consumer = MinioDataConsumer()
    consumer.consume_and_save_topic(topic)

with DAG(
    dag_id='indicators_monthly',
    default_args=args,
    schedule_interval=timedelta(days=30)
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    send_data_to_kafka = PythonOperator(
        task_id='send_data_to_kafka',
        python_callable=produce
    )

    consume_data_from_kafka = PythonOperator(
        task_id='get_data_to_kafka',
        python_callable=consume
    )

    end = DummyOperator(task_id='end')

    start >> send_data_to_kafka >> consume_data_from_kafka >> end
