from alpha_vantage.get_data import AlphaVantageAPIProducer

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

from airflow.models import Variable
ALPHA_VANTAGE_API_KEY = Variable.get("ALPHA_VANTAGE_API_KEY", default_var=None)


start_date = '2024-05-10'
end_date = '2024-05-11'

args = {
    'owner': 'airflow',
    'email': ['wenmin961028@gmail.com'],
    'email_on_failure': False,
    'start_date': datetime.today()
}

def run():
    fetcher = AlphaVantageAPIProducer(ALPHA_VANTAGE_API_KEY)
    fetcher.get_and_send_news(start_date=start_date, end_date=end_date)

with DAG(
    dag_id='producer_alphavantage_daily',
    default_args=args,
    schedule_interval=timedelta(days=1)
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    send_data_to_kafka = PythonOperator(
        task_id='send_data_to_kafka',
        python_callable=run
    )

    end = DummyOperator(task_id='end')

    start >> send_data_to_kafka >> end
