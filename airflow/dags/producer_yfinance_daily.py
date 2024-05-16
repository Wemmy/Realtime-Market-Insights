from yahoo_finance.get_data import YFinanceDataProducer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
    'email': ['wenmin961028@gmail.com'],
    'email_on_failure': False,
    'start_date': datetime.today()
}

def run():
    fetcher = YFinanceDataProducer()
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'BAC', 'AMD', 'NIO', 'NVDA', 'AMZN', 'T']
    start_date = '2024-05-01'
    end_date = '2024-05-10'
    for ticker in tickers:
        fetcher.get_and_send_historical_data(ticker, start_date, end_date)
        fetcher.get_and_send_news_data(ticker)

with DAG(
    dag_id='producer_yfinance_daily',
    default_args=args,
    schedule_interval=timedelta(days=1)
) as dag:

    start  = DummyOperator(
        task_id='start',
    )

    send_data_to_kafka = PythonOperator(
        task_id='send_data_to_kafka',
        python_callable=run,
    )

    end = DummyOperator(task_id='end')

    start >> send_data_to_kafka >> end
