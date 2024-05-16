import csv
import requests

from datetime import datetime, timedelta,timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

from airflow.models import Variable
ALPHA_VANTAGE_API_KEY = Variable.get("ALPHA_VANTAGE_API_KEY", default_var=None)

import json
import asyncio
from aiokafka import AIOKafkaProducer
import aiohttp

from yahoo_finance.get_data_async import YFinanceDataFetcher

args = {
    'owner': 'airflow',
    'email': ['wenmin961028@gmail.com'],
    'email_on_failure': True,
    'start_date': datetime(2024, 4, 12)
}

fetcher = YFinanceDataFetcher()
tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'BAC', 'AMD', 'NIO', 'NVDA', 'AMZN', 'T']
start_date = '2024-01-01'
end_date = '2024-04-10'

def run():
    asyncio.run(main())

with DAG(
    dag_id='test',
    default_args=args,
    schedule_interval=timedelta(days=1)
) as dag:

    start  = DummyOperator(
        task_id='start',
    )

    # fetch_active_list = PythonOperator(
    #     task_id='get_active_list',
    #     python_callable=get_active_list
    # )

    # send_tickers_to_kafka = PythonOperator(
    #     task_id='send_list_to_kafka',
    #     python_callable=send_list_to_kafka_sync,
    #     op_kwargs={'tickers': {{ ti.xcom_pull(task_ids='get_active_list') }}}
    # )

    send_data_to_kafka = PythonOperator(
        task_id='send_data_to_kafka',
        python_callable=fetcher.fetch_and_send_data_asynchronously,
        op_kwargs={'tickers': tickers, 'start_date': start_date, 'end_date': end_date}
    )

    end = DummyOperator(task_id='end')

    start >> send_data_to_kafka >> end