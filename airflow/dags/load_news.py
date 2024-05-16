from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from llamaindex.data_loader import DataLoader
from utils_chromadb.load import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today()
}

def run_alpha_vantage():
    prefix = f'alpha_vantage/2024-05-11/news/'
    docs = DataLoader("transformed", prefix).load_data_alpha_vantage()
    load_data(docs, 'alphavantage')

def run_yfinance_news():
    prefix = f'yfinance/2024-05-12/news_data/'
    docs = DataLoader("transformed", prefix).load_data_yahoo_news()
    load_data(docs, 'yfinance')

dag = DAG(
    'load_news',
    default_args=default_args,
    description='DAG for loading news and store in Elasticsearch',
    schedule_interval=timedelta(days=1),  # Adjust according to your needs
    catchup=False,
)

load_news = PythonOperator(
        task_id='loading_news',
        python_callable=run_alpha_vantage,
        dag = dag
    )

load_news