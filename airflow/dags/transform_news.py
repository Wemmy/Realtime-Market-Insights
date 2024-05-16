from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from utils_minio.save_data import MinioDataConsumer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today()
}

def run_alhpa_vantage():
    worker = MinioDataConsumer()
    # hard code prefix (in the future to dynamatically idnetify prefix)
    prefix = f'alpha_vantage/2024-05-11/news/'
    output_key = f'{datetime.now().strftime("%H%M%S%f")}.json'
    worker.transform_alphavantage_news("raw", "transformed", output_key, prefix)

def run_yfinance():
    worker = MinioDataConsumer()
    # hard code prefix (in the future to dynamatically idnetify prefix)
    prefix = f'yfinance/2024-05-12/news_data/'
    output_key = f'{datetime.now().strftime("%H%M%S%f")}.json'
    worker.transform_yfinance_news("raw", "transformed", output_key, prefix)

dag = DAG(
    'transform_news',
    default_args=default_args,
    description='DAG for tranform news and store in MinIO ',
    schedule_interval=timedelta(days=1), 
    catchup=False,
)

transform_news = PythonOperator(
        task_id='consume_and_save_to_minio',
        python_callable=run_alhpa_vantage,
        dag = dag
    )

transform_news