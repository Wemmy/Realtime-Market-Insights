from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today()
}

with DAG(
    dag_id='transform_data_with_spark',
    description='DAG for processing MinIO data using Spark',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
) as dag:

    spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/airflow/plugins/spark_jobs/transform_indicator.py',  
        conn_id='spark_default',  
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    spark_job
