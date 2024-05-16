from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today()
}

dag = DAG(
    'load_csv_to_inceberg',
    default_args=default_args,
    catchup=False,
)

spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/opt/airflow/plugins/spark_jobs/csv_2_iceberg.py',  # Path to your Spark script
    conn_id='spark_default',  # Connection to Spark, configure in Airflow connections
    packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.80.0",
    dag=dag,
)

spark_job
