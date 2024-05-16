import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col

MINIO_ACCESS_KEY = 'nstdQKO5At6ox9bJDR9H'
MINIO_SECRET_KEY = 'yFhcYC3owrpAHLkvH4Duk5n6uZh2P3iL8KH4N6et'

conf = (
    pyspark.SparkConf()
        .setAppName('load data to iceberg')
  		#packages
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.80.0')
  		#SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
  		#Configuring Catalog
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', "http://nessie:19120/api/v1")
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', 's3a://warehouse')
        .set('spark.sql.catalog.nessie.s3.endpoint', 'http://minio:9000')
        # .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
  		#MINIO CREDENTIALS
        .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .set('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.path.style.access", True) \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", False)\
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

# Assume df is your DataFrame loaded with data to be written
table_name = "nessie.historical_data"
## LOAD A CSV INTO AN SQL VIEW
csv_path = "s3a://transformed/yfinance/historical_data/"
csv_df = spark.read.format("csv").option("header", "true").load(csv_path)
if spark.catalog.tableExists(table_name):
    # Append data to the existing table
    csv_df.writeTo(table_name).using("iceberg").partitionedBy(col("Ticker")).overwrite()
else:
    # Create a new table with partitioning and write data
    csv_df.writeTo(table_name).using("iceberg").partitionedBy(col("Ticker")).create()

table_name = "nessie.indicator"
## LOAD A CSV INTO AN SQL VIEW
csv_path = "s3a://transformed/alpha_vantage/indicator/"
csv_df = spark.read.format("csv").option("header", "true").load(csv_path)
if spark.catalog.tableExists(table_name):
    # Append data to the existing table
    csv_df.writeTo(table_name).using("iceberg").partitionedBy(col("name")).overwrite()
else:
    # Create a new table with partitioning and write data
    csv_df.writeTo(table_name).using("iceberg").partitionedBy(col("name")).create()

## CREATE AN ICEBERG TABLE FROM THE SQL VIEW
# csv_df.createOrReplaceTempView("csv_open_2023")
# spark.sql("CREATE TABLE IF NOT EXISTS nessie.df_open_2023_lesson3 USING iceberg PARTITIONED BY (countryOfOriginCode) AS SELECT * FROM csv_open_2023 ORDER BY countryOfOriginCode;").show()
