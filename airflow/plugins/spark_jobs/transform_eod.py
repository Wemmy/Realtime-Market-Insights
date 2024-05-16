from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType,IntegerType


MINIO_ACCESS_KEY = 'nstdQKO5At6ox9bJDR9H'
MINIO_SECRET_KEY = 'yFhcYC3owrpAHLkvH4Duk5n6uZh2P3iL8KH4N6et'

if __name__ == "__main__":

    # .config('spark.jars.packages', 'software.amazon.awssdk:bundle:2.25.34,software.amazon.awssdk:url-connection-client:2.25.34') \
    spark = SparkSession.builder \
        .appName("MinIO Data Processing") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config('spark.jars.packages', 'com.amazonaws:aws-java-sdk-bundle:1.12.262') \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False)\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print('created spark session')
    print("Spark Version")
    print(spark.version)

    # define the schema
    
    schema = StructType([
        StructField("Ticker", StringType(), True),
        StructField("Data", ArrayType(StructType([
            StructField("Date", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Adj Close", DoubleType(), True),
            StructField("Volume", IntegerType(), True)
        ]), True), True)
    ])

    # Define the schema if known, otherwise, let Spark infer the schema
    read_path = "s3a://raw/yfinance/*/historical_data/*.json"
    df = spark.read.schema(schema).json(read_path)

    # Explode the data array
    df_exploded = df.withColumn("Data", explode(col("Data")))

    # Select necessary fields while handling empty data arrays
    df_flat = df_exploded.select(
        col("Ticker"),
        col("Data.Date").alias("Date"),
        col("Data.Open").alias("Open"),
        col("Data.High").alias("High"),
        col("Data.Low").alias("Low"),
        col("Data.Close").alias("Close"),
        col("Data.Adj Close").alias("Adj_Close"),
        col("Data.Volume").alias("Volume")
    ).na.drop(subset=["Date"])  # Optionally drop rows where Date is null

    # Assuming df_flat is your DataFrame after flattening the structure
    df_rounded = df_flat.select(
        col("Ticker"),
        col("Date"),
        round(col("Open"), 2).alias("Open"),
        round(col("High"), 2).alias("High"),
        round(col("Low"), 2).alias("Low"),
        round(col("Close"), 2).alias("Close"),
        round(col("Adj_Close"), 2).alias("Adj_Close"),
        col("Volume")
    )

    # save to
    save_path = f"s3a://transformed/yfinance/historical_data/"
    df_flat.write.partitionBy("Ticker").format("csv").option("header", "true").mode("overwrite").save(save_path)