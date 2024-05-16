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
        StructField("name", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("data", ArrayType(
            StructType([
                StructField("date", StringType(), True),
                StructField("value", StringType(), True)
                ]), True), True)
        ])

    
    # Define the schema if known, otherwise, let Spark infer the schema
    read_path = "s3a://raw/alpha_vantage/*/indicator/*.json"
    df = spark.read.schema(schema).json(read_path)

    # Explode the data array
    df_exploded = df.withColumn("data_exploded", explode(col("data")))
    # Select necessary fields while handling empty data arrays
    df_flat = df_exploded.select(
        col("name"),
        col("interval"),
        col("unit"),
        col("data_exploded.date").alias("date"),
        col("data_exploded.value").cast("double").alias("value")
    )

    # save to
    save_path = f"s3a://transformed/alpha_vantage/indicator/"
    df_flat.write.partitionBy("name").format("csv").option("header", "true").mode("overwrite").save(save_path)