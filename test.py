from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import os

# Create Spark session with Hadoop AWS support (adjust versions to match your Spark/Hadoop)
spark = (
    SparkSession.builder
    .appName("LogAnalysis")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .getOrCreate()
)

# Configure Hadoop S3A credentials provider (use appropriate provider for your environment)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
# Optional: explicit implementation and performance settings (tune if needed)
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.maximum","100")

# Use s3a:// path
logs_df = spark.read.text("s3a://jz982-assignment-spark-cluster-logs/data/*/*.log")
#logs_df = spark.read.option("recursiveFileLookup", "true").text('s3a://jz982-assignment-spark-cluster-logs/data/*/*.log')
parsed_logs = logs_df.select(
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('level'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
    col('value').alias('message')
)


x=1