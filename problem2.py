from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, unix_timestamp
import os
import argparse
from pyspark.sql.functions import to_timestamp, input_file_name, min, max, countDistinct




def parse_args():
    parser = argparse.ArgumentParser(description="Log Analysis with Spark")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/output/",
        help="Output directory for results (default: data/output/)",
    )
    parser.add_argument(
        "--master",
        required=True,
        type=str,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    output_dir = args.output_dir
    master_url = args.master

spark = (
    SparkSession.builder
    .appName("Problem2LogAnalysis")

    # Memory Configuration
    .config("spark.driver.memory", "4g")
    .config("spark.driver.maxResultSize", "2g")

    # Performance settings for local execution
    .config("spark.master", "local[*]")  # Use all available cores
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Serialization
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # Arrow optimization for Pandas conversion
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Add Hadoop AWS + AWS SDK for S3A support (adjust versions if needed)
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    .master(master_url)
    
    .getOrCreate()
)


# Configure Hadoop S3A credentials provider (use appropriate provider for your environment)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.maximum","100")

#logs_df = spark.read.text("data/sample/application_*/*.log")
logs_df = spark.read.text("s3a://jz982-assignment-spark-cluster-logs/data/*/*.log")

parsed_df = logs_df.select(
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
    col('value').alias('message')
)


df = parsed_df.withColumn('file_path', input_file_name())
df = df.withColumn('application_id',
    regexp_extract('file_path', r'application_(\d+_\d+)', 0))
df = df.withColumn('cluster_id',
    regexp_extract('file_path', r'(container_\d+_\d+_\d+_\d+)', 1))

# Timestamp calcs will fail for missing data
df = df.filter((col("application_id") != "") & (col("timestamp") != ""))

df = df.withColumn('timestamp',
    to_timestamp('timestamp', 'yy/MM/dd HH:mm:ss'))


## PART 1 ##
# Group by application_id and cluster id
df = df.groupBy('cluster_id', 'application_id').agg(
    min('timestamp').alias('start_time'),
    max('timestamp').alias('end_time'),
)

df = df.withColumn('app_number',
    regexp_extract('application_id', r'_(\d+)$', 1)
)

df = df.withColumn(
    'container_number',
    regexp_extract('container_id', r'container_(\d+)_\d+_\d+_\d+', 1)
)

df.write.mode('overwrite').csv("data/output/problem2_timeline.csv")


## PART 2 ##
cluster_summary = df.groupBy('cluster_id').agg(
    countDistinct('application_id').alias('num_applications'),
    min('start_time').alias('cluster_first_app'),
    max('end_time').alias('cluster_last_app')
)

cluster_summary.write.mode('overwrite').csv("data/output/problem2_cluster_summary.csv") 

## PART 3 ##
path = 'data/output/problem2_stats.txt'
num_clusters = cluster_summary.count()
num_applications = df.select('application_id').distinct().count()
apps_per_cluster = num_applications / num_clusters 

print(f"Total unique clusters: {num_clusters}")
print(f"Total applications: {num_applications}")
print(f"Average applications per cluster: {apps_per_cluster:.2f}\n")

# Most heavily used clusters (top N)
top_clusters = cluster_summary.orderBy(col('num_applications').desc()).limit(10).collect()

with open(path, "w") as f:
    f.write(f"Total unique clusters: {num_clusters}\n")
    f.write(f"Total applications: {num_applications}\n")
    f.write(f"Average applications per cluster: {apps_per_cluster:.2f}\n\n")
    f.write("Most heavily used clusters:\n")
    for row in top_clusters:
        cluster = row['cluster_id']
        num = row['num_applications']
        f.write(f"  Cluster {cluster}: {num} applications\n")



x=1