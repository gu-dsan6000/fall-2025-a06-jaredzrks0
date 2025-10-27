from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand
import os
import argparse



def parse_args():
    parser = argparse.ArgumentParser(description="Log Analysis with Spark")
    parser.add_argument(
        "--input-path",
        type=str,
        default="s3a://jz982-assignment-spark-cluster-logs/data/*/*.log",
        help="Input path for log files (default: s3a://jz982-assignment-spark-cluster-logs/data/*/*.log)",
    )
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
    input_path = args.input_path
    output_dir = args.output_dir
    master_url = args.master

spark = (
    SparkSession.builder
    .appName("LogAnalysis")

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

logs_df = spark.read.text("s3a://jz982-assignment-spark-cluster-logs/data/*/*.log")
parsed_logs = logs_df.select(
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('level'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
    col('value').alias('message')
)

# Analyze log levels
# Num 1
log_level_counts = parsed_logs.groupBy('level').count().withColumnRenamed('level', 'log_level').write.mode('overwrite').csv('data/output/problem1_counts.csv')

# Num 2
random_sample = (
    parsed_logs.orderBy(rand()).limit(10).select(['level', 'message'])
    .withColumnRenamed('message', 'log_entry')
    .withColumnRenamed('level', 'log_level')
).write.mode('overwrite').csv('data/output/problem1_sample.csv')

# Num 3
total_lines = logs_df.count()
lines_with_levels = parsed_logs.filter(col('level') != '').count()
counts = parsed_logs.filter(col('level') != '').groupBy('level').count()
unique_levels = counts.count()
counts_sorted = counts.orderBy(col('count').desc()).collect()

out_lines = []
out_lines.append(f"Total log lines processed: {total_lines:,}")
out_lines.append(f"Total lines with log levels: {lines_with_levels:,}")
out_lines.append(f"Unique log levels found: {unique_levels}")
out_lines.append("")
out_lines.append("Log level distribution:")

for row in counts_sorted:
    level = row['level']
    count_val = int(row['count'])
    pct = (count_val / lines_with_levels * 100) if lines_with_levels else 0.0
    out_lines.append(f"  {level:<5}: {count_val:12,d} ({pct:6.2f}%)")

out_path = 'data/output/problem1_summary.txt'
os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, 'w') as f:
    f.write('\n'.join(out_lines))


