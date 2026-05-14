from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from utils.logger import PipelineLogger
logger = PipelineLogger("bronze_layer", log_to_file=False)


spark = (
    SparkSession.builder
    .appName("bronze-layer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.section("loading bronze...")

orders_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "retail_order_events")   
    .option("startingOffsets", "earliest")
    .load()
)

orders_bronze = orders_df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("bronze_ingestion_time")
)

logger.log_count("orders bronze count is:", orders_bronze.count())

orders_query = (
    orders_bronze.writeStream
    .format("parquet")   # can switch to delta later
    .option("checkpointLocation", "s3a://bronze/checkpoints/orders/")
    .option("path", "s3a://bronze/orders/")
    .outputMode("append")
    .start()
)

customers_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "retail_customer_events")   
    .option("startingOffsets", "earliest")
    .load()
)

customers_bronze = customers_df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("bronze_ingestion_time")
)

logger.log_count("customers bronze count is:", orders_bronze.count())

customers_query = (
    customers_bronze.writeStream
    .format("parquet")
    .option("checkpointLocation", "s3a://bronze/checkpoints/customers/")
    .option("path", "s3a://bronze/customers/")
    .outputMode("append")
    .start()
)

orders_query.awaitTermination()
customers_query.awaitTermination()