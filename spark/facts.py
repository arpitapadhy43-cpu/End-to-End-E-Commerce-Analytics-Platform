from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format, when, abs as spark_abs
)
from pyspark.sql.types import IntegerType
from dotenv import load_dotenv
import os
load_dotenv()

from utils.logger import PipelineLogger
logger = PipelineLogger("facts_layer", log_to_file=False)


spark = (
    SparkSession.builder
    .appName("facts")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_URL"])
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info('facts layer loading...')

orders_silver = spark.read.parquet(os.environ["ORDERS_SILVER_PATH"])
customers_silver = spark.read.parquet(os.environ["CUSTOMER_SILVER_PATH"])

logger.info(f"Orders silver loaded : {orders_silver.count()} rows")
logger.info(f"Customers silver loaded: {customers_silver.count()} rows")


# ══════════════════════════════════════════════════════════════════════════════
# FACT TABLE — fact_order_items
# ══════════════════════════════════════════════════════════════════════════════
# One row per order line item. Includes both created and cancelled events.
# is_cancelled = true for order_item_cancelled rows.
# Quantity is stored as-is (positive for created, positive for cancelled too).
# total_amount is stored as absolute value so revenue calcs are straightforward.

fact_order_items = orders_silver \
    .withColumn("date_key",
        date_format(to_date("event_time"), "yyyyMMdd").cast(IntegerType())
    ) \
    .withColumn("is_cancelled",
        when(col("event_type") == "order_item_cancelled", True).otherwise(False)
    ) \
    .withColumn("quantity", spark_abs(col("quantity"))) \
    .withColumn("total_amount", spark_abs(col("total_amount"))) \
    .select(
        "invoice_id",
        "stock_code",
        col("customer_id").cast("string"),
        "country",
        "date_key",
        "year",
        "month",
        "quantity",
        "unit_price",
        "total_amount",
        "is_cancelled",
        "event_time",
        "kafka_timestamp",
        "bronze_ingestion_time",
        "silver_ingestion_time"
    )

fact_order_items.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(os.environ["FACT_OFFERS_PATH"])

logger.info(f"fact_order_items written: {fact_order_items.count()} rows")
logger.info("Tables written to s3a://gold/facts/")

spark.stop()