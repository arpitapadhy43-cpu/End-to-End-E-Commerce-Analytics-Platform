from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, sum as spark_sum, avg, stddev,
    lit, current_timestamp, round as spark_round, abs as spark_abs,
    month, year, to_date
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from datetime import datetime

from utils.logger import PipelineLogger
logger = PipelineLogger("data_quality", log_to_file=False)

spark = (
    SparkSession.builder
    .appName("data-quality-checks")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info('data quality checks and metrics loading...')

RUN_TIMESTAMP = current_timestamp()
RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

orders_silver = spark.read.parquet("s3a://silver/orders/")
customers_silver = spark.read.parquet("s3a://silver/customers/")
orders_bronze = spark.read.parquet("s3a://bronze/orders/")
customers_bronze = spark.read.parquet("s3a://bronze/customers/")

metrics_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("pipeline", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("expected", StringType(), True),
    StructField("actual", StringType(), True),
    StructField("status", StringType(), False),         # PASS / WARN / FAIL
    StructField("details", StringType(), True),
])

metrics_rows = []

def add_metric(pipeline, layer, check_name, expected, actual, status, details=""):
    metrics_rows.append((
        RUN_ID,
        datetime.utcnow(),
        pipeline,
        layer,
        check_name,
        str(expected),
        str(actual),
        status,
        details
    ))
    logger.info(f"[{status}] {pipeline} | {layer} | {check_name} | expected={expected} actual={actual} | {details}")


bronze_orders_count    = orders_bronze.count()
bronze_customers_count = customers_bronze.count()
silver_orders_count    = orders_silver.count()
silver_customers_count = customers_silver.count()

RAW_ORDERS_COUNT    = 525461  
RAW_CUSTOMERS_COUNT = 4383

# Bronze vs Raw
bronze_orders_drop_pct = round((RAW_ORDERS_COUNT - bronze_orders_count) / RAW_ORDERS_COUNT * 100, 2)
add_metric(
    "retail_orders", "bronze", "row_count_vs_raw",
    RAW_ORDERS_COUNT, bronze_orders_count,
    "PASS" if bronze_orders_drop_pct < 1 else "WARN",
    f"Drop: {bronze_orders_drop_pct}%"
)

bronze_customers_drop_pct = round((RAW_CUSTOMERS_COUNT - bronze_customers_count) / RAW_CUSTOMERS_COUNT * 100, 2)
add_metric(
    "retail_customers", "bronze", "row_count_vs_raw",
    RAW_CUSTOMERS_COUNT, bronze_customers_count,
    "PASS" if bronze_customers_drop_pct < 1 else "WARN",
    f"Drop: {bronze_customers_drop_pct}%"
)

# Silver vs Bronze
silver_orders_drop_pct = round((bronze_orders_count - silver_orders_count) / bronze_orders_count * 100, 2)
add_metric(
    "retail_orders", "silver", "row_count_vs_bronze",
    bronze_orders_count, silver_orders_count,
    "PASS" if silver_orders_drop_pct < 5 else "WARN",
    f"Drop: {silver_orders_drop_pct}% (expected: dedup only)"
)

silver_customers_drop_pct = round((bronze_customers_count - silver_customers_count) / bronze_customers_count * 100, 2)
add_metric(
    "retail_customers", "silver", "row_count_vs_bronze",
    bronze_customers_count, silver_customers_count,
    "PASS" if silver_customers_drop_pct < 1 else "WARN",
    f"Drop: {silver_customers_drop_pct}%"
)


def null_check(df, pipeline, layer, column, threshold_pct):
    total = df.count()
    null_count = df.filter(col(column).isNull()).count()
    null_pct = round(null_count / total * 100, 2)
    status = "PASS" if null_pct <= threshold_pct else "WARN"
    add_metric(
        pipeline, layer, f"null_check_{column}",
        f"<={threshold_pct}%", f"{null_pct}%",
        status,
        f"{null_count} nulls out of {total}"
    )

# Orders silver critical columns
null_check(orders_silver, "retail_orders", "silver", "invoice_id",  0.0)
null_check(orders_silver, "retail_orders", "silver", "stock_code",  0.0)
null_check(orders_silver, "retail_orders", "silver", "event_time",  0.0)
null_check(orders_silver, "retail_orders", "silver", "event_type",  0.0)
null_check(orders_silver, "retail_orders", "silver", "customer_id", 25.0)   # known: ~20% guest orders
null_check(orders_silver, "retail_orders", "silver", "description", 1.0)    # known: 2928 null descriptions

# Customers silver
null_check(customers_silver, "retail_customers", "silver", "customer_id",   0.0)
null_check(customers_silver, "retail_customers", "silver", "first_seen_time", 0.0)


orders_dupes = orders_silver.groupBy("invoice_id", "stock_code", "event_time", "quantity") \
    .count().filter(col("count") > 1)
orders_dupe_count = orders_dupes.count()

add_metric(
    "retail_orders", "silver", "duplicate_check",
    0, orders_dupe_count,
    "PASS" if orders_dupe_count == 0 else "FAIL",
    "Duplicates remaining after dedup on (invoice_id, stock_code, event_time, quantity)"
)

customers_dupes = customers_silver.groupBy("customer_id").count().filter(col("count") > 1)
customers_dupe_count = customers_dupes.count()

add_metric(
    "retail_customers", "silver", "duplicate_check",
    0, customers_dupe_count,
    "PASS" if customers_dupe_count == 0 else "FAIL",
    "Duplicate customer_ids in silver"
)

# Quantity should never be 0
zero_qty = orders_silver.filter(col("quantity") == 0).count()
add_metric(
    "retail_orders", "silver", "business_rule_zero_quantity",
    0, zero_qty,
    "PASS" if zero_qty == 0 else "FAIL",
    "Rows where quantity = 0"
)

# unit_price should not be negative
neg_price = orders_silver.filter(col("unit_price") < 0).count()
add_metric(
    "retail_orders", "silver", "business_rule_negative_price",
    0, neg_price,
    "PASS" if neg_price == 0 else "WARN",
    "Rows where unit_price < 0"
)

# event_type should only be known values
invalid_event_types = orders_silver.filter(
    ~col("event_type").isin("order_item_created", "order_item_cancelled")
).count()
add_metric(
    "retail_orders", "silver", "business_rule_event_type_values",
    0, invalid_event_types,
    "PASS" if invalid_event_types == 0 else "FAIL",
    "Rows with unexpected event_type values"
)


# Orders with non-null customer_id that don't exist in customers silver
customer_ids = customers_silver.select(col("customer_id").cast("string")).distinct()

orphan_orders = orders_silver.filter(col("customer_id").isNotNull()) \
    .join(
        customer_ids,
        orders_silver["customer_id"].cast("string") == customer_ids["customer_id"],
        "left_anti"
    )

orphan_count = orphan_orders.count()
add_metric(
    "retail_orders", "silver", "referential_integrity_customer_id",
    0, orphan_count,
    "PASS" if orphan_count == 0 else "WARN",
    f"{orphan_count} orders have customer_id not found in customers silver"
)

# Save offending rows separately for investigation
if orphan_count > 0:
    orphan_orders.write \
        .mode("overwrite") \
        .parquet(f"s3a://gold/data_quality_issues/orphan_orders/run_id={RUN_ID}/")
    logger.info(f"  --> Orphan orders saved to gold/data_quality_issues/orphan_orders/run_id={RUN_ID}/")


monthly_revenue = orders_silver \
    .filter(col("event_type") == "order_item_created") \
    .groupBy("year", "month") \
    .agg(spark_sum("total_amount").alias("monthly_revenue")) \
    .orderBy("year", "month")

# Calculate mean and stddev across all months
stats = monthly_revenue.agg(
    avg("monthly_revenue").alias("mean_revenue"),
    stddev("monthly_revenue").alias("stddev_revenue")
).collect()[0]

mean_rev   = stats["mean_revenue"]
stddev_rev = stats["stddev_revenue"]

# Flag 1: months where revenue = 0
zero_rev_months = monthly_revenue.filter(col("monthly_revenue") == 0).count()
add_metric(
    "retail_orders", "silver", "anomaly_zero_revenue_months",
    0, zero_rev_months,
    "PASS" if zero_rev_months == 0 else "FAIL",
    "Months with zero revenue"
)

# Flag 2: months deviating > 30% from mean
anomalous_months = monthly_revenue.filter(
    spark_abs(col("monthly_revenue") - lit(mean_rev)) > lit(0.30 * mean_rev)
)
anomalous_count = anomalous_months.count()

anomalous_details = ""
if anomalous_count > 0:
    anomalous_list = anomalous_months.collect()
    anomalous_details = ", ".join([
        f"{r['year']}-{str(r['month']).zfill(2)}: £{round(r['monthly_revenue'], 2)}"
        for r in anomalous_list
    ])

add_metric(
    "retail_orders", "silver", "anomaly_monthly_revenue_deviation",
    f"within 30% of mean (£{round(mean_rev, 2)})",
    anomalous_count,
    "PASS" if anomalous_count == 0 else "WARN",
    f"Anomalous months: {anomalous_details}" if anomalous_details else "None"
)


metrics_df = spark.createDataFrame(metrics_rows, schema=metrics_schema)

metrics_df.write \
    .mode("append") \
    .parquet("s3a://gold/data_quality_metrics/")

logger.info(f" Data quality checks complete. Run ID: {RUN_ID}")
logger.info(f"   Total checks run : {len(metrics_rows)}")
logger.info(f"   PASS : {sum(1 for r in metrics_rows if r[7] == 'PASS')}")
logger.info(f"   WARN : {sum(1 for r in metrics_rows if r[7] == 'WARN')}")
logger.info(f"   FAIL : {sum(1 for r in metrics_rows if r[7] == 'FAIL')}")


spark.stop()