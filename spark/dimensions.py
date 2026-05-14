from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round,
    rank, dense_rank, row_number, desc, asc,
    year, month, quarter, dayofweek, dayofmonth,
    to_date, date_format, lit, when, coalesce,
    first, last, max as spark_max, min as spark_min,
    stddev, lag, abs as spark_abs, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

from utils.logger import PipelineLogger
logger = PipelineLogger("dimensions layer", log_to_file=False)


spark = (
    SparkSession.builder
    .appName("dimensions")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

logger.info('dimension layer loading...')

orders_silver = spark.read.parquet("s3a://silver/orders/")
customers_silver = spark.read.parquet("s3a://silver/customers/")

logger.info(f"Orders silver loaded : {orders_silver.count()} rows")
logger.info(f"Customers silver loaded: {customers_silver.count()} rows")

# dim_date 
# Generated from the date range in orders_silver — not derived from row data

date_range = orders_silver.select(
    spark_min(to_date("event_time")).alias("min_date"),
    spark_max(to_date("event_time")).alias("max_date")
).collect()[0]

dim_date = spark.sql(f"""
    SELECT
        CAST(date_format(date, 'yyyyMMdd') AS INT)  AS date_key,
        date                                         AS full_date,
        dayofmonth(date)                             AS day,
        month(date)                                  AS month,
        quarter(date)                                AS quarter,
        year(date)                                   AS year,
        date_format(date, 'EEEE')                    AS day_of_week,
        CASE WHEN dayofweek(date) IN (1,7)
             THEN true ELSE false END                AS is_weekend
    FROM (
        SELECT explode(sequence(
            DATE '{date_range["min_date"]}',
            DATE '{date_range["max_date"]}',
            INTERVAL 1 DAY
        )) AS date
    )
""")

dim_date.write.mode("overwrite").parquet("s3a://gold/dimensions/dim_date/")
logger.info(f"dim_date written : {dim_date.count()} rows")


# dim_product
# One row per stock_code — most frequent non-null description wins

w_desc = Window.partitionBy("stock_code").orderBy(desc("freq"))

dim_product = orders_silver \
    .filter(col("description").isNotNull()) \
    .groupBy("stock_code", "description") \
    .agg(count("*").alias("freq")) \
    .withColumn("rn", row_number().over(w_desc)) \
    .filter(col("rn") == 1) \
    .drop("freq", "rn") \
    .withColumnRenamed("stock_code", "stock_code") \
    .select("stock_code", "description")

dim_product.write.mode("overwrite").parquet("s3a://gold/dimensions/dim_product/")
logger.info(f"dim_product written : {dim_product.count()} rows")


# dim_customer

dim_customer = customers_silver.select(
    col("customer_id"),
    col("country"),
    col("first_seen_time"),
    year(col("first_seen_time")).alias("first_seen_year"),
    when(year(col("first_seen_time")) == 2009, "cohort_2009")
    .when(year(col("first_seen_time")) == 2010, "cohort_2010")
    .otherwise("unknown").alias("cohort")
)

dim_customer.write.mode("overwrite").parquet("s3a://gold/dimensions/dim_customer/")
logger.info(f"dim_customer written : {dim_customer.count()} rows")


# dim_geography 

dim_geography = orders_silver \
    .select("country") \
    .distinct() \
    .filter(col("country").isNotNull())

dim_geography.write.mode("overwrite").parquet("s3a://gold/dimensions/dim_geography/")
logger.info(f"dim_geography written : {dim_geography.count()} rows")

logger.info(" Dimension Tables written to s3a://gold/dimensions/")

spark.stop()