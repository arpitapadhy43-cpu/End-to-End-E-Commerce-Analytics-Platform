from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("silver-layer")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("invoice_id", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("event_time", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("ingestion_time", StringType(), True)
])

orders_df = spark.read.parquet("s3a://bronze/orders/")
customers_df = spark.read.parquet("s3a://bronze/customers/")

orders_parsed = orders_df.withColumn(
    "parsed_json",
    from_json(col("raw_json"), orders_schema)
)

orders_flat = orders_parsed.select(
    "kafka_key",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "bronze_ingestion_time",
    col("parsed_json.*")
)


# Convert timestamps
orders_clean = orders_flat \
    .withColumn("event_time", to_timestamp("event_time")) \
    .withColumn("ingestion_time", to_timestamp("ingestion_time"))

# Drop corrupt records (where JSON parsing failed)
orders_clean = orders_clean.filter(col("event_time").isNotNull())

# Handle nulls
orders_clean = orders_clean.fillna({
    "quantity": 0,
    "unit_price": 0.0,
    "total_amount": 0.0
})

# Deduplication (VERY IMPORTANT)
orders_clean = orders_clean.dropDuplicates([
    "invoice_id", "stock_code", "event_time"
])

orders_clean = orders_clean \
    .withColumn("silver_ingestion_time", current_timestamp()) \
    .withColumn("year", year("event_time")) \
    .withColumn("month", month("event_time"))


orders_final = orders_clean.repartition(20, "year", "month")

orders_final.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3a://silver/orders/")


customers_clean = customers_df \
    .withColumn("silver_ingestion_time", current_timestamp())

customers_final = customers_clean.repartition(10)

customers_final.write \
    .mode("overwrite") \
    .parquet("s3a://silver/customers/")


spark.stop()