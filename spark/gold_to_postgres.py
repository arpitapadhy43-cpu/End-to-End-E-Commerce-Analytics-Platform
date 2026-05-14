from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import PipelineLogger

logger = PipelineLogger("gold_to_postgres", log_to_file=False)

# ── Spark Session ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("gold-to-postgres")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Postgres Config ───────────────────────────────────────────────────────────
# Uses the existing postgres container with a separate analytics database
# Create the database first by running:
#   docker compose exec postgres psql -U airflow -c "CREATE DATABASE analytics;"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/analytics"
POSTGRES_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df, table_name):
    """Write a dataframe to postgres, overwriting existing table."""
    count = df.count()
    df.write \
        .jdbc(
            url=POSTGRES_URL,
            table=table_name,
            mode="overwrite",
            properties=POSTGRES_PROPS
        )
    logger.log_count(table_name, count)


# ══════════════════════════════════════════════════════════════════════════════
# REVENUE & SALES TABLES
# ══════════════════════════════════════════════════════════════════════════════

logger.section("loading revenue & sales tables")

revenue_monthly = spark.read.parquet("s3a://gold/revenue_sales/revenue_monthly/")
revenue_by_country_month = spark.read.parquet("s3a://gold/revenue_sales/revenue_by_country_month/")
top_products_global = spark.read.parquet("s3a://gold/revenue_sales/top_products_global/")
aov_monthly = spark.read.parquet("s3a://gold/revenue_sales/aov_monthly/")

logger.section("writing revenue & sales to postgres")

write_to_postgres(revenue_monthly, "revenue_monthly")
write_to_postgres(revenue_by_country_month, "revenue_by_country_month")
write_to_postgres(top_products_global, "top_products_global")
write_to_postgres(aov_monthly, "aov_monthly")

print("Revenue & sales tables written")


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOMER ANALYTICS TABLES
# ══════════════════════════════════════════════════════════════════════════════

logger.section("loading customer analytics tables")

new_vs_returning = spark.read.parquet("s3a://gold/customer_analytics/new_vs_returning/")
clv = spark.read.parquet("s3a://gold/customer_analytics/customer_lifetime_value/")
customers_per_country = spark.read.parquet("s3a://gold/customer_analytics/customers_per_country/")
retention_rate = spark.read.parquet("s3a://gold/customer_analytics/retention_rate/")

logger.section("writing customer analytics to postgres")

write_to_postgres(new_vs_returning, "new_vs_returning")
write_to_postgres(clv, "customer_lifetime_value")
write_to_postgres(customers_per_country, "customers_per_country")
write_to_postgres(retention_rate, "retention_rate")

print("Customer analytics tables written")


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCT INSIGHTS TABLES
# ══════════════════════════════════════════════════════════════════════════════

logger.section("loading product insights tables")

product_demand_trends = spark.read.parquet("s3a://gold/revenue_sales/product_demand_trends/")
basket_size_monthly = spark.read.parquet("s3a://gold/product_insights/basket_size_monthly/")

logger.section("writing product insights to postgres")

write_to_postgres(product_demand_trends, "product_demand_trends")
write_to_postgres(basket_size_monthly, "basket_size_monthly")

print("Product insights tables written")


# ── Summary ───────────────────────────────────────────────────────────────────

logger.section("gold to postgres complete")
logger.info("All 10 gold tables written to analytics database in Postgres")
logger.info("Connect Superset to: postgresql://airflow:airflow@postgres:5432/analytics")

spark.stop()