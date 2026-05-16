from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round,
    min as spark_min, max as spark_max, countDistinct
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
from utils.logger import PipelineLogger

from dotenv import load_dotenv
import os
load_dotenv()

logger = PipelineLogger("gold_quality_checks", log_to_file=False)

# ── Spark Session ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("gold-quality-checks")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_URL"])
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# ── Load Gold Tables ──────────────────────────────────────────────────────────

logger.section("loading gold tables")

fact_order_items       = spark.read.parquet("s3a://gold/facts/fact_order_items/")
dim_product            = spark.read.parquet("s3a://gold/dimensions/dim_product/")
dim_customer           = spark.read.parquet("s3a://gold/dimensions/dim_customer/")
dim_date               = spark.read.parquet("s3a://gold/dimensions/dim_date/")
dim_geography          = spark.read.parquet("s3a://gold/dimensions/dim_geography/")

revenue_daily          = spark.read.parquet("s3a://gold/revenue_sales/revenue_daily/")
revenue_monthly        = spark.read.parquet("s3a://gold/revenue_sales/revenue_monthly/")
revenue_yearly         = spark.read.parquet("s3a://gold/revenue_sales/revenue_yearly/")
top_products_global    = spark.read.parquet("s3a://gold/revenue_sales/top_products_global/")
aov_monthly            = spark.read.parquet("s3a://gold/revenue_sales/aov_monthly/")
revenue_by_country     = spark.read.parquet("s3a://gold/revenue_sales/revenue_by_country_month/")
product_demand_trends  = spark.read.parquet("s3a://gold/revenue_sales/product_demand_trends/")

new_vs_returning       = spark.read.parquet("s3a://gold/customer_analytics/new_vs_returning/")
clv                    = spark.read.parquet("s3a://gold/customer_analytics/customer_lifetime_value/")
customers_per_country  = spark.read.parquet("s3a://gold/customer_analytics/customers_per_country/")
retention_rate         = spark.read.parquet("s3a://gold/customer_analytics/retention_rate/")
basket_size_monthly    = spark.read.parquet("s3a://gold/product_insights/basket_size_monthly/")

logger.info("All gold tables loaded successfully")

# ── Metrics Helpers ───────────────────────────────────────────────────────────

metrics_schema = StructType([
    StructField("run_id",        StringType(),    False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("pipeline",      StringType(),    False),
    StructField("layer",         StringType(),    False),
    StructField("check_name",    StringType(),    False),
    StructField("expected",      StringType(),    True),
    StructField("actual",        StringType(),    True),
    StructField("status",        StringType(),    False),
    StructField("details",       StringType(),    True),
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
    logger.log_metric(check_name, expected, actual, status)


# ══════════════════════════════════════════════════════════════════════════════
# FACT TABLE CHECKS
# ══════════════════════════════════════════════════════════════════════════════

logger.section("fact table checks")

# ── 1. Row count: fact vs silver ──────────────────────────────────────────────
# fact_order_items should equal silver orders count exactly
# (silver count known from previous run — read from metrics table if available)

SILVER_ORDERS_COUNT = 518446   # from silver layer validation

fact_count = fact_order_items.count()
logger.log_count("fact_order_items", fact_count)

add_metric(
    "fact_order_items", "gold",
    "row_count_vs_silver",
    SILVER_ORDERS_COUNT, fact_count,
    "PASS" if fact_count == SILVER_ORDERS_COUNT else "FAIL",
    f"Difference: {abs(fact_count - SILVER_ORDERS_COUNT)} rows"
)

# ── 2. No nulls on fact surrogate keys ───────────────────────────────────────

for key_col in ["invoice_id", "stock_code", "date_key", "country"]:
    null_count = fact_order_items.filter(col(key_col).isNull()).count()
    add_metric(
        "fact_order_items", "gold",
        f"null_check_{key_col}",
        0, null_count,
        "PASS" if null_count == 0 else "FAIL",
        f"{null_count} nulls in {key_col}"
    )

# ── 3. is_cancelled distribution ─────────────────────────────────────────────
# Should have both True and False — if one is missing something went wrong

cancelled_counts = fact_order_items.groupBy("is_cancelled").count().collect()
cancelled_map = {str(r["is_cancelled"]): r["count"] for r in cancelled_counts}

created_count   = cancelled_map.get("False", 0)
cancelled_count = cancelled_map.get("True", 0)

add_metric(
    "fact_order_items", "gold",
    "event_split_created",
    ">0", created_count,
    "PASS" if created_count > 0 else "FAIL",
    "order_item_created count in fact"
)
add_metric(
    "fact_order_items", "gold",
    "event_split_cancelled",
    ">0", cancelled_count,
    "PASS" if cancelled_count > 0 else "FAIL",
    "order_item_cancelled count in fact"
)

# ── 4. No negative total_amount or quantity in fact ──────────────────────────

neg_amount = fact_order_items.filter(col("total_amount") < 0).count()
neg_qty    = fact_order_items.filter(col("quantity") < 0).count()

add_metric(
    "fact_order_items", "gold",
    "no_negative_total_amount",
    0, neg_amount,
    "PASS" if neg_amount == 0 else "FAIL",
    "Rows with negative total_amount (should be 0 — absolute values stored)"
)
add_metric(
    "fact_order_items", "gold",
    "no_negative_quantity",
    0, neg_qty,
    "PASS" if neg_qty == 0 else "FAIL",
    "Rows with negative quantity (should be 0 — absolute values stored)"
)

# ── 5. date_key referential integrity — all date_keys exist in dim_date ───────

orphan_dates = fact_order_items \
    .select("date_key").distinct() \
    .join(dim_date.select("date_key"), "date_key", "left_anti") \
    .count()

add_metric(
    "fact_order_items", "gold",
    "referential_integrity_date_key",
    0, orphan_dates,
    "PASS" if orphan_dates == 0 else "FAIL",
    f"{orphan_dates} date_keys in fact not found in dim_date"
)

# ── 6. stock_code referential integrity — all stock_codes exist in dim_product ─

orphan_products = fact_order_items \
    .select("stock_code").distinct() \
    .join(dim_product.select("stock_code"), "stock_code", "left_anti") \
    .count()

add_metric(
    "fact_order_items", "gold",
    "referential_integrity_stock_code",
    0, orphan_products,
    "PASS" if orphan_products == 0 else "FAIL",
    f"{orphan_products} stock_codes in fact not found in dim_product"
)


# ══════════════════════════════════════════════════════════════════════════════
# DIMENSION TABLE CHECKS
# ══════════════════════════════════════════════════════════════════════════════

logger.section("dimension table checks")

# ── dim_product: no duplicate stock_codes ────────────────────────────────────

product_dupes = dim_product.groupBy("stock_code").count().filter(col("count") > 1).count()
add_metric(
    "dim_product", "gold",
    "duplicate_stock_codes",
    0, product_dupes,
    "PASS" if product_dupes == 0 else "FAIL",
    "Duplicate stock_codes in dim_product"
)

# ── dim_product: no null descriptions ────────────────────────────────────────

null_desc = dim_product.filter(col("description").isNull()).count()
add_metric(
    "dim_product", "gold",
    "null_descriptions",
    0, null_desc,
    "PASS" if null_desc == 0 else "WARN",
    f"{null_desc} products with null description"
)

# ── dim_customer: no duplicate customer_ids ───────────────────────────────────

customer_dupes = dim_customer.groupBy("customer_id").count().filter(col("count") > 1).count()
add_metric(
    "dim_customer", "gold",
    "duplicate_customer_ids",
    0, customer_dupes,
    "PASS" if customer_dupes == 0 else "FAIL",
    "Duplicate customer_ids in dim_customer"
)

# ── dim_date: date range covers full 2009-2010 ────────────────────────────────

date_stats = dim_date.agg(
    spark_min("full_date").alias("min_date"),
    spark_max("full_date").alias("max_date"),
    count("date_key").alias("total_days")
).collect()[0]

add_metric(
    "dim_date", "gold",
    "date_range_min",
    "2009-12-01", str(date_stats["min_date"]),
    "PASS" if str(date_stats["min_date"]) <= "2009-12-01" else "WARN",
    "Earliest date in dim_date"
)
add_metric(
    "dim_date", "gold",
    "date_range_max",
    "2010-12-31", str(date_stats["max_date"]),
    "PASS" if str(date_stats["max_date"]) >= "2010-12-09" else "WARN",
    "Latest date in dim_date"
)
logger.info(f"dim_date covers {date_stats['total_days']} days from {date_stats['min_date']} to {date_stats['max_date']}")


# ══════════════════════════════════════════════════════════════════════════════
# REPORTING TABLE CHECKS
# ══════════════════════════════════════════════════════════════════════════════

logger.section("reporting table checks")

# ── Revenue reconciliation ────────────────────────────────────────────────────
# Total revenue in revenue_yearly must match sum from fact_order_items directly

fact_total_revenue = fact_order_items \
    .filter(col("is_cancelled") == False) \
    .agg(spark_round(spark_sum("total_amount"), 2).alias("total")) \
    .collect()[0]["total"]

reporting_total_revenue = revenue_yearly \
    .agg(spark_round(spark_sum("total_revenue"), 2).alias("total")) \
    .collect()[0]["total"]

revenue_match = abs(float(fact_total_revenue) - float(reporting_total_revenue)) < 1.0  # allow £1 rounding tolerance

add_metric(
    "revenue_yearly", "gold",
    "revenue_reconciliation_vs_fact",
    round(float(fact_total_revenue), 2),
    round(float(reporting_total_revenue), 2),
    "PASS" if revenue_match else "FAIL",
    f"Difference: £{abs(float(fact_total_revenue) - float(reporting_total_revenue)):.2f}"
)

# ── revenue_monthly: no zero revenue months ───────────────────────────────────

zero_rev_months = revenue_monthly.filter(col("total_revenue") <= 0).count()
add_metric(
    "revenue_monthly", "gold",
    "no_zero_revenue_months",
    0, zero_rev_months,
    "PASS" if zero_rev_months == 0 else "FAIL",
    f"{zero_rev_months} months with zero or negative revenue"
)

# ── revenue_monthly: MoM change within reasonable bounds ─────────────────────
# Flag if any month shows >200% growth or >80% drop (likely a data issue)

extreme_mom = revenue_monthly.filter(
    (col("mom_change_pct") > 200) | (col("mom_change_pct") < -80)
).count()

add_metric(
    "revenue_monthly", "gold",
    "extreme_mom_change",
    0, extreme_mom,
    "WARN" if extreme_mom > 0 else "PASS",
    f"{extreme_mom} months with extreme MoM revenue change (>200% or <-80%)"
)

# ── AOV sanity check: avg order value should be positive and reasonable ───────

aov_stats = aov_monthly.agg(
    spark_min("avg_order_value").alias("min_aov"),
    spark_max("avg_order_value").alias("max_aov")
).collect()[0]

add_metric(
    "aov_monthly", "gold",
    "aov_positive",
    ">0", aov_stats["min_aov"],
    "PASS" if aov_stats["min_aov"] > 0 else "FAIL",
    f"Min AOV across months: £{aov_stats['min_aov']}"
)

# ── CLV: no negative lifetime revenue ────────────────────────────────────────

neg_clv = clv.filter(col("lifetime_revenue") < 0).count()
add_metric(
    "customer_lifetime_value", "gold",
    "no_negative_clv",
    0, neg_clv,
    "PASS" if neg_clv == 0 else "FAIL",
    f"{neg_clv} customers with negative lifetime revenue"
)

# ── CLV: customer count matches dim_customer + guest row ─────────────────────

dim_customer_count  = dim_customer.count()
clv_count           = clv.count()
expected_clv_count  = dim_customer_count + 1  # +1 for guest bucket

add_metric(
    "customer_lifetime_value", "gold",
    "clv_customer_count",
    expected_clv_count, clv_count,
    "PASS" if clv_count == expected_clv_count else "WARN",
    f"Expected dim_customer ({dim_customer_count}) + 1 guest = {expected_clv_count}"
)

# ── Retention rate: always between 0 and 100 ─────────────────────────────────

invalid_retention = retention_rate.filter(
    (col("retention_rate_pct") < 0) | (col("retention_rate_pct") > 100)
).count()

add_metric(
    "retention_rate", "gold",
    "retention_rate_bounds",
    "0-100%", f"{invalid_retention} violations",
    "PASS" if invalid_retention == 0 else "FAIL",
    "Retention rate must be between 0 and 100"
)

# ── Basket size: avg basket size should be >= 1 ───────────────────────────────

min_basket = basket_size_monthly.agg(
    spark_min("avg_basket_size").alias("min")
).collect()[0]["min"]

add_metric(
    "basket_size_monthly", "gold",
    "basket_size_min",
    ">=1", min_basket,
    "PASS" if min_basket >= 1 else "FAIL",
    f"Minimum avg basket size across months: {min_basket}"
)

# ── new_vs_returning: both types exist in every month ────────────────────────

months_total    = new_vs_returning.select("year", "month").distinct().count()
months_with_new = new_vs_returning.filter(col("customer_type") == "new") \
    .select("year", "month").distinct().count()

add_metric(
    "new_vs_returning", "gold",
    "new_customers_every_month",
    months_total, months_with_new,
    "PASS" if months_total == months_with_new else "WARN",
    f"Months missing 'new' customer records: {months_total - months_with_new}"
)


# ══════════════════════════════════════════════════════════════════════════════
# WRITE METRICS (APPEND TO UNIFIED TABLE)
# ══════════════════════════════════════════════════════════════════════════════

logger.section("writing metrics")

metrics_df = spark.createDataFrame(metrics_rows, schema=metrics_schema)

metrics_df.write \
    .mode("append") \
    .parquet("s3a://gold/data_quality_metrics/")

# ── Summary ───────────────────────────────────────────────────────────────────

total  = len(metrics_rows)
passed = sum(1 for r in metrics_rows if r[7] == "PASS")
warned = sum(1 for r in metrics_rows if r[7] == "WARN")
failed = sum(1 for r in metrics_rows if r[7] == "FAIL")

logger.section("gold quality checks summary")
logger.info(f"Run ID          : {RUN_ID}")
logger.info(f"Total checks    : {total}")
logger.info(f"PASS            : {passed}")
logger.info(f"WARN            : {warned}")
logger.info(f"FAIL            : {failed}")

if failed > 0:
    logger.error(f"{failed} CRITICAL checks failed — investigate before promoting to production")
elif warned > 0:
    logger.warn(f"{warned} warnings raised — review recommended")
else:
    logger.info("All checks passed cleanly")

spark.stop()