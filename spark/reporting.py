from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round,
    dense_rank, desc, lag, expr, countDistinct,
    year, month, when, coalesce, lit, min as spark_min,
    max as spark_max, first
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("reporting")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

dim_date = spark.read.parquet("s3a://gold/dimensions/dim_date/")
dim_product = spark.read.parquet("s3a://gold/dimensions/dim_product/")
dim_customer = spark.read.parquet("s3a://gold/dimensions/dim_customer/")
fact_order_items = spark.read.parquet("s3a://gold/facts/fact_order_items/")

# ══════════════════════════════════════════════════════════════════════════════
# REPORTING TABLES — Revenue & Sales
# ══════════════════════════════════════════════════════════════════════════════

# Reusable base — only confirmed sales (not cancellations)
sales = fact_order_items.filter(col("is_cancelled") == False)


# ── 1. Total Revenue per Day / Month / Year ───────────────────────────────────
dim_date_slim = dim_date.drop("year", "month")

revenue_daily = sales \
    .join(dim_date_slim, "date_key") \
    .groupBy("full_date", "year", "month", "day", "day_of_week", "is_weekend") \
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        count("invoice_id").alias("total_line_items"),
        spark_sum("quantity").alias("total_units_sold")
    ) \
    .orderBy("full_date")

# Add MoM revenue change at daily grain isn't meaningful — do it at monthly
revenue_monthly = sales \
    .groupBy("year", "month") \
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("invoice_id").alias("total_line_items"),
        spark_sum("quantity").alias("total_units_sold")
    ) \
    .withColumn("total_revenue", spark_round("total_revenue", 2)) \
    .withColumn("year_month", expr("concat(year, '-', lpad(month, 2, '0'))"))

w_monthly = Window.orderBy("year", "month")
revenue_monthly = revenue_monthly \
    .withColumn("prev_month_revenue", lag("total_revenue", 1).over(w_monthly)) \
    .withColumn("mom_change_pct",
        spark_round(
            (col("total_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100,
        2)
    ) \
    .orderBy("year", "month")

revenue_yearly = sales \
    .groupBy("year") \
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("invoice_id").alias("total_line_items"),
        spark_sum("quantity").alias("total_units_sold")
    ) \
    .withColumn("total_revenue", spark_round("total_revenue", 2)) \
    .orderBy("year")

revenue_daily.write.mode("overwrite").parquet("s3a://gold/revenue_sales/revenue_daily/")
revenue_monthly.write.mode("overwrite").parquet("s3a://gold/revenue_sales/revenue_monthly/")
revenue_yearly.write.mode("overwrite").parquet("s3a://gold/revenue_sales/revenue_yearly/")
print(" Table 1 — revenue daily/monthly/yearly written")


# ── 2. Top Selling Products per Country ──────────────────────────────────────
# Ranked by total revenue within each country

w_country = Window.partitionBy("country").orderBy(desc("total_revenue"))

top_products_by_country = sales \
    .join(dim_product, "stock_code", "left") \
    .groupBy("country", "stock_code", "description") \
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        spark_sum("quantity").alias("total_units_sold"),
        count("invoice_id").alias("total_line_items")
    ) \
    .withColumn("total_revenue", spark_round("total_revenue", 2)) \
    .withColumn("country_rank", dense_rank().over(w_country)) \
    .filter(col("country_rank") <= 20) \
    .orderBy("country", "country_rank")

top_products_by_country.write.mode("overwrite").parquet("s3a://gold/revenue_sales/top_products_by_country/")
print(" Table 2 — top products by country written")


# ── 3. Top Selling Products (Global, No Grain) ───────────────────────────────

top_products_global = sales \
    .join(dim_product, "stock_code", "left") \
    .groupBy("stock_code", "description") \
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        spark_sum("quantity").alias("total_units_sold"),
        count("invoice_id").alias("total_line_items")
    ) \
    .withColumn("total_revenue", spark_round("total_revenue", 2)) \
    .withColumn("revenue_rank", dense_rank().over(
        Window.orderBy(desc("total_revenue"))
    )) \
    .withColumn("units_rank", dense_rank().over(
        Window.orderBy(desc("total_units_sold"))
    )) \
    .orderBy("revenue_rank")

top_products_global.write.mode("overwrite").parquet("s3a://gold/revenue_sales/top_products_global/")
print(" Table 3 — top products global written")


# ── 4. Average Order Value ────────────────────────────────────────────────────
# AOV = total revenue / number of distinct invoices
# Computed at monthly grain + overall

aov_monthly = sales \
    .groupBy("year", "month", "invoice_id") \
    .agg(spark_sum("total_amount").alias("invoice_revenue")) \
    .groupBy("year", "month") \
    .agg(
        spark_round(avg("invoice_revenue"), 2).alias("avg_order_value"),
        count("invoice_id").alias("total_invoices"),
        spark_round(spark_sum("invoice_revenue"), 2).alias("total_revenue")
    ) \
    .withColumn("year_month", expr("concat(year, '-', lpad(month, 2, '0'))")) \
    .orderBy("year", "month")

aov_overall = sales \
    .groupBy("invoice_id") \
    .agg(spark_sum("total_amount").alias("invoice_revenue")) \
    .agg(
        spark_round(avg("invoice_revenue"), 2).alias("avg_order_value"),
        count("invoice_id").alias("total_invoices"),
        spark_round(spark_sum("invoice_revenue"), 2).alias("total_revenue")
    )

aov_monthly.write.mode("overwrite").parquet("s3a://gold/revenue_sales/aov_monthly/")
aov_overall.write.mode("overwrite").parquet("s3a://gold/revenue_sales/aov_overall/")
print(" Table 4 — average order value written")


# ── 5. Total Revenue per Country per Month / Year ────────────────────────────

revenue_by_country_month = sales \
    .groupBy("country", "year", "month") \
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        count("invoice_id").alias("total_line_items"),
        spark_sum("quantity").alias("total_units_sold")
    ) \
    .withColumn("year_month", expr("concat(year, '-', lpad(month, 2, '0'))")) \
    .orderBy("country", "year", "month")

revenue_by_country_year = sales \
    .groupBy("country", "year") \
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        count("invoice_id").alias("total_line_items"),
        spark_sum("quantity").alias("total_units_sold")
    ) \
    .orderBy("country", "year")

revenue_by_country_month.write.mode("overwrite").parquet("s3a://gold/revenue_sales/revenue_by_country_month/")
revenue_by_country_year.write.mode("overwrite").parquet("s3a://gold/revenue_sales/revenue_by_country_year/")
print(" Table 5 — revenue by country written")


# ── 6. Product Demand Trends ──────────────────────────────────────────────────
# Monthly units sold per product — useful for spotting seasonal demand spikes

w_product_trend = Window.partitionBy("stock_code").orderBy("year", "month")

product_demand_trends = sales \
    .join(dim_product, "stock_code", "left") \
    .groupBy("stock_code", "description", "year", "month") \
    .agg(
        spark_sum("quantity").alias("units_sold"),
        spark_round(spark_sum("total_amount"), 2).alias("revenue"),
        count("invoice_id").alias("order_line_count")
    ) \
    .withColumn("year_month", expr("concat(year, '-', lpad(month, 2, '0'))")) \
    .withColumn("prev_month_units", lag("units_sold", 1).over(w_product_trend)) \
    .withColumn("mom_units_change_pct",
        spark_round(
            (col("units_sold") - col("prev_month_units")) / col("prev_month_units") * 100,
        2)
    ) \
    .orderBy("stock_code", "year", "month")

product_demand_trends.write.mode("overwrite").parquet("s3a://gold/revenue_sales/product_demand_trends/")
print("Table 6 — product demand trends written")


# ── Summary ───────────────────────────────────────────────────────────────────

print("\n Gold layer — revenue & sales complete.")

# ══════════════════════════════════════════════════════════════════════════════
# REPORTING TABLES — Customer and product insights
# ══════════════════════════════════════════════════════════════════════════════

# Base — sales only, guest orders bucketed as "guest"
sales = fact_order_items \
    .filter(col("is_cancelled") == False) \
    .withColumn("customer_id_clean",
        coalesce(col("customer_id"), lit("guest"))
    )
 
print("Base sales loaded:", sales.count())
 
 
# 1. NEW VS RETURNING CUSTOMERS PER MONTH
# A customer is "new" in the month they first appear in sales data.
# "Returning" in all subsequent months they make a purchase.
# Guest orders are excluded here — no way to track their history.
 
customer_monthly_activity = sales \
    .filter(col("customer_id").isNotNull()) \
    .select("customer_id", "year", "month") \
    .distinct()
 
# First purchase month per customer
first_purchase = customer_monthly_activity \
    .groupBy("customer_id") \
    .agg(
        spark_min("year").alias("first_year"),
        first("month").alias("first_month")
    )
 
# This approach is fragile for multi-year data — use year*12+month as a period key
customer_monthly_activity = customer_monthly_activity \
    .withColumn("period", col("year") * 12 + col("month"))
 
first_purchase = sales \
    .filter(col("customer_id").isNotNull()) \
    .groupBy("customer_id") \
    .agg(spark_min(col("year") * 12 + col("month")).alias("first_period"))
 
customer_monthly_tagged = customer_monthly_activity \
    .join(first_purchase, "customer_id") \
    .withColumn("customer_type",
        when(col("period") == col("first_period"), "new")
        .otherwise("returning")
    )
 
new_vs_returning = customer_monthly_tagged \
    .groupBy("year", "month", "customer_type") \
    .agg(count("customer_id").alias("customer_count")) \
    .withColumn("year_month", expr("concat(year, '-', lpad(month, 2, '0'))")) \
    .orderBy("year", "month", "customer_type")
 
new_vs_returning.write.mode("overwrite").parquet("s3a://gold/customer_analytics/new_vs_returning/")
print(" Table 1 — new vs returning customers written")
 
 
# 2. CUSTOMER LIFETIME VALUE (CLV)
# Per customer: total revenue, total orders, avg order value, first and last order date
# Guest bucket included as a single row
 
clv = sales \
    .groupBy("customer_id_clean") \
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("lifetime_revenue"),
        countDistinct("invoice_id").alias("total_orders"),
        spark_round(avg("total_amount"), 2).alias("avg_line_item_value"),
        spark_sum("quantity").alias("total_units_purchased"),
        spark_min("event_time").alias("first_order_date"),
        spark_max("event_time").alias("last_order_date")
    ) \
    .withColumn("avg_order_value",
        spark_round(col("lifetime_revenue") / col("total_orders"), 2)
    ) \
    .withColumn("clv_rank",
        dense_rank().over(Window.orderBy(desc("lifetime_revenue")))
    ) \
    .orderBy("clv_rank")
 
clv.write.mode("overwrite").parquet("s3a://gold/customer_analytics/customer_lifetime_value/")
print(" Table 2 — customer lifetime value written")
 
 
# 3. CUSTOMERS PER COUNTRY
# From dim_customer — registered customers only (not guests, guests have no country)
# Also add total revenue per country from sales for context
 
customers_per_country = dim_customer \
    .groupBy("country") \
    .agg(count("customer_id").alias("total_customers")) \
    .orderBy(desc("total_customers"))
 
revenue_per_country = sales \
    .filter(col("customer_id").isNotNull()) \
    .groupBy("country") \
    .agg(spark_round(spark_sum("total_amount"), 2).alias("total_revenue"))
 
customers_per_country = customers_per_country \
    .join(revenue_per_country, "country", "left") \
    .withColumn("revenue_per_customer",
        spark_round(col("total_revenue") / col("total_customers"), 2)
    ) \
    .orderBy(desc("total_customers"))
 
customers_per_country.write.mode("overwrite").parquet("s3a://gold/customer_analytics/customers_per_country/")
print(" Table 3 — customers per country written")
 
 
# 4. CUSTOMERS WITH MAXIMUM ORDERS
# Ranked by distinct invoice count — guests excluded since they cant be tracked
 
top_customers_by_orders = sales \
    .filter(col("customer_id").isNotNull()) \
    .groupBy("customer_id") \
    .agg(
        countDistinct("invoice_id").alias("total_orders"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_min("event_time").alias("first_order_date"),
        spark_max("event_time").alias("last_order_date")
    ) \
    .join(
        dim_customer.select("customer_id", "country", "cohort"),
        "customer_id", "left"
    ) \
    .withColumn("order_rank",
        dense_rank().over(Window.orderBy(desc("total_orders")))
    ) \
    .orderBy("order_rank")
 
top_customers_by_orders.write.mode("overwrite").parquet("s3a://gold/customer_analytics/top_customers_by_orders/")
print(" Table 4 — top customers by orders written")
 
 
# 5. RETENTION RATE (Month over Month)
# Of customers active in month X, what % were also active in month X+1
# guests excluded — no identity to track across months
 
monthly_active = sales \
    .filter(col("customer_id").isNotNull()) \
    .select("customer_id", "year", "month") \
    .distinct() \
    .withColumn("period", col("year") * 12 + col("month"))
 
# Self join: current month customers vs next month customers
current_month = monthly_active.alias("curr")
next_month    = monthly_active.alias("next")
 
retention = current_month \
    .join(
        next_month,
        (col("curr.customer_id") == col("next.customer_id")) &
        (col("next.period") == col("curr.period") + 1),
        "left"
    ) \
    .groupBy(
        col("curr.year").alias("year"),
        col("curr.month").alias("month"),
        col("curr.period").alias("period")
    ) \
    .agg(
        count("curr.customer_id").alias("active_customers"),
        count("next.customer_id").alias("retained_next_month")
    ) \
    .withColumn("retention_rate_pct",
        spark_round(
            col("retained_next_month") / col("active_customers") * 100,
        2)
    ) \
    .withColumn("year_month", expr("concat(year, '-', lpad(month, 2, '0'))")) \
    .orderBy("year", "month") \
    .drop("period")
 
retention.write.mode("overwrite").parquet("s3a://gold/customer_analytics/retention_rate/")
print(" Table 5 — retention rate written")
 
 
# 6. BASKET SIZE (Product Insights)
# Per invoice: how many distinct line items, total quantity, total value
# Basket size = avg distinct line items per invoice
 
basket_per_invoice = sales \
    .groupBy("invoice_id", "year", "month") \
    .agg(
        count("stock_code").alias("line_item_count"),        # distinct products in basket
        spark_sum("quantity").alias("total_units"),
        spark_round(spark_sum("total_amount"), 2).alias("basket_value")
    )
 
basket_size_monthly = basket_per_invoice \
    .groupBy("year", "month") \
    .agg(
        spark_round(avg("line_item_count"), 2).alias("avg_basket_size"),
        spark_round(avg("total_units"), 2).alias("avg_units_per_order"),
        spark_round(avg("basket_value"), 2).alias("avg_basket_value"),
        count("invoice_id").alias("total_invoices")
    ) \
    .withColumn("year_month", expr("concat(year, '-', lpad(month, 2, '0'))")) \
    .orderBy("year", "month")
 
basket_size_overall = basket_per_invoice \
    .agg(
        spark_round(avg("line_item_count"), 2).alias("avg_basket_size"),
        spark_round(avg("total_units"), 2).alias("avg_units_per_order"),
        spark_round(avg("basket_value"), 2).alias("avg_basket_value"),
        count("invoice_id").alias("total_invoices")
    )
 
basket_size_monthly.write.mode("overwrite").parquet("s3a://gold/product_insights/basket_size_monthly/")
basket_size_overall.write.mode("overwrite").parquet("s3a://gold/product_insights/basket_size_overall/")
print(" Table 6 — basket size written")
 
 
print("\n Gold layer — customer analytics & product insights complete.")

spark.stop()
