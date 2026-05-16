from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging

logger = logging.getLogger("retail_pipeline_dag")

default_args = {
    "owner": "retail_pipeline",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
}

# ── Spark connection config ───────────────────────────────────────────────────
# This references the 'spark_default' connection configured in Airflow UI
# Connection type: Spark
# Host: spark://spark-master
# Port: 7077

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_PATH = "/opt/spark/jobs"

# Jars needed for MinIO (S3A) access — same as your existing spark setup
SPARK_JARS = ",".join([
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark/jars/postgresql-42.6.0.jar",
])

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "admin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
}


# ── Callbacks ─────────────────────────────────────────────────────────────────

def on_failure_callback(context):
    task_id  = context["task_instance"].task_id
    dag_id   = context["task_instance"].dag_id
    log_url  = context["task_instance"].log_url
    exc      = context.get("exception", "Unknown error")
    logger.error(
        f"PIPELINE FAILURE | DAG: {dag_id} | Task: {task_id} | "
        f"Exception: {exc} | Logs: {log_url}"
    )

def on_success_callback(context):
    task_id = context["task_instance"].task_id
    dag_id  = context["task_instance"].dag_id
    logger.info(f"TASK SUCCESS | DAG: {dag_id} | Task: {task_id}")


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    description="End-to-end retail pipeline: silver → gold → data quality checks",
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC
    catchup=False,                    # don't backfill missed runs
    max_active_runs=1,                # only one run at a time
    tags=["retail", "spark", "gold"],
) as dag:

    # ── Start ─────────────────────────────────────────────────────────────────

    start = EmptyOperator(task_id="start")

    # ── Silver Layer ──────────────────────────────────────────────────────────
    # Reads from bronze, cleans, deduplicates, writes silver parquet

    silver_layer = SparkSubmitOperator(
        task_id="silver_layer",
        application=f"{SPARK_JOBS_PATH}/silver_layer.py",
        conn_id=SPARK_CONN_ID,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        py_files=f"{SPARK_JOBS_PATH}/utils.zip",
        name="retail-silver-layer",
        verbose=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    # ── Silver Data Quality Checks ────────────────────────────────────────────
    # Validates bronze vs silver row counts, nulls, dedup, referential integrity

    silver_quality_checks = SparkSubmitOperator(
        task_id="silver_quality_checks",
        application=f"{SPARK_JOBS_PATH}/data_quality_checks.py",
        conn_id=SPARK_CONN_ID,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        py_files=f"{SPARK_JOBS_PATH}/utils.zip",
        name="retail-silver-dq-checks",
        verbose=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    # ── Gold: Dimensions ──────────────────────────────────────────────────────
    # dim_date, dim_product, dim_customer, dim_geography

    gold_dimensions = SparkSubmitOperator(
        task_id="gold_dimensions",
        application=f"{SPARK_JOBS_PATH}/dimensions.py",
        conn_id=SPARK_CONN_ID,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        py_files=f"{SPARK_JOBS_PATH}/utils.zip",
        name="retail-gold-dimensions",
        verbose=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    # ── Gold: Fact Table ──────────────────────────────────────────────────────
    # fact_order_items — depends on dimensions being ready

    gold_facts = SparkSubmitOperator(
        task_id="gold_facts",
        application=f"{SPARK_JOBS_PATH}/facts.py",
        conn_id=SPARK_CONN_ID,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        py_files=f"{SPARK_JOBS_PATH}/utils.zip",
        name="retail-gold-facts",
        verbose=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    # ── Gold: Reporting Tables ────────────────────────────────────────────────
    # Revenue, customer analytics, product insights — depends on fact table

    gold_reporting = SparkSubmitOperator(
        task_id="gold_reporting",
        application=f"{SPARK_JOBS_PATH}/reporting.py",
        conn_id=SPARK_CONN_ID,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        py_files=f"{SPARK_JOBS_PATH}/utils.zip",
        name="retail-gold-reporting",
        verbose=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    # ── Gold: Data Quality Checks ─────────────────────────────────────────────
    # Validates fact, dims, and all reporting tables
    # Appends results to unified data_quality_metrics table

    gold_quality_checks = SparkSubmitOperator(
        task_id="gold_quality_checks",
        application=f"{SPARK_JOBS_PATH}/gold_quality_checks.py",
        conn_id=SPARK_CONN_ID,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        py_files=f"{SPARK_JOBS_PATH}/utils.zip",
        name="retail-gold-dq-checks",
        verbose=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    gold_to_postgres = SparkSubmitOperator(
        task_id="gold_to_postgres",
        application=f"{SPARK_JOBS_PATH}/gold_to_postgres.py",
        conn_id=SPARK_CONN_ID,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        py_files=f"{SPARK_JOBS_PATH}/utils.zip",
        name="retail-gold-to-postgres",
        verbose=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    # ── End ───────────────────────────────────────────────────────────────────

    end = EmptyOperator(task_id="end")

    # ── Task Dependencies ─────────────────────────────────────────────────────
    #
    # start
    #   └── silver_layer
    #         └── silver_quality_checks
    #               └── gold_dimensions
    #                     └── gold_facts
    #                           └── gold_reporting
    #                                 └── gold_quality_checks
    #                                       └── end

    (
        start
        >> silver_layer
        >> silver_quality_checks
        >> gold_dimensions
        >> gold_facts
        >> gold_reporting
        >> gold_quality_checks
        >> gold_to_postgres
        >> end
    )