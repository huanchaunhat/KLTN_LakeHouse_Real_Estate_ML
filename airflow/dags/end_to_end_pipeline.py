import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"
VN_TZ = "Asia/Ho_Chi_Minh"

S3_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
DELTA_PACKAGES = (
    "io.delta:delta-core_2.12:2.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

MINIO_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

COMMON_S3_CONF = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    # MinIO/S3A write tuning. These do not change the Landing contract.
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
    "spark.hadoop.fs.s3a.connection.maximum": "64",
    "spark.hadoop.fs.s3a.threads.max": "64",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
}

ID_CRAWL_CONF = {
    **COMMON_S3_CONF,
    "spark.dynamicAllocation.enabled": "false",
    "spark.cores.max": "4",
    "spark.speculation": "false",
}

PROFILE_CRAWL_CONF = {
    **COMMON_S3_CONF,
    "spark.dynamicAllocation.enabled": "false",
    "spark.cores.max": "4",
    "spark.speculation": "false",
    # Current profile target construction uses joins/groupBy. Avoid Spark's default
    # 200 shuffle partitions on a 4-core local cluster.
    "spark.sql.shuffle.partitions": "12",
    "spark.default.parallelism": "12",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

BRONZE_CONF = {
    **COMMON_S3_CONF,
    "spark.dynamicAllocation.enabled": "false",
    "spark.cores.max": "1",
    "spark.speculation": "false",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
}


def vn_crawl_date(dt):
    return pendulum.instance(dt).in_timezone(VN_TZ).strftime("%Y-%m-%d")


def vn_run_id(dt):
    return pendulum.instance(dt).in_timezone(VN_TZ).strftime("%Y%m%d_%H%M%S")


@dag(
    dag_id="end_to_end_lakehouse_pipeline",
    description="Nha Tot: crawl IDs -> crawl profiles -> Bronze -> Silver",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 10, 10, tz=VN_TZ),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    user_defined_macros={
        "vn_crawl_date": vn_crawl_date,
        "vn_run_id": vn_run_id,
    },
    tags=["reip", "nhatot", "lakehouse"],
)
def reip_pipeline():
    crawl_nhatot_ids = SparkSubmitOperator(
        task_id="crawl_nhatot_ids",
        application="/usr/local/airflow/scripts/crawl_nhatot_ids.py",
        conn_id="spark_default",
        packages=S3_PACKAGES,
        conf=ID_CRAWL_CONF,
        driver_memory="1024m",
        executor_memory="1024m",
        executor_cores=1,
        total_executor_cores=4,
        application_args=[
            "--run-id", "{{ vn_run_id(dag_run.start_date) }}",
            "--crawl-date", "{{ vn_crawl_date(dag_run.start_date) }}",
            "--list-partitions", "44",
            "--request-delay", "0.15",
        ],
    )

    crawl_nhatot_profiles = SparkSubmitOperator(
        task_id="crawl_nhatot_profiles",
        application="/usr/local/airflow/scripts/crawl_nhatot_profiles.py",
        conn_id="spark_default",
        packages=DELTA_PACKAGES,
        conf=PROFILE_CRAWL_CONF,
        driver_memory="1024m",
        executor_memory="1024m",
        executor_cores=1,
        total_executor_cores=4,
        application_args=[
            "--run-id", "{{ vn_run_id(dag_run.start_date) }}",
            "--crawl-date", "{{ vn_crawl_date(dag_run.start_date) }}",
            "--detail-partitions", "12",
            # Start conservatively: 4 active Spark tasks * 4 HTTP workers ~= 16
            # simultaneous profile requests. Increase to 6/8 only if 429 stays low.
            "--http-workers-per-partition", "4",
            "--output-partitions", "8",
            "--request-delay", "0.15",
            "--historical-lookback-days", "0",
        ],
    )

    load_bronze = SparkSubmitOperator(
        task_id="load_bronze",
        application="/usr/local/airflow/scripts/load_bronze.py",
        conn_id="spark_default",
        packages=DELTA_PACKAGES,
        conf=BRONZE_CONF,
        driver_memory="1024m",
        executor_memory="1024m",
        executor_cores=1,
        total_executor_cores=1,
        application_args=[
            "--run-id", "{{ vn_run_id(dag_run.start_date) }}",
            "--crawl-date", "{{ vn_crawl_date(dag_run.start_date) }}",
        ],
    )

    # Phase B was removed. Only the Phase A Silver graph is selected.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
set -e
cd {DBT_PROJECT_DIR}
dbt run \\
  --select stg_nhatot_properties+ \\
  --project-dir {DBT_PROJECT_DIR} \\
  --profiles-dir {DBT_PROJECT_DIR}
""",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
set -e
cd {DBT_PROJECT_DIR}
dbt test \\
  --select stg_nhatot_properties+ \\
  --project-dir {DBT_PROJECT_DIR} \\
  --profiles-dir {DBT_PROJECT_DIR}
""",
    )

    crawl_nhatot_ids >> crawl_nhatot_profiles >> load_bronze >> dbt_run >> dbt_test


reip_pipeline()
