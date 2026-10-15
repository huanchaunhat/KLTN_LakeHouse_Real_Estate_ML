import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"
VN_TZ = "Asia/Ho_Chi_Minh"

# Không dùng --packages: Ivy tải lại aws-java-sdk-bundle (~280MB) từ Maven mỗi lần
# và hay đứt giữa chừng. Driver (client mode, chạy trong Airflow) đọc jar local;
# executor đã có sẵn cùng bộ jar trong /opt/bitnami/spark/jars (spark/Dockerfile).
# Phiên bản phải khớp Spark/Hadoop 3.3.2.
AIRFLOW_JARS_DIR = "/usr/local/airflow/jars"
S3_JARS = [
    f"{AIRFLOW_JARS_DIR}/hadoop-aws-3.3.2.jar",
    f"{AIRFLOW_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
]
DELTA_JARS = S3_JARS + [
    f"{AIRFLOW_JARS_DIR}/delta-core_2.12-2.2.0.jar",
    f"{AIRFLOW_JARS_DIR}/delta-storage-2.2.0.jar",
]
S3_DRIVER_CONF = {"spark.driver.extraClassPath": ":".join(S3_JARS)}
DELTA_DRIVER_CONF = {"spark.driver.extraClassPath": ":".join(DELTA_JARS)}

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

# Cluster có 4 core (2 worker x 2). Thrift server (Superset, dbt) giữ cố định 1 core,
# nên crawler chỉ được lấy tối đa 3; lấy 4 thì Thrift bị WAITING với 0 core và mọi
# query từ Superset treo tới timeout.
CRAWL_MAX_CORES = 3

ID_CRAWL_CONF = {
    **COMMON_S3_CONF,
    **S3_DRIVER_CONF,
    "spark.dynamicAllocation.enabled": "false",
    "spark.cores.max": str(CRAWL_MAX_CORES),
    "spark.speculation": "false",
}

PROFILE_CRAWL_CONF = {
    **COMMON_S3_CONF,
    **DELTA_DRIVER_CONF,
    "spark.dynamicAllocation.enabled": "false",
    "spark.cores.max": str(CRAWL_MAX_CORES),
    "spark.speculation": "false",
    # Current profile target construction uses joins/groupBy. Avoid Spark's default
    # 200 shuffle partitions on a 3-core budget.
    "spark.sql.shuffle.partitions": "12",
    "spark.default.parallelism": "12",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

BRONZE_CONF = {
    **COMMON_S3_CONF,
    **DELTA_DRIVER_CONF,
    "spark.dynamicAllocation.enabled": "false",
    "spark.cores.max": "1",
    "spark.speculation": "false",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    # Driver chạy client mode trong container Airflow (không có spark-defaults.conf),
    # nên phải chỉ rõ warehouse trên MinIO, nếu không CREATE DATABASE sẽ dùng file:/ local.
    "spark.sql.warehouse.dir": "s3a://lakehouse/warehouse",
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
        conf=ID_CRAWL_CONF,
        driver_memory="1024m",
        executor_memory="1024m",
        executor_cores=1,
        total_executor_cores=CRAWL_MAX_CORES,
        application_args=[
            "--run-id", "{{ vn_run_id(dag_run.logical_date) }}",
            "--crawl-date", "{{ vn_crawl_date(dag_run.data_interval_end) }}",
            "--list-partitions", "44",
            "--request-delay", "0.15",
        ],
    )

    crawl_nhatot_profiles = SparkSubmitOperator(
        task_id="crawl_nhatot_profiles",
        application="/usr/local/airflow/scripts/crawl_nhatot_profiles.py",
        conn_id="spark_default",
        conf=PROFILE_CRAWL_CONF,
        driver_memory="1024m",
        executor_memory="1024m",
        executor_cores=1,
        total_executor_cores=CRAWL_MAX_CORES,
        application_args=[
            "--run-id", "{{ vn_run_id(dag_run.logical_date) }}",
            "--crawl-date", "{{ vn_crawl_date(dag_run.data_interval_end) }}",
            "--detail-partitions", "12",
            # Start conservatively: 3 active Spark tasks * 4 HTTP workers ~= 12
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
        conf=BRONZE_CONF,
        driver_memory="1024m",
        executor_memory="1024m",
        executor_cores=1,
        total_executor_cores=1,
        application_args=[
            "--run-id", "{{ vn_run_id(dag_run.logical_date) }}",
            "--crawl-date", "{{ vn_crawl_date(dag_run.data_interval_end) }}",
        ],
    )

    DBT_ARGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    # gold+ also excludes singular tests that read Gold: `stg+` selects them directly,
    # so --indirect-selection cannot skip them, and Gold is stale at this point.
    SILVER_SELECT = "--select stg_nhatot_properties+ --exclude path:models/gold+"
    GOLD_SELECT = "--select path:models/gold"

    # Silver: staging + intermediate + silver models (schema `silver`).
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"""
set -e
cd {DBT_PROJECT_DIR}
dbt deps {DBT_ARGS}
dbt run {SILVER_SELECT} {DBT_ARGS}
""",
    )

    # Tests that reference Gold models run in dbt_test_gold.
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"""
set -e
cd {DBT_PROJECT_DIR}
dbt test {SILVER_SELECT} --indirect-selection cautious {DBT_ARGS}
""",
    )

    # Gold: star schema (dims + facts), BI marts and ML features (schema `gold`).
    # Only built after Silver passed its quality gate.
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"""
set -e
cd {DBT_PROJECT_DIR}
dbt run {GOLD_SELECT} {DBT_ARGS}
""",
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"""
set -e
cd {DBT_PROJECT_DIR}
dbt test {GOLD_SELECT} {DBT_ARGS}
""",
    )

    (
        crawl_nhatot_ids
        >> crawl_nhatot_profiles
        >> load_bronze
        >> dbt_run_silver
        >> dbt_test_silver
        >> dbt_run_gold
        >> dbt_test_gold
    )


reip_pipeline()
