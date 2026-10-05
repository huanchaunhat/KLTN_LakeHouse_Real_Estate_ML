#!/usr/bin/env python3

import argparse

from delta.tables import DeltaTable
from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat,
    count,
    current_timestamp,
    input_file_name,
    lit,
    to_date,
    to_timestamp,
    when,
)

SOURCE = "nhatot"
DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/"
LANDING_ROOT = "s3a://lakehouse/landing/source=nhatot"
BRONZE_LOCATION = "s3a://lakehouse/bronze/source=nhatot/raw_properties"
BRONZE_TABLE = "bronze.raw_properties"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--crawl-date", required=True)
    parser.add_argument(
        "--validate-only", action="store_true",
        help="Read, normalize and count without writing Bronze or Hive",
    )
    return parser.parse_args()


def create_spark():
    # Keep tuning local to Bronze; explicit spark-submit settings take precedence.
    conf = SparkConf()
    defaults = {
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "32m",
        "spark.sql.files.maxPartitionBytes": "32m",
        "spark.sql.inMemoryColumnarStorage.batchSize": "1000",
    }
    for key, value in defaults.items():
        if not conf.contains(key):
            conf.set(key, value)
    return (
        SparkSession.builder
        .config(conf=conf)
        .appName("REIP-Load-Bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )


def add_missing_column(df, name, value):
    return df if name in df.columns else df.withColumn(name, value)


def main():
    args = parse_args()
    spark = create_spark()
    try:
        load_bronze(spark, args)
    finally:
        spark.stop()


def load_bronze(spark, args):
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"{LANDING_ROOT}/crawl_date={args.crawl_date}/run_id={args.run_id}"

    print(f"[1/7] Reading Landing: {input_path}", flush=True)

    data_path = f"{input_path}/part-*"

    jpath = spark._jvm.org.apache.hadoop.fs.Path(data_path)
    fs = jpath.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )
    files = fs.globStatus(jpath)

    if not files:
        raise RuntimeError(
            f"No part files found: {data_path}"
        )
    
    df = (
        spark.read
        .option("mode", "PERMISSIVE")
        .json(data_path)
    )

    print(f"[2/7] Schema inferred, columns={len(df.columns)}", flush=True)

    if "source_listing_id" not in df.columns:
        raise RuntimeError(f"Missing source_listing_id in {input_path}")

    if "crawl_timestamp" not in df.columns:
        raise RuntimeError(f"Missing crawl_timestamp in {input_path}")

    df = add_missing_column(df, "snapshot_id", lit(args.run_id))
    df = add_missing_column(df, "crawl_date", lit(args.crawl_date))
    df = add_missing_column(df, "http_status", lit(200))
    df = add_missing_column(df, "url", concat(lit(DETAIL_URL), col("source_listing_id").cast("string")))
    df = add_missing_column(df, "error", lit(None).cast("string"))

    df = (
        df
        .withColumn("source", lit(SOURCE))
        .withColumn("source_listing_id", col("source_listing_id").cast("string"))
        .withColumn("snapshot_id", coalesce(col("snapshot_id").cast("string"), lit(args.run_id)))
        .withColumn("crawl_date", coalesce(to_date("crawl_date"), lit(args.crawl_date).cast("date")))
        .withColumn("crawl_timestamp", to_timestamp("crawl_timestamp"))
        .withColumn(
            "http_status",
            coalesce(
                col("http_status").cast("int"),
                when(col("payload").isNotNull(), lit(200)),
            ),
        )
        .withColumn(
            "url",
            coalesce(
                col("url"),
                concat(lit(DETAIL_URL), col("source_listing_id")),
            ),
        )
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .dropDuplicates(["source", "source_listing_id", "snapshot_id"])
    )

    print("[3/7] Technical normalization prepared", flush=True)

    required = (
        col("source_listing_id").isNotNull()
        & col("snapshot_id").isNotNull()
        & col("crawl_timestamp").isNotNull()
    )

    print("[4/7] Counting records...", flush=True)

    # Reuse the same deduplicated rows for metrics and MERGE without retaining
    # the wide nested payload in executor memory.
    df = df.persist(StorageLevel.DISK_ONLY)

    try:
        metrics = df.agg(
            count(lit(1)).alias("total"),
            count(when(required, lit(1))).alias("valid"),
        ).first()
        total_count = metrics["total"]
        valid = df.filter(required)
        run_count = metrics["valid"]
        invalid_count = total_count - run_count

        print(
            f"[5/7] total={total_count:,}, valid={run_count:,}, invalid={invalid_count:,}",
            flush=True,
        )

        if run_count == 0:
            raise RuntimeError(f"No valid Bronze records found at {input_path}")

        if args.validate_only:
            print("Validation complete; Bronze and Hive were not modified", flush=True)
            return

        print("[6/7] Writing Delta Bronze...", flush=True)
        if DeltaTable.isDeltaTable(spark, BRONZE_LOCATION):
            target = DeltaTable.forPath(spark, BRONZE_LOCATION)

            (
                target.alias("t")
                .merge(
                    valid.alias("s"),
                    """
                    t.source = s.source
                    AND t.source_listing_id = s.source_listing_id
                    AND t.snapshot_id = s.snapshot_id
                    """,
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            (
                valid.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("crawl_date")
                .save(BRONZE_LOCATION)
            )

        print("[7/7] Registering Hive table...", flush=True)
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
            USING DELTA
            LOCATION '{BRONZE_LOCATION}'
            """
        )
        spark.sql(f"REFRESH TABLE {BRONZE_TABLE}")

        print(f"input={input_path}")
        print(f"bronze_rows_this_run={run_count:,}")
        print(f"invalid_envelopes={invalid_count:,}")
        print(f"table={BRONZE_TABLE}")
    finally:
        df.unpersist()


if __name__ == "__main__":
    main()
