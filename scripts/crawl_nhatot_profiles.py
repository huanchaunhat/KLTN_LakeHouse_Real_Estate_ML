#!/usr/bin/env python3

import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from delta.tables import DeltaTable
from requests.adapters import HTTPAdapter
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    date_sub,
    lit,
    max as spark_max,
    to_date,
)

DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{}"
ID_LANDING_ROOT = "s3a://lakehouse/landing/source=nhatot_ids"
PROFILE_LANDING_ROOT = "s3a://lakehouse/landing/source=nhatot"
BRONZE_LOCATION = "s3a://lakehouse/bronze/source=nhatot/raw_properties"
HEADERS = {"User-Agent": "Mozilla/5.0"}
# Statuses meaning the listing was taken down.
GONE_HTTP_STATUSES = (404, 410)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--crawl-date", required=True)
    parser.add_argument("--detail-partitions", type=int, default=12)
    parser.add_argument("--http-workers-per-partition", type=int, default=4)
    parser.add_argument("--output-partitions", type=int, default=8)
    parser.add_argument("--request-delay", type=float, default=0.15)
    parser.add_argument(
        "--historical-lookback-days",
        type=int,
        default=0,
        help=(
            "0 = crawl every listing that has ever had a successful Bronze profile; "
            ">0 = only keep listings successfully seen within this many days."
        ),
    )
    parser.add_argument(
        "--max-consecutive-gone",
        type=int,
        default=1,
        help=(
            "Stop re-crawling a historical listing after this many consecutive "
            "404/410 responses since its last successful profile. A listing that "
            "reappears in the current ID list is still crawled. 0 = never stop."
        ),
    )
    return parser.parse_args()


def create_spark():
    return (
        SparkSession.builder
        .appName("REIP-NhaTot-Profile-Crawler")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_session(pool_size=8):
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=0,
        pool_block=True,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def path_exists(sc, path):
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(sc._jsc.hadoopConfiguration())
    return fs.exists(jpath)


def glob_exists(sc, path):
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(sc._jsc.hadoopConfiguration())
    return bool(fs.globStatus(jpath))


def delete_path(sc, path):
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(sc._jsc.hadoopConfiguration())
    return fs.delete(jpath, True)


def make_detail_envelope(list_id, run_id, crawl_date, http_status, payload, error):
    url = DETAIL_URL.format(list_id)
    return json.dumps(
        {
            "source": "nhatot",
            "source_listing_id": str(list_id),
            "url": url,
            "snapshot_id": run_id,
            "crawl_date": crawl_date,
            "crawl_timestamp": datetime.now(timezone.utc).isoformat(),
            "http_status": http_status,
            "error": error,
            "payload": payload,
        },
        ensure_ascii=False,
    )


def fetch_detail(session, list_id, run_id, crawl_date):
    url = DETAIL_URL.format(list_id)
    last_status = None
    last_error = None

    for attempt in range(3):
        try:
            response = session.get(url, timeout=(5, 20))
            last_status = response.status_code

            if response.status_code == 429:
                last_error = "HTTP_429"
                time.sleep(2 ** (attempt + 1))
                continue

            if response.status_code >= 500:
                last_error = f"HTTP_{response.status_code}"
                time.sleep(2 ** attempt)
                continue

            if 400 <= response.status_code < 500:
                last_error = f"HTTP_{response.status_code}"
                break

            response.raise_for_status()
            return True, make_detail_envelope(
                list_id,
                run_id,
                crawl_date,
                response.status_code,
                response.json(),
                None,
            )
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:300]}"
            if attempt < 2:
                time.sleep(2 ** attempt)

    return False, make_detail_envelope(
        list_id,
        run_id,
        crawl_date,
        last_status,
        None,
        last_error or "DETAIL_FETCH_FAILED",
    )


def _batched(iterator, batch_size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def create_detail_partition_func(
    run_id,
    crawl_date,
    request_delay,
    http_workers_per_partition,
):
    def crawl_partition(ids):
        # IMPORTANT: create threading.local() INSIDE the Spark executor partition.
        # A module-level threading.local object cannot be cloudpickled by PySpark.
        thread_state = threading.local()

        def fetch_one(list_id):
            # Each HTTP worker thread lazily gets its own requests.Session.
            # The session is reused by that thread across multiple listing IDs.
            session = getattr(thread_state, "session", None)
            if session is None:
                session = create_session(pool_size=http_workers_per_partition)
                thread_state.session = session

            result = fetch_detail(session, list_id, run_id, crawl_date)

            if request_delay > 0:
                time.sleep(request_delay)

            return result

        processed = 0
        success = 0
        batch_size = max(64, http_workers_per_partition * 32)

        with ThreadPoolExecutor(max_workers=http_workers_per_partition) as executor:
            for batch in _batched(ids, batch_size):
                future_to_id = {
                    executor.submit(fetch_one, list_id): list_id
                    for list_id in batch
                }

                for future in as_completed(future_to_id):
                    list_id = future_to_id[future]
                    try:
                        ok, envelope = future.result()
                    except Exception as exc:
                        ok = False
                        envelope = make_detail_envelope(
                            list_id,
                            run_id,
                            crawl_date,
                            None,
                            None,
                            f"THREAD_FETCH_ERROR: {type(exc).__name__}: {str(exc)[:300]}",
                        )

                    processed += 1
                    if ok:
                        success += 1

                    yield envelope

                    if processed % 500 == 0:
                        print(
                            f"[PROFILE partition] processed={processed:,} "
                            f"success={success:,} failed={processed - success:,}",
                            flush=True,
                        )

        print(
            f"[PROFILE partition done] processed={processed:,} "
            f"success={success:,} failed={processed - success:,}",
            flush=True,
        )

    return crawl_partition


def read_current_ids(spark, args):
    input_path = (
        f"{ID_LANDING_ROOT}/crawl_date={args.crawl_date}/run_id={args.run_id}"
    )
    data_path = f"{input_path}/part-*"

    if not glob_exists(spark.sparkContext, data_path):
        raise RuntimeError(f"No ID part files found: {data_path}")

    current = (
        spark.read
        .option("mode", "PERMISSIVE")
        .json(data_path)
        .select(
            col("source_listing_id").cast("string").alias("source_listing_id"),
            col("listing_type").cast("string").alias("listing_type"),
        )
        .filter(col("source_listing_id").isNotNull())
        .filter(col("listing_type").isin("s", "u"))
        .dropDuplicates(["source_listing_id"])
    )
    return input_path, current


def read_gone_ids(observations, successful, max_consecutive_gone):
    """IDs whose responses since their last successful profile are all gone
    statuses, at least max_consecutive_gone times (counted per crawl run)."""
    last_success = (
        successful
        .groupBy("source_listing_id")
        .agg(spark_max("crawl_timestamp").alias("last_success_at"))
    )

    return (
        observations
        .filter(col("http_status").isin(*GONE_HTTP_STATUSES))
        .join(last_success, on="source_listing_id", how="inner")
        .filter(col("crawl_timestamp") > col("last_success_at"))
        .groupBy("source_listing_id")
        .agg(countDistinct("snapshot_id").alias("gone_count"))
        .filter(col("gone_count") >= max_consecutive_gone)
        .select("source_listing_id")
    )


def read_historical_ids(spark, args):
    empty = spark.createDataFrame([], "source_listing_id string")
    if not DeltaTable.isDeltaTable(spark, BRONZE_LOCATION):
        empty_typed = spark.createDataFrame([], "source_listing_id string, listing_type string")
        return empty_typed, empty, empty

    bronze = spark.read.format("delta").load(BRONZE_LOCATION)

    observations = (
        bronze
        .filter(col("source") == "nhatot")
        .filter(col("source_listing_id").isNotNull())
        .withColumn("source_listing_id", col("source_listing_id").cast("string"))
    )

    successful = (
        observations
        .filter(col("http_status") == 200)
        .filter(col("payload").isNotNull())
        .filter(col("payload.ad.type").isin("s", "u"))
    )

    known_all = (
        successful
        .select("source_listing_id")
        .dropDuplicates(["source_listing_id"])
    )

    if args.max_consecutive_gone > 0:
        gone = read_gone_ids(observations, successful, args.max_consecutive_gone)
    else:
        gone = empty

    if args.historical_lookback_days > 0:
        cutoff = date_sub(to_date(lit(args.crawl_date)), args.historical_lookback_days)
        successful = successful.filter(to_date(col("crawl_date")) >= cutoff)

    historical = (
        successful
        .select(
            "source_listing_id",
            col("payload.ad.type").cast("string").alias("listing_type"),
        )
        .groupBy("source_listing_id")
        .agg(spark_max("listing_type").alias("listing_type"))
        .join(gone, on="source_listing_id", how="left_anti")
    )

    return historical, known_all, gone


def main():
    args = parse_args()

    if args.detail_partitions < 1:
        raise ValueError("--detail-partitions must be >= 1")
    if args.http_workers_per_partition < 1:
        raise ValueError("--http-workers-per-partition must be >= 1")
    if args.output_partitions < 1:
        raise ValueError("--output-partitions must be >= 1")

    spark = create_spark()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    output = f"{PROFILE_LANDING_ROOT}/crawl_date={args.crawl_date}/run_id={args.run_id}"
    success_path = f"{output}/_SUCCESS"

    try:
        if path_exists(sc, success_path):
            print(f"Profile Landing already complete: {output}", flush=True)
            return

        if path_exists(sc, output):
            print(f"Removing incomplete Profile Landing run: {output}", flush=True)
            delete_path(sc, output)

        id_input_path, current_ids = read_current_ids(spark, args)
        historical_ids, known_all, gone_ids = read_historical_ids(spark, args)

        # ID sets are compact compared with full profile payloads, so MEMORY_AND_DISK
        # avoids repeatedly reading/shuffling Bronze while keeping spill safety.
        current_ids = current_ids.persist(StorageLevel.MEMORY_AND_DISK)
        historical_ids = historical_ids.persist(StorageLevel.MEMORY_AND_DISK)
        known_all = known_all.persist(StorageLevel.MEMORY_AND_DISK)
        gone_ids = gone_ids.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            current_count = current_ids.count()
            if current_count == 0:
                raise RuntimeError("ID stage produced zero current IDs")

            historical_count = historical_ids.count()
            known_count = known_all.count()
            gone_count = gone_ids.count()
            new_id_count = current_ids.join(
                known_all,
                on="source_listing_id",
                how="left_anti",
            ).count()

            # Crawl every ID discovered today PLUS previously successful IDs so that
            # price changes can be tracked across snapshots. Historical IDs that keep
            # returning 404 are dropped, unless they are listed again today.
            targets = (
                current_ids
                .unionByName(historical_ids)
                .groupBy("source_listing_id")
                .agg(spark_max("listing_type").alias("listing_type"))
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            try:
                target_count = targets.count()
                if target_count == 0:
                    raise RuntimeError("No profile targets found")

                historical_only_count = targets.join(
                    current_ids.select("source_listing_id"),
                    on="source_listing_id",
                    how="left_anti",
                ).count()

                print(
                    "Profile target plan: "
                    f"current={current_count:,}, known_before={known_count:,}, "
                    f"new={new_id_count:,}, historical_pool={historical_count:,}, "
                    f"historical_only={historical_only_count:,}, "
                    f"gone_skipped={gone_count:,} "
                    f"(>= {args.max_consecutive_gone} consecutive 404/410), "
                    f"target_total={target_count:,}",
                    flush=True,
                )
                print(
                    "Profile execution plan: "
                    f"spark_partitions={args.detail_partitions}, "
                    f"http_workers_per_partition={args.http_workers_per_partition}, "
                    f"max_http_concurrency~=Spark_active_tasks*{args.http_workers_per_partition}, "
                    f"output_partitions={args.output_partitions}, "
                    f"request_delay={args.request_delay}s",
                    flush=True,
                )

                # targets is produced by a shuffle. Coalesce before converting to RDD
                # avoids the extra RDD repartition/shuffle used by the previous version.
                target_rdd = (
                    targets
                    .select("source_listing_id")
                    .coalesce(args.detail_partitions)
                    .rdd
                    .map(lambda row: row[0])
                )

                detail_results = target_rdd.mapPartitions(
                    create_detail_partition_func(
                        args.run_id,
                        args.crawl_date,
                        args.request_delay,
                        args.http_workers_per_partition,
                    )
                )

                # Phase B audit was intentionally removed. There is no pre-write sum()
                # and no DISK_ONLY cache of large JSON payloads. This is one Spark action:
                # API crawl -> JSON serialization -> MinIO Landing.
                output_partition_count = min(
                    args.output_partitions,
                    args.detail_partitions,
                )
                landing_rdd = detail_results.coalesce(output_partition_count)

                print(
                    f"Crawling profiles and writing Landing in one Spark action: {output}",
                    flush=True,
                )
                started = time.monotonic()
                landing_rdd.saveAsTextFile(output)
                elapsed = time.monotonic() - started

                throughput = target_count / elapsed if elapsed > 0 else 0.0
                print(
                    f"Profile Landing complete: {output}; target={target_count:,}; "
                    f"elapsed={elapsed:.1f}s; avg_target_rate={throughput:.2f} ids/s; "
                    f"ID input={id_input_path}",
                    flush=True,
                )
            finally:
                targets.unpersist()
        finally:
            current_ids.unpersist()
            historical_ids.unpersist()
            known_all.unpersist()
            gone_ids.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
