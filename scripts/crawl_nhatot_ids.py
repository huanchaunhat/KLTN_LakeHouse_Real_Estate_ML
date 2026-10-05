#!/usr/bin/env python3

import argparse
import json
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from pyspark import StorageLevel
from pyspark.sql import SparkSession

BASE_URL = "https://gateway.chotot.com/v1/public/ad-listing"
ID_LANDING_ROOT = "s3a://lakehouse/landing/source=nhatot_ids"
HEADERS = {"User-Agent": "Mozilla/5.0"}
PAGE_LIMIT = 50
MAX_OFFSET = 19950
AREAS = [
    13096, 13098, 13099, 13100, 13101,
    13102, 13103, 13105, 13106, 13107,
    13108, 13109, 13110, 13111, 13112,
    13113, 13115, 13116, 13117, 13118,
    13119, 13120,
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--crawl-date", required=True)
    parser.add_argument("--list-partitions", type=int, default=44)
    parser.add_argument("--request-delay", type=float, default=0.15)
    return parser.parse_args()


def create_spark():
    return (
        SparkSession.builder
        .appName("REIP-NhaTot-ID-Crawler")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def path_exists(sc, path):
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(sc._jsc.hadoopConfiguration())
    return fs.exists(jpath)


def delete_path(sc, path):
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(sc._jsc.hadoopConfiguration())
    return fs.delete(jpath, True)


def crawl_scope(scope):
    area_v2, listing_type, request_delay = scope
    session = create_session()
    results = []

    try:
        for offset in range(0, MAX_OFFSET + 1, PAGE_LIMIT):
            params = {
                "cg": 1000,
                "region_v2": 13000,
                "area_v2": area_v2,
                "o": offset,
                "limit": PAGE_LIMIT,
            }

            ads = None
            stop_scope = False

            for attempt in range(3):
                try:
                    response = session.get(BASE_URL, params=params, timeout=(5, 20))

                    # Nhà Tốt uses 400 when the requested offset is outside the available range.
                    if response.status_code == 400:
                        ads = []
                        stop_scope = True
                        break

                    if response.status_code == 429:
                        time.sleep(2 ** (attempt + 1))
                        continue

                    if response.status_code >= 500:
                        time.sleep(2 ** attempt)
                        continue

                    response.raise_for_status()
                    ads = response.json().get("ads", [])
                    break
                except Exception:
                    if attempt < 2:
                        time.sleep(2 ** attempt)

            # Do not silently advance to the next offset when a page could not be read.
            if ads is None:
                break

            if not ads:
                break

            for ad in ads:
                list_id = ad.get("list_id")
                if list_id is not None and ad.get("type") == listing_type:
                    results.append((listing_type, str(list_id)))

            if stop_scope or len(ads) < PAGE_LIMIT:
                break

            if request_delay > 0:
                time.sleep(request_delay)
    finally:
        session.close()

    return results


def main():
    args = parse_args()
    spark = create_spark()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    output = f"{ID_LANDING_ROOT}/crawl_date={args.crawl_date}/run_id={args.run_id}"
    success_path = f"{output}/_SUCCESS"
    audit_success_path = f"{output}/audit/_SUCCESS"

    try:
        if path_exists(sc, success_path) and path_exists(sc, audit_success_path):
            print(f"ID Landing already complete: {output}", flush=True)
            return

        if path_exists(sc, output):
            print(f"Removing incomplete ID Landing run: {output}", flush=True)
            delete_path(sc, output)

        scopes = [
            (area, listing_type, args.request_delay)
            for area in AREAS
            for listing_type in ("s", "u")
        ]

        ids = (
            sc.parallelize(scopes, max(args.list_partitions, len(scopes)))
            .flatMap(crawl_scope)
            .distinct()
            .persist(StorageLevel.DISK_ONLY)
        )

        try:
            total_count = ids.count()
            sale_count = ids.filter(lambda row: row[0] == "s").count()
            rent_count = ids.filter(lambda row: row[0] == "u").count()

            if total_count == 0:
                raise RuntimeError("No listing IDs discovered")

            discovered_at = datetime.now(timezone.utc).isoformat()

            def to_json(row):
                listing_type, list_id = row
                return json.dumps(
                    {
                        "source": "nhatot",
                        "source_listing_id": list_id,
                        "listing_type": listing_type,
                        "crawl_date": args.crawl_date,
                        "run_id": args.run_id,
                        "discovered_at": discovered_at,
                    },
                    ensure_ascii=False,
                )

            print(
                f"Discovered IDs: sale={sale_count:,}, rent={rent_count:,}, total={total_count:,}",
                flush=True,
            )
            print(f"Writing ID Landing: {output}", flush=True)
            ids.map(to_json).saveAsTextFile(output)

            audit = {
                "source": "nhatot",
                "run_id": args.run_id,
                "crawl_date": args.crawl_date,
                "discovered_at": discovered_at,
                "sale_count": sale_count,
                "rent_count": rent_count,
                "total_count": total_count,
                "list_partitions": args.list_partitions,
                "request_delay": args.request_delay,
                "output_path": output,
            }
            sc.parallelize([json.dumps(audit, ensure_ascii=False)], 1).saveAsTextFile(
                f"{output}/audit"
            )

            print(f"ID Landing written: {output}", flush=True)
        finally:
            ids.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
