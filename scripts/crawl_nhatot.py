#!/usr/bin/env python3

import argparse
import json
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from pyspark.sql import SparkSession


BASE_URL = "https://gateway.chotot.com/v1/public/ad-listing"
DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{}"
LANDING_ROOT = "s3a://lakehouse/landing/source=nhatot"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

PAGE_LIMIT = 50
MAX_OFFSET = 19950

AREAS = [
    13096, 13098, 13099, 13100, 13101,
    13102, 13103, 13105, 13106, 13107,
    13108, 13109, 13110, 13111, 13112,
    13113, 13115, 13116, 13117, 13118,
    13119, 13120,
]


def path_exists(sc, path):
    jvm = sc._jvm
    hconf = sc._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hconf)
    return fs.exists(jpath)


def delete_path(sc, path):
    jvm = sc._jvm
    hconf = sc._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hconf)
    fs.delete(jpath, True)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--run-id", required=True)
    parser.add_argument("--crawl-date", required=True)

    parser.add_argument(
        "--target-sale",
        type=int,
        default=35000,
    )

    parser.add_argument(
        "--target-rent",
        type=int,
        default=15000,
    )

    parser.add_argument(
        "--list-partitions",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--detail-partitions",
        type=int,
        default=8,
    )

    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.2,
    )

    return parser.parse_args()


def create_spark():
    return (
        SparkSession.builder
        .appName("REIP-NhaTot-Crawler")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_session():
    session = requests.Session()
    session.headers.update(HEADERS)

    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=0,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def crawl_scope(scope):
    area_v2, listing_type, request_delay = scope
    session = create_session()
    results = []

    for offset in range(0, MAX_OFFSET + 1, PAGE_LIMIT):
        params = {
            "cg": 1000,
            "region_v2": 13000,
            "area_v2": area_v2,
            "o": offset,
            "limit": PAGE_LIMIT,
        }

        response = None

        for attempt in range(3):
            try:
                response = session.get(
                    BASE_URL,
                    params=params,
                    timeout=(5, 20),
                )

                if response.status_code == 400:
                    session.close()
                    return results

                if response.status_code == 429:
                    time.sleep(2 ** (attempt + 1))
                    continue

                if response.status_code >= 500:
                    time.sleep(2 ** attempt)
                    continue

                response.raise_for_status()
                break

            except Exception:
                response = None

                if attempt < 2:
                    time.sleep(2 ** attempt)

        if response is None or not response.ok:
            continue

        try:
            ads = response.json().get("ads", [])
        except Exception:
            continue

        if not ads:
            break

        for ad in ads:
            list_id = ad.get("list_id")

            if list_id is not None and ad.get("type") == listing_type:
                results.append(
                    (
                        listing_type,
                        str(list_id),
                    )
                )

        if len(ads) < PAGE_LIMIT:
            break

        if request_delay > 0:
            time.sleep(request_delay)

    session.close()
    return results


def fetch_detail(
    session,
    list_id,
    run_id,
    crawl_date,
):
    url = DETAIL_URL.format(list_id)

    for attempt in range(3):
        try:
            response = session.get(
                url,
                timeout=(5, 20),
            )

            if response.status_code == 429:
                time.sleep(2 ** (attempt + 1))
                continue

            if response.status_code >= 500:
                time.sleep(2 ** attempt)
                continue

            response.raise_for_status()

            payload = response.json()

            return json.dumps(
                {
                    "source": "nhatot",
                    "source_listing_id": str(
                        list_id
                    ),
                    "url": url,
                    "snapshot_id": run_id,
                    "crawl_date": crawl_date,
                    "crawl_timestamp": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "http_status": response.status_code,
                    "payload": payload,
                },
                ensure_ascii=False,
            )

        except Exception:
            if attempt < 2:
                time.sleep(2 ** attempt)

    return None


def create_detail_partition_func(
    run_id,
    crawl_date,
    request_delay,
):
    def crawl_partition(ids):
        session = create_session()

        processed = 0
        success = 0

        for list_id in ids:
            result = fetch_detail(
                session,
                list_id,
                run_id,
                crawl_date,
            )

            processed += 1

            if result is not None:
                success += 1
                yield result

            if processed % 500 == 0:
                print(
                    f"[DETAIL] processed={processed:,} "
                    f"success={success:,}"
                )

            if request_delay > 0:
                time.sleep(request_delay)

        session.close()

    return crawl_partition


if __name__ == "__main__":
    args = parse_args()

    spark = create_spark()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    scopes = [
        (
            area,
            listing_type,
            args.request_delay,
        )
        for area in AREAS
        for listing_type in ("s", "u")
    ]

    raw_ids = (
        sc.parallelize(
            scopes,
            max(
                args.list_partitions,
                len(scopes),
            ),
        )
        .flatMap(crawl_scope)
        .distinct()
    )

    collected = raw_ids.collect()

    sale_ids = []
    rent_ids = []

    for listing_type, list_id in collected:
        if (
            listing_type == "s"
            and len(sale_ids) < args.target_sale
        ):
            sale_ids.append(list_id)

        elif (
            listing_type == "u"
            and len(rent_ids) < args.target_rent
        ):
            rent_ids.append(list_id)

        if (
            len(sale_ids) >= args.target_sale
            and len(rent_ids) >= args.target_rent
        ):
            break

    selected_ids = list(
        dict.fromkeys(
            sale_ids + rent_ids
        )
    )

    print(
        f"Selected "
        f"sale={len(sale_ids):,}, "
        f"rent={len(rent_ids):,}, "
        f"total={len(selected_ids):,}"
    )

    if not selected_ids:
        raise RuntimeError(
            "No listing IDs found"
        )

    detail_rdd = (
        sc.parallelize(
            selected_ids,
            args.detail_partitions,
        )
        .mapPartitions(
            create_detail_partition_func(
                args.run_id,
                args.crawl_date,
                args.request_delay,
            )
        )
    )

    output = (
        f"{LANDING_ROOT}/"
        f"crawl_date={args.crawl_date}/"
        f"run_id={args.run_id}"
    )

    print(
        f"Writing Landing: {output}"
    )

    success_path = f"{output}/_SUCCESS"

    if path_exists(sc, success_path):
        print(f"Landing already complete: {output}")
        spark.stop()
        raise SystemExit(0)

    if path_exists(sc, output):
        print(f"Removing incomplete Landing run: {output}")
        delete_path(sc, output)

    detail_rdd.saveAsTextFile(
        output
    )

    print(
        f"Landing written: {output}"
    )

    spark.stop()