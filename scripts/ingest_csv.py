#!/usr/bin/env python3
"""CSV file -> Landing, in the same JSON envelope the Nhà Tốt crawler writes.

The rest of the pipeline is unchanged: load_bronze.py reads the run from
Landing and dbt cleans it. Rows with a missing or invalid price, area or
location are not dropped here: dbt sends them to silver.listing_quarantine.

CSV columns (header row, UTF-8, ',' or ';'). Only the first six are needed for
a row to be valid; every other column is optional and may be left empty.
    transaction_type   sale | rent (also: bán, thuê)
    price              4100000000, 4.1 tỷ, 15 triệu
    area_m2            68 or 68,5
    property_type      Căn hộ/Chung cư | Nhà ở | Đất | Văn phòng, Mặt bằng kinh doanh | Phòng trọ
    district           Quận 7, Thành phố Thủ Đức, Huyện Bình Chánh ...
    province           Tp Hồ Chí Minh (TP.HCM, HCM also accepted)
    listing_id, title, description, property_subtype, ward, street, address,
    bedrooms, bathrooms, total_floors, floor_number, latitude, longitude,
    width_m, length_m, living_area_m2, legal_status, project_name,
    house_direction, furnishing, handover_status, property_characteristics
    (comma separated, e.g. "Hẻm xe hơi, Mặt tiền"), is_broker, posted_at
    (YYYY-MM-DD or DD/MM/YYYY), url

listing_id: the Nhà Tốt id when the row is a Nhà Tốt listing (it then updates
that listing). Without it a stable id "csv_<hash>" is derived from the row, so
loading the same file twice does not create duplicates.

Location ids (province_id, district_id, ward_id) are looked up by name in
silver.listing_current; a row whose district is not found keeps its names but
is not used for price training (dbt needs district_id for that).
"""

import argparse
import csv
import hashlib
import io
import json
import os
import re
import unicodedata
from datetime import datetime, timezone

from pyspark.sql import SparkSession

DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{}"
LANDING_ROOT = "s3a://lakehouse/landing/source=nhatot"
LOCATION_TABLE = "silver.listing_current"

TRANSACTION_TYPES = {
    "sale": "s", "s": "s", "bán": "s", "ban": "s", "mua bán": "s", "mua ban": "s",
    "rent": "u", "u": "u", "thuê": "u", "thue": "u", "cho thuê": "u", "cho thue": "u",
}
LOCATION_PREFIXES = (
    "thành phố ", "tp. ", "tp.", "tp ", "tỉnh ", "quận ", "huyện ", "thị xã ",
    "phường ", "xã ", "thị trấn ",
)
LOCATION_ALIASES = {"hcm": "hồ chí minh", "hồ chí minh city": "hồ chí minh", "sài gòn": "hồ chí minh"}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CSV path, e.g. s3a://lakehouse/uploads/x.csv")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--crawl-date", required=True)
    return parser.parse_args()


# ---------------------------------------------------------------- parsing


def text(value):
    value = (value or "").strip()
    return value or None


def number(value):
    value = text(value)
    if value is None:
        return None
    try:
        return float(value.replace(" ", "").replace(",", "."))
    except ValueError:
        return None  # left empty: dbt quarantines the row (MISSING_PRICE / MISSING_AREA)


def integer(value):
    value = number(value)
    return None if value is None else int(value)


def price_vnd(value):
    value = text(value)
    if value is None:
        return None
    lowered = value.lower()
    for unit, factor in (("tỷ", 1e9), ("ty", 1e9), ("triệu", 1e6), ("trieu", 1e6)):
        if unit in lowered:
            amount = number(lowered.replace(unit, ""))
            return None if amount is None else int(round(amount * factor))
    digits = re.sub(r"[\s.,]", "", value)
    return int(digits) if digits.isdigit() else None


def boolean(value):
    value = text(value)
    return None if value is None else value.lower() in ("1", "true", "yes", "có", "co", "x")


def epoch_ms(value):
    value = text(value)
    if value is not None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
            try:
                return int(datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
            except ValueError:
                pass
    return None


def normalize_place(name):
    name = unicodedata.normalize("NFC", (name or "").lower())
    name = re.sub(r"\(.*?\)", " ", name)  # "Phường X (Quận Y cũ)" -> "phường x"
    name = re.sub(r"\s+", " ", name.replace(".", ". ")).strip()
    for prefix in LOCATION_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
            break
    return LOCATION_ALIASES.get(name, name)


# ---------------------------------------------------------------- locations


def load_locations(spark):
    """Name -> (id, canonical name) maps built from the listings already in Silver."""
    try:
        rows = spark.sql(f"""
            select distinct province, province_id, district, district_id, ward, ward_id
            from {LOCATION_TABLE}
            where province_id is not null
        """).collect()
    except Exception as exc:  # first run: Silver does not exist yet
        print(f"Location lookup disabled ({type(exc).__name__}); ids stay empty", flush=True)
        return {}, {}, {}

    provinces, districts, wards = {}, {}, {}
    for r in rows:
        provinces.setdefault(normalize_place(r.province), (r.province_id, r.province))
        if r.district_id is not None:
            districts.setdefault((r.province_id, normalize_place(r.district)), (r.district_id, r.district))
        if r.ward_id is not None and r.district_id is not None:
            wards.setdefault((r.district_id, normalize_place(r.ward)), (r.ward_id, r.ward))
    print(f"Locations known: {len(provinces)} provinces, {len(districts)} districts, {len(wards)} wards", flush=True)
    return provinces, districts, wards


def resolve_location(row, provinces, districts, wards):
    province = provinces.get(normalize_place(row.get("province")))
    district = province and districts.get((province[0], normalize_place(row.get("district"))))
    ward = district and wards.get((district[0], normalize_place(row.get("ward"))))
    return province, district, ward


# ---------------------------------------------------------------- envelope


def param(key, value):
    return None if value is None else {"id": key, "value": value}


def drop_empty(mapping):
    # Missing values are omitted rather than written as null, so Spark never
    # infers a different type for a column Bronze already has.
    return {k: v for k, v in mapping.items() if v is not None}


def make_envelope(row, args, crawled_at, locations):
    ad_type = TRANSACTION_TYPES.get((text(row.get("transaction_type")) or "").lower())
    province, district, ward = resolve_location(row, *locations)
    posted = epoch_ms(row.get("posted_at")) or int(crawled_at.timestamp() * 1000)

    listing_id = text(row.get("listing_id"))
    url = text(row.get("url"))
    if listing_id is None:
        content = json.dumps(row, sort_keys=True, ensure_ascii=False)
        listing_id = "csv_" + hashlib.sha1(content.encode("utf-8")).hexdigest()[:16]
        # No listing page to link to: keep the file name as lineage instead.
        url = url or f"csv://{os.path.basename(args.input)}"

    furnishing = text(row.get("furnishing"))
    ad = drop_empty({
        "subject": text(row.get("title")),
        "body": text(row.get("description")),
        "type": ad_type or text(row.get("transaction_type")),
        "price": price_vnd(row.get("price")),
        "size": number(row.get("area_m2")),
        "category_name": text(row.get("property_type")),
        "rooms": integer(row.get("bedrooms")),
        "toilets": integer(row.get("bathrooms")),
        "floors": integer(row.get("total_floors")),
        "floornumber": integer(row.get("floor_number")),
        "street_name": text(row.get("street")),
        "ward_name": ward[1] if ward else text(row.get("ward")),
        "area_name": district[1] if district else text(row.get("district")),
        "region_name": province[1] if province else text(row.get("province")),
        "ward": ward[0] if ward else None,
        "area_v2": district[0] if district else None,
        "region_v2": province[0] if province else None,
        "latitude": number(row.get("latitude")),
        "longitude": number(row.get("longitude")),
        "pty_project_name": text(row.get("project_name")),
        "width": number(row.get("width_m")),
        "length": number(row.get("length_m")),
        "living_size": number(row.get("living_area_m2")),
        "company_ad": boolean(row.get("is_broker")),
        "list_time": posted,
        "status": "active",
    })
    ad_params = drop_empty({
        "address": param("address", text(row.get("address"))),
        # Staging reads the subtype from whichever of these three is present.
        "house_type": param("house_type", text(row.get("property_subtype"))),
        "property_legal_document": param("property_legal_document", text(row.get("legal_status"))),
        "direction": param("direction", text(row.get("house_direction"))),
        ("furnishing_rent" if ad_type == "u" else "furnishing_sell"): param("furnishing", furnishing),
        "property_status": param("property_status", text(row.get("handover_status"))),
        "pty_characteristics": param("pty_characteristics", text(row.get("property_characteristics"))),
    })

    return json.dumps(
        {
            "source": "nhatot",
            # Marks the row as uploaded, not crawled: dbt never infers that a
            # listing last seen in a CSV was removed (int_listing_removals).
            "ingest_source": "csv",
            "source_listing_id": listing_id,
            "url": url or DETAIL_URL.format(listing_id),
            "snapshot_id": args.run_id,
            "crawl_date": args.crawl_date,
            "crawl_timestamp": crawled_at.isoformat(),
            "http_status": 200,
            "payload": {"ad": ad, "ad_params": ad_params},
        },
        ensure_ascii=False,
    )


def read_csv(spark, path):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    if not jpath.getFileSystem(spark._jsc.hadoopConfiguration()).exists(jpath):
        raise FileNotFoundError(f"CSV not found: {path} (upload it to MinIO first)")
    # One whole file as one string, so quoted values may span several lines.
    content = spark.sparkContext.wholeTextFiles(path).first()[1].lstrip("\ufeff")
    delimiter = ";" if content[:4096].count(";") > content[:4096].count(",") else ","
    reader = csv.DictReader(io.StringIO(content, newline=""), delimiter=delimiter)
    reader.fieldnames = [c.strip().lower() for c in reader.fieldnames or []]
    rows = [r for r in reader if any((v or "").strip() for v in r.values())]
    required = {"transaction_type", "price", "area_m2", "property_type", "district", "province"}
    missing = required - set(reader.fieldnames)
    if missing:
        raise ValueError(f"CSV is missing columns: {sorted(missing)}")
    return rows


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("REIP-Ingest-CSV").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        rows = read_csv(spark, args.input)
        if not rows:
            raise ValueError(f"No data rows in {args.input}")
        print(f"[1/3] Read {len(rows):,} rows from {args.input}", flush=True)

        locations = load_locations(spark)
        crawled_at = datetime.now(timezone.utc)
        lines = [make_envelope(r, args, crawled_at, locations) for r in rows]
        resolved = sum('"area_v2"' in line for line in lines)
        print(f"[2/3] Envelopes built; district id found for {resolved:,}/{len(lines):,} rows", flush=True)

        output = f"{LANDING_ROOT}/crawl_date={args.crawl_date}/run_id={args.run_id}"
        jpath = spark._jvm.org.apache.hadoop.fs.Path(output)
        jpath.getFileSystem(spark._jsc.hadoopConfiguration()).delete(jpath, True)  # Airflow retry
        spark.sparkContext.parallelize(lines, max(1, len(lines) // 20000)).saveAsTextFile(output)
        print(f"[3/3] Landing written: {output}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
