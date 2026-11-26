#!/usr/bin/env python3
"""Generate the Superset dashboards as an import bundle.

    python superset/build_dashboards.py            # writes superset/dashboards/ + dashboards.zip
    bash superset/import_dashboards.sh             # builds, then imports into the running Superset

Everything (datasets = Spark SQL over Gold, charts, layout, filters) is declared
here, so the dashboards can be rebuilt on a fresh Superset with one command
instead of being clicked together by hand. UUIDs are derived from the names,
so re-importing updates the existing objects in place.

Five dashboards follow the five parts of the data-exploration chapter:
1 "Tổng quan thị trường"      what is on the market, where (incl. a map).
2 "Mặt bằng giá"              price distributions, by type, district, ward, legal status, area.
3 "Phân khúc và cơ cấu"       price segments by type and by district.
4 "Biến động theo thời gian"  supply and price by POSTING week, supply vs price.
5 "Chuyên sâu theo loại hình" what is specific to houses, apartments, land.
Dashboard "Model": test-set error of the latest price model.
"""

import json
import os
import shutil
import uuid
import zipfile
from datetime import datetime, timezone

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "dashboards")
ZIP_PATH = os.path.join(HERE, "dashboards.zip")

# The connection already configured in Superset (same uuid, so it is reused).
DATABASE = {
    "database_name": "Lakehouse (Spark Thrift)",
    "sqlalchemy_uri": "hive://spark@spark-thrift-server:10000/gold?auth=NOSASL",
    "uuid": "b647a080-f246-4101-b80e-4444cad77f69",
}
NAMESPACE = uuid.UUID("5f0d2b6e-6c2a-4f0e-9b0a-000000000000")


def uid(kind, name):
    return str(uuid.uuid5(NAMESPACE, f"{kind}:{name}"))


# ---------------------------------------------------------------------------
# Datasets: virtual tables over Gold. Column names are Vietnamese and shared
# across datasets (loai_bds, quan, khu_vuc, giao_dich) so one dashboard filter
# applies to every chart whose dataset has that column.
# ---------------------------------------------------------------------------
TYPES = "property_category IN ('HOUSE', 'APARTMENT', 'LAND')"

# Time axis by POSTING date, not crawl date. Listings posted long before the
# first crawl were mostly gone (sold, expired, rent ads removed within ~2 weeks)
# by the time crawling began, so those weeks look empty. Keep only full weeks
# that start at most 14 days before the first crawl and end before the last
# day Gold has data for (Gold can lag Silver until dbt runs again).
_WEEK = "cast(date_trunc('week', {col}) AS date)"
POSTED_SINCE = (_WEEK + " >= date_sub((SELECT to_date(min(first_seen_at)) FROM gold.fct_listing), 14) "
                "AND date_add(" + _WEEK + ", 6) < (SELECT to_date(max(last_seen_at)) FROM gold.fct_listing)")

DATASETS = {
    # ---- Tổng quan -------------------------------------------------------
    "ov_kpi": dict(
        description="Các con số đầu dashboard tổng quan, một dòng.",
        sql=f"""
SELECT
    (SELECT count(*) FROM gold.fct_listing
      WHERE is_market_eligible AND transaction_type = 'sale' AND is_active)      AS tin_ban_dang_rao,
    (SELECT count(*) FROM gold.fct_listing
      WHERE is_market_eligible AND transaction_type = 'rent' AND is_active)      AS tin_thue_dang_rao,
    (SELECT count(*) FROM gold.fct_listing_event
      WHERE event_type = 'NEW' AND transaction_type = 'sale' AND is_market_eligible
        AND NOT is_initial_load
        AND event_at >= date_sub((SELECT max(event_at) FROM gold.fct_listing_event), 7)) AS tin_ban_moi_7_ngay,
    (SELECT round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)
       FROM gold.ml_price_features WHERE property_category = 'HOUSE')            AS median_gia_m2_nha_trieu,
    (SELECT round(percentile_approx(price_vnd, 0.5) / 1e9, 2)
       FROM gold.ml_price_features WHERE property_category = 'HOUSE')            AS median_gia_nha_ty,
    (SELECT count(*) FROM gold.ml_price_features)                                AS tin_ban_sach,
    (SELECT count(*) FROM silver.int_crawl_snapshots)                            AS so_lan_crawl,
    (SELECT to_date(min(snapshot_started_at)) FROM silver.int_crawl_snapshots)   AS tu_ngay,
    (SELECT to_date(max(snapshot_started_at)) FROM silver.int_crawl_snapshots)   AS den_ngay
""",
        columns={"tin_ban_dang_rao": "BIGINT", "tin_thue_dang_rao": "BIGINT", "tin_ban_moi_7_ngay": "BIGINT",
                 "median_gia_m2_nha_trieu": "DOUBLE", "median_gia_nha_ty": "DOUBLE",
                 "tin_ban_sach": "BIGINT", "so_lan_crawl": "BIGINT",
                 "tu_ngay": "DATE", "den_ngay": "DATE"},
    ),
    "ov_tin_theo_loai_quan": dict(
        description="Số tin (mọi tin hợp lệ từng thu thập) theo loại hình, bán/thuê, quận.",
        sql="""
SELECT
    pt.property_type                                                   AS loai_bds,
    CASE f.transaction_type WHEN 'sale' THEN 'Bán' ELSE 'Cho thuê' END AS giao_dich,
    loc.district                                                       AS quan,
    loc.area_zone                                                      AS khu_vuc,
    count(*)                                                           AS so_tin,
    sum(CASE WHEN f.is_active THEN 1 ELSE 0 END)                       AS so_tin_dang_rao
FROM gold.fct_listing f
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
JOIN gold.dim_location loc     ON f.location_key = loc.location_key
WHERE f.is_market_eligible
GROUP BY 1, 2, 3, 4
""",
        columns={"loai_bds": "STRING", "giao_dich": "STRING", "quan": "STRING", "khu_vuc": "STRING",
                 "so_tin": "BIGINT", "so_tin_dang_rao": "BIGINT"},
    ),
    "ov_gia_ban_theo_quan": dict(
        description="Giá bán trung vị theo quận và loại hình (tin bán đã làm sạch, gold.ml_price_features).",
        sql=f"""
SELECT
    district                                              AS quan,
    area_zone                                             AS khu_vuc,
    property_type                                         AS loai_bds,
    count(*)                                              AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)  AS median_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)     AS median_gia_ty,
    round(percentile_approx(area_m2, 0.5), 0)             AS median_dien_tich_m2
FROM gold.ml_price_features
WHERE {TYPES}
GROUP BY 1, 2, 3
HAVING count(*) >= 30
""",
        columns={"quan": "STRING", "khu_vuc": "STRING", "loai_bds": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE", "median_gia_ty": "DOUBLE", "median_dien_tich_m2": "DOUBLE"},
    ),
    "ov_gia_thue_theo_quan": dict(
        description="Giá thuê trung vị (triệu đồng/tháng) theo quận và loại hình.",
        sql="""
SELECT
    loc.district                                                  AS quan,
    loc.area_zone                                                 AS khu_vuc,
    pt.property_type                                              AS loai_bds,
    count(*)                                                      AS so_tin,
    round(percentile_approx(f.current_price_vnd, 0.5) / 1e6, 1)   AS median_gia_thue_trieu_thang
FROM gold.fct_listing f
JOIN gold.dim_location loc     ON f.location_key = loc.location_key
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
WHERE f.transaction_type = 'rent'
  AND f.is_market_eligible
  AND NOT f.is_price_outlier
  AND pt.property_category IN ('HOUSE', 'APARTMENT', 'ROOM')
  AND loc.district <> 'Không rõ'
GROUP BY 1, 2, 3
HAVING count(*) >= 30
""",
        columns={"quan": "STRING", "khu_vuc": "STRING", "loai_bds": "STRING", "so_tin": "BIGINT",
                 "median_gia_thue_trieu_thang": "DOUBLE"},
    ),
    "ov_ban_do_phuong": dict(
        description="Tâm (trung bình toạ độ) của từng phường với số tin bán sạch và giá/m² trung vị, cho bản đồ.",
        sql=f"""
WITH t AS (
    SELECT ward_id, ward, district, area_zone, property_type, latitude, longitude, price_per_m2
    FROM gold.ml_price_features
    WHERE {TYPES} AND latitude BETWEEN 10.3 AND 11.2 AND longitude BETWEEN 106.3 AND 107.1
),
c AS (SELECT ward_id, round(avg(latitude), 5) AS lat, round(avg(longitude), 5) AS lon FROM t GROUP BY ward_id)
SELECT
    t.ward                                                 AS phuong,
    t.district                                             AS quan,
    t.area_zone                                            AS khu_vuc,
    t.property_type                                        AS loai_bds,
    c.lat, c.lon,
    count(*)                                               AS so_tin,
    round(percentile_approx(t.price_per_m2, 0.5) / 1e6, 1) AS median_gia_m2_trieu
FROM t JOIN c ON t.ward_id = c.ward_id
GROUP BY 1, 2, 3, 4, 5, 6
""",
        columns={"phuong": "STRING", "quan": "STRING", "khu_vuc": "STRING", "loai_bds": "STRING",
                 "lat": "DOUBLE", "lon": "DOUBLE", "so_tin": "BIGINT", "median_gia_m2_trieu": "DOUBLE"},
    ),
    "ov_phan_bo": dict(
        description="Từng tin bán đã làm sạch, để vẽ phân bố giá và diện tích.",
        sql=f"""
SELECT
    property_type                   AS loai_bds,
    district                        AS quan,
    area_zone                       AS khu_vuc,
    round(price_vnd / 1e9, 2)       AS gia_ty,
    round(price_per_m2 / 1e6, 1)    AS gia_m2_trieu,
    area_m2                         AS dien_tich_m2
FROM gold.ml_price_features
WHERE {TYPES}
""",
        columns={"loai_bds": "STRING", "quan": "STRING", "khu_vuc": "STRING", "gia_ty": "DOUBLE",
                 "gia_m2_trieu": "DOUBLE", "dien_tich_m2": "DOUBLE"},
    ),
    # ---- Phân tích: yếu tố ảnh hưởng giá -----------------------------------
    "pt_yeu_to": dict(
        description="eta²: phần phương sai của log(giá/m²) mà mỗi yếu tố tự nó giải thích được.",
        sql=f"""
WITH base AS (
    SELECT
        property_type,
        ln(price_per_m2)                                                      AS y,
        district,
        ward,
        coalesce(project_name, 'Không thuộc dự án')                           AS project,
        property_subtype,
        legal_category,
        coalesce(house_direction, 'Không rõ')                                 AS house_direction,
        coalesce(furnishing, 'Không rõ')                                      AS furnishing,
        CASE WHEN is_broker THEN 'Môi giới' ELSE 'Cá nhân' END                AS seller,
        cast(is_frontage AS string)                                           AS is_frontage,
        cast(has_car_alley AS string)                                         AS has_car_alley,
        CASE WHEN area_m2 < 30 THEN '1' WHEN area_m2 < 50 THEN '2' WHEN area_m2 < 80 THEN '3'
             WHEN area_m2 < 120 THEN '4' WHEN area_m2 < 200 THEN '5' ELSE '6' END AS area_bin,
        coalesce(cast(least(total_floors, 6) AS string), 'Không rõ')          AS floors_bin,
        coalesce(cast(least(bedrooms, 6) AS string), 'Không rõ')              AS bedrooms_bin,
        CASE WHEN width_m IS NULL THEN 'Không rõ' WHEN width_m < 4 THEN '1'
             WHEN width_m < 5 THEN '2' WHEN width_m < 7 THEN '3' ELSE '4' END  AS width_bin,
        CASE
            WHEN latitude IS NULL
              OR NOT (latitude BETWEEN 10.3 AND 11.2 AND longitude BETWEEN 106.3 AND 107.1)
                THEN 'Không rõ'
            ELSE cast(least(floor(
                6371 * 2 * asin(sqrt(
                    pow(sin(radians(latitude - 10.7725) / 2), 2)
                  + cos(radians(latitude)) * cos(radians(10.7725))
                  * pow(sin(radians(longitude - 106.6980) / 2), 2)
                )) / 2), 15) AS string)
        END                                                                   AS distance_bin
    FROM gold.ml_price_features
    WHERE {TYPES}
),
long AS (
    SELECT property_type, y, yeu_to, muc
    FROM base
    LATERAL VIEW stack(15,
        'Quận/huyện', district, 'Phường/xã', ward, 'Dự án', project,
        'Loại hình chi tiết', property_subtype, 'Pháp lý', legal_category,
        'Hướng nhà', house_direction, 'Nội thất', furnishing, 'Người đăng', seller,
        'Mặt tiền', is_frontage, 'Hẻm xe hơi', has_car_alley,
        'Diện tích (nhóm)', area_bin, 'Số tầng (nhóm)', floors_bin,
        'Phòng ngủ (nhóm)', bedrooms_bin, 'Chiều ngang (nhóm)', width_bin,
        'Khoảng cách TT (2km)', distance_bin
    ) t AS yeu_to, muc
),
level_sizes AS (
    SELECT property_type, yeu_to, muc, count(*) AS n
    FROM long GROUP BY property_type, yeu_to, muc
),
grouped AS (
    SELECT l.property_type, l.yeu_to,
           CASE WHEN s.n >= 30 THEN l.muc ELSE 'Khác' END AS muc,
           count(*) AS n, avg(l.y) AS group_mean
    FROM long l
    JOIN level_sizes s ON l.property_type = s.property_type AND l.yeu_to = s.yeu_to AND l.muc <=> s.muc
    GROUP BY 1, 2, 3
),
totals AS (
    SELECT property_type, avg(y) AS grand_mean, var_pop(y) * count(*) AS ss_total
    FROM base GROUP BY property_type
)
SELECT
    g.property_type                                                              AS loai_bds,
    g.yeu_to,
    count(*)                                                                     AS so_muc,
    round(sum(g.n * pow(g.group_mean - t.grand_mean, 2)) / t.ss_total * 100, 1)  AS eta2_pct
FROM grouped g
JOIN totals t ON g.property_type = t.property_type
GROUP BY g.property_type, g.yeu_to, t.ss_total
""",
        columns={"loai_bds": "STRING", "yeu_to": "STRING", "so_muc": "BIGINT", "eta2_pct": "DOUBLE"},
    ),
    "pt_khoang_cach": dict(
        description="Giá/m² theo khoảng cách tới trung tâm (chợ Bến Thành), bước 2 km.",
        sql=f"""
WITH dist AS (
    SELECT property_type, price_per_m2,
        6371 * 2 * asin(sqrt(
            pow(sin(radians(latitude - 10.7725) / 2), 2)
          + cos(radians(latitude)) * cos(radians(10.7725))
          * pow(sin(radians(longitude - 106.6980) / 2), 2))) AS km
    FROM gold.ml_price_features
    WHERE {TYPES}
      AND latitude BETWEEN 10.3 AND 11.2 AND longitude BETWEEN 106.3 AND 107.1
)
SELECT
    cast(least(floor(km / 2) * 2, 30) AS int)                  AS khoang_cach_km,
    property_type                                              AS loai_bds,
    count(*)                                                   AS so_tin,
    round(percentile_approx(price_per_m2, 0.25) / 1e6, 1)      AS p25_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)       AS median_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.75) / 1e6, 1)      AS p75_gia_m2_trieu
FROM dist
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"khoang_cach_km": "BIGINT", "loai_bds": "STRING", "so_tin": "BIGINT",
                 "p25_gia_m2_trieu": "DOUBLE", "median_gia_m2_trieu": "DOUBLE", "p75_gia_m2_trieu": "DOUBLE"},
    ),
    "pt_dien_tich": dict(
        description="Giá/m² trung vị theo nhóm diện tích, loại hình và khu vực.",
        sql=f"""
SELECT
    CASE WHEN area_m2 < 30 THEN '1. < 30 m²' WHEN area_m2 < 50 THEN '2. 30–50 m²'
         WHEN area_m2 < 80 THEN '3. 50–80 m²' WHEN area_m2 < 120 THEN '4. 80–120 m²'
         WHEN area_m2 < 200 THEN '5. 120–200 m²' ELSE '6. ≥ 200 m²' END  AS nhom_dien_tich,
    property_type                                                       AS loai_bds,
    count(*)                                                            AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                AS median_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)                   AS median_gia_ty
FROM gold.ml_price_features
WHERE {TYPES}
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"nhom_dien_tich": "STRING", "loai_bds": "STRING",
                 "so_tin": "BIGINT", "median_gia_m2_trieu": "DOUBLE", "median_gia_ty": "DOUBLE"},
    ),
    "pt_dac_diem": dict(
        description="Nhà ở: chênh lệch giá/m² của nhà có một đặc điểm so với nhà không có, thô và sau khi so cùng quận cùng nhóm diện tích.",
        sql="""
WITH flags AS (
    SELECT district,
        CASE WHEN area_m2 < 30 THEN 1 WHEN area_m2 < 50 THEN 2 WHEN area_m2 < 80 THEN 3
             WHEN area_m2 < 120 THEN 4 WHEN area_m2 < 200 THEN 5 ELSE 6 END AS nhom_dt,
        price_per_m2, dac_diem, co
    FROM gold.ml_price_features
    LATERAL VIEW stack(7,
        'Mặt tiền', is_frontage, 'Hẻm xe hơi', has_car_alley, 'Nở hậu', is_wide_back,
        'Thuộc dự án', is_in_project, 'Nhà nát / xuống cấp', is_dilapidated,
        'Dính quy hoạch', is_planning_affected,
        'Có sổ', CASE WHEN legal_category <> 'Không rõ' THEN legal_category = 'Có sổ' END
    ) t AS dac_diem, co
    WHERE property_category = 'HOUSE' AND co IS NOT NULL
),
raw AS (
    SELECT dac_diem, count(CASE WHEN co THEN 1 END) AS so_tin_co,
        percentile_approx(CASE WHEN co THEN price_per_m2 END, 0.5)
          / percentile_approx(CASE WHEN NOT co THEN price_per_m2 END, 0.5) - 1 AS tho
    FROM flags GROUP BY dac_diem
),
cells AS (
    SELECT dac_diem, count(CASE WHEN co THEN 1 END) AS n_co,
        percentile_approx(CASE WHEN co THEN price_per_m2 END, 0.5)
          / percentile_approx(CASE WHEN NOT co THEN price_per_m2 END, 0.5) - 1 AS chenh
    FROM flags GROUP BY dac_diem, district, nhom_dt
    HAVING count(CASE WHEN co THEN 1 END) >= 5 AND count(CASE WHEN NOT co THEN 1 END) >= 5
),
controlled AS (
    SELECT dac_diem, sum(chenh * n_co) / sum(n_co) AS cung_quan_dt FROM cells GROUP BY dac_diem
)
SELECT r.dac_diem, r.so_tin_co,
       round(r.tho * 100, 1)            AS chenh_lech_tho_pct,
       round(c.cung_quan_dt * 100, 1)   AS chenh_lech_cung_quan_dt_pct
FROM raw r JOIN controlled c ON r.dac_diem = c.dac_diem
WHERE r.so_tin_co >= 30
""",
        columns={"dac_diem": "STRING", "so_tin_co": "BIGINT", "chenh_lech_tho_pct": "DOUBLE",
                 "chenh_lech_cung_quan_dt_pct": "DOUBLE"},
    ),
    "pt_so_tang": dict(
        description="Nhà ở: giá bán trung vị theo số tầng, trong từng nhóm diện tích và khu vực.",
        sql="""
SELECT
    least(total_floors, 6)                                            AS so_tang,
    CASE WHEN area_m2 < 50 THEN '1. < 50 m²' WHEN area_m2 < 80 THEN '2. 50–80 m²'
         WHEN area_m2 < 120 THEN '3. 80–120 m²' ELSE '4. ≥ 120 m²' END AS nhom_dien_tich,
    count(*)                                                          AS so_tin,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)                 AS median_gia_ty
FROM gold.ml_price_features
WHERE property_category = 'HOUSE' AND total_floors >= 1
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"so_tang": "BIGINT", "nhom_dien_tich": "STRING",
                 "so_tin": "BIGINT", "median_gia_ty": "DOUBLE"},
    ),
    "pt_phap_ly": dict(
        description="Giá/m² trung vị theo tình trạng pháp lý và loại hình.",
        sql=f"""
SELECT
    property_type                                         AS loai_bds,
    legal_category                                        AS phap_ly,
    count(*)                                              AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)  AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE {TYPES}
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"loai_bds": "STRING", "phap_ly": "STRING", "so_tin": "BIGINT", "median_gia_m2_trieu": "DOUBLE"},
    ),
    "pt_huong": dict(
        description="Giá/m² trung vị theo hướng nhà và loại hình.",
        sql=f"""
SELECT
    property_type                                         AS loai_bds,
    house_direction                                       AS huong,
    count(*)                                              AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)  AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE {TYPES} AND house_direction IS NOT NULL
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"loai_bds": "STRING", "huong": "STRING", "so_tin": "BIGINT", "median_gia_m2_trieu": "DOUBLE"},
    ),
    "pt_nha_chieu_ngang": dict(
        description="Nhà ở: giá/m² trung vị theo chiều ngang (mặt tiền lô).",
        sql="""
SELECT
    CASE WHEN width_m < 3.5 THEN '1. < 3,5 m' WHEN width_m < 4 THEN '2. 3,5–4 m' WHEN width_m < 5 THEN '3. 4–5 m'
         WHEN width_m < 7 THEN '4. 5–7 m' ELSE '5. ≥ 7 m' END               AS chieu_ngang,
    count(*)                                                             AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                 AS median_gia_m2_trieu,
    round(percentile_approx(area_m2, 0.5), 0)                            AS median_dien_tich_m2
FROM gold.ml_price_features
WHERE property_category = 'HOUSE' AND width_m IS NOT NULL
GROUP BY 1
HAVING count(*) >= 30
""",
        columns={"chieu_ngang": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE", "median_dien_tich_m2": "DOUBLE"},
    ),
    "pt_can_ho_phong_ngu": dict(
        description="Căn hộ: giá bán và giá/m² trung vị theo số phòng ngủ.",
        sql="""
SELECT
    cast(least(bedrooms, 4) AS int)                        AS so_phong_ngu,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)      AS median_gia_ty,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu,
    round(percentile_approx(area_m2, 0.5), 0)              AS median_dien_tich_m2
FROM gold.ml_price_features
WHERE property_category = 'APARTMENT' AND bedrooms >= 1
GROUP BY 1
HAVING count(*) >= 30
""",
        columns={"so_phong_ngu": "BIGINT", "so_tin": "BIGINT", "median_gia_ty": "DOUBLE",
                 "median_gia_m2_trieu": "DOUBLE", "median_dien_tich_m2": "DOUBLE"},
    ),
    "pt_can_ho_noi_that": dict(
        description="Căn hộ: giá/m² trung vị theo tình trạng nội thất và bàn giao.",
        sql="""
SELECT
    coalesce(furnishing, 'Không rõ')                       AS noi_that,
    coalesce(handover_status, 'Không rõ')                  AS ban_giao,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category = 'APARTMENT'
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"noi_that": "STRING", "ban_giao": "STRING", "so_tin": "BIGINT", "median_gia_m2_trieu": "DOUBLE"},
    ),
    "pt_du_an": dict(
        description="Căn hộ: giá/m² trung vị của từng dự án có từ 30 tin.",
        sql="""
SELECT
    concat(project_name, ' (', district, ')')              AS du_an,
    district                                               AS quan,
    area_zone                                              AS khu_vuc,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)      AS median_gia_ty
FROM gold.ml_price_features
WHERE property_category = 'APARTMENT' AND project_name IS NOT NULL
GROUP BY 1, 2, 3
HAVING count(*) >= 30
""",
        columns={"du_an": "STRING", "quan": "STRING", "khu_vuc": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE", "median_gia_ty": "DOUBLE"},
    ),
    "pt_dat": dict(
        description="Đất: giá/m² trung vị theo loại đất, nhóm diện tích và khu vực.",
        sql="""
SELECT
    property_subtype                                       AS loai_dat,
    CASE WHEN area_m2 < 60 THEN '1. < 60 m²' WHEN area_m2 < 100 THEN '2. 60–100 m²'
         WHEN area_m2 < 200 THEN '3. 100–200 m²' WHEN area_m2 < 500 THEN '4. 200–500 m²'
         ELSE '5. ≥ 500 m²' END                            AS nhom_dien_tich,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category = 'LAND'
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"loai_dat": "STRING", "nhom_dien_tich": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE"},
    ),

    # ---- Phân tích: vị trí chi tiết ----------------------------------------
    "pt_phuong": dict(
        description="Giá/m² trung vị theo phường (từ 30 tin), kèm quận để lọc.",
        sql=f"""
SELECT
    concat(ward, ' (', district, ')')                      AS phuong,
    district                                               AS quan,
    area_zone                                              AS khu_vuc,
    property_type                                          AS loai_bds,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE {TYPES} AND ward IS NOT NULL AND ward <> 'Không rõ'
GROUP BY 1, 2, 3, 4
HAVING count(*) >= 30
""",
        columns={"phuong": "STRING", "quan": "STRING", "khu_vuc": "STRING", "loai_bds": "STRING",
                 "so_tin": "BIGINT", "median_gia_m2_trieu": "DOUBLE"},
    ),
    "pt_quan_dien_tich": dict(
        description="Giá/m² trung vị theo quận × nhóm diện tích, từng loại hình.",
        sql=f"""
SELECT
    district                                               AS quan,
    property_type                                          AS loai_bds,
    CASE WHEN area_m2 < 50 THEN '1. < 50 m²' WHEN area_m2 < 80 THEN '2. 50–80 m²'
         WHEN area_m2 < 120 THEN '3. 80–120 m²' ELSE '4. ≥ 120 m²' END AS nhom_dien_tich,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE {TYPES} AND district <> 'Không rõ'
GROUP BY 1, 2, 3
HAVING count(*) >= 20
""",
        columns={"quan": "STRING", "loai_bds": "STRING", "nhom_dien_tich": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE"},
    ),

    # ---- Phân tích: phân khúc giá ------------------------------------------
    "tk_phan_khuc": dict(
        description="Số tin bán theo phân khúc giá, loại hình và khu vực.",
        sql=f"""
SELECT
    CASE WHEN price_vnd < 2e9 THEN '1. < 2 tỷ' WHEN price_vnd < 4e9 THEN '2. 2–4 tỷ'
         WHEN price_vnd < 7e9 THEN '3. 4–7 tỷ' WHEN price_vnd < 12e9 THEN '4. 7–12 tỷ'
         WHEN price_vnd < 25e9 THEN '5. 12–25 tỷ' ELSE '6. ≥ 25 tỷ' END AS phan_khuc,
    property_type                                          AS loai_bds,
    district                                               AS quan,
    area_zone                                              AS khu_vuc,
    count(*)                                               AS so_tin,
    round(percentile_approx(area_m2, 0.5), 0)              AS median_dien_tich_m2
FROM gold.ml_price_features
WHERE {TYPES}
GROUP BY 1, 2, 3, 4
""",
        columns={"phan_khuc": "STRING", "loai_bds": "STRING", "quan": "STRING", "khu_vuc": "STRING",
                 "so_tin": "BIGINT", "median_dien_tich_m2": "DOUBLE"},
    ),

    # ---- Xu hướng theo ngày đăng tin ----------------------------------------
    "xh_tin_dang_tuan": dict(
        description="Số tin theo tuần ĐĂNG tin (posted_at), loại hình, bán/thuê, quận. Chỉ các tuần trọn (POSTED_SINCE).",
        sql=f"""
SELECT
    cast(date_trunc('week', f.posted_at) AS date)                      AS tuan_dang,
    pt.property_type                                                   AS loai_bds,
    CASE f.transaction_type WHEN 'sale' THEN 'Bán' ELSE 'Cho thuê' END AS giao_dich,
    loc.district                                                       AS quan,
    loc.area_zone                                                      AS khu_vuc,
    count(*)                                                           AS so_tin
FROM gold.fct_listing f
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
JOIN gold.dim_location loc     ON f.location_key = loc.location_key
WHERE f.is_market_eligible AND {POSTED_SINCE.format(col="f.posted_at")}
GROUP BY 1, 2, 3, 4, 5
""",
        columns={"tuan_dang": "DATE", "loai_bds": "STRING", "giao_dich": "STRING", "quan": "STRING",
                 "khu_vuc": "STRING", "so_tin": "BIGINT"},
        dttm="tuan_dang",
    ),
    "xh_gia_tuan_dang": dict(
        description="Giá/m² trung vị theo tuần ĐĂNG tin và loại hình (tin bán đã làm sạch), toàn TP.",
        sql=f"""
SELECT
    cast(date_trunc('week', posted_date) AS date)          AS tuan_dang,
    property_type                                          AS loai_bds,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)      AS median_gia_ty
FROM gold.ml_price_features
WHERE {TYPES} AND {POSTED_SINCE.format(col="posted_date")}
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"tuan_dang": "DATE", "loai_bds": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE", "median_gia_ty": "DOUBLE"},
        dttm="tuan_dang",
    ),
    "xh_gia_loai": dict(
        description="Mặt bằng giá/m² của từng loại hình: P25, trung vị, P75 (tin bán đã làm sạch).",
        sql=f"""
SELECT
    property_type                                          AS loai_bds,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_per_m2, 0.25) / 1e6, 1)  AS p25_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.75) / 1e6, 1)  AS p75_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)      AS median_gia_ty
FROM gold.ml_price_features
WHERE {TYPES}
GROUP BY 1
""",
        columns={"loai_bds": "STRING", "so_tin": "BIGINT", "p25_gia_m2_trieu": "DOUBLE",
                 "median_gia_m2_trieu": "DOUBLE", "p75_gia_m2_trieu": "DOUBLE", "median_gia_ty": "DOUBLE"},
    ),

    # ---- Mô hình dự đoán giá -----------------------------------------------
    "mh_sai_so": dict(
        description="Sai số của phiên bản model mới nhất trên tập test (gold.ml_price_predictions), từng tin.",
        sql="""
SELECT
    p.listing_key,
    p.posted_date                          AS ngay_dang,
    cast(date_trunc('week', p.posted_date) AS date) AS tuan_dang,
    CASE WHEN p.price_vnd < 2e9 THEN '1. < 2 tỷ' WHEN p.price_vnd < 4e9 THEN '2. 2–4 tỷ'
         WHEN p.price_vnd < 7e9 THEN '3. 4–7 tỷ' WHEN p.price_vnd < 12e9 THEN '4. 7–12 tỷ'
         WHEN p.price_vnd < 25e9 THEN '5. 12–25 tỷ' ELSE '6. ≥ 25 tỷ' END AS phan_khuc,
    p.district                             AS quan,
    p.property_type                        AS loai_bds,
    round(p.area_m2, 1)                    AS dien_tich_m2,
    round(p.price_vnd / 1e9, 3)            AS gia_ty,
    round(p.predicted_price_vnd / 1e9, 3)  AS gia_du_doan_ty,
    round(p.abs_pct_error * 100, 2)        AS sai_so_pct,
    CASE WHEN p.predicted_price_vnd > p.price_vnd THEN 'Dự đoán cao hơn' ELSE 'Dự đoán thấp hơn' END AS chieu_sai,
    CASE WHEN p.abs_pct_error <= 0.10 THEN 1 ELSE 0 END AS trong_10pct,
    CASE WHEN p.abs_pct_error <= 0.20 THEN 1 ELSE 0 END AS trong_20pct,
    cast(p.model_version AS int)           AS phien_ban
FROM gold.ml_price_predictions p
WHERE p.split = 'test'
  AND cast(p.model_version AS int) = (SELECT max(cast(model_version AS int)) FROM gold.ml_price_predictions)
""",
        columns={"listing_key": "STRING", "ngay_dang": "DATE", "tuan_dang": "DATE", "phan_khuc": "STRING",
                 "quan": "STRING", "loai_bds": "STRING",
                 "dien_tich_m2": "DOUBLE", "gia_ty": "DOUBLE", "gia_du_doan_ty": "DOUBLE", "sai_so_pct": "DOUBLE",
                 "chieu_sai": "STRING", "trong_10pct": "BIGINT", "trong_20pct": "BIGINT", "phien_ban": "BIGINT"},
        dttm="ngay_dang",
    ),
}


# ---------------------------------------------------------------------------
# Chart helpers. Metrics are SQL expressions; filters are simple column tests.
# ---------------------------------------------------------------------------
def metric(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label,
            "hasCustomLabel": True, "optionName": "metric_" + uid("m", sql + label)[:8]}


def where(sql):
    return {"expressionType": "SQL", "sqlExpression": sql, "clause": "WHERE",
            "subject": None, "operator": None, "comparator": None,
            "filterOptionName": "filter_" + uid("f", sql)[:8]}


TIME_COLS = {"tuan", "tuan_dang", "khoang_cach_km", "so_tang", "so_phong_ngu"}
COMMON = {"color_scheme": "supersetColors", "extra_form_data": {}, "dashboards": []}


def big_number(name, dataset, sql, label, subheader, fmt="SMART_NUMBER", **extra):
    return dict(name=name, dataset=dataset, viz_type="big_number_total", params={
        **COMMON, "metric": metric(sql, label), "adhoc_filters": [], "subheader": subheader,
        "header_font_size": 0.4, "subheader_font_size": 0.125, "y_axis_format": fmt,
        "time_format": "smart_date", **extra})


def bar(name, dataset, x, metrics, series=None, filters=(), horizontal=False, stack=None,
        sort_by_metric=False, y_format="SMART_NUMBER", x_title="", y_title="", show_value=False,
        description=None, time_axis=False, row_limit=1000, order_desc=True,
        sort_series="sum", legend_type="scroll"):
    params = {
        **COMMON, "x_axis": x, "time_grain_sqla": None,
        "metrics": metrics, "groupby": [series] if series else [],
        "adhoc_filters": [where(f) for f in filters],
        "orientation": "horizontal" if horizontal else "vertical",
        # order_desc + row_limit are applied in SQL (ORDER BY first metric LIMIT n),
        # so top/bottom-N charts are cut server side; x_axis_sort only orders the display.
        "stack": stack, "row_limit": row_limit, "order_desc": order_desc,
        "x_axis_sort_asc": (not order_desc) if sort_by_metric else True,
        "x_axis_sort": metrics[0]["label"] if sort_by_metric and not series else None,
        "x_axis_sort_series": "name", "x_axis_sort_series_ascending": True,
        "truncate_metric": True, "show_empty_columns": True, "comparison_type": "values",
        "annotation_layers": [], "forecastPeriods": 10, "forecastInterval": 0.8,
        "x_axis_title": x_title, "x_axis_title_margin": 30 if x_title else 15,
        "y_axis_title": y_title, "y_axis_title_margin": 30 if y_title else 15,
        "y_axis_title_position": "Left", "sort_series_type": sort_series, "sort_series_ascending": sort_series == "name",
        "only_total": True, "show_value": show_value,
        "show_legend": bool(series) or len(metrics) > 1, "legendType": legend_type, "legendOrientation": "top",
        "x_axis_time_format": "smart_date", "y_axis_format": y_format, "y_axis_bounds": [None, None],
        "rich_tooltip": True, "tooltipTimeFormat": "smart_date",
        # ECharts silently drops overlapping category labels; rotate so all show.
        "xAxisLabelRotation": 0 if horizontal or x in TIME_COLS else 45,
    }
    return dict(name=name, dataset=dataset, viz_type="echarts_timeseries_bar", params=params,
                description=description)


def line(name, dataset, x, metrics, series=None, filters=(), y_format="SMART_NUMBER",
         x_title="", y_title="", description=None, time_axis=False, area=False):
    params = {
        **COMMON, "x_axis": x, "time_grain_sqla": None,
        "metrics": metrics, "groupby": [series] if series else [],
        "adhoc_filters": [where(f) for f in filters],
        "row_limit": 1000, "order_desc": True, "x_axis_sort_asc": True,
        "x_axis_sort_series": "name", "x_axis_sort_series_ascending": True,
        "truncate_metric": True, "show_empty_columns": True, "comparison_type": "values",
        "annotation_layers": [], "forecastPeriods": 10, "forecastInterval": 0.8,
        "x_axis_title": x_title, "x_axis_title_margin": 30 if x_title else 15,
        "y_axis_title": y_title, "y_axis_title_margin": 30 if y_title else 15,
        "y_axis_title_position": "Left", "sort_series_type": "sum",
        "seriesType": "line", "opacity": 0.2, "markerEnabled": True, "markerSize": 6,
        "only_total": True, "show_legend": True, "legendType": "scroll", "legendOrientation": "top",
        "x_axis_time_format": "smart_date", "y_axis_format": y_format, "y_axis_bounds": [None, None],
        "rich_tooltip": True, "tooltipTimeFormat": "smart_date",
        "xAxisLabelRotation": 0 if x in TIME_COLS else 45,
    }
    viz = "echarts_area" if area else "echarts_timeseries_line"
    return dict(name=name, dataset=dataset, viz_type=viz, params=params, description=description)


def heatmap(name, dataset, x, y, sql, label, filters=(), fmt=".1f", description=None, normalize="heatmap"):
    return dict(name=name, dataset=dataset, viz_type="heatmap", description=description, params={
        **COMMON, "all_columns_x": x, "all_columns_y": y, "metric": metric(sql, label),
        "adhoc_filters": [where(f) for f in filters], "row_limit": 10000,
        "linear_color_scheme": "superset_seq_1", "xscale_interval": None, "yscale_interval": None,
        "canvas_image_rendering": "pixelated", "normalize_across": normalize,
        "left_margin": "auto", "bottom_margin": "auto", "y_axis_format": fmt,
        "show_legend": True, "show_perc": False, "show_values": True,
        "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc"})


def histogram(name, dataset, column, bins=30, filters=(), x_label="", description=None):
    return dict(name=name, dataset=dataset, viz_type="histogram", description=description, params={
        **COMMON, "all_columns_x": [column], "adhoc_filters": [where(f) for f in filters],
        "row_limit": 50000, "groupby": [], "link_length": str(bins), "normalized": False,
        "x_axis_label": x_label, "y_axis_label": "Số tin"})


def funnel(name, dataset, dim, sql, label, description=None):
    return dict(name=name, dataset=dataset, viz_type="funnel", description=description, params={
        **COMMON, "groupby": [dim], "metric": metric(sql, label), "adhoc_filters": [],
        "row_limit": 10, "sort_by_metric": True, "number_format": "SMART_NUMBER",
        "show_labels": True, "show_legend": False, "label_type": "key_value_percent",
        "tooltip_label_type": "key_value_percent"})


def pie(name, dataset, dim, sql, label, filters=(), description=None):
    return dict(name=name, dataset=dataset, viz_type="pie", description=description, params={
        **COMMON, "groupby": [dim], "metric": metric(sql, label), "adhoc_filters": [where(f) for f in filters],
        "row_limit": 100, "sort_by_metric": True, "number_format": "SMART_NUMBER", "date_format": "smart_date",
        "show_labels_threshold": 3, "show_legend": True, "legendType": "plain", "legendOrientation": "top",
        "label_type": "percent", "show_labels": True, "labels_outside": False, "label_line": False,
        "outerRadius": 70, "donut": True, "innerRadius": 35, "show_total": True})


def scatter_map(name, dataset, sql, label, description=None):
    """deck.gl scatter: one circle per (lat, lon), radius = metric. Base map tiles need MAPBOX_API_KEY."""
    return dict(name=name, dataset=dataset, viz_type="deck_scatter", description=description, params={
        **COMMON, "spatial": {"type": "latlong", "latCol": "lat", "lonCol": "lon"},
        "point_radius_fixed": {"type": "metric", "value": metric(sql, label)},
        "point_unit": "square_m", "min_radius": 4, "max_radius": 60, "multiplier": 1,
        "dimension": None, "color_picker": {"r": 0, "g": 122, "b": 135, "a": 0.6},
        "viewport": {"latitude": 10.78, "longitude": 106.68, "zoom": 10.2, "bearing": 0, "pitch": 0},
        "autozoom": False, "mapbox_style": "mapbox://styles/mapbox/light-v9",
        "adhoc_filters": [], "row_limit": 5000, "filter_nulls": True, "legend_position": "tr",
        "js_columns": [], "js_data_mutator": "", "js_tooltip": "", "js_onclick_href": ""})


def mixed(name, dataset, x, metric_a, metric_b, series=None, filters=(), y_title_a="", y_title_b="",
          fmt_a="SMART_NUMBER", fmt_b="SMART_NUMBER", description=None):
    """Bars (query A, left axis) + line (query B, right axis) on the same x, both split by `series`."""
    f = [where(c) for c in filters]
    g = [series] if series else []
    return dict(name=name, dataset=dataset, viz_type="mixed_timeseries", description=description, params={
        **COMMON, "x_axis": x, "time_grain_sqla": None,
        "metrics": [metric_a], "groupby": g, "adhoc_filters": f, "row_limit": 1000, "order_desc": True,
        "truncate_metric": True, "show_empty_columns": True,
        "metrics_b": [metric_b], "groupby_b": g, "adhoc_filters_b": f, "row_limit_b": 1000, "order_desc_b": True,
        "truncate_metric_b": True, "show_empty_columns_b": True,
        "seriesType": "bar", "stack": None, "area": False, "show_value": False, "opacity": 0.5,
        "markerEnabled": False, "markerSize": 6, "yAxisIndex": 0,
        "seriesTypeB": "line", "stackB": None, "areaB": False, "show_valueB": False, "opacityB": 1,
        "markerEnabledB": True, "markerSizeB": 6, "yAxisIndexB": 1,
        "y_axis_format": fmt_a, "y_axis_format_secondary": fmt_b,
        "yAxisTitle": y_title_a, "yAxisTitleSecondary": y_title_b, "yAxisTitleMargin": 30,
        "logAxis": False, "logAxisSecondary": False, "minorSplitLine": False, "truncateYAxis": False,
        "y_axis_bounds": [None, None], "y_axis_bounds_secondary": [None, None],
        "x_axis_title": "", "x_axis_title_margin": 15, "x_axis_time_format": "smart_date",
        "rich_tooltip": True, "tooltipTimeFormat": "smart_date", "zoomable": False,
        "show_legend": True, "legendType": "plain", "legendOrientation": "top"})


# ---------------------------------------------------------------------------
# The charts. Keys are referenced by the dashboard layouts below.
# ---------------------------------------------------------------------------
M = metric
CHARTS = {
    # ---- Tổng quan: KPI ----------------------------------------------------
    "kpi_ban": big_number("Tin đang rao bán", "ov_kpi", "MAX(tin_ban_dang_rao)", "Tin bán", "tin bán hợp lệ còn trên sàn"),
    "kpi_thue": big_number("Tin đang cho thuê", "ov_kpi", "MAX(tin_thue_dang_rao)", "Tin thuê", "tin thuê hợp lệ còn trên sàn"),
    "kpi_moi": big_number("Tin bán mới 7 ngày qua", "ov_kpi", "MAX(tin_ban_moi_7_ngay)", "Tin mới", "tính đến lần crawl gần nhất"),
    "kpi_gia": big_number("Giá/m² nhà ở trung vị", "ov_kpi", "MAX(median_gia_m2_nha_trieu)", "Triệu/m²", "triệu đồng/m², tin bán đã làm sạch", fmt=",.1f"),
    "kpi_gia_ty": big_number("Giá bán nhà ở trung vị", "ov_kpi", "MAX(median_gia_nha_ty)", "Tỷ đồng", "tỷ đồng, tin bán đã làm sạch", fmt=",.2f"),
    "kpi_sach": big_number("Tin bán sạch để phân tích giá", "ov_kpi", "MAX(tin_ban_sach)", "Tin", "đủ thông tin, không trùng, giá không bất thường"),

    # ---- Tổng quan: cơ cấu ------------------------------------------------
    "co_cau": bar("Số tin theo loại hình, bán và cho thuê", "ov_tin_theo_loai_quan", "loai_bds",
                  [M("SUM(so_tin)", "Số tin")], series="giao_dich", stack="Stack", y_title="Số tin",
                  description="Mọi tin hợp lệ từng thu thập, kể cả tin đã gỡ."),
    "theo_quan": bar("Số tin theo quận", "ov_tin_theo_loai_quan", "quan",
                     [M("SUM(so_tin)", "Số tin")], series="giao_dich", stack="Stack", horizontal=True,
                     filters=["quan <> 'Không rõ'"], y_title="Số tin"),

    # ---- Tổng quan: theo quận ---------------------------------------------
    "gia_quan": bar("Giá/m² trung vị theo quận và loại hình (tin bán)", "ov_gia_ban_theo_quan", "quan",
                    [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds", horizontal=True,
                    y_format=",.1f", y_title="Triệu đồng/m²",
                    description="Chỉ quận có từ 30 tin trở lên cho loại hình đó."),
    "gia_thue_quan": bar("Giá thuê trung vị theo quận (triệu/tháng)", "ov_gia_thue_theo_quan", "quan",
                         [M("MAX(median_gia_thue_trieu_thang)", "Triệu/tháng")], series="loai_bds",
                         horizontal=True, y_format=",.1f", y_title="Triệu đồng/tháng"),

    # ---- Tổng quan: phân bố -----------------------------------------------
    "pb_gia": histogram("Phân bố giá bán (tỷ đồng)", "ov_phan_bo", "gia_ty", filters=["gia_ty <= 50"], x_label="Tỷ đồng"),
    "pb_gia_m2": histogram("Phân bố giá/m² (triệu đồng)", "ov_phan_bo", "gia_m2_trieu", filters=["gia_m2_trieu <= 500"], x_label="Triệu đồng/m²"),
    "pb_dien_tich": histogram("Phân bố diện tích (m²)", "ov_phan_bo", "dien_tich_m2", filters=["dien_tich_m2 <= 500"], x_label="m²"),

    # ---- Phân tích A: yếu tố ảnh hưởng giá ----------------------------------
    "yeu_to": bar("Yếu tố nào giải thích được nhiều biến động giá/m² nhất (eta²)", "pt_yeu_to", "yeu_to",
                  [M("MAX(eta2_pct)", "% phương sai giải thích")], series="loai_bds", horizontal=True,
                  y_format=",.1f",
                  description="eta² = phần phương sai của log(giá/m²) giải thích được nếu chỉ biết nhóm của yếu tố đó. "
                              "Các yếu tố vị trí chồng lấn nhau, không cộng lại được."),
    "khoang_cach": line("Giá/m² giảm thế nào khi càng xa trung tâm", "pt_khoang_cach", "khoang_cach_km",
                        [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds", y_format=",.1f",
                        x_title="Km tới chợ Bến Thành (bước 2 km)", y_title="Triệu đồng/m²"),
    "dien_tich": line("Diện tích càng lớn thì giá/m² cao hay thấp hơn", "pt_dien_tich", "nhom_dien_tich",
                      [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds", y_format=",.1f",
                      y_title="Triệu đồng/m²"),
    "so_tang": line("Cùng diện tích, nhà nhiều tầng hơn đắt hơn bao nhiêu", "pt_so_tang", "so_tang",
                    [M("MAX(median_gia_ty)", "Tỷ đồng")], series="nhom_dien_tich", y_format=",.2f",
                    x_title="Số tầng (6 = từ 6 tầng)", y_title="Giá bán trung vị (tỷ)",
                    description="Nhà ở; mỗi đường là một nhóm diện tích, độ dốc là ảnh hưởng của số tầng."),
    "dac_diem": bar("Đặc điểm và pháp lý nào làm nhà ở đắt hơn (%)", "pt_dac_diem", "dac_diem",
                    [M("MAX(chenh_lech_tho_pct)", "So mọi nhà"),
                     M("MAX(chenh_lech_cung_quan_dt_pct)", "So nhà cùng quận, cùng diện tích")],
                    horizontal=True, y_format=",.1f",
                    description="Chênh lệch giá/m² của nhà có đặc điểm so với nhà không có. Hai cột lệch nhau nhiều "
                                "nghĩa là phần lớn chênh lệch thô là do vị trí và diện tích."),
    "phap_ly": bar("Giá/m² theo pháp lý", "pt_phap_ly", "phap_ly",
                   [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds", y_format=",.1f",
                   y_title="Triệu đồng/m²"),

    # ---- Phân tích: đi sâu từng loại hình ----------------------------------
    "huong": bar("Giá/m² theo hướng nhà", "pt_huong", "huong",
                 [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds", y_format=",.1f",
                 y_title="Triệu đồng/m²",
                 description="Chỉ tin có ghi hướng (khoảng một phần ba). Chênh lệch nhỏ so với vị trí và diện tích."),
    "chieu_ngang": line("Nhà ở: chiều ngang lô càng rộng, giá/m² càng cao?", "pt_nha_chieu_ngang", "chieu_ngang",
                        [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], y_format=",.1f",
                        y_title="Triệu đồng/m²"),
    "phong_ngu": bar("Căn hộ: giá bán trung vị theo số phòng ngủ", "pt_can_ho_phong_ngu", "so_phong_ngu",
                     [M("MAX(median_gia_ty)", "Tỷ đồng")], y_format=",.2f",
                     x_title="Số phòng ngủ (4 = từ 4)", y_title="Tỷ đồng"),
    "noi_that": bar("Căn hộ: giá/m² theo nội thất", "pt_can_ho_noi_that", "noi_that",
                    [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="ban_giao", y_format=",.1f",
                    y_title="Triệu đồng/m²"),
    "du_an": bar("Căn hộ: 10 dự án giá/m² cao nhất (từ 30 tin)", "pt_du_an", "du_an",
                 [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], horizontal=True, sort_by_metric=True,
                 y_format=",.1f", y_title="Triệu đồng/m²", show_value=True, row_limit=10,
                 description="Chọn một Quận: các dự án cùng quận vẫn chênh giá/m² lớn, "
                             "nên dự án là yếu tố riêng của căn hộ ngoài vị trí."),
    "loai_dat": bar("Đất: giá/m² theo loại đất và nhóm diện tích", "pt_dat", "nhom_dien_tich",
                    [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_dat", y_format=",.1f",
                    y_title="Triệu đồng/m²",
                    description="Lô lớn thường chỉ một phần là thổ cư nên giá/m² thấp hơn hẳn."),

    # ---- Phân tích: vị trí chi tiết ----------------------------------------
    "phuong_cao": bar("10 phường giá/m² cao nhất (từ 30 tin)", "pt_phuong", "phuong",
                      [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], horizontal=True, sort_by_metric=True,
                      row_limit=10, order_desc=True, y_format=",.1f", y_title="Triệu đồng/m²",
                      description="Theo loại hình đang chọn ở bộ lọc (mặc định nhà ở). Lọc Quận để xếp hạng phường trong quận."),
    "phuong_thap": bar("10 phường giá/m² thấp nhất (từ 30 tin)", "pt_phuong", "phuong",
                       [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], horizontal=True, sort_by_metric=True,
                       row_limit=10, order_desc=False, y_format=",.1f", y_title="Triệu đồng/m²",
                       description="Theo loại hình đang chọn ở bộ lọc (mặc định nhà ở). Lọc Quận để xếp hạng phường trong quận."),
    "quan_dien_tich": heatmap("Giá/m² theo quận × nhóm diện tích (triệu/m²)", "pt_quan_dien_tich",
                              "nhom_dien_tich", "quan", "MAX(median_gia_m2_trieu)", "Triệu/m²",
                              description="Đọc theo hàng: cùng một quận, nhà nhỏ có giá/m² cao hơn nhà lớn bao nhiêu."),

    # ---- Xu hướng theo ngày đăng tin ----------------------------------------
    "xh_gia": line("Giá đang tăng hay giảm: giá/m² trung vị theo tuần đăng tin", "xh_gia_tuan_dang", "tuan_dang",
                   [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds", y_format=",.1f",
                   y_title="Triệu đồng/m²",
                   description="Tin bán đã làm sạch, gom theo tuần đăng. Tuần dưới 30 tin bị bỏ."),
    "xh_gia_loai": bar("Loại hình nào có mặt bằng giá cao hơn (giá/m²)", "xh_gia_loai", "loai_bds",
                       [M("MAX(p25_gia_m2_trieu)", "P25"), M("MAX(median_gia_m2_trieu)", "Trung vị"),
                        M("MAX(p75_gia_m2_trieu)", "P75")],
                       horizontal=True, y_format=",.1f", y_title="Triệu đồng/m²", show_value=True,
                       description="P25–P75 là khoảng giá của một nửa số tin ở giữa."),
    "xh_phan_khuc": bar("Phân khúc giá nào chiếm ưu thế trong từng loại hình (% số tin)", "tk_phan_khuc", "loai_bds",
                        [M("SUM(so_tin)", "Số tin")], series="phan_khuc", stack="Expand", y_format=".0%",
                        sort_series="name", legend_type="plain",
                        description="Mỗi cột là một loại hình, chia theo tỷ trọng từng phân khúc giá bán."),
    "xh_co_cau": pie("Cơ cấu bất động sản trên thị trường (bán + cho thuê)", "xh_tin_dang_tuan", "loai_bds",
                     "SUM(so_tin)", "Số tin",
                     description="Tỷ trọng số tin đăng theo loại hình trong cùng khoảng thời gian với các chart theo tuần."),
    "xh_so_tin": line("Số tin đăng mới theo tuần đăng tin", "xh_tin_dang_tuan", "tuan_dang",
                      [M("SUM(so_tin)", "Số tin")], series="giao_dich", y_title="Số tin",
                      description="Đếm theo ngày người bán đăng tin, không theo ngày crawl."),
    "xh_dong_gop": bar("Loại hình nào đóng góp nhiều nhất vào nguồn cung", "xh_tin_dang_tuan",
                       "tuan_dang", [M("SUM(so_tin)", "Số tin")], series="loai_bds", stack="Stack", y_title="Số tin",
                       legend_type="plain",
                       description="Mỗi cột là số tin đăng trong tuần, chia theo loại hình (bán + cho thuê)."),

    # ---- Bổ sung cho cấu trúc 5 phần ----------------------------------------
    "ban_do": scatter_map("Bản đồ nguồn cung: mỗi chấm là một phường, to theo số tin bán", "ov_ban_do_phuong",
                          "SUM(so_tin)", "Số tin",
                          description="Tâm phường = trung bình toạ độ các tin. Lọc Loại hình hoặc Quận để thu hẹp."),
    "dien_tich_gia": line("Giá bán trung vị theo nhóm diện tích (tỷ đồng)", "pt_dien_tich", "nhom_dien_tich",
                          [M("MAX(median_gia_ty)", "Tỷ đồng")], series="loai_bds", y_format=",.2f",
                          y_title="Tỷ đồng",
                          description="Đọc cùng chart giá/m²: giá bán tăng theo diện tích nhưng giá/m² lại giảm."),
    "phan_khuc_tin": bar("Số tin bán theo phân khúc giá và loại hình", "tk_phan_khuc", "phan_khuc",
                         [M("SUM(so_tin)", "Số tin")], series="loai_bds", stack="Stack", y_title="Số tin",
                         description="Lọc Quận để xem cơ cấu phân khúc của một quận."),
    "phan_khuc_quan": bar("Cơ cấu phân khúc giá của từng quận (% số tin)", "tk_phan_khuc", "quan",
                          [M("SUM(so_tin)", "Số tin")], series="phan_khuc", stack="Expand", horizontal=True,
                          sort_series="name", legend_type="plain",
                          filters=["quan <> 'Không rõ'"], y_format=".0%",
                          description="Mỗi thanh là một quận, chia theo tỷ trọng từng phân khúc. Quận có phần tím/xanh đậm dài là quận bán tầm giá cao."),
    "xh_co_cau_tuan": bar("Cơ cấu nguồn cung theo loại hình từng tuần (% số tin)", "xh_tin_dang_tuan", "tuan_dang",
                          [M("SUM(so_tin)", "Số tin")], series="loai_bds", stack="Expand", y_format=".0%",
                          legend_type="plain",
                          description="Tỷ trọng từng loại hình trong số tin đăng mỗi tuần; cơ cấu ổn định hay đang dịch chuyển."),
    "cung_gia": mixed("Nguồn cung mới và giá/m² theo tuần đăng tin (tin bán)", "xh_gia_tuan_dang", "tuan_dang",
                      M("SUM(so_tin)", "Tin bán mới"), M("MAX(median_gia_m2_trieu)", "Giá/m²"), series="loai_bds",
                      y_title_a="Số tin", y_title_b="Triệu đồng/m²", fmt_b=",.1f",
                      description="Cột: số tin bán sạch đăng trong tuần (trục trái). Đường: giá/m² trung vị của chúng (trục phải), "
                                  "mỗi loại hình một màu. Lọc một loại hình để đọc rõ: nhiều cung hơn có đi kèm giá thấp hơn không."),

    # ---- Mô hình dự đoán giá -----------------------------------------------
    "mh_mape": big_number("MAPE trên tập test", "mh_sai_so", "AVG(sai_so_pct)", "MAPE %", "sai số tuyệt đối trung bình, %", fmt=",.2f"),
    "mh_mdape": big_number("Sai số trung vị", "mh_sai_so", "PERCENTILE_APPROX(sai_so_pct, 0.5)", "MdAPE %", "nửa số tin sai ít hơn mức này", fmt=",.2f"),
    "mh_20": big_number("Tin sai số ≤ 20%", "mh_sai_so", "100 * AVG(trong_20pct)", "%", "% tin test", fmt=",.1f"),
    "mh_10": big_number("Tin sai số ≤ 10%", "mh_sai_so", "100 * AVG(trong_10pct)", "%", "% tin test", fmt=",.1f"),
    "mh_so_tin": big_number("Số tin test", "mh_sai_so", "COUNT(*)", "Tin", "20% tin mới nhất theo ngày đăng"),
    "mh_phien_ban": big_number("Phiên bản model", "mh_sai_so", "MAX(phien_ban)", "v", "price_model trong MLflow", fmt=",d"),
    "mh_loai": bar("Sai số theo loại hình", "mh_sai_so", "loai_bds",
                   [M("AVG(sai_so_pct)", "MAPE %"), M("PERCENTILE_APPROX(sai_so_pct, 0.5)", "Sai số trung vị %")],
                   y_format=",.1f", description="Đất sai nhiều nhất: lô lớn thường chỉ một phần là thổ cư, model không có thông tin này."),
    "mh_chieu": bar("Model đoán cao hay thấp hơn giá rao?", "mh_sai_so", "loai_bds",
                    [M("COUNT(*)", "Số tin")], series="chieu_sai", stack="Stack", y_title="Số tin",
                    description="Cân bằng giữa hai chiều nghĩa là model không lệch hệ thống."),
    "mh_quan": bar("Sai số theo quận (MAPE %)", "mh_sai_so", "quan",
                   [M("AVG(sai_so_pct)", "MAPE %")], horizontal=True, sort_by_metric=True, y_format=",.1f"),
    "mh_phan_bo": histogram("Phân bố sai số (%)", "mh_sai_so", "sai_so_pct", bins=40, filters=["sai_so_pct <= 100"], x_label="Sai số %"),
    "mh_theo_tuan": line("Sai số theo tuần đăng tin", "mh_sai_so", "tuan_dang",
                         [M("AVG(sai_so_pct)", "MAPE %")], series="loai_bds", y_format=",.1f",
                         description="Sai số tăng dần theo thời gian là dấu hiệu model cần train lại (drift)."),
    "mh_phan_khuc": bar("Sai số theo phân khúc giá", "mh_sai_so", "phan_khuc",
                        [M("AVG(sai_so_pct)", "MAPE %")], series="loai_bds", y_format=",.1f", x_title="Phân khúc giá rao"),
}


# ---------------------------------------------------------------------------
# Dashboards: rows of (chart_key, width) with markdown section headers.
# Width is in 12ths of the page; height in grid units of 8 px.
# ---------------------------------------------------------------------------
def md(text, height=12):
    return ("MD", text, height)


DASHBOARDS = {
    "tong_quan": dict(
        title="1. Tổng quan thị trường bất động sản",
        slug="tong-quan-thi-truong",
        description="Thị trường có gì, bao nhiêu tin theo loại hình và mục đích, phân bố theo quận và trên bản đồ.",
        filters=[("Loại hình", "loai_bds", "ov_tin_theo_loai_quan"),
                 ("Quận", "quan", "ov_tin_theo_loai_quan"),
                 ("Khu vực", "khu_vuc", "ov_tin_theo_loai_quan")],
        rows=[
            [md("### 1. Tổng quan thị trường bất động sản TP.HCM\n"
                "Nguồn: tầng Gold của lakehouse, dữ liệu Nhà Tốt, cập nhật sau mỗi lần crawl. "
                "Bộ lọc **Loại hình**, **Quận**, **Khu vực** áp dụng cho các chart có cột tương ứng.", 12)],
            [("kpi_ban", 2, 32), ("kpi_thue", 2, 32), ("kpi_moi", 2, 32), ("kpi_gia", 2, 32), ("kpi_gia_ty", 2, 32), ("kpi_sach", 2, 32)],
            [md("#### 1.1. Cơ cấu loại hình bất động sản")],
            [("xh_co_cau", 12, 55)],
            [md("#### 1.2. Số lượng tin theo loại hình và mục đích")],
            [("co_cau", 12, 55)],
            [md("#### 1.3. Phân bố tin đăng theo quận")],
            [("theo_quan", 12, 65)],
            [md("#### 1.4. Bản đồ phân bố nguồn cung")],
            [("ban_do", 12, 90)],
        ],
    ),
    "mat_bang_gia": dict(
        title="2. Phân tích mặt bằng giá bất động sản",
        slug="mat-bang-gia",
        description="Phân bố giá, so sánh giữa loại hình, giá bán và giá thuê theo quận, xếp hạng phường, pháp lý, diện tích.",
        filters=[("Loại hình", "loai_bds", "ov_gia_ban_theo_quan", "Nhà ở"),
                 ("Quận", "quan", "ov_gia_ban_theo_quan"),
                 ("Khu vực", "khu_vuc", "ov_gia_ban_theo_quan")],
        rows=[
            [md("### 2. Phân tích mặt bằng giá bất động sản\n"
                "Giá tính trên tin bán đã làm sạch (`gold.ml_price_features`), đơn vị triệu đồng/m², dùng trung vị, "
                "nhóm dưới 30 tin bị loại. Bộ lọc **Loại hình** mặc định là nhà ở; chọn thêm để so sánh.", 12)],
            [md("#### 2.1. Phân bố giá bán, giá/m² và diện tích")],
            [("pb_gia", 4, 45), ("pb_gia_m2", 4, 45), ("pb_dien_tich", 4, 45)],
            [md("#### 2.2. So sánh giá giữa các loại hình")],
            [("xh_gia_loai", 12, 50)],
            [md("#### 2.3. Giá bán theo quận")],
            [("gia_quan", 7, 65), ("khoang_cach", 5, 65)],
            [md("#### 2.4. Giá thuê theo quận")],
            [("gia_thue_quan", 12, 60)],
            [md("#### 2.5. Xếp hạng giá theo phường")],
            [("phuong_cao", 6, 50), ("phuong_thap", 6, 50)],
            [md("#### 2.6. Giá theo tình trạng pháp lý")],
            [("phap_ly", 12, 50)],
            [md("#### 2.7. Mối quan hệ giữa diện tích và giá bán")],
            [("dien_tich_gia", 6, 50), ("dien_tich", 6, 50)],
            [("quan_dien_tich", 12, 70)],
        ],
    ),
    "phan_khuc": dict(
        title="3. Phân tích phân khúc và cơ cấu thị trường",
        slug="phan-khuc-thi-truong",
        description="Thị trường tập trung ở phân khúc giá nào, theo loại hình và theo quận.",
        filters=[("Loại hình", "loai_bds", "tk_phan_khuc"),
                 ("Quận", "quan", "tk_phan_khuc"),
                 ("Khu vực", "khu_vuc", "tk_phan_khuc")],
        rows=[
            [md("### 3. Phân tích phân khúc và cơ cấu thị trường\n"
                "Sáu phân khúc theo giá bán: dưới 2 tỷ, 2–4, 4–7, 7–12, 12–25 và từ 25 tỷ. Tin bán đã làm sạch.", 12)],
            [md("#### 3.1. Cơ cấu phân khúc giá theo loại hình")],
            [("phan_khuc_tin", 6, 55), ("xh_phan_khuc", 6, 55)],
            [md("#### 3.2. Cơ cấu phân khúc giá theo khu vực")],
            [("phan_khuc_quan", 12, 75)],
        ],
    ),
    "bien_dong": dict(
        title="4. Phân tích biến động thị trường theo thời gian",
        slug="bien-dong-thi-truong",
        description="Nguồn cung mới, cơ cấu nguồn cung và giá/m² theo tuần đăng tin; quan hệ giữa cung và giá.",
        filters=[("Loại hình", "loai_bds", "xh_tin_dang_tuan"),
                 ("Quận", "quan", "xh_tin_dang_tuan"),
                 ("Khu vực", "khu_vuc", "xh_tin_dang_tuan")],
        rows=[
            [md("### 4. Phân tích biến động thị trường theo thời gian\n"
                "Trục thời gian là **tuần người bán đăng tin** (`posted_at`), không phải ngày crawl. "
                "Chỉ lấy các tuần trọn 7 ngày, bắt đầu không sớm hơn 14 ngày trước lần crawl đầu tiên, "
                "vì tin đăng sớm hơn phần lớn đã bị gỡ trước khi hệ thống kịp thấy. Giá chỉ tính tin bán đã làm sạch.", 14)],
            [md("#### 4.1. Số lượng tin đăng mới theo tuần")],
            [("xh_so_tin", 12, 50)],
            [md("#### 4.2. Cơ cấu nguồn cung theo loại hình")],
            [("xh_dong_gop", 6, 50), ("xh_co_cau_tuan", 6, 50)],
            [md("#### 4.3. Biến động giá/m² theo tuần")],
            [("xh_gia", 12, 55)],
            [md("#### 4.4. Mối quan hệ giữa nguồn cung và giá")],
            [("cung_gia", 12, 55)],
        ],
    ),
    "chuyen_sau": dict(
        title="5. Phân tích chuyên sâu theo loại hình",
        slug="chuyen-sau-loai-hinh",
        description="Đặc điểm riêng quyết định giá của nhà ở, căn hộ và đất.",
        filters=[("Loại hình", "loai_bds", "ov_gia_ban_theo_quan", "Nhà ở"),
                 ("Quận", "quan", "ov_gia_ban_theo_quan"),
                 ("Khu vực", "khu_vuc", "ov_gia_ban_theo_quan")],
        rows=[
            [md("### 5. Phân tích chuyên sâu theo loại hình\n"
                "Mở đầu là xếp hạng yếu tố theo eta² cho từng loại hình; sau đó đi vào đặc điểm riêng của nhà ở, căn hộ, đất. "
                "Bộ lọc **Loại hình** mặc định là nhà ở, chỉ ảnh hưởng tới các chart dùng chung (eta², hướng nhà).", 12)],
            [("yeu_to", 12, 60)],
            [md("#### 5.1. Nhà ở")],
            [("so_tang", 6, 50), ("chieu_ngang", 6, 50)],
            [("dac_diem", 7, 55), ("huong", 5, 55)],
            [md("#### 5.2. Căn hộ")],
            [("phong_ngu", 6, 50), ("noi_that", 6, 50)],
            [("du_an", 12, 50)],
            [md("#### 5.3. Đất")],
            [("loai_dat", 12, 55)],
        ],
    ),
    "mo_hinh": dict(
        title="Model dự đoán giá bán: sai số trên tập test",
        slug="mo-hinh-du-doan-gia",
        description="Phiên bản champion hiện tại sai bao nhiêu, sai ở đâu, và có lệch hệ thống hay trôi theo thời gian không.",
        filters=[("Loại hình", "loai_bds", "mh_sai_so"),
                 ("Quận", "quan", "mh_sai_so")],
        rows=[
            [md("### Model dự đoán giá bán\n"
                "Mỗi lần train, pipeline chấm model trên **tập test = 20% tin mới nhất theo ngày đăng** (model chưa từng thấy) "
                "và ghi từng dự đoán vào `gold.ml_price_predictions`. Dashboard này đọc phiên bản mới nhất. "
                "Sai số = |giá dự đoán − giá rao| / giá rao. Giá rao tự nó có nhiễu (cùng một căn, hai người đăng hai giá), "
                "nên sai số không thể về 0.", 14)],
            [("mh_mape", 2, 32), ("mh_mdape", 2, 32), ("mh_20", 2, 32), ("mh_10", 2, 32), ("mh_so_tin", 2, 32), ("mh_phien_ban", 2, 32)],
            [md("#### Sai ở đâu")],
            [("mh_loai", 4, 50), ("mh_phan_khuc", 4, 50), ("mh_chieu", 4, 50)],
            [("mh_quan", 7, 65), ("mh_phan_bo", 5, 65)],
            [md("#### Có trôi theo thời gian không")],
            [("mh_theo_tuan", 12, 50)],
        ],
    ),
}


# ---------------------------------------------------------------------------
# YAML emitters
# ---------------------------------------------------------------------------
def dataset_yaml(key, spec):
    cols = [{"column_name": c, "verbose_name": None, "is_dttm": t == "DATE", "is_active": True,
             "type": t, "advanced_data_type": None, "groupby": True, "filterable": True,
             "expression": None, "description": None, "python_date_format": None, "extra": None}
            for c, t in spec["columns"].items()]
    return {
        "table_name": key, "main_dttm_col": spec.get("dttm"), "description": spec["description"],
        "default_endpoint": None, "offset": 0, "cache_timeout": None, "schema": "gold",
        "sql": spec["sql"].strip() + "\n", "params": None, "template_params": None,
        "filter_select_enabled": True, "fetch_values_predicate": None, "extra": None,
        "normalize_columns": False, "uuid": uid("dataset", key),
        "metrics": [{"metric_name": "count", "verbose_name": "COUNT(*)", "metric_type": "count",
                     "expression": "COUNT(*)", "description": None, "d3format": None, "currency": None,
                     "extra": None, "warning_text": None}],
        "columns": cols, "version": "1.0.0", "database_uuid": DATABASE["uuid"],
    }


def chart_yaml(key, spec):
    params = {"datasource": "0__table", "viz_type": spec["viz_type"], **spec["params"]}
    return {
        "slice_name": spec["name"], "description": spec.get("description"), "certified_by": None,
        "certification_details": None, "viz_type": spec["viz_type"], "params": params,
        "query_context": None, "cache_timeout": None, "uuid": uid("chart", key), "version": "1.0.0",
        "dataset_uuid": uid("dataset", spec["dataset"]),
    }


def dashboard_yaml(key, spec):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": spec["title"]}},
    }
    chart_ids = []
    for r, row in enumerate(spec["rows"]):
        row_id = f"ROW-{key}-{r}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                            "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for c, item in enumerate(row):
            if item[0] == "MD":
                cid = f"MARKDOWN-{key}-{r}-{c}"
                position[cid] = {"type": "MARKDOWN", "id": cid, "children": [],
                                 "parents": ["ROOT_ID", "GRID_ID", row_id],
                                 "meta": {"code": item[1], "width": 12, "height": item[2]}}
            else:
                chart_key, width, height = item
                cid = f"CHART-{chart_key}"
                chart_ids.append(chart_key)
                position[cid] = {"type": "CHART", "id": cid, "children": [],
                                 "parents": ["ROOT_ID", "GRID_ID", row_id],
                                 "meta": {"uuid": uid("chart", chart_key), "chartId": 0, "width": width, "height": height,
                                          "sliceName": CHARTS[chart_key]["name"]}}
            position[row_id]["children"].append(cid)

    native_filters = []
    for name, column, dataset, *default in spec["filters"]:
        default = default[0] if default else None
        mask = ({"extraFormData": {"filters": [{"col": column, "op": "IN", "val": [default]}]},
                 "filterState": {"value": [default]}, "ownState": {}}
                if default else {"extraFormData": {}, "filterState": {}, "ownState": {}})
        native_filters.append({
            "id": "NATIVE_FILTER-" + uid("filter", f"{key}:{column}")[:12],
            "name": name, "filterType": "filter_select",
            "targets": [{"datasetUuid": uid("dataset", dataset), "column": {"name": column}}],
            "defaultDataMask": mask,
            "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False,
                              "multiSelect": True, "searchAllOptions": False, "inverseSelection": False},
            "type": "NATIVE_FILTER", "description": "",
        })
    return {
        "dashboard_title": spec["title"], "description": spec["description"], "css": "",
        "slug": spec["slug"], "uuid": uid("dashboard", key), "position": position,
        "metadata": {"color_scheme": "supersetColors", "refresh_frequency": 0, "expanded_slices": {},
                     "label_colors": {}, "timed_refresh_immune_slices": [], "cross_filters_enabled": True,
                     "default_filters": "{}", "native_filter_configuration": native_filters},
        "version": "1.0.0",
    }, chart_ids


def main():
    for key, spec in CHARTS.items():
        assert spec["dataset"] in DATASETS, f"chart {key}: unknown dataset {spec['dataset']}"
    used = {c for d in DASHBOARDS.values() for row in d["rows"] for c in
            [i[0] for i in row if i[0] != "MD"]}
    assert used == set(CHARTS), f"charts not placed: {set(CHARTS) - used}; unknown: {used - set(CHARTS)}"

    shutil.rmtree(OUT_DIR, ignore_errors=True)
    for sub in ("databases", "datasets/Lakehouse_Spark_Thrift", "charts", "dashboards"):
        os.makedirs(os.path.join(OUT_DIR, sub))

    def dump(rel, obj):
        with open(os.path.join(OUT_DIR, rel), "w", encoding="utf-8") as f:
            yaml.safe_dump(obj, f, allow_unicode=True, sort_keys=False, width=120)

    dump("metadata.yaml", {"version": "1.0.0", "type": "Dashboard",
                           "timestamp": datetime.now(timezone.utc).isoformat()})
    dump("databases/Lakehouse_Spark_Thrift.yaml", {
        **DATABASE, "cache_timeout": None, "expose_in_sqllab": True, "allow_run_async": False,
        "allow_ctas": False, "allow_cvas": False, "allow_dml": False, "allow_file_upload": False,
        "extra": {"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {},
                  "schemas_allowed_for_file_upload": []}, "version": "1.0.0"})
    for key, spec in DATASETS.items():
        dump(f"datasets/Lakehouse_Spark_Thrift/{key}.yaml", dataset_yaml(key, spec))
    for key, spec in CHARTS.items():
        dump(f"charts/{key}.yaml", chart_yaml(key, spec))
    for key, spec in DASHBOARDS.items():
        doc, _ = dashboard_yaml(key, spec)
        dump(f"dashboards/{key}.yaml", doc)

    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(OUT_DIR):
            for name in files:
                path = os.path.join(root, name)
                z.write(path, os.path.join("reip_dashboards", os.path.relpath(path, OUT_DIR)))
    print(f"{len(DATASETS)} datasets, {len(CHARTS)} charts, {len(DASHBOARDS)} dashboards -> {ZIP_PATH}")


if __name__ == "__main__":
    main()

