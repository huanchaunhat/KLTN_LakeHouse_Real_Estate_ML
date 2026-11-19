#!/usr/bin/env python3
"""Generate the two Superset dashboards as an import bundle.

    python superset/build_dashboards.py            # writes superset/dashboards/ + dashboards.zip
    bash superset/import_dashboards.sh             # builds, then imports into the running Superset

Everything (datasets = Spark SQL over Gold, charts, layout, filters) is declared
here, so the dashboards can be rebuilt on a fresh Superset with one command
instead of being clicked together by hand. UUIDs are derived from the names,
so re-importing updates the existing objects in place.

Dashboard 1 "Tổng quan thị trường": what is on the market, where, at what
price, and how price and supply move week by week.
Dashboard 2 "Phân tích giá & thị trường": what drives price (the questions
that shaped the feature set), what is specific to each property type, price
at ward level, and price segments.
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
    "ov_xu_huong_gia_tuan": dict(
        description="Giá/m² trung vị toàn thành phố theo tuần (gold.agg_price_trend_weekly, cấp CITY).",
        sql="""
SELECT
    period_start                                 AS tuan,
    transaction_type_label                       AS giao_dich,
    property_type                                AS loai_bds,
    listing_count                                AS so_tin,
    round(median_price_per_m2 / 1e6, 2)          AS median_gia_m2_trieu,
    round(p25_price_per_m2 / 1e6, 2)             AS p25_gia_m2_trieu,
    round(p75_price_per_m2 / 1e6, 2)             AS p75_gia_m2_trieu,
    round(median_price_per_m2_change_pct, 2)     AS thay_doi_so_tuan_truoc_pct,
    is_reliable                                  AS du_tin_cay
FROM gold.agg_price_trend_weekly
WHERE geo_level = 'CITY'
""",
        columns={"tuan": "DATE", "giao_dich": "STRING", "loai_bds": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE", "p25_gia_m2_trieu": "DOUBLE", "p75_gia_m2_trieu": "DOUBLE",
                 "thay_doi_so_tuan_truoc_pct": "DOUBLE", "du_tin_cay": "BOOLEAN"},
        dttm="tuan",
    ),
    "ov_nguon_cung_tuan": dict(
        description="Nguồn cung và thanh khoản toàn thành phố theo tuần (gold.agg_market_supply_weekly, cấp CITY).",
        sql="""
SELECT
    period_start                      AS tuan,
    transaction_type_label            AS giao_dich,
    property_type                     AS loai_bds,
    active_listing_count              AS tin_dang_rao,
    new_listing_count                 AS tin_moi,
    removed_listing_count             AS tin_go,
    net_supply_change                 AS bien_dong_rong,
    price_cut_listing_count           AS tin_giam_gia,
    removal_rate_pct                  AS ty_le_go_pct,
    price_cut_rate_pct                AS ty_le_giam_gia_pct,
    median_price_cut_pct              AS median_muc_giam_pct,
    median_days_on_market_at_removal  AS median_ngay_rao_khi_go,
    is_reliable                       AS du_tin_cay
FROM gold.agg_market_supply_weekly
WHERE geo_level = 'CITY'
""",
        columns={"tuan": "DATE", "giao_dich": "STRING", "loai_bds": "STRING", "tin_dang_rao": "BIGINT",
                 "tin_moi": "BIGINT", "tin_go": "BIGINT", "bien_dong_rong": "BIGINT", "tin_giam_gia": "BIGINT",
                 "ty_le_go_pct": "DOUBLE", "ty_le_giam_gia_pct": "DOUBLE", "median_muc_giam_pct": "DOUBLE",
                 "median_ngay_rao_khi_go": "DOUBLE", "du_tin_cay": "BOOLEAN"},
        dttm="tuan",
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
    "ov_lam_sach": dict(
        description="Từ tin bán thu được còn bao nhiêu tin sạch để phân tích giá (đếm theo tin).",
        sql="""
SELECT '1. Tin bán thu thập được' AS buoc, count(*) AS so_tin
  FROM gold.fct_listing
 WHERE transaction_type = 'sale' AND is_market_eligible
UNION ALL
SELECT '2. Đủ thông tin, giá không bất thường', count(*)
  FROM gold.fct_listing
 WHERE transaction_type = 'sale' AND is_price_training_eligible AND NOT is_price_outlier
UNION ALL
SELECT '3. Bỏ tin trùng, vị trí sai, văn phòng', count(*)
  FROM gold.ml_price_features
""",
        columns={"buoc": "STRING", "so_tin": "BIGINT"},
    ),
    "ov_do_day_du": dict(
        description="Tỷ lệ tin có dữ liệu ở từng thuộc tính, theo loại hình (silver.listing_current).",
        sql="""
WITH coverage AS (
    SELECT
        trim(property_type)                                   AS loai_bds,
        avg(cast(ward_id         IS NOT NULL AS int)) * 100   AS phuong,
        avg(cast(street          IS NOT NULL AS int)) * 100   AS duong,
        avg(cast(bedrooms        IS NOT NULL AS int)) * 100   AS phong_ngu,
        avg(cast(bathrooms       IS NOT NULL AS int)) * 100   AS phong_tam,
        avg(cast(total_floors    IS NOT NULL AS int)) * 100   AS so_tang,
        avg(cast(width_m         IS NOT NULL AS int)) * 100   AS chieu_ngang,
        avg(cast(house_direction IS NOT NULL AS int)) * 100   AS huong,
        avg(cast(legal_status    IS NOT NULL AS int)) * 100   AS phap_ly,
        avg(cast(furnishing      IS NOT NULL AS int)) * 100   AS noi_that
    FROM silver.listing_current
    WHERE is_market_eligible
    GROUP BY trim(property_type)
)
SELECT loai_bds, thuoc_tinh, round(ty_le, 1) AS ty_le_co_du_lieu_pct
FROM coverage
LATERAL VIEW stack(9,
    'Phường', phuong, 'Đường', duong, 'Phòng ngủ', phong_ngu, 'Phòng tắm', phong_tam,
    'Số tầng', so_tang, 'Chiều ngang', chieu_ngang, 'Hướng nhà', huong,
    'Pháp lý', phap_ly, 'Nội thất', noi_that
) t AS thuoc_tinh, ty_le
""",
        columns={"loai_bds": "STRING", "thuoc_tinh": "STRING", "ty_le_co_du_lieu_pct": "DOUBLE"},
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
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE {TYPES}
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"nhom_dien_tich": "STRING", "loai_bds": "STRING",
                 "so_tin": "BIGINT", "median_gia_m2_trieu": "DOUBLE"},
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
    "pt_nguoi_dang": dict(
        description="Môi giới so với cá nhân: tỷ lệ tin và giá/m² trung vị theo loại hình.",
        sql=f"""
SELECT
    property_type                                                           AS loai_bds,
    CASE WHEN is_broker THEN 'Môi giới' ELSE 'Cá nhân' END                  AS nguoi_dang,
    count(*)                                                                AS so_tin,
    round(100 * count(*) / sum(count(*)) OVER (PARTITION BY property_type), 1) AS ty_le_pct,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                    AS median_gia_m2_trieu,
    round(percentile_approx(area_m2, 0.5), 0)                               AS median_dien_tich_m2
FROM gold.ml_price_features
WHERE {TYPES} AND is_broker IS NOT NULL
GROUP BY 1, 2
""",
        columns={"loai_bds": "STRING", "nguoi_dang": "STRING", "so_tin": "BIGINT", "ty_le_pct": "DOUBLE",
                 "median_gia_m2_trieu": "DOUBLE", "median_dien_tich_m2": "DOUBLE"},
    ),

    # ---- Phân tích: đi sâu từng loại hình ----------------------------------
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
        description="Nhà ở: giá/m² trung vị theo chiều ngang (mặt tiền lô) và khu vực.",
        sql="""
SELECT
    CASE WHEN width_m < 3.5 THEN '1. < 3,5 m' WHEN width_m < 4 THEN '2. 3,5–4 m' WHEN width_m < 5 THEN '3. 4–5 m'
         WHEN width_m < 7 THEN '4. 5–7 m' ELSE '5. ≥ 7 m' END               AS chieu_ngang,
    area_zone                                                            AS khu_vuc,
    count(*)                                                             AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                 AS median_gia_m2_trieu,
    round(percentile_approx(area_m2, 0.5), 0)                            AS median_dien_tich_m2
FROM gold.ml_price_features
WHERE property_category = 'HOUSE' AND width_m IS NOT NULL AND area_zone <> 'Không rõ'
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"chieu_ngang": "STRING", "khu_vuc": "STRING", "so_tin": "BIGINT",
                 "median_gia_m2_trieu": "DOUBLE", "median_dien_tich_m2": "DOUBLE"},
    ),
    "pt_can_ho_phong_ngu": dict(
        description="Căn hộ: giá bán và giá/m² trung vị theo số phòng ngủ và khu vực.",
        sql="""
SELECT
    cast(least(bedrooms, 4) AS int)                        AS so_phong_ngu,
    area_zone                                              AS khu_vuc,
    count(*)                                               AS so_tin,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)      AS median_gia_ty,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)   AS median_gia_m2_trieu,
    round(percentile_approx(area_m2, 0.5), 0)              AS median_dien_tich_m2
FROM gold.ml_price_features
WHERE property_category = 'APARTMENT' AND bedrooms >= 1 AND area_zone <> 'Không rõ'
GROUP BY 1, 2
HAVING count(*) >= 30
""",
        columns={"so_phong_ngu": "BIGINT", "khu_vuc": "STRING", "so_tin": "BIGINT", "median_gia_ty": "DOUBLE",
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
    project_name                                           AS du_an,
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

    # ---- Phân tích: diễn biến theo tuần -------------------------------------
    "tg_gia_tuan_tp": dict(
        description="Giá/m² và số tin đổi giá theo tuần, cấp toàn TP và nội/ngoại thành (agg_price_trend_weekly).",
        sql="""
SELECT
    period_start                                 AS tuan,
    geo_level                                    AS cap_do,
    CASE geo_level WHEN 'CITY' THEN 'Toàn TP' ELSE geo_name END AS khu_vuc,
    transaction_type_label                       AS giao_dich,
    property_type                                AS loai_bds,
    listing_count                                AS so_tin,
    new_listing_count                            AS tin_moi,
    price_cut_count                              AS tin_giam_gia,
    price_rise_count                             AS tin_tang_gia,
    round(p25_price_per_m2 / 1e6, 2)             AS p25_gia_m2_trieu,
    round(median_price_per_m2 / 1e6, 2)          AS median_gia_m2_trieu,
    round(p75_price_per_m2 / 1e6, 2)             AS p75_gia_m2_trieu,
    round(median_price_per_m2_change_pct, 2)     AS thay_doi_so_tuan_truoc_pct,
    is_reliable                                  AS du_tin_cay
FROM gold.agg_price_trend_weekly
WHERE geo_level IN ('CITY', 'ZONE')
""",
        columns={"tuan": "DATE", "cap_do": "STRING", "khu_vuc": "STRING", "giao_dich": "STRING", "loai_bds": "STRING",
                 "so_tin": "BIGINT", "tin_moi": "BIGINT", "tin_giam_gia": "BIGINT", "tin_tang_gia": "BIGINT",
                 "p25_gia_m2_trieu": "DOUBLE", "median_gia_m2_trieu": "DOUBLE", "p75_gia_m2_trieu": "DOUBLE",
                 "thay_doi_so_tuan_truoc_pct": "DOUBLE", "du_tin_cay": "BOOLEAN"},
        dttm="tuan",
    ),
    "tg_cung_tuan_tp": dict(
        description="Nguồn cung theo tuần, cấp toàn TP và nội/ngoại thành (agg_market_supply_weekly).",
        sql="""
SELECT
    period_start                      AS tuan,
    geo_level                         AS cap_do,
    CASE geo_level WHEN 'CITY' THEN 'Toàn TP' ELSE geo_name END AS khu_vuc,
    transaction_type_label            AS giao_dich,
    property_type                     AS loai_bds,
    active_listing_count              AS tin_dang_rao,
    new_listing_count                 AS tin_moi,
    removed_listing_count             AS tin_go,
    net_supply_change                 AS bien_dong_rong,
    is_reliable                       AS du_tin_cay
FROM gold.agg_market_supply_weekly
WHERE geo_level IN ('CITY', 'ZONE')
""",
        columns={"tuan": "DATE", "cap_do": "STRING", "khu_vuc": "STRING", "giao_dich": "STRING", "loai_bds": "STRING",
                 "tin_dang_rao": "BIGINT", "tin_moi": "BIGINT", "tin_go": "BIGINT", "bien_dong_rong": "BIGINT",
                 "du_tin_cay": "BOOLEAN"},
        dttm="tuan",
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


COMMON = {"color_scheme": "supersetColors", "extra_form_data": {}, "dashboards": []}


def big_number(name, dataset, sql, label, subheader, fmt="SMART_NUMBER", **extra):
    return dict(name=name, dataset=dataset, viz_type="big_number_total", params={
        **COMMON, "metric": metric(sql, label), "adhoc_filters": [], "subheader": subheader,
        "header_font_size": 0.4, "subheader_font_size": 0.125, "y_axis_format": fmt,
        "time_format": "smart_date", **extra})


def bar(name, dataset, x, metrics, series=None, filters=(), horizontal=False, stack=None,
        sort_by_metric=False, y_format="SMART_NUMBER", x_title="", y_title="", show_value=False,
        description=None, time_axis=False, row_limit=1000, order_desc=True):
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
        "y_axis_title_position": "Left", "sort_series_type": "sum",
        "only_total": True, "show_value": show_value,
        "show_legend": bool(series) or len(metrics) > 1, "legendType": "scroll", "legendOrientation": "top",
        "x_axis_time_format": "smart_date", "y_axis_format": y_format, "y_axis_bounds": [None, None],
        "rich_tooltip": True, "tooltipTimeFormat": "smart_date",
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

    # ---- Tổng quan: theo tuần --------------------------------------------
    "xu_huong_gia": line("Giá/m² trung vị theo tuần (tin bán, toàn TP)", "ov_xu_huong_gia_tuan", "tuan",
                         [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds",
                         filters=["giao_dich = 'Bán'", "du_tin_cay"], y_format=",.1f", y_title="Triệu đồng/m²",
                         time_axis=True,
                         description="Trung vị giá/m² của mọi tin bán còn rao trong tuần. Tuần ít tin bị bỏ (du_tin_cay)."),
    "nguon_cung": bar("Tin bán mới và tin bị gỡ theo tuần (toàn TP)", "ov_nguon_cung_tuan", "tuan",
                      [M("SUM(tin_moi)", "Tin mới"), M("SUM(tin_go)", "Tin gỡ")],
                      filters=["giao_dich = 'Bán'"], y_title="Số tin", time_axis=True,
                      description="Tin mới không tính tin đã có sẵn ở lần crawl đầu tiên. Tin gỡ gồm cả tin hết hạn sau 30 ngày, không chỉ tin đã bán."),

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

    # ---- Tổng quan: chất lượng dữ liệu --------------------------------------
    "lam_sach": funnel("Từ tin bán thu được tới tin sạch để phân tích giá", "ov_lam_sach", "buoc", "MAX(so_tin)", "Số tin"),
    "do_day_du": heatmap("Tỷ lệ tin có dữ liệu ở từng thuộc tính (%)", "ov_do_day_du", "thuoc_tinh", "loai_bds",
                         "MAX(ty_le_co_du_lieu_pct)", "% có dữ liệu"),

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
    "nguoi_dang": bar("Môi giới so với cá nhân: giá/m² trung vị", "pt_nguoi_dang", "loai_bds",
                      [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="nguoi_dang", y_format=",.1f",
                      y_title="Triệu đồng/m²"),

    # ---- Phân tích: đi sâu từng loại hình ----------------------------------
    "huong": bar("Giá/m² theo hướng nhà", "pt_huong", "huong",
                 [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="loai_bds", y_format=",.1f",
                 y_title="Triệu đồng/m²",
                 description="Chỉ tin có ghi hướng (khoảng một phần ba). Chênh lệch nhỏ so với vị trí và diện tích."),
    "chieu_ngang": line("Nhà ở: chiều ngang lô càng rộng, giá/m² càng cao?", "pt_nha_chieu_ngang", "chieu_ngang",
                        [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="khu_vuc", y_format=",.1f",
                        y_title="Triệu đồng/m²"),
    "phong_ngu": bar("Căn hộ: giá bán trung vị theo số phòng ngủ", "pt_can_ho_phong_ngu", "so_phong_ngu",
                     [M("MAX(median_gia_ty)", "Tỷ đồng")], series="khu_vuc", y_format=",.2f",
                     x_title="Số phòng ngủ (4 = từ 4)", y_title="Tỷ đồng"),
    "noi_that": bar("Căn hộ: giá/m² theo nội thất", "pt_can_ho_noi_that", "noi_that",
                    [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="ban_giao", y_format=",.1f",
                    y_title="Triệu đồng/m²"),
    "du_an": bar("Căn hộ: 20 dự án giá/m² cao nhất (từ 30 tin)", "pt_du_an", "du_an",
                 [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], horizontal=True, sort_by_metric=True,
                 y_format=",.1f", y_title="Triệu đồng/m²",
                 description="Lọc Quận để xem dự án trong một quận."),
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

    # ---- Phân tích: phân khúc giá ------------------------------------------
    "phan_khuc_tin": bar("Số tin bán theo phân khúc giá và loại hình", "tk_phan_khuc", "phan_khuc",
                         [M("SUM(so_tin)", "Số tin")], series="loai_bds", stack="Stack", y_title="Số tin",
                         description="Lọc Quận để xem cơ cấu phân khúc của một quận."),
    "phan_khuc_quan": bar("Cơ cấu phân khúc giá của từng quận (% số tin)", "tk_phan_khuc", "quan",
                          [M("SUM(so_tin)", "Số tin")], series="phan_khuc", stack="Expand", horizontal=True,
                          filters=["quan <> 'Không rõ'"], y_format=".0%",
                          description="Mỗi thanh là một quận, chia theo tỷ trọng từng phân khúc. Quận có phần tím/xanh đậm dài là quận bán tầm giá cao."),
    "phan_khuc_khu_vuc": bar("Phân khúc giá: nội thành so với ngoại thành", "tk_phan_khuc", "khu_vuc",
                             [M("SUM(so_tin)", "Số tin")], series="phan_khuc", stack="Stack", y_title="Số tin",
                             filters=["khu_vuc <> 'Không rõ'"]),

    # ---- Phân tích: diễn biến theo tuần -------------------------------------
    "tg_gia_khu_vuc": line("Giá/m² trung vị theo tuần: nội thành so với ngoại thành", "tg_gia_tuan_tp", "tuan",
                           [M("MAX(median_gia_m2_trieu)", "Triệu/m²")], series="khu_vuc",
                           filters=["cap_do = 'ZONE'", "giao_dich = 'Bán'", "du_tin_cay"], y_format=",.1f",
                           y_title="Triệu đồng/m²",
                           description="Trung vị giá/m² của mọi tin bán còn rao trong tuần, theo loại hình đang chọn."),
    "tg_thay_doi": bar("Giá/m² toàn TP thay đổi so với tuần trước (%)", "tg_gia_tuan_tp", "tuan",
                       [M("MAX(thay_doi_so_tuan_truoc_pct)", "% so với tuần trước")], series="loai_bds",
                       filters=["cap_do = 'CITY'", "giao_dich = 'Bán'", "thay_doi_so_tuan_truoc_pct IS NOT NULL"],
                       y_format=",.2f", description="Dao động vài phần trăm quanh 0 là bình thường: tin mới đăng và tin hết hạn mỗi tuần làm trung vị nhích lên xuống."),
    "tg_dang_rao": line("Tin bán đang rao theo tuần", "tg_cung_tuan_tp", "tuan",
                        [M("MAX(tin_dang_rao)", "Tin đang rao")], series="khu_vuc",
                        filters=["cap_do = 'ZONE'", "giao_dich = 'Bán'"], y_title="Số tin",
                        description="Thị trường đang phình hay co: số tin bán còn hiển thị mỗi tuần."),
    "tg_tin_moi": bar("Tin bán mới theo tuần: nội thành so với ngoại thành", "tg_cung_tuan_tp", "tuan",
                      [M("SUM(tin_moi)", "Tin mới")], series="khu_vuc", stack="Stack",
                      filters=["cap_do = 'ZONE'", "giao_dich = 'Bán'"], y_title="Số tin"),
    "tg_doi_gia": bar("Tin tăng giá và giảm giá theo tuần (toàn TP)", "tg_gia_tuan_tp", "tuan",
                      [M("SUM(tin_tang_gia)", "Tăng giá"), M("SUM(tin_giam_gia)", "Giảm giá")],
                      filters=["cap_do = 'CITY'", "giao_dich = 'Bán'"], y_title="Số tin",
                      description="Giảm nhiều hơn tăng kéo dài nhiều tuần là tín hiệu thị trường đang hạ nhiệt."),

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
        title="Tổng quan thị trường BĐS TP.HCM",
        slug="tong-quan-thi-truong",
        description="Thị trường có gì, ở đâu, giá bao nhiêu, và giá cùng nguồn cung đổi thế nào theo tuần.",
        filters=[("Loại hình", "loai_bds", "ov_tin_theo_loai_quan"),
                 ("Quận", "quan", "ov_tin_theo_loai_quan"),
                 ("Khu vực", "khu_vuc", "ov_tin_theo_loai_quan")],
        rows=[
            [md("### Thị trường bất động sản TP.HCM qua dữ liệu Nhà Tốt\n"
                "Nguồn: tầng Gold của lakehouse, cập nhật sau mỗi lần crawl hằng ngày. "
                "Giá bán tính trên tin bán đã làm sạch và loại giá bất thường. "
                "Bộ lọc **Loại hình**, **Quận**, **Khu vực** áp dụng cho các chart có cột tương ứng; "
                "các chart theo tuần tính trên toàn thành phố.", 14)],
            [("kpi_ban", 2, 32), ("kpi_thue", 2, 32), ("kpi_moi", 2, 32), ("kpi_gia", 2, 32), ("kpi_gia_ty", 2, 32), ("kpi_sach", 2, 32)],
            [md("#### 1. Thị trường có gì và ở đâu")],
            [("co_cau", 5, 50), ("theo_quan", 7, 50)],
            [md("#### 2. Giá và nguồn cung đổi thế nào theo tuần")],
            [("xu_huong_gia", 7, 50), ("nguon_cung", 5, 50)],
            [md("#### 3. Giá theo quận")],
            [("gia_quan", 7, 70), ("gia_thue_quan", 5, 70)],
            [md("#### 4. Phân bố giá và diện tích (tin bán)")],
            [("pb_gia", 4, 40), ("pb_gia_m2", 4, 40), ("pb_dien_tich", 4, 40)],
            [md("#### 5. Dữ liệu sạch đến đâu")],
            [("lam_sach", 5, 45), ("do_day_du", 7, 45)],
        ],
    ),
    "phan_tich": dict(
        title="Phân tích giá và thị trường BĐS TP.HCM",
        slug="phan-tich-gia-thi-truong",
        description="Yếu tố nào quyết định giá, từng loại hình khác nhau ra sao, vị trí chi tiết và phân khúc giá.",
        filters=[("Loại hình", "loai_bds", "ov_gia_ban_theo_quan", "Nhà ở"),
                 ("Quận", "quan", "ov_gia_ban_theo_quan"),
                 ("Khu vực", "khu_vuc", "ov_gia_ban_theo_quan")],
        rows=[
            [md("### Phân tích giá bất động sản TP.HCM\n"
                "Tin bán đã làm sạch (`gold.ml_price_features`), giá/m² tính bằng triệu đồng, dùng trung vị, "
                "nhóm dưới 30 tin bị loại. Bộ lọc **Loại hình** mặc định là nhà ở; chọn thêm để so sánh các loại. **A** xếp hạng các yếu tố và xem từng yếu tố sau khi đã so cùng quận, cùng diện tích. "
                "**B** đi sâu vào đặc điểm riêng của nhà ở, căn hộ, đất. **C** nhìn vị trí ở mức phường. "
                "**D** cho biết thị trường tập trung ở phân khúc giá nào. **E** dùng dữ liệu lịch sử để xem giá và nguồn cung đổi thế nào theo tuần.", 16)],
            [md("#### A. Yếu tố nào quyết định giá?")],
            [("yeu_to", 6, 60), ("khoang_cach", 6, 60)],
            [("dien_tich", 6, 50), ("dac_diem", 6, 50)],
            [("phap_ly", 4, 45), ("nguoi_dang", 4, 45), ("huong", 4, 45)],
            [md("#### B. Đi sâu từng loại hình")],
            [("so_tang", 6, 50), ("chieu_ngang", 6, 50)],
            [("phong_ngu", 4, 50), ("noi_that", 4, 50), ("loai_dat", 4, 50)],
            [("du_an", 12, 70)],
            [md("#### C. Vị trí ở mức phường")],
            [("phuong_cao", 6, 50), ("phuong_thap", 6, 50)],
            [("quan_dien_tich", 12, 70)],
            [md("#### D. Phân khúc giá")],
            [("phan_khuc_tin", 6, 50), ("phan_khuc_khu_vuc", 6, 50)],
            [("phan_khuc_quan", 12, 70)],
            [md("#### E. Diễn biến theo tuần (dữ liệu lịch sử từ 05/10/2026)\n"
                "Mỗi lần crawl là một ảnh chụp thị trường; các chart dưới đây so các ảnh chụp với nhau. "
                "Mới có vài tuần nên chỉ nên đọc chiều hướng, chưa nên kết luận xu hướng dài hạn.", 12)],
            [("tg_gia_khu_vuc", 6, 50), ("tg_thay_doi", 6, 50)],
            [("tg_dang_rao", 4, 45), ("tg_tin_moi", 4, 45), ("tg_doi_gia", 4, 45)],
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
