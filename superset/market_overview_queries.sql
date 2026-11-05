-- =============================================================================
-- Dashboard "Tổng quan tập dữ liệu BĐS TP.HCM" - query cho Superset SQL Lab.
-- Mỗi query: dán vào SQL Lab -> Save dataset -> vẽ chart ghi ở đầu query.
-- Sắp chart trên dashboard theo đúng thứ tự:
--   Phần A. Tổng quan  (Q1 -> Q9):  dữ liệu có gì.
--   Phần B. Phân tích  (Q10 -> Q12): vì sao nhà đắt / rẻ (so cùng quận, cùng diện tích).
--
-- Đơn vị: giá bán = tỷ đồng, giá/m² = triệu đồng, giá thuê = triệu đồng/tháng.
-- Số tin lấy từ gold.fct_listing; giá bán lấy từ gold.ml_price_features
-- (tin bán đã làm sạch, đã loại outlier).
-- =============================================================================


-- =============================================================================
-- PHẦN A. TỔNG QUAN
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q1. Tập dữ liệu trong vài con số
-- Chart: Big Number, mỗi cột một chart.
-- -----------------------------------------------------------------------------
SELECT
    (SELECT count(*) FROM silver.int_crawl_snapshots)                       AS so_lan_crawl,
    (SELECT to_date(min(snapshot_started_at)) FROM silver.int_crawl_snapshots) AS tu_ngay,
    (SELECT to_date(max(snapshot_started_at)) FROM silver.int_crawl_snapshots) AS den_ngay,
    (SELECT count(*) FROM silver.stg_nhatot_properties)                     AS so_ban_ghi_crawl,
    (SELECT count(*) FROM gold.fct_listing WHERE is_market_eligible)        AS so_tin,
    (SELECT count(*) FROM gold.fct_listing
      WHERE is_market_eligible AND transaction_type = 'sale')               AS so_tin_ban,
    (SELECT count(*) FROM gold.fct_listing
      WHERE is_market_eligible AND transaction_type = 'rent')               AS so_tin_thue,
    (SELECT round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)
       FROM gold.ml_price_features
      WHERE property_category = 'HOUSE')                                    AS median_gia_m2_nha;


-- -----------------------------------------------------------------------------
-- Q2. Từ tin bán thu được, còn bao nhiêu tin sạch để phân tích giá?
-- Chart: Funnel. Dimension = buoc, Metric = MAX(so_tin)
-- Mọi bước đều đếm theo TIN (không đếm bản ghi crawl lặp lại).
-- -----------------------------------------------------------------------------
SELECT '1. Tin bán thu thập được' AS buoc, count(*) AS so_tin
  FROM gold.fct_listing
 WHERE transaction_type = 'sale' AND is_market_eligible
UNION ALL
SELECT '2. Đủ thông tin, giá không bất thường', count(*)
  FROM gold.fct_listing
 WHERE transaction_type = 'sale' AND is_price_training_eligible AND NOT is_price_outlier
UNION ALL
SELECT '3. Bỏ tin trùng, vị trí sai, văn phòng', count(*)
  FROM gold.ml_price_features;


-- -----------------------------------------------------------------------------
-- Q3. Tập dữ liệu gồm những loại BĐS nào, bán hay thuê?
-- Chart: Bar Chart (stacked). X = loai_bds, Series = giao_dich, Metric = SUM(so_tin)
-- -----------------------------------------------------------------------------
SELECT
    pt.property_type                                                 AS loai_bds,
    CASE f.transaction_type WHEN 'sale' THEN 'Bán' ELSE 'Cho thuê' END AS giao_dich,
    count(*)                                                         AS so_tin
FROM gold.fct_listing f
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
WHERE f.is_market_eligible
GROUP BY pt.property_type, f.transaction_type;


-- -----------------------------------------------------------------------------
-- Q4. Tin tập trung ở quận nào?
-- Chart: Bar Chart (ngang, stacked). X = quan, Series = giao_dich, Metric = SUM(so_tin)
-- -----------------------------------------------------------------------------
SELECT
    loc.district                                                     AS quan,
    CASE f.transaction_type WHEN 'sale' THEN 'Bán' ELSE 'Cho thuê' END AS giao_dich,
    count(*)                                                         AS so_tin
FROM gold.fct_listing f
JOIN gold.dim_location loc ON f.location_key = loc.location_key
WHERE f.is_market_eligible
  AND loc.district <> 'Không rõ'
GROUP BY loc.district, f.transaction_type;


-- -----------------------------------------------------------------------------
-- Q5. Mỗi thuộc tính được điền đầy đủ bao nhiêu %?
-- Chart: Heatmap. X = thuoc_tinh, Y = loai_bds, Metric = MAX(ty_le_co_du_lieu_pct)
-- -----------------------------------------------------------------------------
WITH coverage AS (
    SELECT
        trim(property_type)                                          AS loai_bds,
        avg(cast(ward_id         IS NOT NULL AS int)) * 100          AS phuong,
        avg(cast(street          IS NOT NULL AS int)) * 100          AS duong,
        avg(cast(bedrooms        IS NOT NULL AS int)) * 100          AS phong_ngu,
        avg(cast(bathrooms       IS NOT NULL AS int)) * 100          AS phong_tam,
        avg(cast(total_floors    IS NOT NULL AS int)) * 100          AS so_tang,
        avg(cast(width_m         IS NOT NULL AS int)) * 100          AS chieu_ngang,
        avg(cast(house_direction IS NOT NULL AS int)) * 100          AS huong,
        avg(cast(legal_status    IS NOT NULL AS int)) * 100          AS phap_ly,
        avg(cast(furnishing      IS NOT NULL AS int)) * 100          AS noi_that
    FROM silver.listing_current
    WHERE is_market_eligible
    GROUP BY trim(property_type)
)

SELECT loai_bds, thuoc_tinh, round(ty_le, 1) AS ty_le_co_du_lieu_pct
FROM coverage
LATERAL VIEW stack(9,
    'Phường',      phuong,
    'Đường',       duong,
    'Phòng ngủ',   phong_ngu,
    'Phòng tắm',   phong_tam,
    'Số tầng',     so_tang,
    'Chiều ngang', chieu_ngang,
    'Hướng nhà',   huong,
    'Pháp lý',     phap_ly,
    'Nội thất',    noi_that
) t AS thuoc_tinh, ty_le;


-- -----------------------------------------------------------------------------
-- Q6. Giá và diện tích phân bố ra sao?
-- Chart: Histogram, mỗi cột một chart (gia_ty, gia_m2_trieu, dien_tich_m2).
--        Thêm filter loai_bds.
-- -----------------------------------------------------------------------------
SELECT
    property_type                                                    AS loai_bds,
    round(price_vnd / 1e9, 2)                                        AS gia_ty,
    round(price_per_m2 / 1e6, 1)                                     AS gia_m2_trieu,
    area_m2                                                          AS dien_tich_m2
FROM gold.ml_price_features;


-- -----------------------------------------------------------------------------
-- Q7. Giá/m² nhà ở mỗi quận bao nhiêu?
-- Chart: Bar Chart (ngang). X = quan, Metric = MAX(median_gia_m2_trieu), sắp giảm dần.
-- -----------------------------------------------------------------------------
SELECT
    district                                                         AS quan,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)             AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category = 'HOUSE'
GROUP BY district
HAVING count(*) >= 30;


-- -----------------------------------------------------------------------------
-- Q8. Giá của tin mới đăng thay đổi thế nào theo thời gian?
-- Chart: Line Chart. X = tuan_dang, Series = loai_bds, Metric = MAX(median_gia_m2_trieu)
-- Tính theo NGÀY ĐĂNG TIN: mỗi tin thuộc đúng một tuần, nên các tuần không
-- chồng lên nhau (khác với tính theo ngày crawl, nơi một tin còn rao được đếm
-- lại ở mọi tuần). Tin đăng tập trung từ 09/2026, trước đó gần như không có
-- (tin hết hạn trước khi bắt đầu crawl), nên chỉ lấy từ 07/09/2026.
-- Tuần cuối có thể thiếu tin vì tin mới đăng chưa kịp được crawl.
-- -----------------------------------------------------------------------------
SELECT
    cast(date_trunc('week', posted_date) AS date)                    AS tuan_dang,
    property_type                                                    AS loai_bds,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)             AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
  AND posted_date >= '2026-09-07'   -- thứ Hai, để tuần đầu đủ 7 ngày
GROUP BY 1, 2
HAVING count(*) >= 30;


-- -----------------------------------------------------------------------------
-- Q9. Giá thuê mỗi quận bao nhiêu?
-- Chart: Bar Chart (grouped). X = quan, Series = loai_bds,
--        Metric = MAX(median_gia_thue_trieu_thang)
-- -----------------------------------------------------------------------------
SELECT
    loc.district                                                     AS quan,
    pt.property_type                                                 AS loai_bds,
    count(*)                                                         AS so_tin,
    round(percentile_approx(f.current_price_vnd, 0.5) / 1e6, 1)      AS median_gia_thue_trieu_thang
FROM gold.fct_listing f
JOIN gold.dim_location loc     ON f.location_key = loc.location_key
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
WHERE f.transaction_type = 'rent'
  AND f.is_market_eligible
  AND NOT f.is_price_outlier
  AND pt.property_category IN ('HOUSE', 'APARTMENT', 'ROOM')
  AND loc.district <> 'Không rõ'
GROUP BY loc.district, pt.property_type
HAVING count(*) >= 30;


-- =============================================================================
-- PHẦN B. PHÂN TÍCH: VÌ SAO NHÀ ĐẮT / RẺ?
-- Thứ tự: vị trí (Q7) -> diện tích (Q10) -> đặc điểm, pháp lý (Q11)
--         -> số tầng (Q12).
-- Vị trí và diện tích là hai yếu tố mạnh nhất, nên Q11 - Q12 so sánh nhà
-- CÙNG QUẬN và CÙNG NHÓM DIỆN TÍCH, để chênh lệch còn lại là của chính yếu tố đó.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q10. Diện tích càng lớn thì giá/m² cao hay thấp hơn?
-- Chart: Line Chart. X = nhom_dien_tich, Series = loai_bds,
--        Metric = MAX(median_gia_m2_trieu)
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN area_m2 < 30  THEN '1. < 30 m²'
        WHEN area_m2 < 50  THEN '2. 30–50 m²'
        WHEN area_m2 < 80  THEN '3. 50–80 m²'
        WHEN area_m2 < 120 THEN '4. 80–120 m²'
        WHEN area_m2 < 200 THEN '5. 120–200 m²'
        ELSE                    '6. ≥ 200 m²'
    END                                                              AS nhom_dien_tich,
    property_type                                                    AS loai_bds,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)             AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
GROUP BY 1, 2
HAVING count(*) >= 30;


-- -----------------------------------------------------------------------------
-- Q11. Đặc điểm và pháp lý nào làm nhà ở đắt hơn?
-- Chart: Bar Chart (ngang). X = dac_diem,
--        Metrics = MAX(chenh_lech_tho_pct), MAX(chenh_lech_cung_quan_dt_pct).
-- chenh_lech_tho_pct:         so mọi nhà "có" với mọi nhà "không".
-- chenh_lech_cung_quan_dt_pct: chỉ so nhà cùng quận, cùng nhóm diện tích, rồi
--                              lấy trung bình các nhóm (trọng số = số tin "có").
-- Hai cột lệch nhau nhiều = phần lớn chênh lệch thô là do vị trí / diện tích.
-- -----------------------------------------------------------------------------
WITH flags AS (
    SELECT
        district,
        CASE
            WHEN area_m2 < 30  THEN 1
            WHEN area_m2 < 50  THEN 2
            WHEN area_m2 < 80  THEN 3
            WHEN area_m2 < 120 THEN 4
            WHEN area_m2 < 200 THEN 5
            ELSE                    6
        END                                                          AS nhom_dt,
        price_per_m2,
        dac_diem,
        co
    FROM gold.ml_price_features
    LATERAL VIEW stack(7,
        'Mặt tiền',            is_frontage,
        'Hẻm xe hơi',          has_car_alley,
        'Nở hậu',              is_wide_back,
        'Thuộc dự án',         is_in_project,
        'Nhà nát / xuống cấp', is_dilapidated,
        'Dính quy hoạch',      is_planning_affected,
        'Có sổ',               CASE WHEN legal_category <> 'Không rõ'
                                    THEN legal_category = 'Có sổ' END
    ) t AS dac_diem, co
    WHERE property_category = 'HOUSE'
      AND co IS NOT NULL
),

raw AS (
    SELECT
        dac_diem,
        count(CASE WHEN co THEN 1 END)                               AS so_tin_co,
        percentile_approx(CASE WHEN co THEN price_per_m2 END, 0.5)
          / percentile_approx(CASE WHEN NOT co THEN price_per_m2 END, 0.5) - 1 AS tho
    FROM flags
    GROUP BY dac_diem
),

cells AS (
    -- Mỗi ô = một quận x một nhóm diện tích, cần đủ tin ở cả hai phía.
    SELECT
        dac_diem,
        count(CASE WHEN co THEN 1 END)                               AS n_co,
        percentile_approx(CASE WHEN co THEN price_per_m2 END, 0.5)
          / percentile_approx(CASE WHEN NOT co THEN price_per_m2 END, 0.5) - 1 AS chenh
    FROM flags
    GROUP BY dac_diem, district, nhom_dt
    HAVING count(CASE WHEN co THEN 1 END) >= 5
       AND count(CASE WHEN NOT co THEN 1 END) >= 5
),

controlled AS (
    SELECT dac_diem, sum(chenh * n_co) / sum(n_co) AS cung_quan_dt
    FROM cells
    GROUP BY dac_diem
)

SELECT
    r.dac_diem,
    r.so_tin_co,
    round(r.tho * 100, 1)                                            AS chenh_lech_tho_pct,
    round(c.cung_quan_dt * 100, 1)                                   AS chenh_lech_cung_quan_dt_pct
FROM raw r
JOIN controlled c ON r.dac_diem = c.dac_diem
WHERE r.so_tin_co >= 30;


-- -----------------------------------------------------------------------------
-- Q12. Cùng diện tích, nhà nhiều tầng hơn thì đắt hơn bao nhiêu?
-- Chart: Line Chart. X = so_tang, Series = nhom_dien_tich,
--        Metric = MAX(median_gia_ty). Filter khu_vuc (Nội thành / Ngoại thành).
-- Mỗi đường là một nhóm diện tích, nên độ dốc của đường là ảnh hưởng của số tầng.
-- -----------------------------------------------------------------------------
SELECT
    least(total_floors, 6)                                           AS so_tang,   -- 6 = từ 6 tầng
    CASE
        WHEN area_m2 < 50  THEN '1. < 50 m²'
        WHEN area_m2 < 80  THEN '2. 50–80 m²'
        WHEN area_m2 < 120 THEN '3. 80–120 m²'
        ELSE                    '4. ≥ 120 m²'
    END                                                              AS nhom_dien_tich,
    area_zone                                                        AS khu_vuc,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)                AS median_gia_ty
FROM gold.ml_price_features
WHERE property_category = 'HOUSE'
  AND total_floors >= 1
  AND area_zone <> 'Không rõ'
GROUP BY 1, 2, 3
HAVING count(*) >= 30;
