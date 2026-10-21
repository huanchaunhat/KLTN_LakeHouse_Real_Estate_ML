-- =============================================================================
-- Dashboard "Tổng quan thị trường BĐS TP.HCM" - query cho Superset SQL Lab (Spark SQL).
-- Mỗi query trả lời một câu hỏi, dán vào SQL Lab rồi "Save dataset" và vẽ chart.
--
-- Mạch câu chuyện (sắp chart trên dashboard theo đúng thứ tự này):
--   0. KPI             Bức tranh trong 5 con số.
--   1. Thị trường có gì?          Cơ cấu nguồn cung và nơi nguồn cung tập trung.
--   2. Giá bao nhiêu, ở đâu đắt?  Mặt bằng giá, bản đồ giá, chênh lệch theo khu vực.
--   3. Vì sao đắt / rẻ?           Diện tích, đặc điểm nhà, pháp lý.
--   4. Thị trường đang đi về đâu? Xu hướng giá, cung mới vs tin gỡ, giảm giá.
--   5. Bán có dễ không, thuê thì sao?  Thanh khoản, giá thuê, tỷ suất cho thuê.
-- Phần chất lượng dữ liệu / chuẩn bị train nằm ở ml_data_quality_queries.sql.
--
-- Quy ước:
--   * Giá/m² tính bằng triệu đồng, giá bán tính bằng tỷ đồng,
--     giá thuê tính bằng triệu đồng / tháng.
--   * Cung (số tin) dùng gold.fct_listing, chỉ lấy tin is_market_eligible.
--   * Giá bán dùng gold.ml_price_features: tin BÁN, trạng thái mới nhất của mọi
--     tin đã thấy trong kỳ crawl (gồm cả tin đã gỡ), đã loại outlier.
--     -> Ghi chú trên dashboard: "Số tin = tin đang rao; Giá = mọi tin bán đã
--        quan sát, đã loại giá bất thường", để số liệu hai phần không bị hỏi vì
--        sao lệch nhau.
--   * Xu hướng theo thời gian dùng các mart agg_*_weekly, luôn lọc is_reliable.
--   * Nhóm có quá ít tin bị loại (HAVING) để median không bị nhiễu.
-- =============================================================================


-- =============================================================================
-- 0. KPI (Big Number) ở đầu dashboard
-- =============================================================================
WITH latest_week_change AS (
    -- Thay đổi median giá/m² nhà ở toàn thành phố của tuần gần nhất có đủ mẫu.
    SELECT max_by(median_price_per_m2_change_pct, period_start) AS change_pct
    FROM gold.agg_price_trend_weekly
    WHERE geo_level = 'CITY'
      AND transaction_type = 'sale'
      AND property_type = 'Nhà ở'
      AND is_reliable
      AND median_price_per_m2_change_pct IS NOT NULL
)

SELECT
    (SELECT count(*) FROM gold.fct_listing
      WHERE is_active AND is_market_eligible AND transaction_type = 'sale') AS so_tin_ban_dang_rao,
    (SELECT count(*) FROM gold.fct_listing
      WHERE is_active AND is_market_eligible AND transaction_type = 'rent') AS so_tin_thue_dang_rao,
    (SELECT round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)
       FROM gold.ml_price_features
      WHERE property_category = 'HOUSE')                                    AS median_gia_m2_nha_ban_trieu,
    (SELECT round(change_pct, 2) FROM latest_week_change)                   AS thay_doi_gia_m2_tuan_pct,
    (SELECT round(avg(cast(total_price_change_pct < 0 AS int)) * 100, 2)
       FROM gold.fct_listing
      WHERE is_market_eligible AND NOT is_price_outlier)                    AS ty_le_tin_da_giam_gia_pct;


-- =============================================================================
-- 1. THỊ TRƯỜNG CÓ GÌ?
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1.1. Cơ cấu thị trường theo loại BĐS và hình thức bán / thuê?
-- Chart: Bar Chart (stacked). X = loai_bds, Series = giao_dich, Metric = SUM(so_tin)
-- -----------------------------------------------------------------------------
SELECT
    pt.property_type                                                 AS loai_bds,
    CASE f.transaction_type WHEN 'sale' THEN 'Bán' ELSE 'Cho thuê' END AS giao_dich,
    count(*)                                                         AS so_tin
FROM gold.fct_listing f
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
WHERE f.is_active
  AND f.is_market_eligible
GROUP BY pt.property_type, f.transaction_type
ORDER BY so_tin DESC;


-- -----------------------------------------------------------------------------
-- Q1.2. Nguồn cung tập trung ở những quận nào?
-- Chart: Bar Chart (ngang, stacked). X = quan, Series = giao_dich, Metric = SUM(so_tin)
-- -----------------------------------------------------------------------------
SELECT
    loc.district                                                     AS quan,
    loc.area_zone                                                    AS khu_vuc,
    CASE f.transaction_type WHEN 'sale' THEN 'Bán' ELSE 'Cho thuê' END AS giao_dich,
    count(*)                                                         AS so_tin
FROM gold.fct_listing f
JOIN gold.dim_location loc ON f.location_key = loc.location_key
WHERE f.is_active
  AND f.is_market_eligible
  AND loc.district <> 'Không rõ'
GROUP BY loc.district, loc.area_zone, f.transaction_type
ORDER BY so_tin DESC;


-- =============================================================================
-- 2. GIÁ BAO NHIÊU, Ở ĐÂU ĐẮT?
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q2.1. Giá bán phổ biến nằm ở khoảng nào, khác nhau ra sao giữa các loại BĐS?
-- Chart: Bar Chart (grouped). X = khoang_gia (đã đánh số để giữ thứ tự),
--        Series = nhom_bds, Metric = SUM(so_tin)
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN price_vnd < 1e9  THEN '01. < 1 tỷ'
        WHEN price_vnd < 2e9  THEN '02. 1–2 tỷ'
        WHEN price_vnd < 3e9  THEN '03. 2–3 tỷ'
        WHEN price_vnd < 5e9  THEN '04. 3–5 tỷ'
        WHEN price_vnd < 7e9  THEN '05. 5–7 tỷ'
        WHEN price_vnd < 10e9 THEN '06. 7–10 tỷ'
        WHEN price_vnd < 15e9 THEN '07. 10–15 tỷ'
        WHEN price_vnd < 20e9 THEN '08. 15–20 tỷ'
        WHEN price_vnd < 30e9 THEN '09. 20–30 tỷ'
        WHEN price_vnd < 50e9 THEN '10. 30–50 tỷ'
        ELSE                       '11. ≥ 50 tỷ'
    END                                                              AS khoang_gia,
    property_type                                                    AS nhom_bds,
    count(*)                                                         AS so_tin
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
GROUP BY 1, 2
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q2.2. Nhìn trên bản đồ, vùng nào đắt, vùng nào rẻ?
-- Chart: deck.gl Scatterplot. Longitude = kinh_do, Latitude = vi_do,
--        màu / kích thước theo gia_m2_trieu (cần MAPBOX_API_KEY cho bản đồ nền).
--        Thêm filter loai_bds để so cùng loại.
-- Khung toạ độ giới hạn quanh TP.HCM để loại toạ độ sai.
-- -----------------------------------------------------------------------------
SELECT
    listing_key,
    latitude                                                         AS vi_do,
    longitude                                                        AS kinh_do,
    district                                                         AS quan,
    property_type                                                    AS loai_bds,
    round(price_per_m2 / 1e6, 1)                                     AS gia_m2_trieu,
    round(price_vnd / 1e9, 2)                                        AS gia_ty
FROM gold.ml_price_features
WHERE latitude  BETWEEN 10.3 AND 11.2
  AND longitude BETWEEN 106.3 AND 107.1;


-- -----------------------------------------------------------------------------
-- Q2.3. Giá/m² nhà ở chênh lệch giữa các quận lớn đến mức nào?
-- Chart: Box Plot. Dimensions = quan, Distribute across = listing_key,
--        Metric = MAX(gia_m2_trieu), Row limit = 50000.
-- Q2.3b: bảng tứ phân vị, dùng chart Table hoặc Bar (median_gia_m2_trieu).
-- -----------------------------------------------------------------------------
SELECT
    listing_key,
    district                                                         AS quan,
    area_zone                                                        AS khu_vuc,
    round(price_per_m2 / 1e6, 1)                                     AS gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category = 'HOUSE';

-- Q2.3b
SELECT
    district                                                         AS quan,
    area_zone                                                        AS khu_vuc,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_per_m2, 0.25) / 1e6, 1)            AS p25_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.5)  / 1e6, 1)            AS median_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.75) / 1e6, 1)            AS p75_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)                AS median_gia_ty
FROM gold.ml_price_features
WHERE property_category = 'HOUSE'
GROUP BY district, area_zone
HAVING count(*) >= 30
ORDER BY median_gia_m2_trieu DESC;


-- -----------------------------------------------------------------------------
-- Q2.4. Khoảng cách nội thành - ngoại thành có giống nhau giữa các loại BĐS?
-- Chart: Bar Chart (grouped). X = loai_bds, Series = khu_vuc, Metric = MAX(median_gia_m2_trieu)
-- -----------------------------------------------------------------------------
SELECT
    property_type                                                    AS loai_bds,
    area_zone                                                        AS khu_vuc,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)             AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE area_zone <> 'Không rõ'
GROUP BY property_type, area_zone
HAVING count(*) >= 30
ORDER BY loai_bds, khu_vuc;


-- =============================================================================
-- 3. VÌ SAO ĐẮT / RẺ?
-- Các so sánh ở chương này là so sánh thô, chưa kiểm soát vị trí / diện tích:
-- dùng để gợi ý yếu tố ảnh hưởng giá (và feature cho mô hình), không phải nhân quả.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q3.1. Giá/m² thay đổi thế nào khi diện tích tăng?
-- Chart: Line Chart. X = nhom_dien_tich, Series = nhom_bds,
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
    property_type                                                    AS nhom_bds,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)             AS median_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)                AS median_gia_ty
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
GROUP BY 1, 2
HAVING count(*) >= 30
ORDER BY nhom_bds, nhom_dien_tich;


-- -----------------------------------------------------------------------------
-- Q3.2. Đặc điểm nào làm giá/m² nhà ở cao hơn (hoặc thấp hơn)?
-- Chart: Bar Chart. X = dac_diem, Metric = MAX(chenh_lech_pct), sắp xếp giảm dần.
-- chenh_lech_pct = median nhóm "có" so với median nhóm "không", tính bằng %.
-- Pháp lý tách riêng ở Q3.3 vì has_certificate gộp "Không rõ" vào "không có sổ".
-- -----------------------------------------------------------------------------
WITH flags AS (
    SELECT price_per_m2, dac_diem, co_dac_diem
    FROM gold.ml_price_features
    LATERAL VIEW stack(6,
        'Mặt tiền',            is_frontage,
        'Hẻm xe hơi',          has_car_alley,
        'Nở hậu',              is_wide_back,
        'Thuộc dự án',         is_in_project,
        'Nhà nát / xuống cấp', is_dilapidated,
        'Dính quy hoạch',      is_planning_affected
    ) t AS dac_diem, co_dac_diem
    WHERE property_category = 'HOUSE'
      AND co_dac_diem IS NOT NULL
),

by_flag AS (
    SELECT
        dac_diem,
        count(CASE WHEN co_dac_diem THEN 1 END)                                 AS so_tin_co,
        count(CASE WHEN NOT co_dac_diem THEN 1 END)                             AS so_tin_khong,
        percentile_approx(CASE WHEN co_dac_diem THEN price_per_m2 END, 0.5)     AS median_co,
        percentile_approx(CASE WHEN NOT co_dac_diem THEN price_per_m2 END, 0.5) AS median_khong
    FROM flags
    GROUP BY dac_diem
)

SELECT
    dac_diem,
    so_tin_co,
    so_tin_khong,
    round(median_co / 1e6, 1)                                        AS median_co_trieu,
    round(median_khong / 1e6, 1)                                     AS median_khong_trieu,
    round((median_co - median_khong) / median_khong * 100, 1)        AS chenh_lech_pct
FROM by_flag
WHERE so_tin_co >= 30
  AND so_tin_khong >= 30
ORDER BY chenh_lech_pct DESC;


-- -----------------------------------------------------------------------------
-- Q3.3. Pháp lý ảnh hưởng đến giá/m² thế nào?
-- Chart: Bar Chart (grouped). X = phap_ly, Series = nhom_bds,
--        Metric = MAX(median_gia_m2_trieu). Bỏ nhóm "Không rõ" để không lẫn.
-- -----------------------------------------------------------------------------
SELECT
    legal_category                                                   AS phap_ly,
    property_type                                                    AS nhom_bds,
    count(*)                                                         AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)             AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'LAND')
  AND legal_category <> 'Không rõ'
GROUP BY legal_category, property_type
HAVING count(*) >= 30
ORDER BY nhom_bds, median_gia_m2_trieu DESC;


-- =============================================================================
-- 4. THỊ TRƯỜNG ĐANG ĐI VỀ ĐÂU?
-- Q4.1 dùng mart theo tuần (đã khử trùng tin crawl nhiều lần trong tuần).
-- Q4.2, Q4.3 tính theo ngày crawl từ fct_listing_event để có số liệu ngay cả khi
-- lịch sử còn ngắn; khi dữ liệu dài ra, đặt Time grain = Week / Month trong
-- Superset để gộp. Ngày / tuần cuối có thể chưa crawl xong.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q4.1. Giá/m² đang tăng hay giảm? (toàn thành phố, nội thành, ngoại thành)
-- Chart: Line Chart. X = tuan, Series = khu_vuc, Metric = MAX(median_gia_m2_trieu).
--        Thêm filter loai_bds (mặc định "Nhà ở") để so cùng loại.
-- -----------------------------------------------------------------------------
SELECT
    period_start                                                     AS tuan,
    geo_name                                                         AS khu_vuc,
    property_type                                                    AS loai_bds,
    listing_count                                                    AS so_tin,
    round(p25_price_per_m2 / 1e6, 1)                                 AS p25_gia_m2_trieu,
    round(median_price_per_m2 / 1e6, 1)                              AS median_gia_m2_trieu,
    round(p75_price_per_m2 / 1e6, 1)                                 AS p75_gia_m2_trieu,
    round(median_price_per_m2_change_pct, 2)                         AS thay_doi_so_voi_tuan_truoc_pct
FROM gold.agg_price_trend_weekly
WHERE transaction_type = 'sale'
  AND geo_level IN ('CITY', 'ZONE')
  AND geo_name <> 'Không rõ'
  AND property_category IN ('HOUSE', 'APARTMENT', 'LAND')
  AND is_reliable
ORDER BY loai_bds, khu_vuc, tuan;


-- -----------------------------------------------------------------------------
-- Q4.2. Nguồn cung đang phình ra hay co lại? (tin mới so với tin bị gỡ)
-- Chart: Mixed Chart. X = ngay. Bar = so_tin_moi, so_tin_go; Line = so_tin_dang_rao.
--        Filter giao_dich.
-- Tin của lần crawl đầu tiên đã có sẵn trên thị trường (is_initial_load) nên
-- không tính là tin mới.
-- -----------------------------------------------------------------------------
WITH active AS (
    SELECT
        crawl_date                                                   AS ngay,
        transaction_type,
        count(DISTINCT listing_key)                                  AS so_tin_dang_rao
    FROM gold.fct_listing_snapshot
    WHERE is_market_eligible
    GROUP BY crawl_date, transaction_type
),

flows AS (
    SELECT
        to_date(event_at)                                            AS ngay,
        transaction_type,
        count(DISTINCT CASE WHEN event_type = 'NEW' AND NOT is_initial_load
                            THEN listing_key END)                    AS so_tin_moi,
        count(DISTINCT CASE WHEN event_type = 'REMOVED'
                            THEN listing_key END)                    AS so_tin_go
    FROM gold.fct_listing_event
    WHERE is_market_eligible
    GROUP BY to_date(event_at), transaction_type
)

SELECT
    a.ngay,
    CASE a.transaction_type WHEN 'sale' THEN 'Bán' ELSE 'Cho thuê' END AS giao_dich,
    a.so_tin_dang_rao,
    coalesce(f.so_tin_moi, 0)                                        AS so_tin_moi,
    coalesce(f.so_tin_go, 0)                                         AS so_tin_go,
    coalesce(f.so_tin_moi, 0) - coalesce(f.so_tin_go, 0)             AS cung_rong
FROM active a
LEFT JOIN flows f
    ON a.ngay = f.ngay
   AND a.transaction_type = f.transaction_type
ORDER BY a.ngay, giao_dich;


-- -----------------------------------------------------------------------------
-- Q4.3. Người bán có đang phải giảm giá nhiều hơn?
-- Chart: Line Chart (2 trục). X = ngay, Series = loai_bds,
--        Metric = MAX(ty_le_tin_giam_gia_pct), trục phụ MAX(muc_giam_median_pct).
-- -----------------------------------------------------------------------------
WITH active AS (
    SELECT
        s.crawl_date                                                 AS ngay,
        pt.property_type                                             AS loai_bds,
        count(DISTINCT s.listing_key)                                AS so_tin_dang_rao
    FROM gold.fct_listing_snapshot s
    JOIN gold.dim_property_type pt ON s.property_type_key = pt.property_type_key
    WHERE s.transaction_type = 'sale'
      AND s.is_market_eligible
      AND pt.property_category IN ('HOUSE', 'APARTMENT', 'LAND')
    GROUP BY s.crawl_date, pt.property_type
),

cuts AS (
    SELECT
        to_date(e.event_at)                                          AS ngay,
        pt.property_type                                             AS loai_bds,
        count(DISTINCT e.listing_key)                                AS so_tin_giam_gia,
        percentile_approx(e.price_change_pct, 0.5)                   AS median_change_pct
    FROM gold.fct_listing_event e
    JOIN gold.dim_property_type pt ON e.property_type_key = pt.property_type_key
    WHERE e.transaction_type = 'sale'
      AND e.is_market_eligible
      AND NOT e.is_price_outlier
      AND e.is_price_decrease
    GROUP BY to_date(e.event_at), pt.property_type
)

SELECT
    a.ngay,
    a.loai_bds,
    a.so_tin_dang_rao,
    coalesce(c.so_tin_giam_gia, 0)                                   AS so_tin_giam_gia,
    round(coalesce(c.so_tin_giam_gia, 0) * 100.0 / a.so_tin_dang_rao, 2) AS ty_le_tin_giam_gia_pct,
    round(-c.median_change_pct, 1)                                   AS muc_giam_median_pct
FROM active a
LEFT JOIN cuts c
    ON a.ngay = c.ngay
   AND a.loai_bds = c.loai_bds
WHERE a.so_tin_dang_rao >= 100
ORDER BY a.loai_bds, a.ngay;


-- -----------------------------------------------------------------------------
-- Q4.4. Phân khúc nào giảm giá nhiều nhất? (cộng dồn từ lúc bắt đầu theo dõi)
-- Chart: Heatmap. X = khu_vuc, Y = loai_bds, Metric = MAX(ty_le_tin_da_giam_pct);
--        tooltip muc_giam_median_pct.
-- -----------------------------------------------------------------------------
SELECT
    loc.area_zone                                                    AS khu_vuc,
    pt.property_type                                                 AS loai_bds,
    count(*)                                                         AS so_tin,
    round(avg(CASE WHEN f.total_price_change_pct < 0 THEN 1.0 ELSE 0.0 END) * 100, 2)
                                                                     AS ty_le_tin_da_giam_pct,
    round(-percentile_approx(CASE WHEN f.total_price_change_pct < 0
                                  THEN f.total_price_change_pct END, 0.5), 1)
                                                                     AS muc_giam_median_pct
FROM gold.fct_listing f
JOIN gold.dim_location loc     ON f.location_key = loc.location_key
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
WHERE f.transaction_type = 'sale'
  AND f.is_market_eligible
  AND NOT f.is_price_outlier
  AND loc.area_zone <> 'Không rõ'
GROUP BY loc.area_zone, pt.property_type
HAVING count(*) >= 30
ORDER BY ty_le_tin_da_giam_pct DESC;


-- =============================================================================
-- 5. BÁN CÓ DỄ KHÔNG, THUÊ THÌ SAO?
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q5.1. Phân khúc nào rời thị trường nhanh nhất? (tin bán, theo quận x loại BĐS)
-- Chart: Heatmap. X = quan, Y = loai_bds, Metric = MAX(ty_le_go_moi_tuan_pct)
-- ty_le_go_moi_tuan_pct = tổng tin gỡ / tổng tin đang rao, cộng qua các tuần.
-- Tính theo tuần thay vì "% tin đã gỡ" để tin xuất hiện muộn (ít thời gian bị gỡ)
-- không kéo tỷ lệ xuống.
-- Lưu ý: tin bị gỡ chỉ là proxy cho giao dịch (có thể hết hạn, rút tin, crawl sót).
-- -----------------------------------------------------------------------------
WITH weekly AS (
    SELECT
        geo_name                                                     AS quan,
        property_type                                                AS loai_bds,
        sum(active_listing_count)                                    AS tin_tuan,
        sum(removed_listing_count)                                   AS so_tin_go
    FROM gold.agg_market_supply_weekly
    WHERE geo_level = 'DISTRICT'
      AND transaction_type = 'sale'
      AND geo_name <> 'Không rõ'
    GROUP BY geo_name, property_type
),

days_on_market AS (
    SELECT
        loc.district                                                 AS quan,
        pt.property_type                                             AS loai_bds,
        percentile_approx(f.days_on_market, 0.5)                     AS median_ngay_rao_khi_go
    FROM gold.fct_listing f
    JOIN gold.dim_location loc     ON f.location_key = loc.location_key
    JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
    WHERE f.transaction_type = 'sale'
      AND f.is_market_eligible
      AND NOT f.is_active
    GROUP BY loc.district, pt.property_type
)

SELECT
    w.quan,
    w.loai_bds,
    w.tin_tuan,
    w.so_tin_go,
    round(w.so_tin_go * 100.0 / w.tin_tuan, 2)                       AS ty_le_go_moi_tuan_pct,
    d.median_ngay_rao_khi_go
FROM weekly w
LEFT JOIN days_on_market d
    ON w.quan = d.quan AND w.loai_bds = d.loai_bds
WHERE w.tin_tuan >= 100
ORDER BY ty_le_go_moi_tuan_pct DESC;


-- -----------------------------------------------------------------------------
-- Q5.2. Giá thuê ở mỗi quận bao nhiêu?
-- Chart: Bar Chart (grouped). X = quan, Series = loai_bds,
--        Metric = MAX(median_gia_thue_trieu_thang)
-- -----------------------------------------------------------------------------
SELECT
    loc.district                                                     AS quan,
    loc.area_zone                                                    AS khu_vuc,
    pt.property_type                                                 AS loai_bds,
    count(*)                                                         AS so_tin,
    round(percentile_approx(f.current_price_vnd, 0.5) / 1e6, 1)      AS median_gia_thue_trieu_thang,
    round(percentile_approx(f.current_price_per_m2, 0.5) / 1e3, 0)   AS median_gia_thue_nghin_m2_thang
FROM gold.fct_listing f
JOIN gold.dim_location loc     ON f.location_key = loc.location_key
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
WHERE f.transaction_type = 'rent'
  AND f.is_market_eligible
  AND NOT f.is_price_outlier
  AND pt.property_category IN ('HOUSE', 'APARTMENT', 'ROOM')
  AND loc.district <> 'Không rõ'
GROUP BY loc.district, loc.area_zone, pt.property_type
HAVING count(*) >= 30
ORDER BY loai_bds, median_gia_thue_trieu_thang DESC;


-- -----------------------------------------------------------------------------
-- Q5.3. Mua để cho thuê thì ở đâu lời hơn? (tỷ suất cho thuê gộp theo năm)
-- Chart: Bar Chart (ngang). X = quan, Series = loai_bds, Metric = MAX(ty_suat_thue_nam_pct)
-- ty_suat_thue_nam_pct = median giá thuê/m²/tháng * 12 / median giá bán/m² * 100.
-- Hai median lấy từ hai tập tin khác nhau (tin thuê và tin bán cùng quận, cùng
-- loại), nên chỉ là chỉ báo tham khảo, không phải lợi suất thực của một căn.
-- -----------------------------------------------------------------------------
WITH by_segment AS (
    SELECT
        loc.district                                                 AS quan,
        pt.property_type                                             AS loai_bds,
        f.transaction_type,
        count(*)                                                     AS so_tin,
        percentile_approx(f.current_price_per_m2, 0.5)               AS median_gia_m2
    FROM gold.fct_listing f
    JOIN gold.dim_location loc     ON f.location_key = loc.location_key
    JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
    WHERE f.is_market_eligible
      AND NOT f.is_price_outlier
      AND pt.property_category IN ('HOUSE', 'APARTMENT')
      AND loc.district <> 'Không rõ'
    GROUP BY loc.district, pt.property_type, f.transaction_type
    HAVING count(*) >= 30
)

SELECT
    s.quan,
    s.loai_bds,
    s.so_tin                                                         AS so_tin_ban,
    r.so_tin                                                         AS so_tin_thue,
    round(s.median_gia_m2 / 1e6, 1)                                  AS median_gia_ban_m2_trieu,
    round(r.median_gia_m2 / 1e3, 0)                                  AS median_gia_thue_m2_nghin_thang,
    round(r.median_gia_m2 * 12 / s.median_gia_m2 * 100, 2)           AS ty_suat_thue_nam_pct
FROM by_segment s
JOIN by_segment r
    ON s.quan = r.quan
   AND s.loai_bds = r.loai_bds
   AND r.transaction_type = 'rent'
WHERE s.transaction_type = 'sale'
ORDER BY ty_suat_thue_nam_pct DESC;
