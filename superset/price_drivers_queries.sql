-- =============================================================================
-- Dashboard "Giá BĐS & các yếu tố ảnh hưởng" - query cho Superset SQL Lab (Spark SQL).
-- Mục tiêu: hiểu giá trước khi làm mô hình dự đoán giá bán.
-- Mỗi query trả lời một câu hỏi phục vụ trực tiếp một quyết định khi modeling.
--
-- Mạch:
--   0. KPI
--   1. Target:      giá phân phối thế nào, nên dự đoán giá hay log giá, giá/m²?
--   2. Xếp hạng:    biến nào giải thích được nhiều biến động giá nhất?
--   3. Vị trí:      yếu tố mạnh nhất - quận, khoảng cách trung tâm, dự án.
--   4. Diện tích & hình dáng lô đất.
--   5. Đặc điểm nhà, pháp lý: hiệu ứng SAU KHI đã so trong cùng quận.
--   6. Tương tác:   một mô hình chung hay tách theo loại BĐS?
--   7. Feature số:  tương quan với giá và đa cộng tuyến.
--   8. Thời gian:   giá có trôi theo thời gian (cần feature thời gian) không?
--
-- Quy ước:
--   * Nguồn chính: gold.ml_price_features (tin BÁN, trạng thái mới nhất, đã loại
--     outlier) - đúng tập dữ liệu sẽ dùng để train, nên mọi kết luận ở đây
--     áp dụng thẳng cho mô hình.
--   * Giá bán: tỷ đồng. Giá/m²: triệu đồng. Dùng median thay vì trung bình.
--   * Nhóm < 30 tin bị loại (HAVING) hoặc gộp vào "Khác" để không nhiễu.
--   * Phân tích theo log(giá/m²) vì giá/m² đã chuẩn hoá theo diện tích và log
--     làm phân phối gần chuẩn (xem chương 1).
--   * Gần như mọi chart nên có native filter "loai_bds" (Nhà ở / Căn hộ / Đất),
--     vì yếu tố ảnh hưởng giá khác hẳn giữa các loại.
--   * Trung tâm TP lấy tại chợ Bến Thành (10.7725, 106.6980).
-- =============================================================================


-- =============================================================================
-- 0. KPI (Big Number)
-- =============================================================================
SELECT
    count(*)                                                                  AS so_tin_train,
    count(DISTINCT district)                                                  AS so_quan_huyen,
    round(percentile_approx(CASE WHEN property_category = 'HOUSE'
                                 THEN price_per_m2 END, 0.5) / 1e6, 1)        AS median_gia_m2_nha_trieu,
    round(percentile_approx(CASE WHEN property_category = 'APARTMENT'
                                 THEN price_per_m2 END, 0.5) / 1e6, 1)        AS median_gia_m2_can_ho_trieu,
    round(percentile_approx(CASE WHEN property_category = 'LAND'
                                 THEN price_per_m2 END, 0.5) / 1e6, 1)        AS median_gia_m2_dat_trieu
FROM gold.ml_price_features;


-- =============================================================================
-- 1. TARGET
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P1.1. Giá bán phân phối thế nào? Lấy log có làm phân phối cân đối không?
-- Chart: Bar Chart. X = bin_log10_gia (nhãn gia_ty_dai_dien), Series = loai_bds,
--        Metric = SUM(so_tin). Bin đều trên thang log10 (mỗi bin rộng 0.1).
-- Đọc: nếu trên thang log hình chuông -> train trên log_price_vnd rồi exp lại.
-- -----------------------------------------------------------------------------
SELECT
    round(floor(log10(price_vnd) * 10) / 10, 1)                              AS bin_log10_gia,
    round(pow(10, floor(log10(price_vnd) * 10) / 10 + 0.05) / 1e9, 2)        AS gia_ty_dai_dien,
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
GROUP BY 1, 2, 3
ORDER BY loai_bds, bin_log10_gia;


-- -----------------------------------------------------------------------------
-- P1.2. Chọn target nào: giá, log(giá) hay log(giá/m²)?
-- Chart: Table.
--   * skew ~ 0 -> phân phối cân đối.
--   * cv (độ lệch chuẩn / trung bình) của giá/m² nhỏ hơn hẳn của giá -> diện tích
--     giải thích một phần lớn giá; có thể dự đoán giá/m² rồi nhân diện tích.
-- -----------------------------------------------------------------------------
SELECT
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)                         AS median_gia_ty,
    round(percentile_approx(price_vnd, 0.05) / 1e9, 2)                        AS p05_gia_ty,
    round(percentile_approx(price_vnd, 0.95) / 1e9, 2)                        AS p95_gia_ty,
    round(skewness(price_vnd), 2)                                             AS skew_gia,
    round(skewness(log_price_vnd), 2)                                         AS skew_log_gia,
    round(skewness(ln(price_per_m2)), 2)                                      AS skew_log_gia_m2,
    round(stddev(price_vnd) / avg(price_vnd), 2)                              AS cv_gia,
    round(stddev(price_per_m2) / avg(price_per_m2), 2)                        AS cv_gia_m2
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
GROUP BY property_type
ORDER BY so_tin DESC;


-- =============================================================================
-- 2. XẾP HẠNG YẾU TỐ
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P2.1. Biến nào giải thích được nhiều biến động giá/m² nhất?
-- Chart: Bar Chart (ngang). X = yeu_to, Metric = MAX(eta2_pct), sắp giảm dần,
--        filter loai_bds.
-- eta² = phần phương sai của log(giá/m²) giải thích được nếu chỉ biết nhóm của
-- biến đó (SS giữa nhóm / SS toàn bộ). Là "feature importance" đơn biến, chưa
-- train gì cả.
-- Lưu ý:
--   * Biến nhiều mức (phường, dự án) luôn có eta² cao hơn - đọc kèm so_muc.
--   * Các biến vị trí chồng lấn nhau, không cộng eta² lại được.
--   * Biến số được chia nhóm để so được với biến phân loại (chương 7 có tương
--     quan trực tiếp).
-- -----------------------------------------------------------------------------
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
        CASE
            WHEN area_m2 < 30  THEN '1'
            WHEN area_m2 < 50  THEN '2'
            WHEN area_m2 < 80  THEN '3'
            WHEN area_m2 < 120 THEN '4'
            WHEN area_m2 < 200 THEN '5'
            ELSE                    '6'
        END                                                                   AS area_bin,
        coalesce(cast(least(total_floors, 6) AS string), 'Không rõ')          AS floors_bin,
        coalesce(cast(least(bedrooms, 6) AS string), 'Không rõ')              AS bedrooms_bin,
        CASE
            WHEN width_m IS NULL THEN 'Không rõ'
            WHEN width_m < 4 THEN '1'
            WHEN width_m < 5 THEN '2'
            WHEN width_m < 7 THEN '3'
            ELSE                  '4'
        END                                                                   AS width_bin,
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
    WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
),

long AS (
    SELECT property_type, y, yeu_to, muc
    FROM base
    LATERAL VIEW stack(15,
        'Quận/huyện',              district,
        'Phường/xã',               ward,
        'Dự án',                   project,
        'Loại hình chi tiết',      property_subtype,
        'Pháp lý',                 legal_category,
        'Hướng nhà',               house_direction,
        'Nội thất',                furnishing,
        'Người đăng',              seller,
        'Mặt tiền',                is_frontage,
        'Hẻm xe hơi',              has_car_alley,
        'Diện tích (nhóm)',        area_bin,
        'Số tầng (nhóm)',          floors_bin,
        'Phòng ngủ (nhóm)',        bedrooms_bin,
        'Chiều ngang (nhóm)',      width_bin,
        'Khoảng cách TT (2km)',    distance_bin
    ) t AS yeu_to, muc
),

-- Mức có quá ít tin được gộp vào "Khác" để eta² không bị thổi phồng.
level_sizes AS (
    SELECT property_type, yeu_to, muc, count(*) AS n
    FROM long
    GROUP BY property_type, yeu_to, muc
),

grouped AS (
    SELECT
        l.property_type,
        l.yeu_to,
        CASE WHEN s.n >= 30 THEN l.muc ELSE 'Khác' END                        AS muc,
        count(*)                                                              AS n,
        avg(l.y)                                                              AS group_mean
    FROM long l
    JOIN level_sizes s
        ON l.property_type = s.property_type
       AND l.yeu_to = s.yeu_to
       AND l.muc <=> s.muc
    GROUP BY 1, 2, 3
),

totals AS (
    SELECT
        property_type,
        avg(y)                                                                AS grand_mean,
        var_pop(y) * count(*)                                                 AS ss_total
    FROM base
    GROUP BY property_type
)

SELECT
    g.property_type                                                           AS loai_bds,
    g.yeu_to,
    count(*)                                                                  AS so_muc,
    round(sum(g.n * pow(g.group_mean - t.grand_mean, 2)) / t.ss_total * 100, 1) AS eta2_pct
FROM grouped g
JOIN totals t
    ON g.property_type = t.property_type
GROUP BY g.property_type, g.yeu_to, t.ss_total
ORDER BY loai_bds, eta2_pct DESC;


-- =============================================================================
-- 3. VỊ TRÍ
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P3.1. Giá/m² giảm thế nào khi càng xa trung tâm?
-- Chart: Line Chart. X = khoang_cach_km, Series = loai_bds,
--        Metric = MAX(median_gia_m2_trieu); có thể thêm band p25-p75.
-- Đọc: đường dốc và mượt -> "khoảng cách tới trung tâm" là feature tốt, nên
-- thêm vào ml_price_features (tính từ lat/lon, không leak).
-- -----------------------------------------------------------------------------
WITH dist AS (
    SELECT
        property_type,
        price_per_m2,
        6371 * 2 * asin(sqrt(
            pow(sin(radians(latitude - 10.7725) / 2), 2)
          + cos(radians(latitude)) * cos(radians(10.7725))
          * pow(sin(radians(longitude - 106.6980) / 2), 2)
        ))                                                                    AS km
    FROM gold.ml_price_features
    WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
      AND latitude  BETWEEN 10.3 AND 11.2
      AND longitude BETWEEN 106.3 AND 107.1
)

SELECT
    cast(least(floor(km / 2) * 2, 30) AS int)                                 AS khoang_cach_km,
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin,
    round(percentile_approx(price_per_m2, 0.25) / 1e6, 1)                     AS p25_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.5)  / 1e6, 1)                     AS median_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.75) / 1e6, 1)                     AS p75_gia_m2_trieu
FROM dist
GROUP BY 1, 2
HAVING count(*) >= 30
ORDER BY loai_bds, khoang_cach_km;


-- -----------------------------------------------------------------------------
-- P3.2. Bản đồ giá/m².
-- Chart: deck.gl Scatterplot (hoặc deck.gl Hexagon / Heatmap để tránh chồng điểm).
--        Longitude = kinh_do, Latitude = vi_do, màu theo gia_m2_trieu.
--        Filter loai_bds - không trộn đất với căn hộ trên cùng bản đồ.
-- -----------------------------------------------------------------------------
SELECT
    listing_key,
    latitude                                                                  AS vi_do,
    longitude                                                                 AS kinh_do,
    district                                                                  AS quan,
    property_type                                                             AS loai_bds,
    round(price_per_m2 / 1e6, 1)                                              AS gia_m2_trieu,
    round(price_vnd / 1e9, 2)                                                 AS gia_ty,
    area_m2                                                                   AS dien_tich_m2
FROM gold.ml_price_features
WHERE latitude  BETWEEN 10.3 AND 11.2
  AND longitude BETWEEN 106.3 AND 107.1;


-- -----------------------------------------------------------------------------
-- P3.3. Xếp hạng quận theo giá/m² và độ phân tán trong quận.
-- Chart: Box Plot từ P3.3a (Dimensions = quan, Distribute across = listing_key,
--        Metric = MAX(gia_m2_trieu), Row limit 50000) hoặc Table từ P3.3b.
-- Đọc: iqr_tuong_doi lớn -> trong quận còn chênh nhiều, cần biến vị trí mịn hơn
-- (phường, toạ độ, khoảng cách) chứ không chỉ quận.
-- -----------------------------------------------------------------------------
-- P3.3a
SELECT
    listing_key,
    district                                                                  AS quan,
    property_type                                                             AS loai_bds,
    round(price_per_m2 / 1e6, 1)                                              AS gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND');

-- P3.3b
SELECT
    district                                                                  AS quan,
    area_zone                                                                 AS khu_vuc,
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin,
    round(percentile_approx(price_per_m2, 0.25) / 1e6, 1)                     AS p25_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.5)  / 1e6, 1)                     AS median_gia_m2_trieu,
    round(percentile_approx(price_per_m2, 0.75) / 1e6, 1)                     AS p75_gia_m2_trieu,
    round((percentile_approx(price_per_m2, 0.75) - percentile_approx(price_per_m2, 0.25))
          / percentile_approx(price_per_m2, 0.5), 2)                          AS iqr_tuong_doi
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
  AND district <> 'Không rõ'
GROUP BY district, area_zone, property_type
HAVING count(*) >= 30
ORDER BY loai_bds, median_gia_m2_trieu DESC;


-- -----------------------------------------------------------------------------
-- P3.4. Căn hộ: dự án nào đắt / rẻ nhất?
-- Chart: Bar Chart (ngang). X = du_an, Metric = MAX(median_gia_m2_trieu),
--        tooltip quan, so_tin. Row limit 30.
-- Đọc: chênh lệch lớn giữa các dự án trong cùng quận -> project_name nên vào
-- mô hình (target encoding tính trong tập train).
-- -----------------------------------------------------------------------------
SELECT
    project_name                                                              AS du_an,
    district                                                                  AS quan,
    count(*)                                                                  AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                      AS median_gia_m2_trieu,
    round(percentile_approx(price_vnd, 0.5) / 1e9, 2)                         AS median_gia_ty
FROM gold.ml_price_features
WHERE property_category = 'APARTMENT'
  AND project_name IS NOT NULL
GROUP BY project_name, district
HAVING count(*) >= 20
ORDER BY median_gia_m2_trieu DESC;


-- =============================================================================
-- 4. DIỆN TÍCH & HÌNH DÁNG LÔ ĐẤT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P4.1. Diện tích và giá: quan hệ có tuyến tính trên thang log không?
-- Chart: Scatter Plot. X = log10_dien_tich, Y = log10_gia, Series = loai_bds.
-- Lấy mẫu ngẫu nhiên 20.000 điểm để chart nhẹ.
-- Đọc: hệ số góc trên thang log-log = độ co giãn giá theo diện tích.
-- < 1 nghĩa là giá/m² giảm khi diện tích tăng.
-- -----------------------------------------------------------------------------
SELECT
    listing_key,
    property_type                                                             AS loai_bds,
    district                                                                  AS quan,
    round(log10(area_m2), 3)                                                  AS log10_dien_tich,
    round(log10(price_vnd), 3)                                                AS log10_gia,
    area_m2                                                                   AS dien_tich_m2,
    round(price_vnd / 1e9, 2)                                                 AS gia_ty
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
ORDER BY rand(42)
LIMIT 20000;


-- -----------------------------------------------------------------------------
-- P4.2. Độ co giãn giá theo diện tích của từng loại BĐS (hồi quy log-log).
-- Chart: Table.
-- elasticity = hệ số góc OLS của ln(giá) theo ln(diện tích) = cov / var.
-- r2 = phần biến động ln(giá) chỉ riêng diện tích giải thích được.
-- -----------------------------------------------------------------------------
SELECT
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin,
    round(covar_pop(ln(area_m2), log_price_vnd) / var_pop(ln(area_m2)), 3)    AS elasticity,
    round(pow(corr(ln(area_m2), log_price_vnd), 2) * 100, 1)                  AS r2_pct
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
GROUP BY property_type
ORDER BY so_tin DESC;


-- -----------------------------------------------------------------------------
-- P4.3. Nhà ở / đất: chiều ngang mặt tiền ảnh hưởng giá/m² thế nào?
-- Chart: Bar Chart. X = chieu_ngang, Series = loai_bds, Metric = MAX(median_gia_m2_trieu)
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN width_m < 3  THEN '1. < 3 m'
        WHEN width_m < 4  THEN '2. 3–4 m'
        WHEN width_m < 5  THEN '3. 4–5 m'
        WHEN width_m < 6  THEN '4. 5–6 m'
        WHEN width_m < 8  THEN '5. 6–8 m'
        ELSE                   '6. ≥ 8 m'
    END                                                                       AS chieu_ngang,
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                      AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'LAND')
  AND width_m IS NOT NULL
GROUP BY 1, 2
HAVING count(*) >= 30
ORDER BY loai_bds, chieu_ngang;


-- =============================================================================
-- 5. ĐẶC ĐIỂM NHÀ & PHÁP LÝ - HIỆU ỨNG ĐÃ LOẠI ẢNH HƯỞNG VỊ TRÍ
-- So sánh thô (median nhóm có vs không) bị nhiễu vì vị trí: nhà mặt tiền tập
-- trung ở quận trung tâm, nhà chưa hoàn công ở ngoại thành... Ở đây mỗi tin
-- được chia cho median giá/m² của các tin cùng quận, cùng loại BĐS, nên con số
-- cho biết tin có đặc điểm đó đắt hơn / rẻ hơn bao nhiêu % so với mặt bằng
-- của chính khu vực nó.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P5.1. Mỗi đặc điểm làm giá/m² chênh bao nhiêu so với mặt bằng cùng quận?
-- Chart: Bar Chart (ngang). X = muc, Metric = MAX(chenh_lech_vs_cung_quan_pct),
--        Dimension/filter = yeu_to, filter loai_bds.
--        Hoặc Heatmap X = muc, Y = yeu_to cho cái nhìn tổng.
-- Đọc: với cờ Có/Không, khoảng cách giữa hai dòng là hiệu ứng ước tính.
-- Cờ đặc điểm lấy từ tag người đăng chọn: "Không" nghĩa là không ghi, không
-- chắc là không có.
-- -----------------------------------------------------------------------------
WITH segment_median AS (
    SELECT
        property_type,
        district,
        percentile_approx(price_per_m2, 0.5)                                  AS seg_median
    FROM gold.ml_price_features
    WHERE district <> 'Không rõ'
    GROUP BY property_type, district
    HAVING count(*) >= 30
),

relative AS (
    SELECT
        f.*,
        f.price_per_m2 / s.seg_median                                         AS rel_price
    FROM gold.ml_price_features f
    JOIN segment_median s
        ON f.property_type = s.property_type
       AND f.district = s.district
    WHERE f.property_category IN ('HOUSE', 'APARTMENT', 'LAND')
),

long AS (
    SELECT property_type, yeu_to, muc, rel_price
    FROM relative
    LATERAL VIEW stack(17,
        'Mặt tiền',           CASE WHEN is_frontage          THEN 'Có' ELSE 'Không' END,
        'Hẻm xe hơi',         CASE WHEN has_car_alley        THEN 'Có' ELSE 'Không' END,
        'Nở hậu',             CASE WHEN is_wide_back         THEN 'Có' ELSE 'Không' END,
        'Tóp hậu',            CASE WHEN is_narrow_back       THEN 'Có' ELSE 'Không' END,
        'Nhà nát',            CASE WHEN is_dilapidated       THEN 'Có' ELSE 'Không' END,
        'Chưa hoàn công',     CASE WHEN is_not_completed     THEN 'Có' ELSE 'Không' END,
        'Dính quy hoạch',     CASE WHEN is_planning_affected THEN 'Có' ELSE 'Không' END,
        'Thuộc dự án',        CASE WHEN is_in_project        THEN 'Có' ELSE 'Không' END,
        'Người đăng',         CASE WHEN is_broker THEN 'Môi giới' ELSE 'Cá nhân' END,
        'Pháp lý',            legal_category,
        'Hướng nhà',          coalesce(house_direction, 'Không rõ'),
        'Nội thất',           coalesce(furnishing, 'Không rõ'),
        'Tình trạng bàn giao', coalesce(handover_status, 'Không rõ'),
        'Loại hình chi tiết', property_subtype,
        'Số tầng',            coalesce(CASE WHEN total_floors >= 6 THEN '6+'
                                            ELSE cast(total_floors AS string) END, 'Không rõ'),
        'Phòng ngủ',          coalesce(CASE WHEN bedrooms >= 6 THEN '6+'
                                            ELSE cast(bedrooms AS string) END, 'Không rõ'),
        'Diện tích',          CASE
                                  WHEN area_m2 < 30  THEN '1. < 30 m²'
                                  WHEN area_m2 < 50  THEN '2. 30–50 m²'
                                  WHEN area_m2 < 80  THEN '3. 50–80 m²'
                                  WHEN area_m2 < 120 THEN '4. 80–120 m²'
                                  WHEN area_m2 < 200 THEN '5. 120–200 m²'
                                  ELSE                    '6. ≥ 200 m²'
                              END
    ) t AS yeu_to, muc
)

SELECT
    property_type                                                             AS loai_bds,
    yeu_to,
    muc,
    count(*)                                                                  AS so_tin,
    round((percentile_approx(rel_price, 0.5) - 1) * 100, 1)                   AS chenh_lech_vs_cung_quan_pct
FROM long
WHERE muc IS NOT NULL
GROUP BY property_type, yeu_to, muc
HAVING count(*) >= 30
ORDER BY loai_bds, yeu_to, muc;


-- =============================================================================
-- 6. TƯƠNG TÁC: MỘT MÔ HÌNH CHUNG HAY TÁCH THEO LOẠI?
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P6.1. Thứ hạng giá của các quận có giống nhau giữa nhà, căn hộ, đất?
-- Chart: Heatmap. X = loai_bds, Y = quan, Metric = MAX(hang_trong_loai),
--        tooltip median_gia_m2_trieu. Sắp Y theo hạng của "Nhà ở".
-- Đọc: nếu một quận đứng đầu ở loại này nhưng giữa bảng ở loại khác -> có
-- tương tác vị trí x loại BĐS: dùng mô hình cây (GBM) hoặc tách mô hình theo
-- loại thay vì hồi quy tuyến tính chung.
-- -----------------------------------------------------------------------------
WITH by_segment AS (
    SELECT
        district,
        property_type,
        count(*)                                                              AS so_tin,
        percentile_approx(price_per_m2, 0.5)                                  AS median_gia_m2
    FROM gold.ml_price_features
    WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
      AND district <> 'Không rõ'
    GROUP BY district, property_type
    HAVING count(*) >= 30
)

SELECT
    district                                                                  AS quan,
    property_type                                                             AS loai_bds,
    so_tin,
    round(median_gia_m2 / 1e6, 1)                                             AS median_gia_m2_trieu,
    rank() OVER (PARTITION BY property_type ORDER BY median_gia_m2 DESC)      AS hang_trong_loai
FROM by_segment
ORDER BY loai_bds, hang_trong_loai;


-- -----------------------------------------------------------------------------
-- P6.2. Hiệu ứng diện tích có khác nhau giữa nội thành và ngoại thành?
-- Chart: Line Chart. X = nhom_dien_tich, Series = khu_vuc,
--        Metric = MAX(median_gia_m2_trieu), filter loai_bds.
-- Đọc: hai đường không song song -> tương tác diện tích x vị trí.
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN area_m2 < 30  THEN '1. < 30 m²'
        WHEN area_m2 < 50  THEN '2. 30–50 m²'
        WHEN area_m2 < 80  THEN '3. 50–80 m²'
        WHEN area_m2 < 120 THEN '4. 80–120 m²'
        WHEN area_m2 < 200 THEN '5. 120–200 m²'
        ELSE                    '6. ≥ 200 m²'
    END                                                                       AS nhom_dien_tich,
    area_zone                                                                 AS khu_vuc,
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin,
    round(percentile_approx(price_per_m2, 0.5) / 1e6, 1)                      AS median_gia_m2_trieu
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
  AND area_zone <> 'Không rõ'
GROUP BY 1, 2, 3
HAVING count(*) >= 30
ORDER BY loai_bds, khu_vuc, nhom_dien_tich;


-- =============================================================================
-- 7. FEATURE SỐ
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P7.1. Tương quan của từng feature số với giá.
-- Chart: Heatmap. X = feature, Y = loai_bds, Metric = MAX(corr_log_gia_m2).
--        Hoặc Bar Chart theo từng loại.
-- corr_log_gia: tương quan với ln(giá) - target của mô hình.
-- corr_log_gia_m2: tương quan với ln(giá/m²) - đã loại phần do diện tích.
-- Pearson chỉ đo quan hệ tuyến tính, đọc kèm các chart theo nhóm ở trên.
-- -----------------------------------------------------------------------------
WITH base AS (
    SELECT
        property_type,
        log_price_vnd,
        ln(price_per_m2)                                                      AS log_ppm2,
        ln(area_m2)                                                           AS log_area,
        ln(living_area_m2)                                                    AS log_living_area,
        width_m,
        length_m,
        bedrooms,
        bathrooms,
        total_floors,
        floor_number,
        CASE WHEN latitude  BETWEEN 10.3 AND 11.2
              AND longitude BETWEEN 106.3 AND 107.1 THEN
            6371 * 2 * asin(sqrt(
                pow(sin(radians(latitude - 10.7725) / 2), 2)
              + cos(radians(latitude)) * cos(radians(10.7725))
              * pow(sin(radians(longitude - 106.6980) / 2), 2)
            ))
        END                                                                   AS distance_km
    FROM gold.ml_price_features
    WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
)

SELECT
    property_type                                                             AS loai_bds,
    feature,
    count(x)                                                                  AS so_tin_co_gia_tri,
    round(corr(x, log_price_vnd), 3)                                          AS corr_log_gia,
    round(corr(x, log_ppm2), 3)                                               AS corr_log_gia_m2
FROM base
LATERAL VIEW stack(10,
    'ln(diện tích)',         log_area,
    'ln(DT sử dụng)',        log_living_area,
    'Chiều ngang',           width_m,
    'Chiều dài',             length_m,
    'Phòng ngủ',             cast(bedrooms AS double),
    'Phòng tắm',             cast(bathrooms AS double),
    'Số tầng',               cast(total_floors AS double),
    'Tầng số (căn hộ)',      cast(floor_number AS double),
    'Khoảng cách TT (km)',   distance_km,
    'ln(khoảng cách TT)',    ln(distance_km + 0.5)
) t AS feature, x
GROUP BY property_type, feature
HAVING count(x) >= 30
ORDER BY loai_bds, abs(corr(x, log_ppm2)) DESC;


-- -----------------------------------------------------------------------------
-- P7.2. Feature nào trùng thông tin nhau (đa cộng tuyến)?
-- Chart: Table hoặc Bar (ngang). X = cap_feature, Metric = MAX(he_so_tuong_quan).
-- |corr| > 0.8 -> với mô hình tuyến tính chỉ giữ một, hoặc gộp
-- (ví dụ phòng tắm / phòng ngủ). Mô hình cây ít bị ảnh hưởng hơn.
-- -----------------------------------------------------------------------------
SELECT
    property_type                                                             AS loai_bds,
    cap_feature,
    count(*)                                                                  AS so_tin,
    round(corr(a, b), 3)                                                      AS he_so_tuong_quan
FROM gold.ml_price_features
LATERAL VIEW stack(7,
    'Diện tích ~ DT sử dụng',      ln(area_m2), ln(living_area_m2),
    'Diện tích ~ Ngang × Dài',     ln(area_m2), ln(width_m * length_m),
    'Diện tích ~ Phòng ngủ',       ln(area_m2), cast(bedrooms AS double),
    'Phòng ngủ ~ Phòng tắm',       cast(bedrooms AS double), cast(bathrooms AS double),
    'Số tầng ~ Phòng ngủ',         cast(total_floors AS double), cast(bedrooms AS double),
    'Số tầng ~ Phòng tắm',         cast(total_floors AS double), cast(bathrooms AS double),
    'Chiều ngang ~ Diện tích',     width_m, ln(area_m2)
) t AS cap_feature, a, b
WHERE property_category IN ('HOUSE', 'APARTMENT', 'LAND')
  AND a IS NOT NULL
  AND b IS NOT NULL
GROUP BY property_type, cap_feature
HAVING count(*) >= 30
ORDER BY loai_bds, abs(corr(a, b)) DESC;


-- -----------------------------------------------------------------------------
-- P7.3. Dữ liệu diện tích có nhất quán không? (diện tích khai vs ngang × dài)
-- Chart: Bar Chart. X = ty_le_ngang_x_dai_tren_dt, Series = loai_bds, Metric = SUM(so_tin)
-- Đọc: phần lớn quanh 1.0 là tốt; nhiều tin lệch xa -> một trong ba số bị nhập
-- sai, cân nhắc dùng ngang × dài làm feature dự phòng hoặc cờ "lệch diện tích".
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN width_m * length_m / area_m2 < 0.5 THEN '1. < 0.5'
        WHEN width_m * length_m / area_m2 < 0.8 THEN '2. 0.5–0.8'
        WHEN width_m * length_m / area_m2 < 0.95 THEN '3. 0.8–0.95'
        WHEN width_m * length_m / area_m2 <= 1.05 THEN '4. 0.95–1.05'
        WHEN width_m * length_m / area_m2 <= 1.25 THEN '5. 1.05–1.25'
        WHEN width_m * length_m / area_m2 <= 2 THEN '6. 1.25–2'
        ELSE '7. > 2'
    END                                                                       AS ty_le_ngang_x_dai_tren_dt,
    property_type                                                             AS loai_bds,
    count(*)                                                                  AS so_tin
FROM gold.ml_price_features
WHERE property_category IN ('HOUSE', 'LAND')
  AND width_m IS NOT NULL
  AND length_m IS NOT NULL
GROUP BY 1, 2
ORDER BY loai_bds, 1;


-- =============================================================================
-- 8. THỜI GIAN
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P8.1. Giá/m² có trôi trong khoảng thời gian thu thập dữ liệu không?
-- Chart: Line Chart. X = tuan, Series = loai_bds, Metric = MAX(median_gia_m2_trieu),
--        band p25-p75.
-- Đọc: đường gần như phẳng -> chưa cần feature thời gian; nếu có xu hướng ->
-- thêm observed_date (tháng) vào mô hình và chia train/test theo thời gian.
-- -----------------------------------------------------------------------------
SELECT
    period_start                                                              AS tuan,
    property_type                                                             AS loai_bds,
    listing_count                                                             AS so_tin,
    round(p25_price_per_m2 / 1e6, 1)                                          AS p25_gia_m2_trieu,
    round(median_price_per_m2 / 1e6, 1)                                       AS median_gia_m2_trieu,
    round(p75_price_per_m2 / 1e6, 1)                                          AS p75_gia_m2_trieu
FROM gold.agg_price_trend_weekly
WHERE transaction_type = 'sale'
  AND geo_level = 'CITY'
  AND property_category IN ('HOUSE', 'APARTMENT', 'LAND')
  AND is_reliable
ORDER BY loai_bds, tuan;


-- -----------------------------------------------------------------------------
-- P8.2. Giá rao có phải giá cuối không? (tin đã đổi giá trong thời gian theo dõi)
-- Chart: Table hoặc Bar. Target của mô hình là giá rao mới nhất; nếu nhiều tin
-- giảm giá mạnh thì nhãn có độ nhiễu, và mô hình dự đoán "giá rao" chứ không
-- phải "giá giao dịch" - cần ghi rõ trong báo cáo.
-- -----------------------------------------------------------------------------
SELECT
    pt.property_type                                                          AS loai_bds,
    count(*)                                                                  AS so_tin,
    round(avg(CASE WHEN f.price_change_count > 0 THEN 1.0 ELSE 0.0 END) * 100, 1) AS ty_le_tin_doi_gia_pct,
    round(avg(CASE WHEN f.total_price_change_pct < 0 THEN 1.0 ELSE 0.0 END) * 100, 1) AS ty_le_tin_giam_gia_pct,
    round(percentile_approx(CASE WHEN f.total_price_change_pct < 0
                                 THEN f.total_price_change_pct END, 0.5), 1)  AS muc_giam_median_pct
FROM gold.fct_listing f
JOIN gold.dim_property_type pt ON f.property_type_key = pt.property_type_key
WHERE f.transaction_type = 'sale'
  AND f.is_price_training_eligible
  AND NOT f.is_price_outlier
  AND pt.property_category IN ('HOUSE', 'APARTMENT', 'LAND')
GROUP BY pt.property_type
ORDER BY so_tin DESC;
