-- =============================================================================
-- Dashboard "Dữ liệu cho mô hình dự đoán giá" - query cho Superset SQL Lab (Spark SQL).
-- Nối tiếp chương 3 của dashboard thị trường: các yếu tố ảnh hưởng giá đã thấy
-- ở đó trở thành feature; dashboard này kiểm tra dữ liệu của chúng có dùng được
-- để train không.
--
-- Mạch: còn bao nhiêu tin để train -> vì sao bị loại -> target có cần biến đổi
--       không -> feature thiếu bao nhiêu, xử lý thế nào.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- KPI (Big Number)
-- -----------------------------------------------------------------------------
SELECT
    (SELECT count(*) FROM gold.fct_listing
      WHERE transaction_type = 'sale' AND is_market_eligible)        AS so_tin_ban_hop_le,
    (SELECT count(*) FROM gold.ml_price_features)                    AS so_tin_dung_de_train,
    (SELECT count(DISTINCT district) FROM gold.ml_price_features)    AS so_quan_huyen;


-- -----------------------------------------------------------------------------
-- M1. Tin bán bị loại khỏi tập train vì lý do gì?
-- Chart: Bar Chart (ngang). X = ly_do, Metric = SUM(so_tin)
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN NOT is_market_eligible            THEN 'Không hợp lệ thị trường'
        WHEN NOT is_price_training_eligible    THEN 'Thiếu thông tin để train'
        WHEN outlier_reason = 'PRICE_PER_M2_OUT_OF_RANGE' THEN 'Outlier giá/m²'
        WHEN outlier_reason = 'AREA_OUT_OF_RANGE'         THEN 'Outlier diện tích'
        WHEN is_price_outlier                  THEN 'Giá / diện tích không dương'
        ELSE                                        'Dùng để train'
    END                                                              AS ly_do,
    count(*)                                                         AS so_tin
FROM gold.fct_listing
WHERE transaction_type = 'sale'
GROUP BY 1
ORDER BY so_tin DESC;


-- -----------------------------------------------------------------------------
-- M2. Giá bán lệch phải, có cần dùng log làm target không?
-- Chart: Table. Skewness gần 0 nghĩa là phân phối cân đối.
-- -----------------------------------------------------------------------------
SELECT
    property_category                                                AS nhom_bds,
    count(*)                                                         AS so_tin,
    round(skewness(price_vnd), 2)                                    AS skew_gia,
    round(skewness(log_price_vnd), 2)                                AS skew_log_gia
FROM gold.ml_price_features
GROUP BY property_category
ORDER BY so_tin DESC;


-- -----------------------------------------------------------------------------
-- M3. Feature nào thiếu dữ liệu nhiều, cần xử lý trước khi train?
-- Chart: Heatmap. X = feature, Y = nhom_bds, Metric = MAX(ty_le_thieu_pct)
-- Thiếu theo loại là bình thường (đất không có phòng ngủ) -> impute theo nhóm.
-- -----------------------------------------------------------------------------
WITH missing AS (
    SELECT
        property_category                                            AS nhom_bds,
        count(*)                                                     AS so_tin,
        avg(cast(width_m         IS NULL AS int)) * 100              AS w,
        avg(cast(length_m        IS NULL AS int)) * 100              AS l,
        avg(cast(living_area_m2  IS NULL AS int)) * 100              AS la,
        avg(cast(bedrooms        IS NULL AS int)) * 100              AS bd,
        avg(cast(bathrooms       IS NULL AS int)) * 100              AS ba,
        avg(cast(total_floors    IS NULL AS int)) * 100              AS fl,
        avg(cast(house_direction IS NULL AS int)) * 100              AS dir,
        avg(cast(furnishing      IS NULL AS int)) * 100              AS fu,
        avg(cast(latitude        IS NULL AS int)) * 100              AS lat,
        avg(cast(legal_category = 'Không rõ' AS int)) * 100          AS lg
    FROM gold.ml_price_features
    GROUP BY property_category
)

SELECT
    nhom_bds,
    so_tin,
    feature,
    round(ty_le_thieu_pct, 1)                                        AS ty_le_thieu_pct
FROM missing
LATERAL VIEW stack(10,
    'Chiều ngang',      w,
    'Chiều dài',        l,
    'DT sử dụng',       la,
    'Phòng ngủ',        bd,
    'Phòng tắm',        ba,
    'Số tầng',          fl,
    'Hướng nhà',        dir,
    'Nội thất',         fu,
    'Toạ độ',           lat,
    'Pháp lý',          lg
) t AS feature, ty_le_thieu_pct
ORDER BY nhom_bds, ty_le_thieu_pct DESC;
