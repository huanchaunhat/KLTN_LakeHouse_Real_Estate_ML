# Mô tả tập dữ liệu

Tài liệu mô tả tập dữ liệu dùng trong đồ án, từ dữ liệu thô thu thập được đến bảng đặc trưng dùng huấn luyện mô hình dự đoán giá. Số liệu lấy tại Delta version 30 của bảng `gold.ml_price_features` (ngày 16/11/2026).

## 1. Nguồn dữ liệu

| Mục | Nội dung |
|---|---|
| Nguồn | API công khai của sàn Nhà Tốt (Chợ Tốt), chuyên mục bất động sản |
| Phạm vi | TP.HCM theo địa giới cũ, 22 quận/huyện, cả tin bán và tin cho thuê |
| Cách thu thập | Crawl tự động hằng ngày bằng Airflow + Spark; mỗi ngày lấy lại cả tin cũ để theo dõi thay đổi giá |
| Thời gian | Tin đăng từ 24/07/2025 đến 16/11/2026 |
| Đơn vị quan sát | Một tin rao (listing). Một bất động sản có thể có nhiều tin do nhiều môi giới đăng |
| Quy mô | 163.087 tin (bán + thuê) trong Silver; 48.628 bất động sản bán trong bảng đặc trưng |
| Định dạng lưu | JSON thô (Landing, Bronze), Delta Lake (Silver, Gold) trên MinIO |

Lưu ý quan trọng: giá trong tập dữ liệu là **giá rao bán** do người đăng tự khai, không phải giá giao dịch thực tế.

## 2. Dữ liệu thô (Bronze)

Mỗi dòng Bronze là một lần crawl một tin, gồm phần bao (envelope) do pipeline sinh và phần payload JSON nguyên gốc của nguồn.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `source` | string | Tên nguồn, hiện là `nhatot` |
| `source_listing_id` | string | Mã tin trên sàn |
| `snapshot_id` | string | Mã lần crawl (ngày + run_id) |
| `crawl_timestamp` | timestamp | Thời điểm lấy tin |
| `http_status` | int | Mã phản hồi HTTP. 200 là lấy được; 404/410 là tin đã bị gỡ; 429/5xx là lỗi tạm thời |
| `error` | string | Thông báo lỗi nếu có |
| `payload` | struct | JSON gốc của tin, gồm `ad` (thông tin chính) và `ad_params` (thuộc tính mở rộng) |

Các trường chính trong `payload.ad`: `subject` (tiêu đề), `body` (mô tả), `type` (s = bán, u = thuê), `price`, `size`, `category_name`, `rooms`, `toilets`, `floors`, `width`, `length`, `living_size`, `street_name`, `ward_name`, `area_name`, `region_name`, `ward`, `area_v2`, `region_v2`, `latitude`, `longitude`, `pty_project_name`, `company_ad`, `images`, `list_time`, `orig_list_time`. Trong `payload.ad_params`: `apartment_type`, `house_type`, `land_type`, `property_legal_document`, `direction`, `furnishing_sell`, `furnishing_rent`, `property_status`, `pty_characteristics`, `address`.

## 3. Dữ liệu đã làm sạch (Silver: `listing_history`, `listing_current`)

Silver ép kiểu, chuẩn hóa và trích thuộc tính từ payload. `listing_history` giữ mọi quan sát hợp lệ, `listing_current` giữ trạng thái mới nhất của mỗi tin. Dòng không qua kiểm tra chất lượng nằm trong `listing_quarantine`.

### 3.1 Khóa và nguồn gốc

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `listing_key` | string (sha256) | Khóa tin, băm từ nguồn + mã tin. Không đổi qua các lần crawl |
| `observation_id` | string (sha256) | Khóa một lần quan sát, băm từ nguồn + mã tin + snapshot |
| `source_listing_id` | string | Mã tin gốc trên sàn |
| `snapshot_id`, `crawl_date`, `crawl_timestamp` | string, date, timestamp | Lần crawl và thời điểm quan sát |
| `posted_at` | timestamp | Thời điểm đăng tin, lấy `orig_list_time` nếu có, không thì `list_time` |
| `source_updated_at` | timestamp | Lần cập nhật gần nhất trên sàn |

### 3.2 Giao dịch và giá

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `transaction_type` | string | `sale` (bán) hoặc `rent` (thuê) |
| `price_vnd` | bigint | Giá rao, đơn vị VND |
| `price_in_billions` | double | Giá quy ra tỷ đồng, tiện đọc |
| `price_raw` | string | Chuỗi giá hiển thị trên sàn, ví dụ "5,9 tỷ" |
| `area_m2` | double | Diện tích đất hoặc diện tích căn hộ, m² |
| `price_per_m2` | double | Giá trên mỗi m², bằng `price_vnd / area_m2` |
| `deposit_vnd` | bigint | Tiền cọc (chỉ tin thuê) |

### 3.3 Loại hình bất động sản

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `property_type` | string | Loại chính: Căn hộ/Chung cư, Nhà ở, Đất, Phòng trọ, Văn phòng và mặt bằng kinh doanh |
| `property_subtype` | string | Loại con, lấy từ `apartment_type`, `house_type` hoặc `land_type`. Ví dụ: Nhà ngõ hẻm, Nhà mặt phố mặt tiền, Chung cư, Đất thổ cư, Đất nông nghiệp |

### 3.4 Vị trí

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `province_id`, `province` | bigint, string | Mã và tên tỉnh/thành. TP.HCM có mã 13000 |
| `district_id`, `district` | bigint, string | Mã và tên quận/huyện (địa giới cũ, 22 đơn vị) |
| `ward_id`, `ward` | bigint, string | Mã và tên phường/xã |
| `street` | string | Tên đường |
| `address` | string | Địa chỉ chi tiết do người đăng ghi |
| `latitude`, `longitude` | double | Tọa độ. Lưu ý nhiều tin chỉ có tọa độ tâm phường, không phải vị trí chính xác |

### 3.5 Kích thước và kết cấu

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `living_area_m2` | double | Diện tích sử dụng (nhà ở) |
| `width_m` | double | Chiều ngang mặt tiền, m |
| `length_m` | double | Chiều dài, m |
| `bedrooms` | int | Số phòng ngủ |
| `bathrooms` | int | Số phòng vệ sinh |
| `total_floors` | int | Tổng số tầng (nhà ở) |
| `floor_number` | int | Tầng của căn hộ |

### 3.6 Đặc điểm và tình trạng

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `legal_status` | string | Tình trạng pháp lý gốc: Đã có sổ, Sổ hồng riêng, Sổ chung / công chứng vi bằng, Đang chờ sổ, Hợp đồng mua bán, Giấy tờ viết tay, Không có sổ, ... |
| `house_direction` | string | Hướng nhà, 8 hướng |
| `furnishing` | string | Nội thất: Nội thất đầy đủ, Nội thất cao cấp, Hoàn thiện cơ bản, Bàn giao thô |
| `handover_status` | string | Tình trạng bàn giao: Đã bàn giao, Chưa bàn giao (chủ yếu căn hộ) |
| `property_characteristics` | array<string> | Danh sách đặc điểm gốc từ nguồn, ví dụ "Hẻm xe hơi", "Nở hậu" |
| `has_car_alley` | boolean | Hẻm xe hơi vào được |
| `is_frontage` | boolean | Nhà mặt tiền |
| `is_wide_back` | boolean | Nở hậu |
| `is_narrow_back` | boolean | Tóp hậu |
| `is_dilapidated` | boolean | Nhà nát |
| `is_not_completed` | boolean | Chưa hoàn công |
| `is_planning_affected` | boolean | Dính quy hoạch hoặc lộ giới |
| `project_name` | string | Tên dự án hoặc khu dân cư, NULL nếu không thuộc dự án |
| `is_broker` | boolean | Tin do môi giới hoặc công ty đăng |

### 3.7 Nội dung và metadata

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `title`, `description` | string | Tiêu đề và mô tả tự do |
| `image_urls`, `image_count` | array, int | Ảnh của tin |
| `phone_hash` | string | Băm sha256 số điện thoại người đăng, không lưu số gốc |
| `content_hash` | string | Băm nội dung chính của tin, dùng phát hiện tin được sửa |

### 3.8 Cờ chất lượng và lịch sử

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `quality_errors` | array<string> | Danh sách lỗi phát hiện được: giá không hợp lệ, tọa độ ngoài miền, số phòng âm, ... |
| `is_valid_observation` | boolean | Không có lỗi nào. Dòng false vào quarantine |
| `is_market_eligible` | boolean | Hợp lệ, là tin bán hoặc thuê, giá và diện tích dương, thuộc TP.HCM. Dùng cho mart xu hướng |
| `is_price_training_eligible` | boolean | Như trên nhưng chỉ tin bán, có quận và loại hình. Dùng cho mô hình giá |
| `change_type` | string | So với lần quan sát trước của cùng tin: NEW, PRICE_CHANGED, UPDATED, UNCHANGED |
| `previous_price_vnd`, `price_change_vnd`, `price_change_pct` | bigint, bigint, double | Giá lần trước và mức thay đổi |
| `is_price_outlier`, `outlier_reason` | boolean, string | Giá/m² hoặc diện tích ngoài khoảng phân vị 1% đến 99% của nhóm loại giao dịch × loại hình |

## 4. Bảng đặc trưng huấn luyện (Gold: `ml_price_features`)

Đây là tập dữ liệu trực tiếp đưa vào mô hình dự đoán giá. Mỗi dòng là một bất động sản bán riêng biệt ở trạng thái mới nhất.

### 4.1 Quy trình lọc từ Silver

| Bước | Quy tắc | Còn lại | Bị loại |
|---|---|---|---|
| 0 | Toàn bộ tin trong `listing_current` | 163.087 | |
| 1 | Chỉ tin bán | 53.674 | 109.413 |
| 2 | Đủ điều kiện huấn luyện (giá, diện tích dương, có quận và loại hình) | 53.672 | 2 |
| 3 | Bỏ ngoại lai giá/m² và diện tích theo phân khúc | 51.705 | 1.967 |
| 4 | Bỏ phường mã 11221 (nguồn dồn tin không ánh xạ được sau sáp nhập 2025) | 51.304 | 401 |
| 5 | Bỏ tin nhắc tới Bình Dương, Bà Rịa - Vũng Tàu trong tiêu đề hoặc địa chỉ | 51.289 | 15 |
| 6 | Gộp tin trùng (cùng loại, phường, tọa độ, diện tích, phòng, tầng, giá) | 49.140 | 2.149 |
| 7 | Bỏ văn phòng và mặt bằng kinh doanh (giá "bán" thực chất là giá thuê hoặc sang nhượng) | 48.626 | 514 |

Giữ lại 90,6% số tin bán. Hai bước loại nhiều nhất là ngoại lai và gộp trùng.

### 4.2 Tổng quan theo loại hình

| Loại hình | Số tin | Tỷ lệ | Giá trung vị (tỷ) | Diện tích trung vị (m²) | Giá/m² trung vị (triệu) | Số loại con |
|---|---|---|---|---|---|---|
| Nhà ở | 35.288 | 72,6% | 5,90 | 56 | 110,0 | 4 |
| Căn hộ/Chung cư | 7.100 | 14,6% | 3,58 | 68 | 53,8 | 8 |
| Đất | 6.240 | 12,8% | 4,60 | 110 | 43,0 | 4 |

Phân bố giá lệch phải rất mạnh (độ lệch 12,6); log giá gần đối xứng (độ lệch -0,13). Trong cùng loại nhà ở, quận đắt nhất (Quận 1, 226 triệu/m²) gấp 11 lần quận rẻ nhất (Củ Chi, 20 triệu/m²). Vị trí là yếu tố quyết định giá.

### 4.3 Các cột

**Định danh và thời gian** (không dùng làm đặc trưng)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `listing_key` | string | Khóa tin, nối về `fct_listing` |
| `posted_date` | date | Ngày đăng. Dùng để chia train/test theo thời gian |
| `observed_date` | date | Ngày quan sát giá gần nhất |
| `duplicate_count` | int | Số tin đã gộp vào dòng này (1 là không trùng) |

**Vị trí** (12 cột)

| Cột | Kiểu | Ý nghĩa | Vai trò trong mô hình |
|---|---|---|---|
| `district_id` | bigint | Mã quận/huyện, 22 giá trị | Phân loại ít mức |
| `district` | string | Tên quận/huyện | Hiển thị |
| `district_type` | string | Quận, Huyện, Thành phố | Phân loại ít mức |
| `area_zone` | string | Nội thành, Ngoại thành | Phân loại ít mức |
| `ward_id` | bigint | Mã phường, 315 giá trị | Phân loại nhiều mức, target encoding |
| `ward` | string | Tên phường | Hiển thị |
| `street` | string | Tên đường, 6.677 giá trị | Phân loại nhiều mức, target encoding |
| `latitude`, `longitude` | double | Tọa độ | Số |
| `project_name` | string | Tên dự án, 789 giá trị, thiếu 87% (nhà ở và đất thường không thuộc dự án) | Phân loại nhiều mức, target encoding |
| `is_in_project` | boolean | Có thuộc dự án | Boolean |

**Loại hình** (3 cột)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `property_category` | string | Mã chuẩn hóa: HOUSE, APARTMENT, LAND |
| `property_type` | string | Căn hộ/Chung cư, Nhà ở, Đất |
| `property_subtype` | string | 16 loại con |

**Kích thước và kết cấu** (8 cột, kiểu số)

| Cột | Ý nghĩa | Tỷ lệ thiếu |
|---|---|---|
| `area_m2` | Diện tích | 0% |
| `living_area_m2` | Diện tích sử dụng, chỉ nhà ở | 64% |
| `width_m` | Chiều ngang, đặt NULL nếu > 100 m | 32% |
| `length_m` | Chiều dài, đặt NULL nếu > 300 m | 35% |
| `bedrooms` | Số phòng ngủ, đất không có | 13% |
| `bathrooms` | Số phòng vệ sinh | 36% |
| `total_floors` | Số tầng, chỉ nhà ở | 47% |
| `floor_number` | Tầng của căn hộ | 97% |

Giá trị thiếu được giữ nguyên là NaN và để cây quyết định tự xử lý, vì thiếu thường mang nghĩa (đất không có phòng ngủ, nhà không có số tầng căn hộ).

**Đặc điểm và tình trạng** (10 cột)

| Cột | Kiểu | Ý nghĩa | Tỷ lệ thiếu |
|---|---|---|---|
| `house_direction` | string, 8 mức | Hướng nhà | 76% |
| `furnishing` | string, 4 mức | Nội thất | 59% |
| `handover_status` | string, 2 mức | Tình trạng bàn giao | 90% |
| `has_car_alley` | boolean | Hẻm xe hơi | 0% |
| `is_frontage` | boolean | Mặt tiền | 0% |
| `is_wide_back` | boolean | Nở hậu | 0% |
| `is_narrow_back` | boolean | Tóp hậu | 0% |
| `is_dilapidated` | boolean | Nhà nát | 0% |
| `is_not_completed` | boolean | Chưa hoàn công | 0% |
| `is_planning_affected` | boolean | Dính quy hoạch | 0% |

**Pháp lý và người bán** (3 cột)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `legal_category` | string, 5 mức | Có sổ, Đang chờ sổ, Giấy tờ khác, Không có sổ, Không rõ |
| `has_certificate` | boolean | Đã có sổ (gộp Đã có sổ, Sổ hồng riêng, Sổ chung) |
| `is_broker` | boolean | Tin do môi giới đăng |

**Biến mục tiêu** (3 cột)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `price_vnd` | bigint | Giá rao, VND. Biến cần dự đoán |
| `log_price_vnd` | double | ln(giá). Mô hình học trên thang này |
| `price_per_m2` | double | Giá/m². Mô hình thực tế học ln(giá/m²) rồi cộng lại ln(diện tích) |

### 4.4 Cột cố ý không đưa vào

Các cột sau có trong Silver nhưng không vào bảng đặc trưng để tránh rò rỉ dữ liệu, vì người dùng không thể cung cấp chúng khi hỏi giá một căn chưa đăng: số ngày rao, số lần đổi giá, số ảnh, tiêu đề, mô tả, số điện thoại. Các đặc trưng tổng hợp theo khu vực (giá trung bình phường, đường, dự án) cũng không được tính sẵn trong Gold mà được tính bên trong tập train lúc huấn luyện.

## 5. Tập dữ liệu cho hai bài toán còn lại

| Bảng | Grain | Cột chính |
|---|---|---|
| `agg_price_trend_weekly`, `agg_price_trend_monthly` | kỳ × cấp địa lý (thành phố, nội/ngoại thành, quận) × loại giao dịch × loại hình | `listing_count`, `new_listing_count`, `price_cut_count`, `price_rise_count`, p25/median/p75/avg giá/m², median giá, median diện tích, `median_price_per_m2_change_pct`, `is_reliable` |
| `agg_market_supply_weekly` | tuần × cấp địa lý × loại giao dịch × loại hình | `active_listing_count`, `new_listing_count`, `removed_listing_count`, `net_supply_change`, `removal_rate_pct`, `price_cut_rate_pct`, `median_price_cut_pct`, `median_days_on_market_at_removal`, `is_reliable` |
| `ml_recommendation_features` | 1 tin đang rao, tách theo `transaction_type` | Thuộc tính mô tả tin (loại, vị trí, giá, diện tích, phòng, tiêu đề, mô tả) dùng cho gợi ý dựa trên nội dung |

Cờ `is_reliable` bằng true khi ô tổng hợp có ít nhất 10 tin; dashboard nên lọc theo cờ này.
