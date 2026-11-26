# Mô hình Dim – Fact tầng Gold

Tài liệu mô tả các bảng chiều (dimension) và bảng sự kiện (fact) ở tầng Gold của Lakehouse. Tầng Gold được xây bằng dbt trên Spark, lưu dưới dạng Delta Lake trong database `gold`. Mọi bảng dim và fact đều lấy dữ liệu từ `int_listing_enriched`, là tập quan sát hợp lệ của Silver đã được làm sạch và gắn cờ ngoại lai.

## 1. Tổng quan

Tầng Gold được thiết kế theo mô hình **sao (star schema)** của Kimball:

- 5 bảng chiều mô tả bối cảnh của một tin rao: thời gian, vị trí, loại bất động sản, pháp lý, dự án.
- 3 bảng sự kiện ghi lại số đo ở ba mức độ hạt khác nhau: mỗi lần crawl, mỗi tin, mỗi sự kiện thay đổi.
- Trên các bảng fact có thêm 3 bảng tổng hợp (mart) phục vụ dashboard và 2 bảng đặc trưng phục vụ mô hình học máy.

| Nhóm | Bảng | Độ hạt (một dòng là gì) |
|---|---|---|
| Dimension | `dim_date` | Một ngày lịch |
| Dimension | `dim_location` | Một phường (tỉnh > quận/huyện > phường) |
| Dimension | `dim_property_type` | Một cặp loại > loại phụ bất động sản |
| Dimension | `dim_legal_status` | Một tình trạng pháp lý |
| Dimension | `dim_project` | Một dự án bất động sản |
| Fact | `fct_listing_snapshot` | Một tin tại một lần crawl |
| Fact | `fct_listing` | Một tin (trạng thái mới nhất và vòng đời) |
| Fact | `fct_listing_event` | Một sự kiện của tin (NEW, PRICE_CHANGED, UPDATED, REMOVED) |
| Mart | `agg_price_trend_weekly`, `agg_price_trend_monthly` | Một kỳ × khu vực × loại giao dịch × loại BĐS |
| Mart | `agg_market_supply_weekly` | Một tuần × khu vực × loại giao dịch × loại BĐS |
| ML | `ml_price_features`, `ml_recommendation_features` | Một bất động sản bán (đã gộp tin trùng) |

## 2. Các bảng chiều (Dimension)

### 2.1 `dim_date` – chiều thời gian

**Mô tả.** Bảng lịch sinh sẵn từ 01/01/2024 đến 31/12/2030 (cấu hình `date_spine_start`, `date_spine_end`), mỗi ngày một dòng.

**Mục đích.** Cho phép nhóm và lọc số liệu theo ngày, tuần, tháng, quý, năm mà không phải tính hàm ngày tháng trong mỗi truy vấn. Dùng chung cho mọi cột ngày của các bảng fact.

**Cột chính.**

| Cột | Ý nghĩa |
|---|---|
| `date_key` | Khóa chính, số nguyên dạng `yyyyMMdd` (ví dụ 20251124) |
| `date_day` | Ngày thực |
| `year`, `quarter`, `year_quarter` | Năm, quý, chuỗi `2025-Q4` |
| `month`, `year_month` | Tháng, chuỗi `2025-11` |
| `week_of_year`, `day_of_month` | Tuần trong năm, ngày trong tháng |
| `day_of_week`, `day_name` | Thứ theo ISO (1 = Thứ Hai … 7 = Chủ Nhật), tên thứ |
| `is_weekend`, `is_month_end` | Cờ cuối tuần, cờ ngày cuối tháng |

**Khóa liên kết.** `date_key` được tham chiếu bởi `snapshot_date_key` (fct_listing_snapshot), `event_date_key` (fct_listing_event) và 4 cột `posted_date_key`, `first_seen_date_key`, `last_seen_date_key`, `removed_date_key` (fct_listing). Đây là chiều đóng nhiều vai (role-playing dimension).

### 2.2 `dim_location` – chiều vị trí

**Mô tả.** Phân cấp hành chính tỉnh > quận/huyện > phường. Mỗi dòng là một bộ (`province_id`, `district_id`, `ward_id`) xuất hiện trong dữ liệu. Tên được lấy từ quan sát mới nhất để phòng nguồn đổi tên.

**Mục đích.** Trục phân tích theo địa lý: so sánh giá và nguồn cung giữa quận, giữa nội thành và ngoại thành. Là chiều quan trọng nhất của đồ án.

**Cột chính.**

| Cột | Ý nghĩa |
|---|---|
| `location_key` | Khóa chính, bigint băm từ 3 mã hành chính |
| `province_id`, `province` | Mã và tên tỉnh (hiện chỉ TP.HCM, mã 13000) |
| `district_id`, `district` | Mã và tên quận/huyện/thành phố |
| `district_type` | `Quận`, `Huyện`, `Thành phố`, `Không rõ` |
| `area_zone` | `Nội thành` (Quận, TP. Thủ Đức), `Ngoại thành` (Huyện), `Không rõ` |
| `ward_id`, `ward` | Mã và tên phường |

**Khóa liên kết.** `location_key` xuất hiện trong cả 3 bảng fact. Dòng không rõ vị trí có mã -1 ở các cấp thiếu và tên `Không rõ`.

### 2.3 `dim_property_type` – chiều loại bất động sản

**Mô tả.** Phân cấp loại > loại phụ, ví dụ `Nhà ở` > `Nhà mặt phố`, `Căn hộ/Chung cư` > `Chung cư`. Mỗi dòng là một cặp (`property_type`, `property_subtype`).

**Mục đích.** Tách thị trường theo loại hình để so sánh đúng phân khúc: giá/m² của đất không so với giá/m² của căn hộ.

**Cột chính.**

| Cột | Ý nghĩa |
|---|---|
| `property_type_key` | Khóa chính, bigint băm từ cặp loại/loại phụ |
| `property_type` | Tên loại theo nguồn, đã cắt khoảng trắng |
| `property_subtype` | Loại phụ; nếu thiếu thì lấy bằng loại |
| `property_category` | Mã chuẩn hóa: `HOUSE`, `APARTMENT`, `LAND`, `ROOM`, `COMMERCIAL`, `OTHER` |
| `is_residential` | True với Nhà ở, Căn hộ/Chung cư, Phòng trọ |

**Khóa liên kết.** `property_type_key` xuất hiện trong cả 3 bảng fact. Khóa băm giữ giá trị gốc chưa cắt khoảng trắng để luôn khớp với fact.

### 2.4 `dim_legal_status` – chiều pháp lý

**Mô tả.** Tình trạng giấy tờ của tin rao, đã gom thành nhóm phân tích. Có thêm một dòng `Không rõ` cho tin không khai.

**Mục đích.** Đánh giá ảnh hưởng của pháp lý lên giá và tốc độ gỡ tin; lọc dữ liệu huấn luyện theo nhóm có sổ.

**Cột chính.**

| Cột | Ý nghĩa |
|---|---|
| `legal_status_key` | Khóa chính, bigint băm từ tình trạng pháp lý gốc |
| `legal_status` | Giá trị gốc đã làm sạch (Đã có sổ, Sổ hồng riêng, Đang chờ sổ, Hợp đồng mua bán, …) |
| `legal_category` | Nhóm: `Có sổ`, `Đang chờ sổ`, `Giấy tờ khác`, `Không có sổ`, `Không rõ` |
| `has_certificate` | True khi thuộc nhóm `Có sổ` |

**Khóa liên kết.** `legal_status_key` xuất hiện trong cả 3 bảng fact.

### 2.5 `dim_project` – chiều dự án

**Mô tả.** Dự án (khu dân cư, chung cư) mà tin thuộc về. Mỗi dự án một dòng, cộng một dòng `Không thuộc dự án`. Dự án được đặt ở quận có nhiều tin nhất, vì một số tin ghi sai quận.

**Mục đích.** Chuẩn hóa tên dự án và gán mỗi dự án về đúng quận. Là nguồn của hai đặc trưng `project_name` và `is_in_project` trong `ml_price_features` (mô hình giá dùng `project_name` qua target encoding), và là trục của biểu đồ giá/m² theo dự án căn hộ trên dashboard.

**Cột chính.**

| Cột | Ý nghĩa |
|---|---|
| `project_key` | Khóa chính, bigint băm từ tên dự án |
| `project_name` | Tên dự án |
| `is_project` | False với dòng `Không thuộc dự án` |
| `province`, `district_id`, `district` | Quận chủ yếu của dự án; NULL với dòng không thuộc dự án |

**Khóa liên kết.** `project_key` xuất hiện trong cả 3 bảng fact.

## 3. Các bảng sự kiện (Fact)

### 3.1 `fct_listing_snapshot` – fact ảnh chụp định kỳ

**Loại.** Periodic snapshot fact. Một dòng là một quan sát hợp lệ của một tin tại một lần crawl (`observation_id`).

**Mục đích.** Lưu giá của mọi tin ở mọi thời điểm crawl, là nguồn cho các mart xu hướng giá theo tuần/tháng và thống kê nguồn cung đang rao. Bảng lớn nhất tầng Gold, được nạp tăng dần (incremental merge theo `observation_id`) và phân vùng theo `snapshot_date_key`.

**Cột chính.**

| Nhóm | Cột |
|---|---|
| Khóa | `observation_id` (PK), `listing_key`, `snapshot_id` |
| Khóa ngoại | `snapshot_date_key`, `location_key`, `property_type_key`, `legal_status_key`, `project_key` |
| Chiều suy biến | `transaction_type` (sale/rent), `change_type`, `crawl_date`, `crawl_timestamp` |
| Số đo | `price_vnd`, `area_m2`, `price_per_m2`, `price_change_vnd`, `price_change_pct` |
| Cờ | `is_new_listing`, `is_price_increase`, `is_price_decrease`, `is_market_eligible`, `is_price_outlier`, `outlier_reason` |

### 3.2 `fct_listing` – fact ảnh chụp tích lũy

**Loại.** Accumulating snapshot fact. Một dòng là một tin (`listing_key`), mô tả trạng thái mới nhất và toàn bộ vòng đời từ lần đầu thấy đến lúc bị gỡ.

**Mục đích.** Trả lời câu hỏi ở mức tin: tin còn hoạt động không, đã đổi giá bao nhiêu lần, tổng giảm giá bao nhiêu phần trăm, nằm trên thị trường bao nhiêu ngày. Là bảng gốc để lọc dữ liệu huấn luyện mô hình giá (`is_price_training_eligible`).

**Cột chính.**

| Nhóm | Cột |
|---|---|
| Khóa | `listing_key` (PK), `latest_observation_id` |
| Khóa ngoại | `location_key`, `property_type_key`, `legal_status_key`, `project_key`; 4 khóa ngày `posted_date_key`, `first_seen_date_key`, `last_seen_date_key`, `removed_date_key` |
| Chiều suy biến | `transaction_type` |
| Số đo hiện tại | `current_price_vnd`, `area_m2`, `current_price_per_m2` |
| Số đo vòng đời | `initial_price_vnd`, `total_price_change_vnd`, `total_price_change_pct`, `price_change_count`, `observation_count`, `days_tracked`, `days_on_market` |
| Mốc thời gian | `posted_at`, `first_seen_at`, `last_seen_at`, `removed_at`, `removed_snapshot_id`, `removal_evidence` (`GONE_STATUS` hoặc `ABSENT_FROM_FULL_SNAPSHOT`) |
| Cờ | `is_active`, `is_initial_inventory`, `is_market_eligible`, `is_price_training_eligible`, `is_price_outlier`, `outlier_reason` |

### 3.3 `fct_listing_event` – fact giao dịch

**Loại.** Transaction fact. Một dòng là một sự kiện trong vòng đời tin:

- `NEW`: lần đầu crawl thấy tin.
- `PRICE_CHANGED`: giá khác lần quan sát trước.
- `UPDATED`: nội dung đổi, giá không đổi.
- `REMOVED`: tin trả 404/410 hoặc biến mất khỏi một lần crawl đầy đủ sau đó.

**Mục đích.** Đo nguồn cung và thanh khoản: số tin mới, số tin gỡ, số lần giảm giá, số ngày trên thị trường lúc gỡ. Là nguồn của mart `agg_market_supply_weekly`. Phân vùng theo `event_date_key`.

**Cột chính.**

| Nhóm | Cột |
|---|---|
| Khóa | `event_id` (PK), `listing_key`, `snapshot_id` |
| Khóa ngoại | `event_date_key`, `location_key`, `property_type_key`, `legal_status_key`, `project_key` |
| Chiều suy biến | `event_type`, `transaction_type`, `event_at` |
| Số đo | `price_vnd`, `previous_price_vnd`, `price_change_vnd`, `price_change_pct`, `area_m2`, `price_per_m2`, `days_since_first_seen`, `days_on_market` |
| Cờ | `is_initial_load` (NEW của đợt nạp đầu, không tính là cung mới), `is_price_decrease`, `is_price_increase`, `is_market_eligible`, `is_price_outlier` |

Sự kiện `REMOVED` không có trong lịch sử quan sát, nên được sinh từ `fct_listing` và mang trạng thái cuối cùng của tin.

## 4. Khóa liên kết

### 4.1 Cách sinh khóa thay thế

Khóa của dim và fact được sinh bởi cùng một bộ macro dbt (`macros/gold_keys.sql`) nên luôn khớp khi join, không cần tra bảng. Hàm băm `xxhash64` trả về bigint, join nhanh và tương thích tốt với công cụ BI hơn chuỗi sha2. Giá trị NULL được đưa về một thành viên "không rõ" rõ ràng thay vì để NULL ở khóa ngoại.

| Khóa | Công thức | Thành viên không rõ |
|---|---|---|
| `date_key` | `cast(date_format(ngày, 'yyyyMMdd') as int)` | – |
| `location_key` | `xxhash64(province_id, district_id, ward_id)` | mã thiếu thay bằng -1 |
| `property_type_key` | `xxhash64(property_type, property_subtype)` | `__UNKNOWN__`, `__NONE__` |
| `legal_status_key` | `xxhash64(legal_status)` | `__UNKNOWN__` |
| `project_key` | `xxhash64(project_name)` | `__NONE__` |

### 4.2 Ma trận bus (dimension × fact)

| Dimension | fct_listing_snapshot | fct_listing | fct_listing_event |
|---|---|---|---|
| `dim_date` | `snapshot_date_key` | `posted_date_key`, `first_seen_date_key`, `last_seen_date_key`, `removed_date_key` | `event_date_key` |
| `dim_location` | `location_key` | `location_key` | `location_key` |
| `dim_property_type` | `property_type_key` | `property_type_key` | `property_type_key` |
| `dim_legal_status` | `legal_status_key` | `legal_status_key` | `legal_status_key` |
| `dim_project` | `project_key` | `project_key` | `project_key` |

Ngoài ra `listing_key` nối ba bảng fact với nhau: `fct_listing_snapshot` và `fct_listing_event` đều tham chiếu `fct_listing`. Mọi quan hệ trên được kiểm tra bằng dbt test `relationships` và `not_null`.

### 4.3 Sơ đồ quan hệ

Fact chính là `fct_listing`: dashboard và bảng đặc trưng mô hình đều đi từ bảng này. Hai fact còn lại là fact phụ cho phân tích theo thời gian, nối với fact chính qua `listing_key` và dùng lại đúng 5 chiều.

```
                       +------------------+
                       |     dim_date     |
                       |------------------|
                       | PK date_key      |
                       +------------------+
                                ^
                                | posted_date_key, first_seen_date_key,
                                | last_seen_date_key, removed_date_key
                                |
+------------------+   +-----------------------------+   +--------------------+
|   dim_location   |   |         fct_listing         |   | dim_property_type  |
|------------------|   |-----------------------------|   |--------------------|
| PK location_key  |<--| PK listing_key              |-->| PK property_type_  |
| province         |   | FK location_key             |   |    key             |
| district         |   | FK property_type_key        |   | property_type      |
| district_type    |   | FK legal_status_key         |   | property_subtype   |
| area_zone        |   | FK project_key              |   | property_category  |
| ward             |   | FK posted_date_key          |   | is_residential     |
+------------------+   | FK first_seen_date_key      |   +--------------------+
                       | FK last_seen_date_key       |
                       | FK removed_date_key         |
+------------------+   |    transaction_type         |   +--------------------+
| dim_legal_status |   |    current_price_vnd        |   |    dim_project     |
|------------------|   |    area_m2                  |   |--------------------|
| PK legal_status_ |<--|    current_price_per_m2     |-->| PK project_key     |
|    key           |   |    initial_price_vnd        |   | project_name       |
| legal_status     |   |    total_price_change_pct   |   | is_project         |
| legal_category   |   |    price_change_count       |   | district           |
| has_certificate  |   |    days_on_market           |   +--------------------+
+------------------+   |    is_active                |
                       |    is_training_eligible     |
                       +-----------------------------+
                                ^ listing_key
                                |
          +---------------------+---------------------+
          |                                           |
+-----------------------+                 +-----------------------+
|  fct_listing_snapshot |                 |   fct_listing_event   |
|  (fact phụ: giá theo  |                 |  (fact phụ: sự kiện   |
|   từng lần crawl)     |                 |   NEW / PRICE_CHANGED |
|  cùng 5 FK chiều      |                 |   UPDATED / REMOVED)  |
+-----------------------+                 |  cùng 5 FK chiều      |
                                          +-----------------------+
```

Mũi tên đi từ khóa ngoại trong fact tới khóa chính của chiều. `dim_date` đóng 4 vai trong `fct_listing` (ngày đăng, ngày thấy đầu, ngày thấy cuối, ngày gỡ).

## 5. Bảng dẫn xuất từ fact

| Bảng | Nguồn | Nội dung |
|---|---|---|
| `agg_price_trend_weekly` / `_monthly` | `fct_listing_snapshot` | Giá/m² trung vị theo kỳ × cấp địa lý (CITY, ZONE, DISTRICT) × loại giao dịch × loại BĐS; loại ngoại lai, mỗi tin tính một lần mỗi kỳ, cờ đủ mẫu khi ≥ 10 tin |
| `agg_market_supply_weekly` | `fct_listing_snapshot`, `fct_listing_event` | Tin đang rao, tin mới, tin gỡ, tỷ lệ gỡ, số lần giảm giá, ngày trên thị trường lúc gỡ; cùng độ hạt với mart giá để join được |
| `ml_price_features` | `fct_listing` + 4 dim (`dim_location`, `dim_property_type`, `dim_legal_status`, `dim_project`) + Silver | Đặc trưng dự đoán giá bán, một dòng một bất động sản, gộp tin trùng, loại ngoại lai và vị trí không tin cậy |
| `ml_recommendation_features` | `fct_listing` + Silver | Đặc trưng cho mô hình gợi ý |

## 6. Tổng kết thiết kế

- **Mô hình sao, chiều dùng chung.** 5 dimension được chuẩn hóa một lần và dùng lại cho cả 3 fact, bảo đảm mọi báo cáo nói về cùng một "Quận 7" hay cùng một "Căn hộ/Chung cư".
- **Ba loại fact cho ba loại câu hỏi.** Snapshot định kỳ trả lời "giá biến động thế nào theo thời gian"; snapshot tích lũy trả lời "một tin đã trải qua những gì"; fact giao dịch trả lời "thị trường có bao nhiêu tin vào, ra, giảm giá". Cùng dữ liệu gốc nhưng độ hạt khác nhau giúp truy vấn đúng bảng, không phải tính lại.
- **Khóa thay thế băm, không tra bảng.** `xxhash64` trên thuộc tính tự nhiên cho phép fact và dim sinh độc lập, không cần lookup, nạp tăng dần vẫn đúng khóa. Thành viên "không rõ" thay cho NULL nên inner join không làm mất dòng.
- **Chiều suy biến giữ tại fact.** `transaction_type`, `change_type`, `event_type` chỉ có vài giá trị nên để ngay trong fact, không tạo bảng chiều riêng.
- **Chất lượng dữ liệu đi theo dòng.** Các cờ `is_market_eligible`, `is_price_training_eligible`, `is_price_outlier`, `is_initial_inventory` nằm trong fact để mart và mô hình tự lọc theo mục đích, dữ liệu gốc không bị xóa.
- **Tối ưu lưu trữ.** Delta Lake, phân vùng theo khóa ngày, bảng snapshot nạp tăng dần theo merge; dbt test kiểm tra khóa duy nhất, khóa ngoại và miền giá trị sau mỗi lần chạy.
