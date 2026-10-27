# GOLD LAYER — STAR SCHEMA DESIGN

Gold layer phục vụ phân tích thị trường bất động sản qua ba bài toán:

1. **Dự đoán giá nhà** (sale) → `ml_price_features`
2. **Xu hướng giá theo khu vực** (thành phố / nội-ngoại thành / quận, theo tuần và tháng) → `agg_price_trend_weekly`, `agg_price_trend_monthly`
3. **Nguồn cung và thanh khoản** (tin mới, tin bị gỡ, giảm giá, thời gian rao) → `agg_market_supply_weekly`

Các dimension và fact còn lại là nền tảng chung cho các bảng trên. Bảng nào không trả lời được một câu hỏi của ba bài toán thì không đưa vào Gold.

## 1. Vị trí trong kiến trúc Lakehouse (Medallion)

```
Nhà Tốt API ──► Landing (MinIO) ──► BRONZE ─────────► SILVER ───────────────────────► GOLD
                                    raw_properties    stg_nhatot_properties           dims + facts
                                    (raw JSON, Delta) int_listing_changes             price-trend marts
                                                      listing_history / current       ML feature table
                                                      listing_quarantine
                                                      int_crawl_snapshots
                                                      int_listing_removals
                                                      int_listing_enriched ─────────►
```

| Tầng   | Schema   | Vai trò |
|--------|----------|---------|
| Bronze | `bronze` | Dữ liệu thô, append-only, giữ nguyên payload JSON để có thể replay. |
| Silver | `silver` | Ép kiểu, làm sạch, kiểm tra chất lượng (tách quarantine), lịch sử thay đổi giá/nội dung của từng tin. |
| Gold   | `gold`   | Mô hình hoá theo nghiệp vụ (Kimball star schema) và các bảng phục vụ trực tiếp BI/ML. |

Airflow: `load_bronze → dbt_run_silver → dbt_test_silver → dbt_run_gold → dbt_test_gold`.
Gold chỉ được build khi Silver đã qua toàn bộ test (quality gate).

## 2. Silver: các thuộc tính quyết định giá

`stg_nhatot_properties` trích từ payload các thuộc tính có ảnh hưởng mạnh tới giá:

| Nhóm | Cột |
|------|-----|
| Kích thước | `area_m2`, `living_area_m2`, `width_m` (chiều ngang), `length_m` (chiều dài) |
| Kết cấu | `bedrooms`, `bathrooms`, `total_floors`, `floor_number` |
| Vị trí lô | `is_frontage` (mặt tiền), `has_car_alley` (hẻm xe hơi), `is_wide_back` (nở hậu), `is_narrow_back` (tóp hậu) |
| Tình trạng | `furnishing`, `house_direction`, `handover_status`, `is_dilapidated`, `is_not_completed`, `is_planning_affected` |
| Pháp lý | `legal_status` (đã loại các mã số thô '1','3','6') |
| Dự án / người bán | `project_name` (chuỗi rỗng → NULL), `is_broker` |

`int_listing_enriched` bổ sung hai quy tắc nghiệp vụ cho Gold:

- `is_price_outlier`, `outlier_reason`: giá/m² hoặc diện tích nằm ngoài [p1, p99] **trong từng phân khúc**
  (transaction_type × property_type), hoặc giá ≤ 0. Outlier chỉ được gắn cờ, không bị xoá.
- `is_full_snapshot`, `is_initial_load_snapshot`: lấy từ `int_crawl_snapshots` (xem mục 3). Lần crawl dở dang
  không được dùng để kết luận một tin đã bị gỡ.

Các ngưỡng nằm trong `vars` của `dbt_project.yml`.

## 3. Star schema

```
          ┌──────────────┐  ┌───────────────────┐  ┌──────────────────┐
          │  dim_date    │  │ dim_property_type │  │ dim_legal_status │
          └──────┬───────┘  └─────────┬─────────┘  └────────┬─────────┘
                 │                    │                     │
                 ▼                    ▼                     ▼
   ┌───────────────────────────────────────────────────────────────────┐
   │ fct_listing_snapshot  (periodic)      1 dòng / tin / lần crawl    │
   │ fct_listing           (accumulating)  1 dòng / tin                │
   │ fct_listing_event     (transaction)   1 dòng / sự kiện của tin    │
   └───────────────────────────────────────────────────────────────────┘
                 ▲                                          ▲
                 │                                          │
          ┌──────┴───────┐                           ┌──────┴──────┐
          │ dim_location │                           │ dim_project │
          └──────────────┘                           └─────────────┘
```

Ba fact tương ứng ba loại fact của Kimball, nối với nhau qua `listing_key`.

Surrogate key được sinh bởi macro trong `macros/gold_keys.sql` (`xxhash64`, BIGINT). Dim và fact dùng
cùng macro nên key luôn khớp. NULL được ánh xạ về member "Không rõ".

| Bảng | Grain | Dùng cho |
|------|-------|----------|
| `dim_date` | 1 ngày | Trục thời gian của xu hướng |
| `dim_location` | (tỉnh, quận, phường) | Cắt xu hướng theo khu vực; feature vị trí. Có `area_zone` (Nội/Ngoại thành), `district_type` |
| `dim_property_type` | (loại, loại con) | Tách phân khúc để so sánh giá/m² cùng loại; feature |
| `dim_legal_status` | tình trạng pháp lý | `legal_category`, `has_certificate`, dùng làm feature |
| `dim_project` | dự án | So sánh giá giữa các dự án; dự án được gán vào quận có nhiều tin nhất. Có member "Không thuộc dự án" |
| `fct_listing_snapshot` | tin × lần crawl | Nguồn của mart xu hướng: giá từng tin qua thời gian, cờ tăng/giảm giá |
| `fct_listing` | 1 tin | Trạng thái mới nhất của mỗi tin, dùng cho bảng ML; số lần đổi giá; milestone `removed_at` (xem dưới), `is_active = removed_at is null` |
| `fct_listing_event` | 1 sự kiện | `NEW`, `PRICE_CHANGED`, `UPDATED`, `REMOVED`. Nguồn của mart nguồn cung/thanh khoản |

**Thứ tự và độ đầy đủ của lần crawl** được tính ở một nơi duy nhất là `int_crawl_snapshots`. Các lần crawl được sắp theo thời gian bắt đầu, không theo chuỗi `snapshot_id`, vì `legacy_…` xếp sau `2026…` theo thứ tự chuỗi. Một lần crawl được coi là đầy đủ khi số tin nó chạm tới mà vẫn còn online (mọi phản hồi trừ 404/410) đạt ≥ `full_snapshot_min_ratio` so với lần lớn nhất.

**Quy tắc gỡ tin** (`int_listing_removals`): một tin "online" trong lần crawl khi được crawl tới với phản hồi khác 404/410. Như vậy tin bị rate-limit (429/5xx) hay bị quarantine (ví dụ người đăng xoá giá) vẫn tính là đang rao. Sau lần cuối tin online, tin bị coi là gỡ tại thời điểm sớm nhất trong hai mốc:
- `GONE_STATUS`: nguồn trả 404/410;
- `ABSENT_FROM_FULL_SNAPSHOT`: lần crawl đầy đủ kế tiếp không chạm tới tin.

Snapshot dở dang không bao giờ kết luận tin bị gỡ. Gold được build lại toàn bộ mỗi lần chạy, nên tin xuất hiện lại sẽ tự động hết bị tính là gỡ. `fct_listing_event` lấy sự kiện `REMOVED` từ `fct_listing`. Test `assert_listing_events_reconcile` đảm bảo hai bảng khớp nhau, còn `assert_removed_listings_not_online` đảm bảo không tin nào bị gỡ mà vẫn online sau đó.

**Tồn kho ban đầu:** tin được thấy lần đầu ở các lần crawl đến hết full snapshot đầu tiên có `fct_listing.is_initial_inventory = true`. Đó là tồn kho có sẵn khi pipeline bắt đầu chạy, không phải nguồn cung mới. Cả `agg_market_supply_weekly` lẫn `agg_price_trend_*` đều dùng cờ này để không đếm những tin đó vào `new_listing_count`.

## 4. Bài toán 1 — Dự đoán giá: `ml_price_features`

- **Grain:** 1 BĐS **bán** riêng biệt (trạng thái mới nhất), chỉ gồm tin `is_price_training_eligible` và không phải outlier.
  Các bản sao của cùng một BĐS do nhiều môi giới đăng (trùng loại, phường, toạ độ làm tròn 4 chữ số, diện tích,
  số phòng, số tầng, giá) được gộp lại, giữ bản thấy gần nhất. `duplicate_count` cho biết đã gộp bao nhiêu tin.
  Nếu không gộp, khi chia train/test ngẫu nhiên, cùng một BĐS sẽ nằm ở cả hai tập.
- **Feature:** vị trí (quận, phường, đường, lat/long, dự án), loại BĐS, kích thước (diện tích, ngang, dài,
  diện tích sử dụng), số phòng/tầng, mặt tiền/hẻm xe hơi/nở hậu, nội thất, hướng, pháp lý, môi giới.
- **Target:** `price_vnd`, `log_price_vnd` (khuyến nghị dùng log vì giá lệch phải), `price_per_m2`.
- **Nguyên tắc chống leakage:**
  - Chỉ đưa vào những thuộc tính người dùng có thể nhập khi hỏi giá. Bỏ `days_on_market` và `image_count`.
  - Không đưa feature tổng hợp theo khu vực (ví dụ giá trung vị của quận). Các feature này phải được tính
    **bên trong tập train** khi huấn luyện.
  - Kích thước vô lý (ngang > 100 m, dài > 300 m) được đặt NULL, không loại cả dòng.
- `observed_date` dùng để chia train/test theo thời gian.

> Giới hạn: đây là **giá rao bán**, không phải giá giao dịch thực tế.

## 5. Bài toán 2 — Xu hướng giá: `agg_price_trend_weekly` / `agg_price_trend_monthly`

- **Grain:** kỳ (tuần/tháng) × `geo_level` × `geo_name` × transaction_type × property_type,
  với `geo_level` ∈ {`CITY`, `ZONE`, `DISTRICT`}.
- **Measures:** `listing_count`, `new_listing_count`, `price_cut_count`, `price_rise_count`,
  p25/median/p75/avg giá/m², median giá, median diện tích, `median_price_per_m2_change_pct` (so với kỳ liền trước).
- **Thiết kế để số liệu đúng:**
  - Mỗi tin chỉ được đếm **1 lần mỗi kỳ** (lấy lần quan sát cuối kỳ), nên số tin không bị nhân lên theo số lần crawl.
  - Median của mọi cấp (quận, nội/ngoại thành, toàn TP) được tính **trực tiếp từ dữ liệu tin**
    bằng `GROUPING SETS`, không lấy trung bình của các median cấp dưới.
  - So sánh giá/m² **trong cùng property_type** để giảm sai lệch do thay đổi cơ cấu hàng (mix shift).
  - `is_reliable = listing_count ≥ trend_min_sample` (mặc định 10). Dashboard nên lọc theo cờ này.
  - % thay đổi chỉ được tính khi kỳ trước là kỳ **liền kề**.

> Giới hạn: xu hướng chỉ có ý nghĩa khi pipeline chạy đều hằng ngày trong nhiều tuần/tháng.
> Không dựng lại lịch sử từ `posted_at`, vì chỉ có tin còn đang đăng mới xuất hiện (survivorship bias).

## 6. Bài toán 3 — Nguồn cung và thanh khoản: `agg_market_supply_weekly`

- **Grain:** giống `agg_price_trend_weekly` (tuần × `geo_level` × `geo_name` × transaction_type × property_type),
  nên hai mart join được với nhau để xem giá và cung cùng lúc.
- **Measures:** `active_listing_count` (tin có mặt trong tuần), `new_listing_count`, `removed_listing_count`,
  `net_supply_change`, `price_cut_listing_count`, `price_rise_listing_count`, `removal_rate_pct`,
  `price_cut_rate_pct`, `median_price_cut_pct`, `median_days_on_market_at_removal`.
- Mọi measure đếm **số tin khác nhau**, nên số lần crawl không làm phồng số liệu. Median tính từ dữ liệu tin
  ở mọi cấp địa lý bằng `GROUPING SETS`. Chỉ dùng tin `is_market_eligible`.
- `is_reliable = active_listing_count ≥ trend_min_sample`.

> Giới hạn: tin bị gỡ chỉ là **proxy** cho giao dịch. Tin cũng có thể hết hạn, bị người đăng rút, hoặc
> rơi ra khỏi phạm vi crawl. Nên diễn giải `removal_rate_pct` là tốc độ "rời thị trường", không phải tỷ lệ bán được.

## 7. Kiểm thử

- `unique` / `not_null` cho khoá chính; `relationships` cho mọi khoá ngoại fact → dim.
- `dbt_expectations`: miền giá trị (giá > 0, chiều ngang trong [0, 100]), tính duy nhất của grain trong các mart.
- `assert_gold_listing_reconciles_silver`: số tin/observation ở Gold bằng đúng Silver.
- `assert_ml_features_clean`: bảng ML chỉ chứa tin bán đủ điều kiện, không phải outlier.
- `assert_listing_events_reconcile`: mỗi tin có đúng 1 sự kiện `NEW`; số `REMOVED` bằng số tin không active;
  số `PRICE_CHANGED` khớp với `fct_listing_snapshot`.

## 8. Chạy thủ công

```bash
cd /opt/airflow/dbt
dbt deps
dbt run  --select stg_nhatot_properties+ --exclude path:models/gold+  # Silver
dbt test --select stg_nhatot_properties+ --exclude path:models/gold+
dbt run  --select path:models/gold                                    # Gold
dbt test --select path:models/gold
```

Khi thêm cột mới vào `stg_nhatot_properties` (incremental), cần chạy một lần với `--full-refresh`.

Khi nâng cấp từ Silver cũ sang phiên bản dùng `changes_loaded_at`, các dòng
lịch sử có sẵn có thể vẫn NULL ở cột mới dù `append_new_columns` đã thêm cột.
Sau khi lần `dbt run` đầu đã thêm cột mới, chạy một lần trong container Airflow
để điền timestamp nạp cho các dòng cũ, cập nhật trạng thái hiện tại và test Silver.
Macro chỉ cập nhật timestamp còn NULL, giữ nguyên dữ liệu và thứ tự lịch sử;
không cần full-refresh toàn bộ lịch sử trên máy ít RAM:

```bash
cd /opt/airflow/dbt
dbt run-operation backfill_legacy_change_watermarks --profiles-dir .
dbt run --select listing_current int_listing_enriched --profiles-dir .
dbt test --select stg_nhatot_properties+ --exclude path:models/gold+ --indirect-selection cautious --profiles-dir .
```

Sau khi test đạt, clear task bị lỗi cùng downstream của đúng DAG run để tiếp
tục sang Gold. Các lần chạy tiếp theo dùng incremental bình thường.

Nếu `not_null_fct_listing_posted_date_key` lỗi trên dữ liệu cũ còn NULL ở
`posted_at`, dù Bronze có `list_time`, backfill theo cùng quy tắc
`coalesce(orig_list_time, list_time)` của staging. Macro chỉ điền ngày còn thiếu,
giữ nguyên ngày đã có và observation đang được chọn ở `listing_current`:

```bash
cd /opt/airflow/dbt
dbt run-operation backfill_legacy_posted_at --profiles-dir .
dbt run --select fct_listing fct_listing_event agg_market_supply_weekly ml_price_features --profiles-dir .
dbt test --select path:models/gold --profiles-dir .
```
