# Gold và mô hình gợi ý bất động sản cho sale/rent

Ngày thực hiện: 23/10/2026. Mã nguồn nằm trong repository LakeHouse hiện tại.

## 1. Bài toán là gì?

Xây hệ thống **gợi ý bất động sản dựa trên nội dung và yêu cầu người dùng**:

1. **Tin tương tự:** từ một `listing_key`, trả Top-K tin khác cùng thị trường,
   loại bất động sản, tỉnh và kỳ giá; xếp theo nội dung, giá, diện tích, vị trí,
   đặc điểm căn nhà và độ mới.
2. **Gợi ý theo nhu cầu:** người dùng chọn mua/thuê, loại BĐS, địa phương,
   ngân sách, diện tích, số phòng và từ khóa. Bộ lọc bắt buộc được áp dụng trước
   khi xếp hạng. Có thể trả ít hơn K nếu không đủ tin đáp ứng.

Đây không phải bài toán dự đoán giá. `price_vnd` là một đặc trưng và ràng buộc
ngân sách, không phải nhãn cần dự đoán. Mô hình dự đoán giá hiện có vẫn độc lập.
Chưa có lịch sử click/save/contact của người dùng nên phiên bản này chưa học
cá nhân hóa theo hành vi, không sử dụng collaborative filtering và không tạo
nhãn tương tác giả từ dữ liệu crawl.

## 2. Dữ liệu để ở đâu? Lưu như thế nào?

| Thành phần | Vị trí thực tế | Định dạng / vai trò |
|---|---|---|
| Silver đầu vào | `silver.listing_current` → `s3://lakehouse/warehouse/silver.db/listing_current` | Delta; trạng thái sạch mới nhất của mỗi tin |
| Bằng chứng gỡ tin | `silver.int_listing_removals` | Delta; nhận diện tin bị gỡ theo quy tắc pipeline hiện có |
| Gold mới | `gold.ml_recommendation_features` → `s3://lakehouse/warehouse/gold.db/ml_recommendation_features` | Delta, partition theo `transaction_type` |
| Snapshot export lần này | `artifacts/recommendation/exports/20261023_v1/` | `catalog.parquet` + `snapshot.json` |
| Bundle đã train | `artifacts/recommendation/models/20261023_v1/` | Manifest + model/index/catalog riêng cho sale và rent |
| Báo cáo chạy thật | `artifacts/recommendation/reports/20261023_v1.json` | Kiểm tra ràng buộc và latency; không phải relevance metrics |
| Mã tạo Gold | `dbt/models/gold/ml/ml_recommendation_features.sql` | dbt/Spark SQL |
| Mã mô hình | `recommendation/` | Python, NumPy, Pandas, SciPy, scikit-learn, PyArrow |

Các đường dẫn `artifacts/...` tính từ thư mục gốc repository. Chúng được giữ
trên máy để dùng lại nhưng bị Git ignore. Dữ liệu chuẩn vẫn nằm ở MinIO;
Hive Metastore quản lý tên/schema/location, không chứa toàn bộ dữ liệu bảng.

`snapshot.json` ghi Delta version, table ID, số file đang hiệu lực, số dòng và
SHA-256 của Parquet. Export chỉ đọc các `add` còn hiệu lực sau khi replay Delta
log/checkpoint; không gom bừa mọi Parquet, vì file cũ sau MERGE vẫn có thể tồn tại.
Exporter hiện hỗ trợ Delta reader protocol 1 và checkpoint đơn cổ điển; gặp
deletion vectors/column mapping/checkpoint không hỗ trợ sẽ dừng để dùng Spark
Delta export, thay vì âm thầm đọc sai.

```text
MinIO / silver.listing_current ─┐
                              ├─ dbt → gold.ml_recommendation_features
MinIO / int_listing_removals ──┘                │
                                      snapshot có version
                                              │
                                      lọc đủ điều kiện
                                      ┌───────┴───────┐
                                    sale             rent
                                  TF-IDF riêng     TF-IDF riêng
                                      └───────┬───────┘
                                      bundle bất biến
                                              │
                                   Python engine / CLI Top-K
```

## 3. Xử lý từ Silver sang Gold như thế nào?

Gold có grain **một dòng trên một tin `listing_key`**, không tuyên bố một dòng
là một căn nhà vật lý duy nhất. Gold được materialize dạng table để loại bỏ
được trạng thái cũ khi tin bị gỡ hoặc xuất hiện lại; không dùng MERGE chỉ thêm
tin mới mà bỏ quên việc thu hồi tin.

Các bước đã triển khai:

1. Đọc `ref('listing_current')`, chỉ nhận hai giao dịch `sale` và `rent`.
2. Join `ref('int_listing_removals')` theo `listing_key`; giữ `removed_at` và
   `removal_evidence` để truy vết. Quy tắc gỡ theo snapshot là bằng chứng của
   pipeline, không phải chứng nhận căn nhà đã giao dịch thành công.
3. Chuẩn hóa `property_category`: `HOUSE`, `APARTMENT`, `LAND`, `ROOM`,
   `COMMERCIAL`, `OTHER`; vẫn giữ tên tiếng Việt ban đầu.
4. Tách `price_period`, tạo log giá/log diện tích/giá trên m², tổng hợp nội dung.
5. Gắn `location_trusted` theo các quy tắc chất lượng địa điểm đã có trong dbt.
   Khi địa điểm không đáng tin, không dùng tọa độ và không cho vượt bộ lọc
   district. Không suy ra khoảng cách chính xác từ tọa độ gán sai.
6. Gắn `is_recommendable` khi tin đạt chất lượng thị trường, giá/diện tích dương,
   không nằm trong bảng gỡ tin, có thời gian quan sát hợp lệ và trạng thái nguồn
   được chấp nhận. Mapping hiện dành cho NhaTot: `active` với `accepted` hoặc
   `shop_accepted`. Nguồn mới cần mapping được kiểm chứng trước khi bật phục vụ.
7. Tạo `duplicate_group` từ mô tả trùng cùng loại, giao dịch, vị trí, giá, kỳ giá
   và diện tích. Mô tả quá ngắn dùng ID riêng. Đây là heuristic trùng nội dung
   bảo thủ; chưa phải hợp nhất chính xác mọi tin về cùng BĐS giữa các môi giới.

Gold giữ cả dòng không đủ điều kiện để kiểm tra. Train chỉ nhận
`is_recommendable=true`, giá/diện tích dương và `last_seen_at` trong 90 ngày
tính đến `as_of`, không ở tương lai. Serving kiểm tra lại thời hạn theo thời
điểm request. Không xuất `phone_hash`, địa chỉ chi tiết hay payload Bronze vào
bảng Gold này; title/description vẫn là văn bản tin nguồn.

## 4. Sale và rent được xử lý khác nhau thế nào?

| Quy tắc | Sale | Rent |
|---|---|---|
| Ý nghĩa giá hiện tại | Tổng giá chào bán, VND | Giá chào thuê, VND theo kỳ |
| `price_period` | `total` | `month`, `day`, `year`, hoặc `unknown` |
| Giá trị xác minh trong snapshot này | 50.747 tin sale | 104.876 tin rent đều có chuỗi giá theo tháng |
| Tập huấn luyện / từ vựng / chỉ mục | Thư mục `sale/` | Thư mục `rent/` |
| Điều kiện truy hồi | Chỉ sale | Chỉ rent và cùng kỳ giá |
| Preferences | Có thể mặc định kỳ `total` | Bắt buộc chỉ rõ kỳ, ví dụ `month` |

Không tự quy đổi ngày/năm sang tháng. Nếu kỳ giá là `unknown`, không áp dụng
điểm giá hoặc nhận ràng buộc ngân sách bằng số. Nhờ vậy giá thuê 10 triệu/tháng
không bị so với 10 triệu/ngày hoặc với tổng giá bán. `deposit_vnd` không được
dùng thay cho tiền thuê. Giá là giá **chào đăng**, không phải giá giao dịch.

## 5. Đặc trưng gì? Dùng mô hình/kỹ thuật gì cho từng đặc trưng?

| Nhóm | Cột Gold | Cách sử dụng trong bản hiện tại |
|---|---|---|
| Phân đoạn | `transaction_type`, `property_category`, `price_period` | Bộ lọc bắt buộc, tách hai chỉ mục sale/rent |
| Văn bản | `content_text`: title, description, subtype, project, pháp lý, nội thất, hướng, hẻm xe hơi/mặt tiền | TF-IDF ký tự 3–5 gram, cosine similarity |
| Giá | `price_vnd`, `log_price_vnd` | Bộ lọc ngân sách; độ tương đồng theo tỷ lệ giá trên thang log |
| Diện tích | `area_m2`, `log_area_m2` | Bộ lọc diện tích tối thiểu; tương đồng tỷ lệ diện tích |
| Vị trí | ID tỉnh/huyện/xã, tọa độ, `location_trusted` | Lọc địa phương; Haversine nếu tọa độ đáng tin, fallback theo đầy đủ phân cấp ID |
| Đặc điểm | `bedrooms`, `bathrooms`, subtype, legal_status, furnishing | Điểm cấu trúc; số phòng tối thiểu là điều kiện cứng khi người dùng nhập |
| Thời gian / trạng thái | `last_seen_at`, `is_recommendable`, trạng thái nguồn/bằng chứng gỡ | Loại tin không đủ điều kiện; điểm độ mới giảm theo thời gian |
| Trùng tin | `duplicate_group` | Không trả cùng nhóm với seed; một nhóm tối đa một kết quả |
| Truy vết | ID tin, nguồn, observation ID, URL, thời điểm xây Gold | Ghép kết quả, điều hướng và kiểm tra nguồn |

Các cột `price_per_m2`, `living_area_m2`, chiều ngang/dài, tầng, ngày đăng và
một số cờ tình trạng cũng được giữ làm đặc trưng mở rộng/audit. Không phải mọi
cột đã được đưa vào điểm số V1; tránh tuyên bố chúng đang tác động đến ranking.

### Mô hình hiện tại

**Content-based recommendation: TF-IDF + cosine + xếp hạng có trọng số.**
Mô hình phù hợp khi có nội dung/thuộc tính tin nhưng thiếu tương tác người dùng.
Không cần nhãn `y` để fit TF-IDF: bước fit học từ vựng và IDF của từng thị trường.

- Chuẩn hóa Unicode, chữ thường, khoảng trắng, dấu tiếng Việt (kể cả `đ`).
  Tin và query đi qua cùng hàm; giới hạn 4.000 ký tự để kiểm soát kích thước.
- `char_wb`, n-gram 3–5, tối đa 30.000 đặc trưng **mỗi thị trường**,
  `min_df=2`, `sublinear_tf=True`, chuẩn hóa L2, ma trận sparse float32.
- Cosine được tính bằng tích vô hướng các vector L2; không tạo ma trận
  tương đồng N×N. Mỗi request chấm các ứng viên vượt bộ lọc.
- Tương đồng số: `exp(-abs(log(candidate / target)) / width)`;
  width giá 0,35, diện tích 0,30.
- Địa lý: `exp(-distance_km / 12)` khi tọa độ hợp lệ. Fallback tỉnh/huyện/xã
  phải kiểm tra đồng thời các cấp cha; không so tên huyện độc lập.
- Độ mới: `exp(-age_days / 60)` sau khi đã qua kiểm tra tuổi tin.

Trọng số khởi đầu cho tin tương tự: **35% nội dung, 25% giá, 15% diện tích,
15% vị trí, 5% cấu trúc, 5% độ mới**. Chỉ các thành phần có thông tin ở truy vấn
mới được đưa vào mẫu số. Đây là lựa chọn baseline kỹ thuật, **chưa được tối ưu
trên nhãn phù hợp**, và điểm không phải xác suất người dùng thích tin.

Preferences dùng nội dung/giá mục tiêu/diện tích mục tiêu khi có và độ mới.
`max_price` khác `target_price`: một trường loại tin vượt ngân sách, trường kia
xếp hạng mức gần mong muốn. Nếu chỉ có các bộ lọc, kết quả được sắp theo độ mới.
Query ngoài từ vựng hoặc không có ứng viên khớp trả danh sách rỗng.

## 6. Kế hoạch huấn luyện và đánh giá

### Đã triển khai

1. Build Gold và chạy dbt tests.
2. Export một Delta version cụ thể; ghi version/table ID/checksum.
3. Chọn `as_of`, lọc eligibility và freshness, yêu cầu tồn tại cả sale và rent.
4. Fit riêng hai TF-IDF trên toàn bộ catalog đủ điều kiện của mỗi thị trường.
   Chạy tuần tự để giảm RAM. Tạo catalog + vocabulary + sparse index nhất quán.
5. Ghi vào thư mục staging, chỉ rename thành bundle hoàn chỉnh sau khi cả hai
   thị trường thành công. Không ghi đè version đã tồn tại.
6. Chạy unittest và benchmark riêng từng thị trường.

Bundle lưu `manifest.json`, `sale/{catalog.parquet,matrix.npz,vectorizer.joblib}`
và cấu trúc tương tự cho rent. Manifest có phiên bản dữ liệu, checksum input,
thời điểm as-of, tham số, trọng số, số dòng/đặc trưng, phiên bản Python/sklearn,
checksum từng file và hash thứ tự ID. Khi load, kiểm tra checksum, shape, thứ tự
ID và phiên bản sklearn; chỉ nạp bundle tin cậy do pipeline tạo.

### Cần làm để chọn mô hình tốt về mức độ phù hợp

- Chuẩn bị khoảng **240 truy vấn**: 120 sale và 120 rent, mỗi nhóm gồm cả
  similar và preferences, phủ các loại nhà và phân khúc giá/khu vực.
- Chia tune/test theo nhóm seed/BĐS để các tin trùng không lọt sang cả hai tập.
  Với dữ liệu lịch sử, pin cả snapshot và `as_of`; không lấy trạng thái hiện
  tại rồi gắn nhãn như thể đó là trạng thái trong quá khứ.
- Pool Top-20 của rule-based và TF-IDF; con người chấm 0–3. Nếu so thêm
  phương án khác thì mở rộng pool. Không chỉ chấm các kết quả của một mô hình.
- Chọn trọng số/ngưỡng/ngram trên tập tune riêng cho sale và rent, sau đó
  đánh giá test một lần. Giá/radius người dùng chọn vẫn là điều kiện cứng.
- Báo cáo Precision@10, nDCG@10, Recall trên tập ứng viên đã chấm
  (`pooled_recall@10`), độ phủ, tỷ lệ ít/rỗng kết quả, trùng tin, vi phạm lọc
  và p95. **Pooled Recall không phải Recall trên toàn bộ kho chưa được chấm.**
- `evaluate.py` yêu cầu dataset_version khớp và mọi kết quả Top-K đều được
  chấm. Nó dùng cùng engine, `as_of` và chính sách tuổi tin với serving;
  không tự bỏ giới hạn tuổi như bản starter cũ.

Schema mỗi dòng JSONL đánh giá (ID dưới đây chỉ minh họa, không phải nhãn thật):

```json
{"dataset_version":"gold.ml_recommendation_features.delta_v0","split":"test","as_of":"2026-10-23T04:55:03Z","method":"preferences","request":{"transaction_type":"rent","property_category":"HOUSE","price_period":"month","max_price":15000000,"max_age_days":90},"relevance":{"REAL_LISTING_KEY_A":3,"REAL_LISTING_KEY_B":0}}
```

Chưa có nhãn con người nên không công bố độ chính xác hay CTR. Embedding đa
ngôn ngữ là phương án đối chứng giai đoạn sau, chưa triển khai. Khi có log
impression/click/save/contact thật, có thể xây learning-to-rank; cần ghi nhận
exposure và vị trí hiển thị, không xem mọi tin không click là nhãn âm.

### Lịch cập nhật và phục vụ

DAG hiện chọn toàn bộ `path:models/gold`, nên model dbt mới được đưa vào bước
Gold khi DAG chạy. Export/train recommendation hiện có CLI để chạy chủ động;
**chưa gắn lịch retrain hoặc tự triển khai vào FastAPI hiện có**.

Đề xuất vận hành: sau mỗi Gold thành công, export/train một version mới,
kiểm tra rồi đổi bundle phục vụ. Tần suất ít nhất theo chu kỳ crawl hằng ngày.
Bundle cũ không biết một tin vừa bị gỡ sau lúc export; bộ lọc tuổi tin không
thay thế việc cập nhật trạng thái. Trọng số chỉ tune khi có nhãn, không đổi
tự động sau từng lần crawl. Giữ version cũ để rollback.

MLflow chưa được ghi trong lần triển khai này. Có thể bổ sung MLflow run để
lưu tham số, metrics và bundle; muốn Model Registry cần wrapper/signature
phù hợp cho retrieval. Không gọi việc lưu artifact là đã đăng ký model.

## 7. Cách chạy lại

Chạy tại thư mục gốc repository. `recommendation/requirements.txt` mô tả môi
trường riêng cho module này; không thay requirements cũ của toàn bộ LakeHouse.
Lần triển khai này dùng thư viện đã có, không cài thêm.

```bash
# Chỉ build bảng recommendation, không rebuild Silver.
docker exec kltn-airflow-scheduler-1 dbt build \
  --select ml_recommendation_features \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --threads 1

# Dùng tên version MỚI ở mỗi lần export/train, các thư mục không được tồn tại.
python3 -m recommendation.export --env-file .env \
  --output artifacts/recommendation/exports/NEW_VERSION

python3 -m recommendation.train \
  --input artifacts/recommendation/exports/NEW_VERSION/catalog.parquet \
  --output artifacts/recommendation/models/NEW_VERSION \
  --dataset-version GOLD_DELTA_VERSION_FROM_SNAPSHOT_JSON

python3 -m unittest discover -s recommendation/tests -v

python3 -m recommendation.benchmark \
  --bundle artifacts/recommendation/models/NEW_VERSION --queries 50 \
  --output artifacts/recommendation/reports/NEW_VERSION.json
```

Để tái lập chính xác, truyền thêm `--version` cho export và `--as-of` cho train.
Credentials đọc từ `.env` hoặc biến môi trường, không chép vào source/manifest.

Ví dụ gợi ý mua nhà:

```bash
python3 -m recommendation --bundle artifacts/recommendation/models/20261023_v1 \
  preferences --request '{"transaction_type":"sale","property_category":"HOUSE","province_id":"13000","max_price":4000000000,"min_area":60,"limit":10}'
```

Ví dụ gợi ý thuê theo tháng:

```bash
python3 -m recommendation --bundle artifacts/recommendation/models/20261023_v1 \
  preferences --request '{"transaction_type":"rent","property_category":"HOUSE","price_period":"month","province_id":"13000","max_price":15000000,"min_bedrooms":2,"query":"gần trường học","limit":10}'
```

Ví dụ tương tự một tin đang có trong bundle:

```bash
python3 -m recommendation --bundle artifacts/recommendation/models/20261023_v1 \
  similar --listing-key REAL_KEY_FROM_CATALOG --limit 10
```

Có thể gọi `RecommendationEngine(bundle).similar(...)` hoặc `.preferences(...)`
trực tiếp từ Python. Mẫu key thật nằm trong báo cáo benchmark.

```bash
python3 -m recommendation.evaluate \
  --bundle artifacts/recommendation/models/20261023_v1 \
  --labels real_labels.jsonl --k 10 --output evaluation.json
```

## 8. Kết quả xác minh và giới hạn

- Gold đã được tạo trên MinIO, snapshot export là Delta v0 với 155.623 dòng.
- `dbt build --select ml_recommendation_features`: 1 model + 12 tests pass.
- 12 unittest Python pass, kiểm tra tách sale/rent, kỳ giá thuê, ngân sách, thời hạn, trùng tin,
  địa lý, query không dấu, checksum/thứ tự index và đọc snapshot Delta.
- Benchmark là kiểm tra cơ học/latency gọi trực tiếp engine, không phải HTTP
  API, tải đồng thời hay thử nghiệm người dùng.
- Bộ lọc locality và duplicate_group có thể làm kết quả ít hơn K; không tự
  nới ngân sách, chuyển sang sale/rent khác hay loại BĐS khác để lấp đủ K.
- Bảng Gold và mô hình là lớp mới; các thay đổi chưa commit sẵn của bạn ở
  Silver, fact snapshot và macro được giữ nguyên.

Kết quả huấn luyện toàn bộ catalog đủ điều kiện, không lấy mẫu 5.000 tin:

| Chỉ số | Sale | Rent |
|---|---:|---:|
| Số tin đủ điều kiện đưa vào model | 24.488 | 40.124 |
| Số chiều TF-IDF tối đa / thực tế | 30.000 / 30.000 | 30.000 / 30.000 |
| Thời gian fit và lưu phân đoạn | 20,105 giây | 30,760 giây |
| Truy vấn similar đã kiểm tra | 50 | 50 |
| Kết quả similar | 500 | 500 |
| Vi phạm thị trường/loại/kỳ giá/tỉnh/self/duplicate/freshness | 0 | 0 |
| p95 similar | 406,37 ms | 295,24 ms |
| Truy vấn preferences đã kiểm tra | 50 | 50 |
| Kết quả preferences | 390 | 250 |
| Vi phạm các bộ lọc preferences đã kiểm tra | 0 | 0 |
| p95 preferences | 16,00 ms | 16,60 ms |

Tổng 64.612 tin được đưa vào hai model; các dòng không đủ điều kiện vẫn ở Gold.
Benchmark load cả hai model trong khoảng 10,46 giây, RAM đỉnh tiến trình khoảng
1.101,6 MB. Bundle chiếm khoảng 203,6 MB, snapshot export khoảng 147,1 MB.
Đây là phép đo trên máy hiện tại, không phải cam kết tài nguyên/latency trên
môi trường khác. Preferences benchmark dùng bộ lọc, không có query văn bản.

Thời điểm đánh giá cố định: `2026-10-23T04:55:03Z` (11:55:03 giờ Việt Nam).
Dataset: `gold.ml_recommendation_features.delta_v0`.
Chi tiết và ví dụ tin gốc/kết quả nằm trong
`artifacts/recommendation/reports/20261023_v1.json`.

**Trạng thái hoàn thành:** Gold đã materialize thật trên MinIO; hai model đã
train và lưu bền vững; engine/CLI, exporter, evaluator, tests và tài liệu đã có.
Chưa công bố Precision/nDCG từ dữ liệu thật vì chưa có bộ nhãn đánh giá;
chưa tự triển khai recommendation vào FastAPI hoặc đăng ký MLflow Registry.
