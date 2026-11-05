# Bộ 240 truy vấn đánh giá và tinh chỉnh trọng số

Bộ dữ liệu nằm tại [`evaluation/query_set_20261103/`](evaluation/query_set_20261103/).
Đây là **kịch bản tổng hợp dựa trên catalog**, chưa phải nhu cầu được thu thập từ người dùng thật.
Giá, diện tích, địa bàn và tin mẫu lấy từ snapshot; các ưu tiên nội dung được tạo bằng mẫu.
Người đánh giá cần đọc, xác nhận nhu cầu hợp lý trước khi chấm. Nếu sửa request hoặc nhu cầu,
phải tạo lại pool tương ứng trước khi chấm và lưu phiên bản mới.

## 1. Thành phần

| Tệp | Cách dùng |
|---|---|
| `queries.md` | Đọc trực tiếp 240 truy vấn tiếng Việt, có ID, split và số ứng viên |
| `queries.csv` | Đọc toàn bộ 240 truy vấn bằng Excel hoặc trình đọc CSV; có nhu cầu tiếng Việt, request JSON và nội dung tin nguồn |
| `queries_tune.jsonl` | 120 truy vấn để chọn trọng số; relevance ban đầu là null |
| `queries_test.jsonl` | 120 truy vấn kiểm tra cuối; relevance ban đầu là null |
| `annotation_tune.csv` | Chấm ứng viên thuộc tập tune |
| `annotation_test.csv` | Chấm ứng viên thuộc tập test, giữ riêng khỏi quá trình chọn trọng số |
| `manifest.json` | Snapshot, thời điểm, checksum manifest bundle, seed và phân bố truy vấn |
| `pool_audit.json` | Nguồn ứng viên để kiểm tra kỹ thuật; không đưa cho người chấm trước khi chấm |

Mỗi tổ hợp `sale/rent × similar/preferences × tune/test` có 30 truy vấn.
Mỗi truy vấn có mục tiêu 40 ứng viên; xem `short_pool_queries` nếu không đủ.

Kết quả tạo và kiểm tra ngày 03/11/2026: **240 truy vấn, 8.924 cặp chấm**.
Có 216 truy vấn đủ 40 ứng viên, 24 truy vấn có 1–38 ứng viên vì bộ lọc chặt hoặc ít dữ liệu.
Đã kiểm tra toàn bộ ID, điều kiện hợp lệ, bộ lọc cứng, cặp CSV/JSONL và 240 nhóm tin nguồn không trùng.
Xem `validation.json` để đọc kết quả kiểm tra. Đây là kiểm tra dữ liệu, chưa phải đánh giá relevance.
Khi phân tích, tách nhóm ít ứng viên, đặc biệt pool nhỏ hơn 10, khỏi nhóm đánh giá xếp hạng thông thường:
Precision@10 hiện vẫn chia 10 khi trả ít kết quả nên giảm điểm dù các kết quả được trả có thể phù hợp.

| Loại BĐS | Sale (mỗi phương thức) | Rent (mỗi phương thức) |
|---|---:|---:|
| Nhà ở | 24 | 14 |
| Căn hộ | 18 | 18 |
| Đất | 12 | 2 |
| Phòng trọ | 0 | 16 |
| Mặt bằng thương mại | 6 | 10 |
| Tổng | 60 | 60 |

Phân bổ này ưu tiên độ phủ loại BĐS, không theo đúng tỷ lệ tin trong catalog.
Chỉ có dữ liệu TP.HCM; không suy rộng kết quả cho tỉnh khác.
Nguồn được lấy luân phiên theo quận và tứ phân vị giá trong từng loại BĐS;
không bảo đảm cân bằng hoàn toàn mọi tổ hợp. Giá/diện tích nguồn vẫn cần người chấm kiểm tra bất thường.

## 2. Nhu cầu và ràng buộc

- `similar`: tin nguồn là sở thích tham chiếu. Bắt buộc cùng thị trường, loại BĐS,
  đơn vị giá, tỉnh; loại nhóm trùng của tin nguồn. Quận, giá, diện tích và thuộc tính
  của tin nguồn là yếu tố để đánh giá độ tương tự, không tự động trở thành bộ lọc cứng.
- `preferences`: mô tả tiếng Việt ghi rõ điều kiện bắt buộc và ưu tiên mềm.
  `max_price`, `min_area`, `min_bedrooms`, `province_id`, `district_id` là bộ lọc cứng khi có.
  `target_price`, `target_area` và `query` là ưu tiên mềm.
- `max_age_days=90`, `as_of` lấy từ bundle. Không dùng ngày hiện tại để chạy lại snapshot cũ.
- Các truy vấn chủ yếu phục vụ đánh giá thứ hạng trong vùng có dữ liệu. Đây không phải bộ kiểm thử
  đầy đủ cho tình huống không có ứng viên hoặc request không hợp lệ; các nhóm ít ứng viên có sẵn
  được ghi riêng trong manifest.

Các nhóm tin nguồn không trùng giữa 240 truy vấn, kể cả giữa similar và preferences.
Các ứng viên vẫn có thể xuất hiện trong nhiều truy vấn và cả tune/test vì dùng chung catalog cố định;
điểm phù hợp của mỗi cặp được chấm độc lập. Đây không phải bài kiểm tra cold-start item.
Chia theo nhóm nguồn không bảo đảm độc lập hoàn toàn về dự án/khu dân cư hoặc các tin trùng chưa phát hiện.

## 3. Cách chấm

Xem [hướng dẫn chi tiết về điều kiện và cách chấm nhãn](HUONG_DAN_CHAM_NHAN_GOI_Y_BAT_DONG_SAN.md)
để phân biệt điều kiện bắt buộc, ưu tiên mềm, thông tin thiếu và các mức 0–3.

Trong `annotation_*.csv`, điền `grade`, `annotator_id` và `reason` theo hướng dẫn chấm chi tiết.
Công cụ nhập hiện kiểm tra hai cột đầu; reason cần được kiểm tra khi bàn giao:

| Grade | Hướng dẫn |
|---|---|
| 0 | Không phù hợp, hoặc vi phạm điều kiện bắt buộc |
| 1 | Hơi phù hợp, còn nhiều điểm đánh đổi |
| 2 | Phù hợp, đáng cân nhắc |
| 3 | Rất phù hợp với nhu cầu |

Không điền điểm theo điểm số mô hình. Tệp chấm đã đảo thứ tự và không có điểm hoặc nguồn baseline.
Với similar, đối chiếu nội dung tin nguồn trong `queries.csv` bằng `query_id`.
Nếu mô tả thiếu bằng chứng, ghi rõ trong `reason`; không tự suy đoán pháp lý, vị trí hay tiện ích.
Đề xuất hai người chấm độc lập 20–30% số cặp rồi thống nhất nhãn cuối;
mỗi cặp trong CSV nhập cuối cùng chỉ có một nhãn đã thống nhất.

Tập ứng viên hợp nhất Top-10 của trọng số hiện tại, bản ưu tiên văn bản và bản ưu tiên thuộc tính,
rồi bổ sung tin hợp lệ ngẫu nhiên đến 40. Các trọng số thử chỉ thay đổi trong bộ nhớ khi dựng pool;
bundle và trọng số đang phục vụ không bị sửa. Không phải 40 tin tốt nhất theo một mô hình.
Pool này chưa có baseline rule-based độc lập; bản ưu tiên thuộc tính vẫn có điểm văn bản nhỏ.

## 4. Nhập nhãn và đánh giá

Chạy từ thư mục gốc repository, trong môi trường có các thư viện của recommendation.
Giữ nguyên `query_id`, `listing_key` trong CSV; không đổi thứ tự cột hoặc tên cột.
Công cụ nhập sẽ từ chối điểm trống, điểm ngoài 0–3, thiếu người chấm, thiếu cặp hoặc trùng cặp.

```bash
python3 scripts/import_recommendation_grades.py \
  --queries docs/evaluation/query_set_20261103/queries_tune.jsonl \
  --annotations docs/evaluation/query_set_20261103/annotation_tune.csv \
  --output docs/evaluation/query_set_20261103/labels_tune.jsonl

python3 -m recommendation.evaluate \
  --bundle artifacts/recommendation/production/cache/4/artifacts/bundle \
  --labels docs/evaluation/query_set_20261103/labels_tune.jsonl \
  --k 10 \
  --output docs/evaluation/query_set_20261103/evaluation_tune.json
```

Chỉ chạy được đánh giá sau khi hoàn thành nhãn. Thay `tune` bằng `test` để nhập và đánh giá tập test
sau khi đã chốt cấu hình. Không dùng test để chọn trọng số; không ghi đè bundle đang phục vụ.
Đường dẫn cache có thể bị dọn sau này: cần giữ bản sao bundle và kiểm tra checksum manifest
đã ghi trong `manifest.json` khi tái lập.

Ưu tiên nDCG@10, kèm Precision@10 và pooled Recall@10, báo cáo riêng sale/rent và hai phương thức.
Evaluator hiện gộp theo thị trường/split; muốn báo cáo theo phương thức thì lọc các dòng JSONL trước khi chạy.
Precision của evaluator coi `grade > 0` là positive. Pooled Recall chỉ đo trên pool đã chấm.
Nếu phiên bản mới đưa tin chưa chấm vào Top-10, phải bổ sung nhãn trước khi so sánh;
không tự gán tin chưa chấm thành 0. Khi mở rộng pool, đánh giá lại cả baseline trên cùng pool mới.

## 5. Dùng bộ này để chọn trọng số

- Chấm tập tune, đo baseline, thử các cấu hình trọng số trên cùng tập tune.
- `similar` dùng text, price, area, location, structure và freshness khi có dữ liệu.
- `preferences` hiện chỉ dùng text, price, area và freshness; vị trí/số phòng là bộ lọc,
  không có điểm location/structure để tối ưu trong phương thức này.
- Chốt cấu hình riêng cho sale/rent nếu kết quả tune hỗ trợ; đánh giá một lần trên test.
- Kiểm tra độ biến thiên của từng điểm thành phần: nếu độ mới hoặc vị trí gần như giống nhau
  trong snapshot/pool, bộ này sẽ khó phân biệt tác dụng của trọng số đó.
- Công cụ `scripts/run_recommendation_tuning.py` thực hiện tìm trọng số có seed cố định,
  khóa lựa chọn trên tune trước khi đánh giá test và xuất bundle độc lập. Nhãn AI01 hiện tại
  là nhãn AI sơ bộ; cần con người kiểm chứng và bổ sung nhu cầu thực trước kết luận thực tế.

### Chạy thí nghiệm trọng số

Nhập đủ cả `labels_tune.jsonl` và `labels_test.jsonl` bằng importer ở trên trước khi chạy.
Dùng môi trường có **đúng phiên bản scikit-learn trong manifest bundle**. Thư mục kết quả
và bundle đầu ra phải mới; công cụ từ chối ghi đè một lần chạy cũ.

```bash
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python3 scripts/run_recommendation_tuning.py \
  --data docs/evaluation/query_set_20261103 \
  --output docs/evaluation/query_set_20261103/experiment_20261105/weight_search \
  --candidate-bundle artifacts/recommendation/models/weights_20261105 \
  --ai-grader docs/evaluation/query_set_20261103/ai_review_20261105/completion_procedure.py \
  --seed 20261105 --random-count 256 --k 10
```

`--ai-grader` chỉ sử dụng khi đã chọn chấm bổ sung bằng AI theo quy trình này.
Nếu bỏ tùy chọn, công cụ sẽ dừng khi có Top-10 chưa chấm; không tự gán điểm 0.
Không sửa CSV gốc: các cặp bổ sung nằm riêng trong `additional_annotations_*.csv`,
kèm dấu vết trong `additional_decisions_*.jsonl`. Dùng `labels_*_expanded.jsonl`
để so sánh baseline và cấu hình mới trên cùng pool đã mở rộng.

Công cụ thử 271 cấu hình mỗi thị trường: baseline, hai cấu hình đã dùng dựng pool,
12 thay đổi từng trọng số quanh baseline và 256 mẫu ngẫu nhiên có seed. Truy hồi vẫn
chạy trên toàn bộ ứng viên hợp lệ của engine, không giới hạn vào các tin đã chấm.
Hợp nhất Top-10 của toàn bộ cấu hình tune trước khi chấm bổ sung, dùng quy trình
AI cố định không nhận điểm mô hình hay vị trí xếp hạng làm đầu vào chấm.

`locked_weights.json` lưu cấu hình được chọn trước khi mở nhãn test để đo chất lượng.
Sau đó chỉ hợp nhất kết quả baseline và cấu hình đã chốt để bổ sung pool test nếu cần.
Không thay trọng số theo kết quả test. `metrics.json` có báo cáo theo thị trường và
phương thức; `test_paired_bootstrap.json` có khoảng bootstrap theo truy vấn.
`verification.json` xác nhận SHA-256 đầu vào, kết quả cache so với engine phục vụ,
chỉ số so với evaluator hiện có và khả năng nạp lại bundle mới. Công cụ không
đăng ký MLflow, đổi alias hoặc nạp lại API.

Tái tạo bằng seed cố định, dùng thư mục đầu ra mới:

```bash
python3 scripts/build_recommendation_queries.py \
  --bundle artifacts/recommendation/production/cache/4/artifacts/bundle \
  --output docs/evaluation/query_set_rebuilt \
  --seed 20261103
```
