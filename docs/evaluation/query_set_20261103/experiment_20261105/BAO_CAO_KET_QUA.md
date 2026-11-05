# Kết quả đánh giá và tinh chỉnh trọng số gợi ý — 05/11/2026

Đã hoàn tất nhập nhãn, đo baseline, tìm trọng số trên tune, khóa trọng số, đánh giá test và tạo bundle độc lập. Kết quả dưới đây đo mức phù hợp với **bộ nhãn AI sơ bộ**, chưa chứng minh mức hài lòng của người dùng thực.

## Dữ liệu và phương pháp

- Dataset: `gold.ml_recommendation_features.0c7012e3-c819-43fd-9437-e64065ff9669.delta_v4`.
- Giữ nguyên `as_of=2026-11-03T05:26:14.962227+00:00`, cửa sổ 90 ngày và snapshot TP.HCM.
- 240 truy vấn tổng hợp từ catalog: 120 tune và 120 test; mỗi tập có 60 sale, 60 rent, mỗi thị trường có 30 similar và 30 preferences.
- 8.924 nhãn gốc được giữ nguyên. Nhập thành `labels_tune.jsonl` và `labels_test.jsonl` ở thư mục cha của thí nghiệm.
- `labels_status=unjudged` trong manifest bộ truy vấn là thông tin lịch sử lúc tạo pool và được giữ nguyên để bảo toàn checksum. Trạng thái hoàn tất của lần chạy này nằm trong `weight_search/verification.json`.
- Chấm bổ sung 3,293 cặp tune và 98 cặp test bằng cùng quy trình AI01 `user_column_authority_20261105`, lưu riêng; không sửa CSV/query gốc.
- Pool tune được mở rộng bằng hợp nhất Top-10 của toàn bộ cấu hình đã định trước. AI chỉ nhận thông tin nhu cầu và snapshot, không nhận điểm mô hình hoặc thứ hạng.
- Thử 271 cấu hình mỗi thị trường: baseline, text-heavy, attribute-heavy, 12 thay đổi từng trọng số quanh baseline và 256 mẫu Dirichlet có seed. Seed sale=20261105, rent=20261106. Chọn nDCG@10 trung bình cao nhất trên tune; nếu hòa, ưu tiên gần baseline rồi chỉ số cấu hình nhỏ hơn.
- Một bộ sáu trọng số dùng chung cho similar/preferences trong mỗi thị trường. Preferences hiện chỉ có điểm text/price/area/freshness; vị trí/phòng ngủ vẫn là bộ lọc.
- Khóa trọng số tại `2026-11-05T08:55:50.227871+00:00` trước khi tính chỉ số test. Pool test chỉ mở rộng từ kết quả baseline và cấu hình đã chốt; không chọn lại theo test.
- Truy hồi trên toàn bộ ứng viên hợp lệ; không giới hạn engine vào pool đã chấm, không thay nhãn thiếu thành 0. Không huấn luyện lại TF-IDF.

## Kết quả test trên cùng pool đã mở rộng

| Nhóm | Số truy vấn | nDCG baseline | nDCG mới | Chênh lệch | P@10 baseline | P@10 mới |
|---|---:|---:|---:|---:|---:|---:|
| all | 120 | 0.861688 | 0.874486 | +0.012798 | 0.972500 | 0.970000 |
| sale | 60 | 0.871048 | 0.882639 | +0.011591 | 0.978333 | 0.978333 |
| rent | 60 | 0.852329 | 0.866333 | +0.014004 | 0.966667 | 0.961667 |
| sale/similar | 30 | 0.873060 | 0.887796 | +0.014736 | 1.000000 | 1.000000 |
| sale/preferences | 30 | 0.869036 | 0.877482 | +0.008446 | 0.956667 | 0.956667 |
| rent/similar | 30 | 0.846018 | 0.859046 | +0.013028 | 0.990000 | 0.980000 |
| rent/preferences | 30 | 0.858639 | 0.873620 | +0.014981 | 0.943333 | 0.943333 |

nDCG@10 trung bình tăng +0.012798 (+1.49% tương đối). Precision@10 chung thay đổi -0.002500; riêng rent thay đổi -0.005000. Đây là sự đánh đổi giữa thứ tự theo điểm 0–3 và số tin có điểm lớn hơn 0, không phải cải thiện đồng đều mọi chỉ số.

Khoảng bootstrap 95% cho chênh lệch nDCG@10, lấy mẫu theo cặp truy vấn (10.000 lần, seed cố định):

| Nhóm | Trung bình chênh lệch | Khoảng 95% |
|---|---:|---|
| all | +0.012798 | [+0.000284; +0.024205] |
| sale | +0.011591 | [+0.003270; +0.020355] |
| rent | +0.014004 | [-0.009345; +0.035149] |

Khoảng của rent chứa 0: dù trung bình nDCG tăng, mức cải thiện còn chưa rõ theo bootstrap truy vấn. Giữ cấu hình này là candidate để rà soát, không coi kết quả là đủ để tự động thay mô hình đang phục vụ.

Nếu khoảng chứa 0, chưa có bằng chứng rõ về cải thiện nhất quán theo truy vấn trong bộ test này. Khoảng này không phản ánh sai số của người chấm, nhu cầu thực hoặc việc khái quát sang tỉnh/thành khác.

## Kết quả tune trên cùng pool đã mở rộng

| Thị trường | nDCG baseline | nDCG mới | Chênh lệch |
|---|---:|---:|---:|
| sale | 0.840725 | 0.842581 | +0.001857 |
| rent | 0.766034 | 0.784594 | +0.018560 |

Baseline tune trên pool gốc nằm ở `baseline_tune.json`. Không so trực tiếp nDCG đó với nDCG sau mở rộng pool: nhãn bổ sung có thể thay đổi IDCG. Tất cả so sánh trước/sau trong báo cáo này dùng chung pool mở rộng.

## Trọng số được chọn

| Thành phần | Baseline | Sale | Rent |
|---|---:|---:|---:|
| text | 0.350000 | 0.364768 | 0.355905 |
| price | 0.250000 | 0.194379 | 0.169650 |
| area | 0.150000 | 0.137578 | 0.133805 |
| location | 0.150000 | 0.144037 | 0.295504 |
| structure | 0.050000 | 0.030130 | 0.032759 |
| freshness | 0.050000 | 0.129108 | 0.012378 |

Đây là cấu hình tốt nhất trong phạm vi tìm kiếm đã khai báo và bộ nhãn AI hiện tại; không phải trọng số tối ưu phổ quát hoặc kết luận nhân quả về tầm quan trọng từng thuộc tính.

## Cách đọc chỉ số và giới hạn

- nDCG@10 dùng gain `2^grade-1`. Precision/pooled Recall hiện coi `grade > 0` là phù hợp, bao gồm cả điểm 1 (phù hợp yếu). Vì vậy không diễn giải Precision cao thành tỷ lệ hài lòng thực.
- Có thêm `precision_grade_ge_2@10` trong CSV/JSON để xem số tin đạt mức đáng cân nhắc trở lên; chỉ số này không được dùng để chọn lại trọng số sau khi xem test.
- Pooled Recall chỉ đo trên tập đã chấm, không phải Recall toàn kho.
- Precision@10 vẫn chia cho 10 khi trả dưới 10 tin. Xem số truy vấn thiếu/rỗng trong `weight_search/metrics.json` và chi tiết trong `query_comparison.csv`.
- Nhãn AI dựa trên quy tắc và cùng dữ liệu thuộc tính mà engine sử dụng, nên có thể ưu ái các quy tắc tương tự. Cần con người chấm độc lập trước khi dùng để kết luận khoa học về mức phù hợp hoặc thay thế mô hình phục vụ.

## Xác minh và bundle

- 20 kiểm thử core/tuning đã đạt; gồm đối chiếu cache với engine, tie/trùng tin, pool ngắn, nhãn thiếu, từ chối dùng test để chọn trọng số và bootstrap theo cặp.
- Adapter chấm bổ sung tái lập đúng điểm của 543/543 quyết định AI01 đã hoàn tất ngày 05/11; đây là kiểm tra nhất quán quy trình, không phải xác nhận nhãn đúng.
- Đã đối chiếu Top-10 cache và engine phục vụ trên toàn bộ tune/test của baseline và cấu hình mới; các chỉ số khớp evaluator gốc. Mọi Top-10 được đo đều có nhãn.
- SHA-256 của nguồn CSV/query/JSONL/bundle giữ nguyên; file khóa trọng số giữ nguyên sau test. Bundle mới nạp lại và cho cùng kết quả test.
- Bundle độc lập: `artifacts/recommendation/models/weights_20261105`. Catalog, matrix và vectorizer giữ nguyên checksum; manifest lưu trọng số và trạng thái nhãn AI chưa được con người xác nhận.
- Benchmark trực tiếp engine trên 15 tin mẫu mỗi thị trường:

| Thị trường | Vi phạm similar | Vi phạm preferences | p95 similar (ms) | p95 preferences (ms) |
|---|---:|---:|---:|---:|
| sale | 0 | 0 | 373.88 | 14.37 |
| rent | 0 | 0 | 260.83 | 15.04 |

Benchmark này đo trực tiếp engine trên máy hiện tại; không phải tải HTTP đồng thời hoặc cam kết hiệu năng sản phẩm. Chưa đăng ký MLflow, đổi alias hay nạp lại API.

## Tệp sử dụng

- `comparison.csv`: chỉ số trước/sau theo split, thị trường và phương thức.
- `query_comparison.csv`: chênh lệch theo từng truy vấn để rà soát.
- `weight_search/locked_weights.json`: trọng số đã khóa trước test.
- `weight_search/labels_tune_expanded.jsonl`, `labels_test_expanded.jsonl`: bộ nhãn dùng so sánh.
- `weight_search/additional_annotations_*.csv`, `additional_decisions_*.jsonl`: nhãn AI mới và dấu vết.
- `weight_search/plan.json`, `search_sale.json`, `search_rent.json`: seed, cấu hình và kết quả tìm kiếm.
- `weight_search/verification.json`, `ai_policy_validation.json`, `candidate_mechanical.json`: xác minh.
- `implementation/`, `implementation_checksums.json`: bản sao mã và checksum để đối chiếu.

## Tái lập

Chạy từ gốc repository, scikit-learn 1.6.1, dùng thư mục kết quả và bundle đầu ra mới:

```bash
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python3 scripts/run_recommendation_tuning.py \
  --data docs/evaluation/query_set_20261103 \
  --output docs/evaluation/query_set_20261103/experiment_reproduction \
  --candidate-bundle artifacts/recommendation/models/weights_reproduction \
  --ai-grader docs/evaluation/query_set_20261103/ai_review_20261105/completion_procedure.py \
  --seed 20261105 --random-count 256 --k 10
```

Chỉ chạy tái lập để kiểm tra kết quả; không dùng các lần chạy test để điều chỉnh lại cấu hình.
