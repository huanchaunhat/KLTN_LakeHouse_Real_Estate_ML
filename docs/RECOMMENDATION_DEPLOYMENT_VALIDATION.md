# Kết quả kiểm chứng triển khai recommendation — 24/10/2026

## Trạng thái thực tế

- `recommendation-api`: chạy, Docker healthcheck **healthy**, bind `127.0.0.1:8501`.
- `recommendation-worker`: chạy trên mạng Docker, không publish cổng ra host.
- Registry: `reip_recommendation`, version **2**, status `READY`.
- Aliases: `candidate=2`, `champion=2`.
- MLflow run version 2: `c1994e3331794dd6b38f081bf65fe198`, status `FINISHED`.
- API sau restart vẫn nạp version 2 và fingerprint `ae06fdcf42bea6bf3362fe47cd22c709e728b6764de99d4b3a6483c8e8e38d12`.
- Version 1 là bản bootstrap từ bundle `20261023_v1`, được giữ trong Registry để rollback.

## Lượt retrain trên Gold thật

- DAG: `train_recommendation_model`.
- Run: `manual_deployment_validation_20261024` — **success**.
- Bắt đầu: 15:10:43, kết thúc: 15:15:26, ngày 24/10/2026, giờ Việt Nam (khoảng 4 phút 44 giây).
- DAG đang bật (`paused=false`), dùng `DatasetTriggeredTimetable`, không có lỗi import.
- Đã kiểm tra `dbt_test_gold` phát cả Dataset price cũ và recommendation mới.
- Lượt kiểm chứng được trigger thủ công qua Airflow; việc tự kích hoạt ở lần Gold thành công tiếp theo được cấu hình bằng Dataset, chưa giả lập một lần chạy lại toàn bộ pipeline Gold.

Snapshot MinIO đọc thật:

| Thuộc tính | Giá trị |
|---|---|
| Bảng | `s3://lakehouse/warehouse/gold.db/ml_recommendation_features` |
| Delta table ID | `eb36adaf-7926-48f5-b7b0-a68fb576961b` |
| Delta version | 1 |
| File đang hiệu lực | 22 |
| Dòng đầu vào | 157.213 |
| Dòng sale đủ điều kiện | 24.252 |
| Dòng rent đủ điều kiện | 39.712 |
| Tổng đủ điều kiện | 63.964 |
| Thời điểm as_of | `2026-10-24T08:10:50.641091+00:00` |

Gold đầu vào tăng so với snapshot cũ, nhưng số tin đủ điều kiện không nhất thiết tăng theo: mỗi lần train áp dụng lại điều kiện dữ liệu và cửa sổ thời gian ở `as_of` mới.

Thư mục job: `artifacts/recommendation/production/jobs/786d7a6f5b9d34870b28f3c9/`.
MLflow run artifacts: `s3://mlflow/2/c1994e3331794dd6b38f081bf65fe198/artifacts`.

## Kiểm tra cơ học trên model v2

Mỗi thị trường chạy 15 truy vấn similar và 15 preferences, tổng 60 truy vấn. Kết quả không chứng minh độ phù hợp với nhu cầu thật.

| Chỉ số | Sale | Rent |
|---|---:|---:|
| Similar: tổng kết quả | 150 | 150 |
| Similar: vi phạm điều kiện | 0 | 0 |
| Similar: p95 | 470,83 ms | 328,56 ms |
| Preferences: tổng kết quả | 140 | 110 |
| Preferences: vi phạm điều kiện | 0 | 0 |
| Preferences: p95 | 14,08 ms | 16,88 ms |

Preferences có thể trả ít hơn K nếu điều kiện không đủ ứng viên. Thời gian nạp engine đo trong benchmark: 12,44 giây; peak RSS của process benchmark: 977,4 MiB. Các số này đo trực tiếp engine trên máy hiện tại, không phải benchmark tải đồng thời hoặc SLA HTTP.

## API, nhãn và độ bền

- 12 kiểm thử core và 5 kiểm thử deployment đạt.
- Kiểm thử sale/rent, ngân sách/đơn vị giá, loại bỏ self-hit/trùng tin, độ mới, checksum và thứ tự catalog.
- Kiểm thử HTTP bằng dữ liệu tổng hợp trong thư mục tạm: validation, lưu 0 khác chưa chấm, từ chối điểm sai, chống ghi đè phiếu cũ, export JSONL/CSV, chống công thức CSV trong ghi chú, metric API bằng metric CLI.
- Phiếu đã lưu giữ nguyên nội dung khi model đổi; metric trực tiếp từ chối fingerprint khác.
- PyFunc save/load/predict roundtrip đạt trên fixture.
- Test lỗi tải candidate giữ engine cũ; test pipeline lỗi reload khôi phục alias; retry job đã hoàn tất không kéo champion lùi.
- HTTP thực tế qua Docker và localhost:8501: `/`, `/docs`, `/openapi.json`, `/health`, `/filters` thành công.
- HTTP recommendation thật: sale/rent similar đều trả 10 tin, preferences căn hộ theo ngân sách đều trả 10 tin; không có kết quả chéo thị trường.
- `/admin/reload` và worker `/jobs` thiếu token đều trả 401.
- SQLite demo mới có **0 phiếu người dùng** khi bàn giao; không trộn nhãn tổng hợp kiểm thử vào dữ liệu thật.
- JavaScript giao diện vượt qua kiểm tra cú pháp. Phiên công cụ không có trình duyệt kết nối nên chưa kiểm tra ảnh chụp/render và thao tác click thực tế.

## Giới hạn kết luận

Model đã chạy được xuyên suốt MinIO → train → MLflow → FastAPI, và có lịch retrain sau Gold thành công. Chưa có bằng chứng model v2 tốt hơn v1 về relevance. Trạng thái vẫn là `not_evaluated_with_human_labels`, chính sách tự chuyển bản là `mechanical_checks_only_demo`. Cần chấm nhãn thật, so sánh trên pool/test cố định trước khi dùng các chỉ số relevance để quyết định promotion trong vận hành chính thức.

Hướng dẫn sử dụng và vận hành: [RECOMMENDATION_DEPLOYMENT.md](RECOMMENDATION_DEPLOYMENT.md).

## Cập nhật tổ chức mã nguồn — 27/10/2026

Backend đã chuyển sang `api/app/recommendation/`, kiểm thử API sang `api/tests/test_recommendation.py`, web sang `web/recommendation/index.html`. Compose đã đổi entrypoint và mount tương ứng; core huấn luyện vẫn ở `recommendation/`.

Sau khi chuyển: 12 kiểm thử core chạy thành công trên môi trường Python sẵn có; kiểm tra cú pháp Python/JavaScript, phân giải đường dẫn module/web, cấu hình Compose và mount đều đạt. Docker daemon đang dừng nên chưa chạy lại 5 kiểm thử API hay kiểm tra HTTP trong container với bố cục mới. Các kết quả HTTP/Registry ở phần trên là kết quả ngày 24/10, trước lần chuyển thư mục này. Khi Docker và dịch vụ phụ thuộc hoạt động, chạy `docker compose -p kltn up -d --no-deps recommendation-api recommendation-worker` để áp dụng command/volume mới.
