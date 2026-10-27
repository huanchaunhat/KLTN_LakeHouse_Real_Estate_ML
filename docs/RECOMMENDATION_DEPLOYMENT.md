# Recommendation: FastAPI, MLflow Registry, Airflow và web demo

## 1. Các địa chỉ sử dụng

- Web demo: http://localhost:8501
- Swagger API: http://localhost:8501/docs
- Trạng thái model đang phục vụ: http://localhost:8501/health
- MLflow: http://localhost:5001 — experiment và registered model `reip_recommendation`.
- Airflow: http://localhost:8088 — DAG `train_recommendation_model`.

Đây là một FastAPI service riêng (`recommendation-api`) trong cùng Docker Compose của LakeHouse. API giá nhà hiện có vẫn ở cổng 8000. Web HTML/JavaScript được FastAPI phục vụ trực tiếp, không cần Streamlit hoặc Node server. Cổng demo chỉ bind `127.0.0.1`; worker không mở cổng ra host.

Mã nguồn được tổ chức như sau (cập nhật 27/10/2026):

```text
api/
  app/
    main.py                   # API dự đoán giá hiện có
    recommendation/
      main.py                 # API recommendation, nhãn, export, metric
      worker.py               # API nội bộ điều phối retrain
  tests/test_recommendation.py
web/
  recommendation/index.html   # Giao diện demo, tách khỏi mã backend
recommendation/               # Core mô hình, train, Registry và pipeline
```

API chỉ định vị và trả file web ở `web/recommendation/index.html`; giao diện vẫn gọi API cùng origin tại cổng 8501. Compose mount riêng `api/`, `web/` và core `recommendation/`. Entrypoint mới là `api.app.recommendation.main:app` và `api.app.recommendation.worker:app`. Sau khi chuyển thư mục, dùng `up -d` dưới đây để tạo lại hai container với command/volume mới; chỉ `restart` container cũ chưa áp dụng được thay đổi Compose.

## 2. Dữ liệu ở đâu, xử lý và lưu thế nào?

```text
Silver listing_current
  → dbt Gold ml_recommendation_features + dbt test
  → Airflow Dataset event
  → worker đọc snapshot Delta trên MinIO
  → export Parquet bất biến
  → fit TF-IDF riêng sale / rent
  → kiểm tra cơ học
  → MLflow run + Registry version + candidate alias
  → champion alias + FastAPI nạp model
  → web tìm gợi ý / thu nhãn thật / xuất kết quả
```

Nguồn hiện tại: `s3://lakehouse/warehouse/gold.db/ml_recommendation_features`. Exporter đọc transaction log Delta để chọn đúng file còn hiệu lực; không quét toàn bộ Parquet và không ghi ngược vào Silver/Gold. Chỉ hỗ trợ Delta reader protocol 1 và checkpoint đơn; gặp column mapping/deletion vectors/protocol không hỗ trợ sẽ dừng.

Mỗi job có thư mục `artifacts/recommendation/production/jobs/<sha256(job_id)[:24]>/`, mount thành `/state/jobs/...`:

| Thành phần | Nội dung |
|---|---|
| `export/catalog.parquet` | Snapshot Gold dùng cho lần train đó |
| `export/snapshot.json` | URI nguồn, Delta table ID/version, số dòng, checksum |
| `bundle/manifest.json` | Phiên bản dữ liệu, as_of, tham số, trọng số, checksum từng artifact |
| `bundle/sale/`, `bundle/rent/` | Catalog, vectorizer và sparse matrix riêng từng giao dịch |
| `mechanical_report.json` | Số truy vấn, vi phạm điều kiện, latency, RAM |
| `plan.json` | Trạng thái pipeline, Registry version/run ID, champion trước đó |
| `worker.json`, `worker.log` | Trạng thái subprocess và log chẩn đoán |

MLflow metadata/Registry lưu ở PostgreSQL của dịch vụ MLflow; artifacts được lưu vào MinIO theo artifact URI của MLflow. Run ghi manifest, snapshot, kiểm thử, checksum mã nguồn và PyFunc model đầy đủ. PyFunc gồm cả hai thị trường; API tải version cụ thể rồi giữ engine trong RAM.

Cache phục vụ: `artifacts/recommendation/production/cache/<version>/`. Nhãn demo: `artifacts/recommendation/production/labels.sqlite`. Token nội bộ: `artifacts/recommendation/production/admin_token` (không commit Git). Thư mục `production/` được ignore; cần backup riêng SQLite, các job cần tái lập và metadata/artifacts MLflow. Các nhãn cũ ở những thư mục demo trước không bị ghi đè hoặc tự gộp.

Chưa tự xóa lịch sử model/export/nhãn. Khi vận hành dài hạn cần chính sách lưu trữ theo dung lượng; giữ tối thiểu champion, bản rollback và các bundle gắn với bộ nhãn đang đánh giá.

## 3. Bài toán, mô hình và đặc trưng

Hai bài toán: tìm tin tương tự một tin gốc và tìm tin phù hợp nhu cầu. Đây là baseline content-based; không phải mô hình cá nhân hóa từ lịch sử click.

- `content_text`: TF-IDF ký tự `char_wb`, n-gram 3–5, tối đa 30.000 đặc trưng; cosine similarity.
- Giá, diện tích: độ gần so với tin gốc hoặc giá trị mục tiêu; ngân sách/diện tích tối thiểu là bộ lọc cứng.
- Vị trí: tỉnh/quận/phường, tọa độ có cờ tin cậy.
- Cấu trúc: phòng ngủ/phòng tắm khi có dữ liệu.
- Thời gian: `last_seen_at`, độ mới và điều kiện còn đủ mới để gợi ý.
- Trạng thái, nhóm trùng tin, loại bất động sản và đơn vị giá: kiểm soát tập ứng viên.

Trọng số khởi đầu: text 0,35; giá 0,25; diện tích 0,15; vị trí 0,15; cấu trúc 0,05; độ mới 0,05. Đây là trọng số đặt trước, chưa học từ nhãn thật. Sale dùng giá tổng; rent tách tháng/ngày/năm và yêu cầu chỉ rõ chu kỳ. Không trộn hai thị trường hoặc đơn vị giá khi xếp hạng.

“Retrain” hiện tại nghĩa là fit lại vocabulary/IDF và index từ snapshot mới, không tự học lại các trọng số này. TF-IDF không có `partial_fit`, nên quy trình hiện tại fit đầy đủ từng thị trường.

## 4. Lịch tự động và dữ liệu mới

`dbt_test_gold` của `end_to_end_lakehouse_pipeline` phát Dataset `lakehouse://gold/ml_recommendation_features` khi thành công. DAG `train_recommendation_model` đăng ký Dataset này, `max_active_runs=1`, không backfill lịch sử. Vì pipeline Gold hiện chạy `@daily`, recommendation sẽ chạy sau lần cập nhật Gold hằng ngày thành công; lần Gold chạy thủ công thành công cũng có thể phát event.

Không có đồng hồ retrain độc lập: Gold chưa chạy hoặc test Gold lỗi thì không tự retrain. Ghi dữ liệu trực tiếp vào MinIO ngoài pipeline cũng chưa tạo Airflow Dataset event; cần chạy đúng pipeline Gold hoặc trigger DAG recommendation thủ công. Dataset event là tín hiệu sẵn sàng; worker chụp **snapshot Gold mới nhất tại thời điểm export**, không cam kết train chính version đã phát event nếu Gold cập nhật thêm trong thời gian chờ.

Trình tự một lần chạy:

1. Airflow đọc Variable `recommendation_admin_token`, gọi worker bằng HTTP nội bộ.
2. Worker tạo subprocess; file lock chặn các pipeline chạy chồng. `job_id` giúp retry cùng lần chạy tiếp tục thư mục cũ.
3. Export ghi snapshot bất biến. Tên dataset có Delta table ID + version để phân biệt cả trường hợp table bị tạo lại.
4. Train dùng thời điểm `as_of` đã cố định trong plan, kiểm tra có dữ liệu hợp lệ ở cả sale và rent.
5. Chạy benchmark 15 truy vấn similar + 15 preferences cho mỗi thị trường; yêu cầu có dữ liệu/kết quả similar và không vi phạm điều kiện.
6. Log run, model PyFunc và tạo Registry version; gắn alias `candidate`.
7. Chuyển `champion`, gọi API nạp đúng version. API xác thực token, kiểm tra artifact và thử truy vấn trước khi thay engine trong RAM. Request đang xử lý giữ engine cũ.
8. Nếu reload lỗi, pipeline khôi phục alias cũ và báo lỗi để Airflow retry. API giữ engine cũ nếu tải/kiểm tra chưa hoàn tất. Khi alias đổi trong lúc đang tải, API từ chối áp dụng bản tải dở.

Job thành công gọi lại cùng ID là no-op, không kéo champion về bản cũ. API lúc khởi động sẽ thử tải champion; nếu MLflow/MinIO chưa sẵn sàng thì báo 503 và thử lại mỗi 30 giây. Worker và API dùng cùng môi trường Python/sklearn của image API, tách khỏi môi trường Airflow cũ.

## 5. Promotion và đánh giá chất lượng

Chính sách đang bật: `mechanical_checks_only_demo`. Model đạt kiểm tra kỹ thuật được tự đưa vào phục vụ demo. Đây **không phải bằng chứng model mới gợi ý tốt hơn model cũ**. Run/manifest ghi rõ `not_evaluated_with_human_labels`. Chưa tự đưa nhãn UI vào huấn luyện hoặc dùng nhãn UI làm quality gate.

Khi có đủ nhãn thật:

1. Chọn các nhu cầu/tin gốc đại diện cả sale và rent; lấy pool ứng viên hợp của các model/baseline để so sánh công bằng.
2. Chấm 0–3, tách `tune` và `test`; không dùng test để chỉnh trọng số.
3. Đánh giá cùng snapshot, thời điểm, điều kiện và pool nhãn. Nếu Top-K của model mới có tin chưa chấm, bổ sung nhãn; không biến “chưa chấm” thành 0.
4. Theo dõi Precision@K, NDCG@K, pooled Recall@K riêng từng thị trường. Pooled Recall chỉ phản ánh pool đã chấm, không phải toàn bộ kho tin. Pool của demo hiện lấy từ một model nên có thiên lệch chọn ứng viên.
5. Bổ sung cổng promotion: tối thiểu số query test, không giảm NDCG vượt ngưỡng chấp nhận, kiểm tra độ phủ/latency. Ngưỡng cần thống nhất từ baseline thực tế trước khi bật tự động.

Nhãn cũ vẫn đọc/chấm/xuất được sau khi cập nhật model vì mỗi phiếu lưu snapshot nội dung ứng viên, request/as_of, version và fingerprint. Nút tính metric trên web chỉ tính với đúng fingerprint đang phục vụ; phiếu cũ xuất JSONL để đánh giá bằng bundle cũ. Dữ liệu mới có thể đổi tập ứng viên nên không tái sử dụng nhãn cũ một cách mù quáng.

## 6. Dùng demo và API

Trên web: chọn sale/rent → chọn tìm theo nhu cầu hoặc tin tương tự → với tìm theo nhu cầu, chọn tỉnh/thành rồi quận/huyện (hoặc tất cả) → tìm kết quả → nhập người chấm, split và K → tạo phiếu → chọn điểm từng tin và bấm **Lưu nhãn** → chấm đủ → xuất JSONL/CSV hoặc xem chỉ số của phiếu. Thứ tự ứng viên trong phiếu được xáo và ẩn điểm model. Nhãn để trống khác 0; ghi đồng thời trên cùng phiếu có kiểm tra version để tránh mất dữ liệu.

Các endpoint chính:

| Endpoint | Chức năng |
|---|---|
| `GET /health`, `/model` | Trạng thái, model version và thông tin dữ liệu |
| `GET /filters`, `/catalog` | Danh sách tỉnh/thành và quận/huyện theo thị trường, tìm tin gốc |
| `POST /recommend` | `method=similar` hoặc `preferences` |
| `POST /tasks`, `GET /tasks` | Tạo và liệt kê phiếu |
| `GET/PATCH /tasks/{id}` | Đọc phiếu và lưu nhãn |
| `GET /tasks/{id}/metrics` | Metric của phiếu đã chấm đủ, cùng model |
| `GET /export?task_id=...&format=jsonl` | Xuất nhãn cho CLI đánh giá |
| `POST /admin/reload` | Nạp version; cần `X-Admin-Token` |

Ví dụ nhu cầu thuê:

```bash
curl -s http://localhost:8501/recommend \
  -H 'Content-Type: application/json' \
  -d '{"method":"preferences","request":{"transaction_type":"rent","property_category":"APARTMENT","price_period":"month","max_price":15000000,"min_area":40,"limit":10}}'
```

Đánh giá file JSONL (thay các đường dẫn bằng file/bundle của chính phiếu):

```bash
docker compose -p kltn exec -T recommendation-worker \
  python -m recommendation.evaluate \
  --bundle /state/jobs/JOB_HASH/bundle \
  --labels /state/my_labels.jsonl --k 10
```

Copy file tải về vào `artifacts/recommendation/production/my_labels.jsonl` trước khi chạy. File CSV dành cho đọc/kiểm tra; CLI nhận JSONL. Các chỉ số từ một phiếu chưa đủ kết luận chất lượng toàn hệ thống.

## 7. Vận hành container và DAG

Chạy lệnh ở thư mục gốc LakeHouse. Cần các dịch vụ MLflow, MinIO và Airflow đang chạy. Hai service mới tái sử dụng image `kltn-fastapi` hiện có; trên máy mới build bằng `docker compose -p kltn build fastapi` trước. Có thể override image bằng `REIP_RECOMMENDATION_IMAGE`; giữ sklearn 1.6.1 tương thích bundle hiện tại.

```bash
# Bật hai service recommendation
docker compose -p kltn up -d --no-deps recommendation-api recommendation-worker

# Xem trạng thái/log
docker compose -p kltn ps recommendation-api recommendation-worker
docker compose -p kltn logs --tail 100 recommendation-api recommendation-worker

# Dừng riêng demo và worker, không dừng cả LakeHouse
docker compose -p kltn stop recommendation-api recommendation-worker

# Sau khi đổi mã nguồn
docker compose -p kltn restart recommendation-api recommendation-worker

# Chạy retrain ngay (không truyền run-id cố định cho lần mới)
docker exec kltn-airflow-scheduler-1 airflow dags trigger train_recommendation_model

# Tắt / bật lịch tự động
docker exec kltn-airflow-scheduler-1 airflow dags pause train_recommendation_model
docker exec kltn-airflow-scheduler-1 airflow dags unpause train_recommendation_model
```

Không cần `docker compose down` toàn stack. Tránh restart worker khi đang train; nếu worker bị dừng, job sẽ báo lỗi/được retry từ các bước đã lưu. Mỗi container giới hạn 3 GiB và 2 CPU; tải đồng thời của Spark + API reload + train vẫn cần theo dõi trên máy có RAM Docker hạn chế. Lịch sau dbt giúp giảm chồng lấn nhưng không bảo đảm không trùng các job khác.

Token đã được tạo và đồng bộ vào Airflow Variable trên máy triển khai. Khi chuyển máy/khôi phục, tạo token ngẫu nhiên vào `/state/admin_token` và đặt cùng giá trị cho `recommendation_admin_token` trong Airflow (đánh dấu secret; không ghi vào Git/log). Worker dùng token cho mọi API job, API dùng token cho reload. Demo chấm nhãn chưa có tài khoản/phân quyền, chỉ phù hợp demo nội bộ tại localhost.

Bootstrap bundle cũ (chỉ cần ở lần khởi tạo Registry):

```bash
docker compose -p kltn exec -T recommendation-worker \
  python -m recommendation.pipeline --job-id bootstrap-20261024 --bootstrap /bootstrap
```

Hoặc trigger DAG để train mới trực tiếp từ Gold; không cần bootstrap nếu muốn bắt đầu từ snapshot mới.

## 8. Rollback có kiểm soát

Tạm pause DAG và chờ job đang chạy kết thúc trước khi rollback thủ công. Chuyển alias trong UI MLflow **chưa đủ** đổi model đang nằm trong RAM. Cần đổi alias rồi gọi reload đúng version, ví dụ đổi về version 1:

```bash
docker compose -p kltn exec -T recommendation-worker python - <<'PY'
import os
from pathlib import Path
import requests
from mlflow.tracking import MlflowClient
c = MlflowClient()
name, version = 'reip_recommendation', '1'
previous = c.get_registered_model(name).aliases.get('champion')
c.set_registered_model_alias(name, 'champion', version)
try:
    token = Path(os.environ['REIP_ADMIN_TOKEN_FILE']).read_text().strip()
    r = requests.post(os.environ['REIP_API_URL']+'/admin/reload',
        headers={'X-Admin-Token': token}, json={'version': version}, timeout=240)
    r.raise_for_status()
    print('Serving version:', r.json()['version'])
except Exception:
    if previous:
        c.set_registered_model_alias(name, 'champion', previous)
    else:
        c.delete_registered_model_alias(name, 'champion')
    raise
PY
```

Không chạy đoạn rollback để kiểm thử thông thường trên nhãn/dữ liệu thật. Alias và API là hai hệ thống, nên kiểm tra `/health` sau sự cố mạng hoặc thao tác Registry thủ công; không coi thay alias là một giao dịch nguyên tử xuyên hệ thống.

## 9. Vai trò các file bổ sung

| File | Vai trò |
|---|---|
| `recommendation/mlflow_model.py` | Wrapper MLflow PyFunc: nhận `method`, `request_json`; trả `result_json` |
| `recommendation/registry.py` | Ghi tham số, metrics, artifacts, package code và đăng ký version |
| `recommendation/pipeline.py` | Export/train/kiểm tra/đăng ký/promotion; khóa chạy và retry |
| `api/app/recommendation/worker.py` | API nội bộ tạo subprocess retrain, theo dõi trạng thái |
| `api/app/recommendation/main.py` | Nạp champion, API gợi ý, SQLite nhãn, export và metric |
| `web/recommendation/index.html` | Giao diện demo cùng origin với API |
| `airflow/dags/train_recommendation_model.py` | Nhận Dataset event, gọi worker và theo dõi đến khi xong |
| `airflow/dags/end_to_end_pipeline.py` | Thêm recommendation Dataset vào outlet của Gold test |
| `api/tests/test_recommendation.py` | HTTP, nhãn, metric, PyFunc roundtrip, rollback/retry |
| `docker-compose.yml` | Hai service mới, network, volume, cấu hình RAM/CPU |

Các file core `contract.py`, `export.py`, `train.py`, `engine.py`, `benchmark.py` tiếp tục giữ vai trò chuẩn hóa, export Delta, huấn luyện, xếp hạng và kiểm tra kỹ thuật. `evaluate.py` dùng chung một hàm tính metric cho CLI và API để tránh lệch công thức.

## 10. Kiểm chứng triển khai

Kết quả thực tế được ghi ở `RECOMMENDATION_DEPLOYMENT_VALIDATION.md` sau khi chạy đủ pipeline. Nhãn tổng hợp dùng cho kiểm thử lưu ở thư mục tạm; không thêm vào SQLite nhãn người dùng.

Chạy bộ kiểm thử core và API sau khi container đã có các mount mới:

```bash
docker compose -p kltn exec -T recommendation-worker \
  python -m unittest recommendation.tests.test_recommendation api.tests.test_recommendation -v
```

Tài liệu API gốc: [MLflow PyFunc](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html), [Airflow Dataset scheduling](https://airflow.apache.org/docs/apache-airflow/2.6.1/authoring-and-scheduling/datasets.html).
