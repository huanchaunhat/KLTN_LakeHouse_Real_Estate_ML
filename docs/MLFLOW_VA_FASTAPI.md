# MLflow và FastAPI: hoạt động và kết nối ra sao

Số liệu và trạng thái trong tài liệu này được kiểm tra trên stack đang chạy ngày 02/11/2026. Version model sẽ đổi sau mỗi lần train, nên xem [mục 9](#9-tự-kiểm-tra-bằng-lệnh) để lấy số mới nhất.

## 1. Tóm tắt

- **MLflow là kho quản lý model.** Nó không tự train và cũng không phục vụ người dùng. Nó ghi lại mỗi lần train, lưu file model và đánh dấu version nào đang được dùng.
- **FastAPI là lớp phục vụ.** Nó tải model từ MLflow vào RAM, nhận request HTTP rồi trả kết quả. Khi đang dự đoán, nó không gọi MLflow nữa.
- **Alias `champion` là điểm nối giữa hai bên.** Quá trình train gắn `champion` vào version đạt yêu cầu, còn API luôn nạp version đang mang alias đó.

Có một điều dễ nhầm: **đổi alias không làm API đổi model ngay**. Model đang nằm trong RAM của API, nên phải gọi endpoint reload thì model mới được nạp.

## 2. Bức tranh tổng thể

```mermaid
flowchart LR
    U[Trình duyệt]

    subgraph Phuc_vu[Phục vụ]
        RA[recommendation-api<br/>localhost:8501]
        FA[fastapi<br/>localhost:8000]
    end

    subgraph Train[Huấn luyện]
        AF[Airflow]
        RW[recommendation-worker<br/>nội bộ :8010]
    end

    subgraph Kho[Kho model]
        ML[MLflow server<br/>localhost:5001]
        PG[(Postgres<br/>metadata)]
        MN[(MinIO<br/>bucket mlflow)]
    end

    U -->|trang web, gợi ý, chấm nhãn| RA
    U -.->|gọi trực tiếp, Swagger| FA
    RA -->|/price/* chuyển tiếp| FA

    AF -->|train_price_model.py| ML
    AF -->|POST /model/reload| FA
    AF -->|POST /jobs, GET /jobs| RW
    RW -->|POST /admin/reload| RA
    RW --> ML

    ML --> PG
    AF -->|ghi file model| MN
    RW -->|ghi file model| MN
    FA -->|hỏi champion| ML
    FA -->|tải file model| MN
    RA -->|hỏi champion| ML
    RA -->|tải file model| MN
```

Tất cả container nằm chung network `data_network` của Docker, nên chúng gọi nhau bằng **tên service và cổng trong container**. Máy thật dùng cổng đã map ra host, vì vậy cùng một dịch vụ có hai địa chỉ:

| Service | Từ máy thật | Từ container khác | Vai trò |
|---|---|---|---|
| `mlflow` | http://localhost:5001 | `http://mlflow:5000` | Tracking và Model Registry |
| `postgres` | `localhost:5433` | `postgres:5432` | Lưu metadata của MLflow |
| `minio` | `localhost:9000` (console 9001) | `http://minio:9000` | Lưu file model (bucket `mlflow`) |
| `fastapi` | http://localhost:8000 | `http://fastapi:8000` | API dự đoán giá |
| `recommendation-api` | http://localhost:8501 (chỉ `127.0.0.1`) | `http://recommendation-api:8000` | API gợi ý và trang web demo |
| `recommendation-worker` | Không mở ra host | `http://recommendation-worker:8010` | Nhận lệnh retrain từ Airflow |

## 3. MLflow hoạt động thế nào

### 3.1 Các khái niệm

| Khái niệm | Ý nghĩa | Ví dụ trong dự án |
|---|---|---|
| Experiment | Nhóm các lần train của một bài toán | `price_prediction`, `reip_recommendation` |
| Run | Một lần train cụ thể | Run `758779c2…` tạo ra `price_model` v11 |
| Param | Cấu hình đầu vào của run | `rows_train`, `test_fraction` |
| Metric | Kết quả đo được | `test_mape`, `baseline_mape` |
| Tag | Nhãn mô tả tự do | `algorithm=xgboost`, `gate=promoted` |
| Artifact | File do run sinh ra | Model, `feature_spec.json` |
| Registered model | Tên một model trong Registry | `price_model`, `reip_recommendation` |
| Version | Số thứ tự tăng dần trong một registered model | v1, v2, …, v11 |
| Alias | Nhãn trỏ tới một version và có thể chuyển sang version khác | `champion` |

Version không bị xóa khi model mới ra đời. Nhờ vậy có thể so sánh lại hoặc quay về bản cũ.

### 3.2 Dữ liệu nằm ở hai nơi

Lệnh khởi động trong `docker-compose.yml` cho thấy cách chia:

```text
mlflow server
  --backend-store-uri postgresql://reip:...@postgres:5432/reip   ← metadata
  --default-artifact-root s3://mlflow/                           ← file
```

| Nơi lưu | Nội dung | Vị trí thực tế |
|---|---|---|
| Postgres | Experiment, run, param, metric, tag, version, alias | Các bảng `runs`, `metrics`, `model_versions`, `registered_model_aliases`, … |
| MinIO | File model và file đi kèm | `s3://mlflow/<experiment_id>/<run_id>/artifacts/…` |

Mất Postgres thì mất "sổ sách" dù file vẫn còn. Mất MinIO thì còn sổ sách nhưng không tải được model.

### 3.3 Client đọc và ghi file thẳng với MinIO

Đây là phần khó hiểu nhất của cấu hình. Vì artifact root là `s3://…`, MLflow server **không trung chuyển file**. Quá trình tải model diễn ra như sau:

```mermaid
sequenceDiagram
    participant C as FastAPI (client)
    participant M as MLflow server
    participant P as Postgres
    participant S as MinIO
    C->>M: champion của price_model là version nào?
    M->>P: Đọc alias và version
    P-->>M: v11, run 758779c2…
    M-->>C: v11, file ở s3://mlflow/1/758779c2…/artifacts/model
    C->>S: Tải file bằng boto3
    S-->>C: File model
```

Vì vậy mọi nơi dùng MLflow đều cần đủ 3 nhóm biến môi trường:

| Biến | Giá trị | Nếu thiếu |
|---|---|---|
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | Client không tìm được MLflow server |
| `MLFLOW_S3_ENDPOINT_URL` | `http://minio:9000` | boto3 gọi tới Amazon S3 thật thay vì MinIO |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Khóa MinIO lấy từ `.env` | MinIO từ chối truy cập |

`fastapi`, `recommendation-api` và `recommendation-worker` khai báo các biến này trong `docker-compose.yml`. Task train giá nhận chúng qua `env_vars` của `SparkSubmitOperator` trong `airflow/dags/end_to_end_pipeline.py`.

Tham số `--allowed-hosts` của MLflow liệt kê các hostname được phép gọi server. Nếu truy cập bằng một hostname không có trong danh sách, request sẽ bị từ chối.

### 3.4 Hiện trạng Registry

| Registered model | Experiment | Số version | `champion` | Ghi chú |
|---|---|---|---|---|
| `price_model` | `price_prediction` (id 1) | 11 | v11 | XGBoost, `test_mape` = 19,84% |
| `reip_recommendation` | `reip_recommendation` (id 2) | 3 | v3 | Sale 21.193 tin, rent 32.408 tin |

Lịch sử `price_model` minh họa cách quality gate hoạt động:

| Version | `test_mape` lúc train | Kết quả gate |
|---|---|---|
| v1, v2 | 29,58% | Được promote |
| v3 | 20,42% | Được promote |
| v4 đến v10 | 20,35% | Bị từ chối: kém hơn champion v3 |
| v11 | 19,84% | Được promote, đang phục vụ |

v4 đến v10 có MAPE thấp hơn con số 20,42% của v3 nhưng vẫn bị từ chối. Lý do là con số 20,42% được đo trên tập test cũ. Gate chấm lại champion trên **tập test của lần train mới**, rồi mới so sánh hai model với nhau, và trên tập đó v3 tốt hơn.

## 4. Hai model khác nhau thế nào

| | `price_model` | `reip_recommendation` |
|---|---|---|
| Bài toán | Dự đoán giá bán | Gợi ý tin tương tự hoặc theo nhu cầu |
| Dữ liệu | `gold.ml_price_features` | `gold.ml_recommendation_features` |
| Code train | `scripts/train_price_model.py` | `recommendation/pipeline.py`, `train.py`, `registry.py` |
| Chạy ở đâu | Spark driver trong container Airflow | Subprocess trong `recommendation-worker` |
| Định dạng model | `mlflow.sklearn` | `mlflow.pyfunc` (kèm bundle dữ liệu và code) |
| Điều kiện lên champion | Thắng baseline và không thua champion về MAPE | Chỉ qua kiểm tra kỹ thuật, chưa đánh giá bằng nhãn người |
| Lịch train | Thứ Hai hằng tuần, hoặc trigger thủ công | Mỗi lần `dbt_test_gold` thành công |
| Người báo API nạp model | Task `reload_price_api` | Pipeline của worker |
| Khi nạp lỗi | API giữ model cũ, alias vẫn trỏ version mới | Tự trả alias về version trước |

Cả hai DAG train được kích hoạt bởi **Dataset** do task `dbt_test_gold` phát ra, nên chỉ chạy khi dữ liệu Gold đã qua test. DAG `end_to_end_lakehouse_pipeline` chạy `@daily`. Riêng `train_price_model` có thêm task `is_training_day` để dừng sớm vào các ngày không phải thứ Hai.

## 5. Luồng model giá: từ train tới API

```mermaid
sequenceDiagram
    participant D as dbt_test_gold
    participant T as train_price_model.py
    participant M as MLflow và MinIO
    participant A as fastapi
    D->>T: Dataset cập nhật (lịch thứ Hai)
    T->>T: Đọc Gold, chia theo posted_date, fit XGBoost
    T->>M: Ghi param, metric, artifact
    T->>M: Đăng ký version mới
    T->>M: Chấm champion hiện tại trên cùng tập test
    alt Thắng baseline và không thua champion
        T->>M: Chuyển alias champion sang version mới
    else Thua
        T->>M: Chỉ ghi tag gate, alias giữ nguyên
    end
    T->>A: POST /model/reload (task reload_price_api)
    A->>M: Nạp lại version đang là champion
```

Điều kiện promote trong `scripts/train_price_model.py`:

```python
beats_baseline = test_mape < baseline_mape
beats_champion = champ_mape is None or test_mape <= champ_mape * (1 + max_mape_regression)
promoted = beats_baseline and beats_champion
```

`max_mape_regression` mặc định bằng 0, nghĩa là model mới không được kém hơn champion. Baseline là giá trung vị theo m² của từng cặp quận và loại bất động sản.

Task reload luôn chạy, kể cả khi model bị từ chối. Khi đó API chỉ nạp lại đúng version cũ, nên không có gì thay đổi.

### Các file đi kèm model giá

API dựa vào những file này để nhận đầu vào giống hệt lúc train:

| Artifact | Tác dụng |
|---|---|
| `model/` | Pipeline sklearn gồm tiền xử lý và XGBoost |
| `feature_spec.json` | Danh sách cột, kiểu dữ liệu, ký hiệu giá trị thiếu, phép biến đổi log |
| `districts.json` | Suy ra `district_type`, `area_zone` từ `district_id` |
| `form_options.json` | Lựa chọn của form web, tọa độ trung vị theo phường |
| `model_comparison.csv`, `test_by_property_type.csv` | Bảng kết quả cho báo cáo, API không dùng |

## 6. Luồng model gợi ý: từ train tới API

```mermaid
sequenceDiagram
    participant F as Airflow
    participant W as recommendation-worker
    participant M as MLflow và MinIO
    participant R as recommendation-api
    F->>W: POST /jobs (X-Admin-Token)
    W-->>F: 202, trạng thái running
    loop Mỗi 10 giây, tối đa 1 giờ
        F->>W: GET /jobs?job_id=...
    end
    W->>W: Export Gold, train, kiểm tra kỹ thuật
    W->>M: Đăng ký version, alias candidate
    W->>M: Chuyển alias champion sang version mới
    W->>R: POST /admin/reload {version}
    alt Nạp thành công
        R-->>W: Trả về đúng version
        W->>W: Ghi plan.json, status=deployed
    else Lỗi
        W->>M: Trả champion về version trước
    end
```

Những điểm cần biết:

- **Worker tách khỏi Airflow** vì môi trường Python của Airflow khác với môi trường train gợi ý. Airflow chỉ gửi lệnh rồi theo dõi.
- **Có thể chạy lại an toàn.** Mỗi job có thư mục riêng dưới `artifacts/recommendation/production/jobs/`. Bước nào đã xong thì lần chạy lại sẽ bỏ qua.
- **Không đăng ký trùng.** `registry.py` so sánh tag `bundle_sha256`. Nếu bundle đã có trong Registry, nó dùng lại version đó.
- **API tự kiểm tra trước khi nhận model.** `Manager.load()` thử gợi ý trên từng thị trường, xác nhận alias vẫn trỏ đúng version, rồi mới thay model bằng một phép gán. Request đang chạy vẫn dùng model cũ cho tới khi xong.
- **Token admin có hai bản phải khớp nhau:** file `artifacts/recommendation/production/admin_token` và Airflow Variable `recommendation_admin_token`.

## 7. FastAPI: thực ra có 3 ứng dụng

| Service | Module | Code lấy từ đâu | Chức năng |
|---|---|---|---|
| `fastapi` | `app.main:app` | **Copy vào image lúc build** | Dự đoán giá |
| `recommendation-api` | `api.app.recommendation.main:app` | Mount từ thư mục dự án | Gợi ý, chấm nhãn, web demo, proxy giá |
| `recommendation-worker` | `api.app.recommendation.worker:app` | Mount từ thư mục dự án | Chạy retrain theo lệnh Airflow |

Cả ba dùng chung image `kltn-fastapi`, build từ `api/Dockerfile`.

### 7.1 Endpoint của API giá (cổng 8000)

Swagger: http://localhost:8000/docs. Mục **Schemas** ở cuối trang đã được ẩn (`swagger_ui_parameters={"defaultModelsExpandDepth": -1}`). Cấu trúc request và response vẫn xem được khi mở từng endpoint.

| Endpoint | Chức năng |
|---|---|
| `GET /` | Trạng thái, có model hay chưa |
| `GET /model` | Version, run id, metric của model đang phục vụ |
| `GET /options` | Danh sách lựa chọn cho form |
| `POST /predict` | Dự đoán giá |
| `POST /model/reload` | Nạp lại champion từ MLflow |
| `GET /web` | **Đang lỗi 500**, xem [mục 11](#11-hạn-chế-hiện-tại) |

### 7.2 Endpoint của API gợi ý (cổng 8501)

Swagger: http://localhost:8501/docs, cũng đã ẩn mục **Schemas**.

| Endpoint | Chức năng |
|---|---|
| `GET /` | Trang web demo |
| `GET /health`, `/model` | Trạng thái và thông tin model gợi ý |
| `GET /catalog`, `/filters` | Tìm tin theo tiêu đề, danh sách tỉnh và quận |
| `GET /listing?id=` | Tra một tin theo `listing_key`, mã nguồn hoặc link |
| `GET /listing/live?id=` | Lấy trực tiếp từ Nhà Tốt một tin **chưa có trong dữ liệu**, theo link hoặc mã số |
| `POST /recommend` | Gợi ý theo `similar` hoặc `preferences` |
| `POST /tasks`, `GET /tasks`, `GET/PATCH /tasks/{id}` | Phiếu chấm nhãn, lưu trong SQLite |
| `GET /tasks/{id}/metrics`, `GET /export` | Chỉ số và xuất nhãn |
| `POST /admin/reload` | Nạp version chỉ định, cần `X-Admin-Token` |
| `GET /price/model`, `/price/options`, `POST /price/predict` | Chuyển tiếp sang API giá |

`/listing/live` là endpoint duy nhất gọi ra Internet: nó gọi API chi tiết tin của Nhà Tốt, cùng địa chỉ mà crawler dùng. Chi tiết ở [WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md, mục 2.4](WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md#24-get-listingliveidgiá-trị).

### 7.3 Vì sao có `/price/*`

Trang web được phục vụ từ cổng 8501, còn API giá ở cổng 8000. Trình duyệt coi đó là hai origin khác nhau và sẽ chặn nếu API không cấu hình CORS. Vì vậy trang web chỉ gọi cùng origin, rồi `recommendation-api` chuyển tiếp nội bộ:

```text
Trình duyệt → localhost:8501/price/predict → http://fastapi:8000/predict
```

Địa chỉ đích lấy từ biến `PRICE_API_URL`. Proxy giữ nguyên mã trạng thái và nội dung trả về từ API giá. Nếu không kết nối được, proxy trả 503 với thông báo tiếng Việt.

Hệ quả: API gợi ý và API giá chạy độc lập. Tắt `fastapi` chỉ làm hỏng tab Dự đoán giá, tab Gợi ý vẫn dùng được.

### 7.4 Khi khởi động, nếu MLflow chưa sẵn sàng

| | `fastapi` | `recommendation-api` |
|---|---|---|
| Số lần thử | **Chỉ một lần** | Thử lại mỗi 30 giây tới khi được |
| Khi thất bại | Vẫn chạy, `/predict` trả 503 | Vẫn chạy, các endpoint cần model trả 503 |
| Cách khắc phục | **Phải gọi `POST /model/reload`** | Tự hồi phục |

`depends_on` của `fastapi` chỉ chờ container MLflow *được khởi động*, không chờ nó sẵn sàng. Trong khi đó container MLflow còn chạy `pip install` trước khi mở server. Nếu sau khi bật stack mà `GET localhost:8000/` trả `model_loaded: false`, hãy gọi reload.

## 8. Đi theo một request dự đoán giá

Request gửi từ tab Dự đoán giá:

```json
{"property_type": "Căn hộ/Chung cư", "district_id": "13106", "area_m2": 58, "bedrooms": 2, "has_car_alley": false}
```

1. **Trình duyệt** gửi `POST /price/predict` tới cổng 8501 (`runPredict` trong `web/recommendation/index.html`).
2. **`recommendation-api`** không kiểm tra nội dung mà chuyển tiếp sang `http://fastapi:8000/predict` (`price_api` trong `api/app/recommendation/main.py`).
3. **`fastapi`** kiểm tra kiểu dữ liệu bằng `PropertyInput`. Ví dụ `area_m2` phải lớn hơn 0 và không quá 20.000, nếu sai sẽ trả 422.
4. **`to_frame`** dựng bảng một dòng theo `feature_spec.json`:
   - Tra `district_id` trong `districts.json` để thêm `district_type` và `area_zone`. Quận lạ trả 422.
   - Tính `is_in_project` từ `project_name`.
   - Suy ra `has_certificate` từ `legal_category` nếu chưa có.
   - Ép kiểu: số thành `float64`, boolean thành 1.0/0.0, chữ thành `str`, giá trị thiếu thành ký hiệu thiếu của lúc train.
   - Sắp xếp cột đúng thứ tự trong `spec["features"]`.
5. **Model** dự đoán log của giá. API lấy `np.exp(...)` để đưa về VND.
6. **Làm tròn và trả về:** giá tròn tới triệu đồng, giá theo m² tròn tới nghìn đồng:

```json
{"predicted_price_vnd": 2873000000.0, "predicted_price_per_m2_vnd": 49528000.0, "model_version": "11"}
```

Toàn bộ các bước trên dùng model trong RAM, **không có lệnh gọi nào tới MLflow hay MinIO**. Vì vậy MLflow có tắt thì `/predict` vẫn trả lời được, chỉ không reload được.

## 9. Tự kiểm tra bằng lệnh

```bash
# API giá có model chưa, đang dùng version nào
curl -s localhost:8000/
curl -s localhost:8000/model

# API gợi ý
curl -s localhost:8501/health

# Đường proxy từ cổng 8501 sang API giá
curl -s localhost:8501/price/model

# Dự đoán thử
curl -s -X POST localhost:8000/predict -H 'Content-Type: application/json' \
  -d '{"district_id":"13106","property_type":"Căn hộ/Chung cư","area_m2":58}'

# Registry: các model và alias
curl -s localhost:5001/api/2.0/mlflow/registered-models/search

# Alias lưu trong Postgres
docker exec kltn-postgres-1 psql -U reip -d reip \
  -c "select name, alias, version from registered_model_aliases;"

# File của một run trong MinIO
docker exec kltn-minio-1 ls /data/mlflow/1
```

Các giao diện web: MLflow http://localhost:5001, Airflow http://localhost:8088, MinIO http://localhost:9001.

## 10. Vận hành

### Sửa code xong cần làm gì

| Sửa file | Cách áp dụng |
|---|---|
| `web/recommendation/index.html` | Tải lại trang, vì file được đọc mỗi request |
| `api/app/recommendation/*.py`, `recommendation/*.py` | `docker compose -p kltn restart recommendation-api recommendation-worker` |
| `api/app/main.py` | `docker compose -p kltn up -d --build fastapi`, vì code nằm trong image |
| `api/requirements.txt` | Build lại như trên, rồi `docker compose -p kltn up -d --no-deps --force-recreate recommendation-api recommendation-worker` |

### Train lại

```bash
# Model giá
docker compose -p kltn exec airflow-scheduler airflow dags trigger train_price_model

# Model gợi ý
docker compose -p kltn exec airflow-scheduler airflow dags trigger train_recommendation_model
```

Trigger thủ công `train_price_model` thì bỏ qua điều kiện thứ Hai. Quality gate vẫn áp dụng.

### Quay về version cũ của model giá

1. Vào http://localhost:5001, mở registered model `price_model`, chuyển alias `champion` sang version muốn dùng.
2. Báo API nạp lại:

```bash
curl -X POST localhost:8000/model/reload
```

Model gợi ý có quy trình riêng, xem [RECOMMENDATION_DEPLOYMENT.md, mục 8](RECOMMENDATION_DEPLOYMENT.md#8-rollback-có-kiểm-soát).

### Lỗi thường gặp

| Triệu chứng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `/predict` trả 503 "No model loaded" | API khởi động trước MLflow | `POST /model/reload` |
| Reload trả 503 kèm `Connection refused` | MLflow chưa chạy xong | `docker logs kltn-mlflow-1`, đợi rồi thử lại |
| Lỗi `NoCredentialsError`, `AccessDenied` | Thiếu hoặc sai khóa MinIO | Kiểm tra `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` trong `.env` |
| Lỗi `Invalid Host header` từ MLflow | Hostname không có trong `--allowed-hosts` | Thêm hostname vào `docker-compose.yml` |
| Đổi alias nhưng API vẫn trả version cũ | Chưa reload | Gọi endpoint reload |
| `/admin/reload` trả 422 "Champion changed" | Version gửi lên không phải champion | Đổi alias trước rồi mới reload |
| Tab Dự đoán giá báo "Chưa kết nối được API" | `fastapi` đang tắt hoặc chưa có model | `curl localhost:8000/` |
| Dán link báo "Không kết nối được Nhà Tốt" (502) | Container không ra được Internet, hoặc Nhà Tốt chặn | Thử `curl https://gateway.chotot.com/v1/public/ad-listing/<mã>` từ máy; vẫn nhập tay form được |
| Airflow báo 401 khi gọi worker | Token và Airflow Variable không khớp | Đồng bộ lại `recommendation_admin_token` |
| Sửa `api/app/main.py` không có tác dụng | Chưa build lại image | `up -d --build fastapi` |

## 11. Hạn chế hiện tại

Các điểm dưới đây chưa được sửa:

1. **`GET localhost:8000/web` trả 500.** Code trỏ tới `api/app/static/index.html`, nhưng file này không có trong repo. Web demo thật ở cổng 8501 không bị ảnh hưởng.
2. **`POST /model/reload` không cần xác thực**, và cổng 8000 mở trên mọi network interface của máy, trong khi `/admin/reload` có token và cổng 8501 chỉ bind `127.0.0.1`. Chấp nhận được với môi trường demo, không phù hợp để đưa ra ngoài.
3. **API giá không tự thử nạp lại model** khi lần đầu thất bại ([mục 7.4](#74-khi-khởi-động-nếu-mlflow-chưa-sẵn-sàng)).
4. **Có một khoảng rất ngắn trả 503 khi reload API giá**, do `_state.clear()` chạy trước `_state.update()`.
5. **Image `mlflow:latest` chưa ghim version**, nên build lại lúc khác có thể ra phiên bản khác. Mật khẩu Postgres của MLflow cũng được ghi thẳng trong `docker-compose.yml`.
6. **Image `kltn-fastapi` đang chạy vẫn còn `catboost`**, trong khi `api/requirements.txt` đã bỏ. Việc này không gây lỗi và sẽ hết khi build lại.
7. **Model gợi ý được lên champion chỉ nhờ kiểm tra kỹ thuật** (`promotion_policy=mechanical_checks_only_demo`). Chất lượng gợi ý chưa được chứng minh bằng nhãn của người thật.

## 12. Nơi đọc code

| Muốn hiểu | File |
|---|---|
| Khai báo service, cổng, biến môi trường | `docker-compose.yml` |
| Train, gate, ghi lên MLflow của model giá | `scripts/train_price_model.py` |
| DAG và task reload của model giá | `airflow/dags/end_to_end_pipeline.py` |
| API giá | `api/app/main.py` |
| Đăng ký model gợi ý | `recommendation/registry.py` |
| Wrapper PyFunc | `recommendation/mlflow_model.py` |
| Pipeline retrain, rollback alias | `recommendation/pipeline.py` |
| Worker retrain | `api/app/recommendation/worker.py` |
| DAG model gợi ý | `airflow/dags/train_recommendation_model.py` |
| API gợi ý và proxy giá | `api/app/recommendation/main.py` |

Tài liệu liên quan: [WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md](WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md), [RECOMMENDATION_DEPLOYMENT.md](RECOMMENDATION_DEPLOYMENT.md), [ARCHITECTURE.md](ARCHITECTURE.md).
