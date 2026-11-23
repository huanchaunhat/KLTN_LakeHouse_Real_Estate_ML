# Web demo

`recommendation/index.html` chứa giao diện demo gợi ý bất động sản: nhập mã tin (mã Nhà Tốt, `listing_key` hoặc link tin) để tự điền tiêu chí và gợi ý tin tương tự, tìm theo nhu cầu, chấm nhãn, xuất JSONL/CSV, xem metric và dự đoán giá bán (cũng tự điền được từ mã tin bán). Link hoặc mã tin Nhà Tốt chưa có trong dữ liệu được lấy trực tiếp từ Nhà Tốt (`GET /listing/live`): tab dự đoán giá dùng bình thường, tab gợi ý chạy theo tiêu chí tự điền từ tin vì tin chưa nằm trong index của model. Có thể mở thẳng một tin bằng `?id=<mã tin>`, thêm `#pricing` để mở ở tab dự đoán giá.

Backend nằm tại `api/app/recommendation/`. FastAPI phục vụ file HTML tại `/` và các endpoint cùng origin; mở demo qua http://localhost:8501 thay vì mở trực tiếp file HTML. Thư mục này chỉ chứa giao diện, dữ liệu nhãn lưu tại `artifacts/recommendation/production/labels.sqlite`.

Thẻ tin gợi ý, tin tra cứu và phiếu chấm nhãn mới hiển thị ảnh đại diện từ `image_urls`. Bấm ảnh để mở bộ ảnh, dùng nút trước/sau hoặc phím mũi tên; nhấn Escape để đóng. Ảnh được tải trực tiếp từ URL đã crawl, có tải lười và thông báo thay thế khi URL không còn truy cập được. Phiếu chấm nhãn cũ giữ nguyên dữ liệu đã đóng băng nên có thể chưa có ảnh.

Gold `ml_recommendation_features` giữ `image_urls` và `image_count` cho các bundle tạo sau lần dbt cập nhật tiếp theo. API trả danh sách URL hợp lệ và số ảnh thực tế; các trường này không tham gia tính điểm. Với bundle cũ, xuất metadata ảnh từ snapshot Silver hiện tại theo đúng `listing_key` của bundle, rồi khởi động lại riêng API. Ví dụ cho version 9 (thay đường dẫn theo version đang phục vụ):

```bash
docker compose -p kltn exec -T recommendation-worker python -m recommendation.media \
  --bundle /state/cache/9/artifacts/bundle
docker compose -p kltn restart recommendation-api
```

Lệnh chỉ đọc các file đang hoạt động theo Delta log và hai cột `listing_key`, `image_urls`, giữ các mã có trong bundle rồi ghi nguyên tử `/state/listing_images.parquet` (kèm phiên bản Silver trong metadata Parquet). API đọc file một lần khi khởi động, ưu tiên ảnh có sẵn trong bundle. Không chỉnh bundle/manifest, không huấn luyện lại hoặc đổi MLflow champion. Có thể chạy lại lệnh khi cần làm mới ảnh của bundle cũ.

Sau khi bật Docker và các dịch vụ LakeHouse cần thiết, chạy từ thư mục gốc dự án:

```bash
docker compose -p kltn up -d --no-deps recommendation-api recommendation-worker
```

Chi tiết vận hành: [RECOMMENDATION_DEPLOYMENT.md](../docs/RECOMMENDATION_DEPLOYMENT.md). Chức năng tra cứu và dự đoán giá: [WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md](../docs/WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md). Kết nối với MLflow và API giá: [MLFLOW_VA_FASTAPI.md](../docs/MLFLOW_VA_FASTAPI.md).
