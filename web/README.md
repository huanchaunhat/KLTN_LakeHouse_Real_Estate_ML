# Web demo

`recommendation/index.html` chứa giao diện demo gợi ý bất động sản: nhập mã tin (mã Nhà Tốt, `listing_key` hoặc link tin) để tự điền tiêu chí và gợi ý tin tương tự, tìm theo nhu cầu, chấm nhãn, xuất JSONL/CSV, xem metric và dự đoán giá bán (cũng tự điền được từ mã tin bán). Link hoặc mã tin Nhà Tốt chưa có trong dữ liệu được lấy trực tiếp từ Nhà Tốt (`GET /listing/live`): tab dự đoán giá dùng bình thường, tab gợi ý chạy theo tiêu chí tự điền từ tin vì tin chưa nằm trong index của model. Có thể mở thẳng một tin bằng `?id=<mã tin>`, thêm `#pricing` để mở ở tab dự đoán giá.

Backend nằm tại `api/app/recommendation/`. FastAPI phục vụ file HTML tại `/` và các endpoint cùng origin; mở demo qua http://localhost:8501 thay vì mở trực tiếp file HTML. Thư mục này chỉ chứa giao diện, dữ liệu nhãn lưu tại `artifacts/recommendation/production/labels.sqlite`.

Sau khi bật Docker và các dịch vụ LakeHouse cần thiết, chạy từ thư mục gốc dự án:

```bash
docker compose -p kltn up -d --no-deps recommendation-api recommendation-worker
```

Chi tiết vận hành: [RECOMMENDATION_DEPLOYMENT.md](../docs/RECOMMENDATION_DEPLOYMENT.md). Chức năng tra cứu và dự đoán giá: [WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md](../docs/WEB_DEMO_TRA_CUU_VA_DU_DOAN_GIA.md). Kết nối với MLflow và API giá: [MLFLOW_VA_FASTAPI.md](../docs/MLFLOW_VA_FASTAPI.md).
