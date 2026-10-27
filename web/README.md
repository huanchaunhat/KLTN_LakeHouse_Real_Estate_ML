# Web demo

`recommendation/index.html` chứa giao diện demo gợi ý bất động sản: tìm theo nhu cầu/tin tương tự, chọn tỉnh và quận/huyện, chấm nhãn, xuất JSONL/CSV và xem metric.

Backend nằm tại `api/app/recommendation/`. FastAPI phục vụ file HTML tại `/` và các endpoint cùng origin; mở demo qua http://localhost:8501 thay vì mở trực tiếp file HTML. Thư mục này chỉ chứa giao diện, dữ liệu nhãn lưu tại `artifacts/recommendation/production/labels.sqlite`.

Sau khi bật Docker và các dịch vụ LakeHouse cần thiết, chạy từ thư mục gốc dự án:

```bash
docker compose -p kltn up -d --no-deps recommendation-api recommendation-worker
```

Chi tiết vận hành: [RECOMMENDATION_DEPLOYMENT.md](../docs/RECOMMENDATION_DEPLOYMENT.md).
