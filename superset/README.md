# Superset: ba dashboard được sinh từ code

| Dashboard | Đường dẫn | Trả lời câu hỏi |
|---|---|---|
| Tổng quan thị trường BĐS TP.HCM | `/superset/dashboard/tong-quan-thi-truong/` | Thị trường có gì, ở đâu, giá bao nhiêu; giá và nguồn cung đổi thế nào theo tuần; phân bố giá, diện tích; dữ liệu sạch đến đâu |
| Phân tích giá và thị trường BĐS TP.HCM | `/superset/dashboard/phan-tich-gia-thi-truong/` | A. Yếu tố nào quyết định giá (căn cứ chọn đặc trưng cho model). B. Đặc điểm riêng của nhà ở, căn hộ, đất. C. Vị trí ở mức phường. D. Phân khúc giá. E. Diễn biến theo tuần từ dữ liệu lịch sử: giá/m² nội và ngoại thành, tin đang rao, tin mới, tin tăng và giảm giá |
| Model dự đoán giá bán: sai số trên tập test | `/superset/dashboard/mo-hinh-du-doan-gia/` | Phiên bản champion sai bao nhiêu, sai ở loại hình, quận, phân khúc nào, có lệch hệ thống hay trôi theo tuần không |

Toàn bộ dashboard (dataset = câu Spark SQL trên tầng Gold, chart, bố cục, bộ lọc)
được khai báo trong `build_dashboards.py`. Không có gì được vẽ tay trong giao diện,
nên dựng lại trên một Superset mới chỉ cần một lệnh:

```bash
docker compose up -d superset        # kéo theo Spark Thrift Server và Hive Metastore
bash superset/import_dashboards.sh   # sinh dashboards.zip -> import -> publish
```

Đăng nhập: `admin` / `admin` tại http://localhost:8089.

## Cách đọc

- Giá bán lấy từ `gold.ml_price_features`: tin bán đã làm sạch, bỏ giá bất thường, bỏ văn phòng/mặt bằng (tin "bán" của nhóm này thường ghi giá thuê). Đơn vị: giá bán tỷ đồng, giá/m² triệu đồng, giá thuê triệu đồng/tháng. Dùng trung vị thay vì trung bình.
- Số tin lấy từ `gold.fct_listing` (`is_market_eligible`). Các chart theo tuần lấy từ hai bảng tổng hợp `gold.agg_price_trend_weekly` và `gold.agg_market_supply_weekly`, cấp toàn thành phố và nội/ngoại thành (dataset `tg_*_tp`, không có cột quận nên bộ lọc Quận bỏ qua). Không dùng time grain của Superset (Spark 3 không chạy được hàm gộp tuần của Hive); dữ liệu đã là một dòng mỗi tuần.
- Ba bộ lọc **Loại hình**, **Quận**, **Khu vực** áp dụng cho mọi chart có cột tương ứng (`loai_bds`, `quan`, `khu_vuc`); chart nào không có cột đó thì bộ lọc bỏ qua. Nhóm dưới 30 tin bị loại khỏi các chart giá.
- Dashboard phân tích chỉ nói về dữ liệu. Model có dashboard riêng, đọc `gold.ml_price_predictions` của phiên bản mới nhất, chỉ tập test (20% tin mới nhất theo ngày đăng), nên con số khớp với MLflow.
- Không có chart về số ngày rao hay mức giảm giá: tin Nhà Tốt hết hạn sau 30 ngày nên số ngày rao gần như bằng nhau ở mọi quận (27 đến 29 ngày), và tỷ lệ giảm giá chỉ dao động 4 đến 10%, chưa đủ khác biệt để kết luận với 6 tuần dữ liệu.

## Sửa dashboard

Sửa `build_dashboards.py` rồi chạy lại `import_dashboards.sh`. Script xóa các chart và dataset
do nó quản lý rồi tạo lại (Superset chỉ tự ghi đè dashboard, không ghi đè chart và dataset có sẵn);
UUID sinh từ tên nên dashboard được cập nhật tại chỗ, link không đổi.
Thêm chart: thêm một dataset (nếu cần) vào `DATASETS`, một mục vào `CHARTS`, rồi
đặt nó vào `rows` của dashboard. Chart mới phải được đặt vào một dashboard, nếu
không script báo lỗi.

Kết quả chart được cache một ngày (`superset_config.py`) vì Gold chỉ đổi sau mỗi lần
pipeline chạy. Sau khi chạy pipeline, bấm "Force refresh" trên dashboard để thấy số mới.
