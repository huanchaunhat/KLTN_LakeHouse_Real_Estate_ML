# Superset: sáu dashboard được sinh từ code

Năm dashboard đầu theo đúng năm phần của chương "Phân tích và khám phá dữ liệu"; dashboard thứ sáu dành cho model.

| Dashboard | Đường dẫn | Nội dung |
|---|---|---|
| 1. Tổng quan thị trường | `/superset/dashboard/tong-quan-thi-truong/` | 1.1 cơ cấu loại hình, 1.2 số tin theo loại hình và mục đích, 1.3 phân bố theo quận, 1.4 bản đồ nguồn cung (deck.gl, mỗi chấm một phường) |
| 2. Mặt bằng giá | `/superset/dashboard/mat-bang-gia/` | 2.1 phân bố giá bán, giá/m², diện tích; 2.2 so sánh loại hình; 2.3 giá bán theo quận và theo khoảng cách tới trung tâm; 2.4 giá thuê theo quận; 2.5 xếp hạng phường; 2.6 pháp lý; 2.7 diện tích và giá bán |
| 3. Phân khúc và cơ cấu | `/superset/dashboard/phan-khuc-thi-truong/` | 3.1 phân khúc giá theo loại hình; 3.2 cơ cấu phân khúc của từng quận |
| 4. Biến động theo thời gian | `/superset/dashboard/bien-dong-thi-truong/` | Theo tuần đăng tin: 4.1 tin mới, 4.2 cơ cấu nguồn cung theo loại hình, 4.3 giá/m², 4.4 nguồn cung và giá trên cùng một chart (cột + đường) |
| 5. Chuyên sâu theo loại hình | `/superset/dashboard/chuyen-sau-loai-hinh/` | Xếp hạng yếu tố (eta²), rồi 5.1 nhà ở (số tầng, chiều ngang, đặc điểm, hướng), 5.2 căn hộ (phòng ngủ, nội thất, dự án), 5.3 đất (loại đất) |
| Model dự đoán giá bán | `/superset/dashboard/mo-hinh-du-doan-gia/` | Phiên bản champion sai bao nhiêu, sai ở loại hình, quận, phân khúc nào, có lệch hệ thống hay trôi theo tuần không |

Bản đồ (1.4) cần token Mapbox miễn phí để có nền bản đồ: đặt `MAPBOX_API_KEY` trong `.env`
rồi `docker compose up -d superset`. Không có token thì các chấm vẫn hiện trên nền trống.

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
- Số tin lấy từ `gold.fct_listing` (`is_market_eligible`). Các chart theo tuần (dashboard 4) tính theo tuần người bán đăng tin (`posted_at`), chỉ lấy các tuần trọn (biểu thức `POSTED_SINCE`). Không dùng time grain của Superset (Spark 3 không chạy được hàm gộp tuần của Hive); dữ liệu đã là một dòng mỗi tuần.
- Ba bộ lọc **Loại hình**, **Quận**, **Khu vực** áp dụng cho mọi chart có cột tương ứng (`loai_bds`, `quan`, `khu_vuc`); chart nào không có cột đó thì bộ lọc bỏ qua. Nhóm dưới 30 tin bị loại khỏi các chart giá.
- Năm dashboard đầu chỉ nói về dữ liệu. Model có dashboard riêng, đọc `gold.ml_price_predictions` của phiên bản mới nhất, chỉ tập test (20% tin mới nhất theo ngày đăng), nên con số khớp với MLflow.
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
