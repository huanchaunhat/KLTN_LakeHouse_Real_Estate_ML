# Hoàn tất nhãn theo quy ước người dùng ngày 05/11/2026

Đây là phần tiếp nối bản chấm AI01 ngày 03/11/2026. Người dùng đã yêu cầu
điền các dòng còn trống và chốt cách xử lý những thông tin không nhất quán.
Bản chấm vẫn là nhãn AI sơ bộ; việc thống nhất quy ước dữ liệu không đồng nghĩa
toàn bộ điểm relevance đã được con người xác nhận.

## Quy ước được người dùng xác nhận

- Giá dùng để chấm lấy từ cột `price_vnd` và đơn vị tại `price_period`.
- Địa bàn lấy từ các cột `province`, `district`, `ward` và ID tương ứng trong
  snapshot. Quy ước áp dụng cả ứng viên và tin nguồn của truy vấn `similar`.
- Số phòng ngủ lấy từ cột `bedrooms`; mô tả khác số không làm thay đổi bộ lọc.
- Diện tích đối chiếu lấy từ cột `area_m2`. Với nhà/đất, người dùng quy ước đó
  là diện tích đất; mô tả có thể ghi diện tích sàn. Ví dụ đất 38 m² có tầng trệt
  và một lầu có thể được mô tả là 76 m² sàn. Khi so với ngưỡng diện tích đất,
  vẫn dùng 38 m². Với căn hộ/phòng trọ, dùng diện tích cột làm mốc đối chiếu.
- Khi một mô tả có nhiều sản phẩm, các thông số định lượng đã có trong cột
  vẫn là thông số dùng để chấm cặp; nội dung mô tả hỗ trợ đánh giá công năng.

Mã quy ước: `user_column_authority_20261105`. Các quyết định mới ghi mã này
trong `reason`, kèm bằng chứng phù hợp và hạn chế của từng cặp.

## Phạm vi của lần hoàn tất

Chỉ điền các dòng `grade` còn trống tại thời điểm chạy. Thực tế có 543 dòng:
365 tune và 178 test. Hai dòng trong danh sách 545 ngoại lệ trước đây đã được
điền thêm trước lần chạy này. Giữ nguyên toàn bộ các dòng đã có nhãn, kể cả mã
người chấm và lý do hiện hữu. Không thay dữ liệu tin, query/request, ID, thứ tự
dòng/cột hoặc thang điểm 0–3.

Tiếp tục dùng thang điểm và quy trình chấm của bản AI01 trước, với quy ước ưu
tiên cột nêu trên thay thế nhánh chờ rà soát mâu thuẫn. Không thay ngưỡng hỗ trợ
chấm sau khi xem kết quả mô hình. Không dùng điểm mô hình hay thứ tự hiển thị.
Không chạy evaluator, tìm trọng số hay cập nhật mô hình đang phục vụ.

Snapshot giữ nguyên `as_of=2026-11-03T05:26:14.962227+00:00` và cửa sổ 90 ngày.
Việc chấm đủ các cặp không sửa hoặc chứng minh các cột dữ liệu phản ánh đúng
thực tế bên ngoài snapshot; đây là quy ước đánh giá đã được người dùng chọn.

## Kết quả và dấu vết

- `summary.json`: thống kê mới và kiểm tra tính toàn vẹn.
- `before_completion/`: hai phiếu ngay trước lần bổ sung nhãn này.
- `completed_decisions.jsonl`: chỉ các quyết định mới, gồm lý do cũ, điểm mới,
  bằng chứng và các cột được dùng để quyết định.
- `query_summary.csv`: thống kê đầy đủ 240 truy vấn.
- `review_required.csv`: danh sách chờ hiện tại; chỉ có tiêu đề nếu đã hoàn tất.
- `completion_procedure.py`: quy trình tái lập, dùng quy trình lịch sử tại
  `../ai_review_20261103/annotation_procedure.py` làm đầu vào đọc.

Thư mục `../ai_review_20261103/` là hồ sơ lịch sử, gồm danh sách ngoại lệ tại
thời điểm chấm lần đầu; xem thư mục hiện tại để biết trạng thái hoàn tất.
Các kiểm tra cấu trúc và nhập nhãn xác nhận tính toàn vẹn và định dạng, không
phải kết quả đo chất lượng gợi ý.
