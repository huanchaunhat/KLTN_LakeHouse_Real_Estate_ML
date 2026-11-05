# Bản chấm AI01 ngày 03/11/2026

Người dùng yêu cầu AI đóng vai trò đánh giá hai phiếu annotation theo
`HUONG_DAN_CHAM_NHAN_GOI_Y_BAT_DONG_SAN.md`. Đây là **nhãn AI sơ bộ**:
được tạo bằng quy tắc đối chiếu điều kiện, nhận diện bằng chứng trong văn bản,
và rà soát định tính có chọn mẫu. Không phải 8.924 nhận định thủ công độc lập,
không phải nhãn được con người xác nhận, và chưa phải bộ ground truth cuối cùng.

## Dữ liệu và phạm vi

- Đọc hai CSV, `queries.csv`, hai JSONL, metadata snapshot và các cột dữ liệu thô
  của đúng catalog được manifest chỉ ra. Đối chiếu checksum manifest bundle.
- Dùng `as_of=2026-11-03T05:26:14.962227+00:00`, cửa sổ 90 ngày.
- Không đọc `pool_audit.json`, điểm mô hình, vectorizer hay trọng số để tạo nhãn.
- Không mở link tin hiện tại; không thay snapshot bằng dữ liệu website mới.
- Chỉ thay ba cột `grade`, `annotator_id`, `reason` của hai phiếu gốc.
- Không đổi query/request, JSONL, ID, cột dữ liệu tin hoặc thứ tự dòng.
- Không chạy evaluator, tìm trọng số hoặc đưa nhãn test vào chọn cấu hình.

## Cách áp dụng thang điểm

Kiểm tra điều kiện bắt buộc trước: thị trường, loại BĐS, đơn vị giá, tỉnh,
và bộ lọc có trong request. Với similar, kiểm tra cả nguồn/nhóm trùng.
Thiếu bằng chứng bắt buộc hoặc có mâu thuẫn đáng kể thì để grade trống.
Vi phạm bắt buộc đã xác định được nhận 0.

Tiếp theo xem giá, diện tích, công năng, nội dung và cấu trúc theo từng nhu cầu.
Ưu tiên mềm không biến thành bộ lọc cứng. Khác quận ở similar không tự nhận 0.
Không dùng tổng có trọng số hay lấy trung bình điểm thành phần.

- 3: gần quy mô tham chiếu/mong muốn, có bằng chứng trực tiếp cho các ưu tiên
  chính, và chưa có đánh đổi lớn được phát hiện.
- 2: đáng cân nhắc, còn ưu tiên chưa rõ hoặc đánh đổi đáng kể.
- 1: vẫn có liên quan nhưng nhiều đánh đổi; thông tin thiếu không được diễn giải
  thành khẳng định phủ định. Giá thấp hơn nhiều không tự chứng minh lựa chọn tốt hơn.
- 0: vi phạm bắt buộc đã xác định hoặc công năng/phân khúc khác rõ đến mức không
  có giá trị thay thế thực tế, ví dụ văn phòng thay cho kho hoặc studio nhỏ thay
  cho căn hộ gia đình lớn với ngân sách rất khác.

Mỗi dòng đã xử lý có mã `AI01` và reason chỉ ra số liệu/bằng chứng và hạn chế.
Các trích đoạn trong ngoặc vuông là nội dung snapshot hoặc tên cột dữ liệu,
không phải thông tin đã được xác minh ngoài đời.

## Chấm thử và quy ước bổ sung của AI

Đã dùng nhóm 12 truy vấn tune để kiểm tra cách hiểu tiêu chí và đọc lại các
cặp được chọn, rồi kiểm tra thêm các dòng dự kiến nhận 0/3 và các ngoại lệ.
Nhóm tune gồm:

`sale_similar_house_001`, `sale_similar_apartment_001`, `sale_similar_land_001`,
`sale_preferences_house_001`, `sale_preferences_apartment_003`,
`sale_preferences_commercial_001`, `rent_similar_house_001`,
`rent_similar_room_001`, `rent_similar_commercial_001`,
`rent_preferences_apartment_001`, `rent_preferences_room_001`,
`rent_preferences_commercial_003`.

Các mốc định lượng dưới đây là **quy ước hỗ trợ bản nháp AI**, không phải ngưỡng
do tài liệu gốc quy định hoặc đã được nhóm người chấm thống nhất. Với giá/diện
tích, dùng tỷ số lớn hơn giữa ứng viên/tham chiếu và tham chiếu/ứng viên:

| Tỷ số | Cách gọi trong quy trình AI |
|---|---|
| <= 1,25 | gần |
| > 1,25 đến 1,67 | lệch vừa |
| > 1,67 đến 2,5 | lệch lớn |
| > 2,5 | lệch rất lớn |

Các mức này chỉ hỗ trợ nhánh quyết định cùng bằng chứng công năng/nội dung,
không thay cho đánh giá tổng thể. Tín hiệu mâu thuẫn tự động cũng có dung sai
cho làm tròn và cách viết giá/diện tích; xem quy trình lưu kèm để tái lập.
Không thay các mốc theo chỉ số mô hình, vì chưa chạy chỉ số mô hình nào.
Nhóm người chấm cần hiệu chỉnh và chấm lại theo quy ước đã thống nhất trước
khi dùng bản này làm nhãn nghiên cứu chính thức.

Các điểm đã phân biệt khi rà soát: phòng trống không đồng nghĩa thiếu nội thất;
nội thất cao cấp không đồng nghĩa đầy đủ; giường tầng không chứng minh ở ghép;
có sổ không tự chứng minh sổ riêng; an ninh/camera không chứng minh có bảo vệ;
diện tích đất và tổng sàn có thể khác nhau; số tầng không dùng để suy số phòng.

## Ngoại lệ và bước bàn giao

`review_required.csv` liệt kê toàn bộ cặp còn trống điểm. Các tín hiệu gồm
mâu thuẫn giá, địa chỉ, ghép nhiều sản phẩm trong một tin, khác cách đo diện
tích, hoặc số phòng chưa đủ nhất quán để xác định điều kiện bắt buộc.
Một số là trường hợp đã đọc lại rõ ràng; các tín hiệu tự động còn cần được
người phụ trách xác nhận, không nên mặc nhiên coi mọi cảnh báo là lỗi dữ liệu.

Năm nguồn cần xác nhận ảnh hưởng toàn bộ truy vấn similar tương ứng:

| query_id | Vấn đề |
|---|---|
| sale_similar_house_015 | Bình Chánh/TP.HCM trong cột, Cần Giuộc/Tây Ninh trong mô tả |
| sale_similar_apartment_003 | Giá cột 17 tỷ, tiêu đề 19,5 tỷ |
| sale_similar_commercial_003 | Bình Chánh trong cột, Quận 8 trong mô tả |
| rent_similar_room_003 | Quận 4 trong cột, vị trí Quận 7 và nhiều phòng trong mô tả |
| rent_similar_commercial_005 | Ghép 1.500 m² với 130 triệu; mô tả ghi 1.500 m²/150 triệu và 730 m²/130 triệu |

Không điền 0 để vượt kiểm tra nhập nhãn. Theo hướng dẫn, cần giải quyết các
ngoại lệ trước khi nhập toàn bộ tập. Nếu sửa request hoặc dữ liệu tin, phải
tạo phiên bản dữ liệu mới có CSV/JSONL/pool nhất quán.

## Các tệp lưu kèm

- `originals/`: hai CSV nguyên gốc trước khi điền nhãn.
- `summary.json`: thống kê và kết quả đối chiếu toàn bộ ô ngoài ba cột nhãn.
- `review_required.csv`: các cặp cần rà soát.
- `query_summary.csv`: số điểm/cặp chờ theo từng truy vấn, không phải chỉ số mô hình.
- `decisions.jsonl`: dấu vết quyết định cho từng cặp.
- `annotation_procedure.py`: quy trình có thể kiểm tra lại; không tự chạy lại
  với `--commit` vì thư mục kết quả đã tồn tại và bản gốc đã được bảo toàn.

Kiểm tra tính toàn vẹn CSV, ID và thang điểm không chứng minh chất lượng
relevance của toàn bộ nhãn. Cần người chấm độc lập và xử lý bất đồng theo
hướng dẫn trước khi kết luận về chất lượng mô hình.
