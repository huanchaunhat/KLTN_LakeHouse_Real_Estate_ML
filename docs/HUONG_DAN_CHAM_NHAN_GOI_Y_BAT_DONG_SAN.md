# Hướng dẫn chi tiết chấm nhãn gợi ý bất động sản

Áp dụng cho [bộ 240 truy vấn ngày 03/11/2026](evaluation/query_set_20261103/queries.md).
Mục tiêu là chấm mức độ phù hợp của **từng cặp nhu cầu–tin BĐS** để đánh giá và chọn trọng số.
Cùng một tin có thể nhận điểm khác nhau ở các nhu cầu khác nhau.

## 1. Chuẩn bị trước khi chấm

Mở các tệp sau:

| Tệp | Mục đích |
|---|---|
| [queries.csv](evaluation/query_set_20261103/queries.csv) | Tra nhu cầu, phương thức, request và tin nguồn bằng `query_id` |
| [annotation_tune.csv](evaluation/query_set_20261103/annotation_tune.csv) | Điền nhãn để chọn trọng số |
| [annotation_test.csv](evaluation/query_set_20261103/annotation_test.csv) | Điền nhãn kiểm tra cấu hình đã chốt |
| [Hướng dẫn bộ dữ liệu](BO_TRUY_VAN_DANH_GIA_GOI_Y.md) | Xem snapshot, cách nhập nhãn và chạy đánh giá |

Khi mở bằng Excel, nhập CSV dưới dạng UTF-8 và giữ các cột ID ở dạng văn bản.
Nếu cột không tách đúng, chọn dấu phẩy làm dấu phân cách. Bật xuống dòng trong ô để đọc đủ mô tả.
Khi lưu, giữ định dạng CSV UTF-8; kiểm tra không mất dòng hoặc đổi tên cột.

Điều kiện để bắt đầu một truy vấn:

1. Hiểu rõ người dùng muốn mua hay thuê, loại BĐS, khu vực và đơn vị giá.
2. Phân biệt được điều kiện bắt buộc và ưu tiên mềm.
3. Với `similar`, đọc cả tin nguồn, không chỉ đọc tiêu đề ứng viên.
4. Nhu cầu tổng hợp phải có ý nghĩa và các thông tin không mâu thuẫn rõ ràng.
5. Dùng dữ liệu snapshot được cung cấp; không nhìn điểm mô hình hoặc nguồn baseline để quyết định điểm.

Nếu nhu cầu vô lý, thiếu ngữ cảnh cần thiết hoặc dữ liệu mâu thuẫn nghiêm trọng, đánh dấu để rà soát
theo mục 8. Không sửa nhu cầu, request hoặc thông tin ứng viên ngay trong bảng chấm.

## 2. Điền những cột nào?

Chỉ sửa ba cột `grade`, `annotator_id`, `reason`. Giữ nguyên các cột còn lại.

| Cột | Cách điền | Ví dụ |
|---|---|---|
| `grade` | Một số nguyên 0, 1, 2 hoặc 3; không ghi `2/3`, `2.5`, `tốt` | `2` |
| `annotator_id` | Mã người chấm, dùng thống nhất | `A01` |
| `reason` | Bằng chứng đáp ứng nhu cầu và điểm hạn chế dẫn đến điểm số | `Đúng quận, trong ngân sách; chưa thấy thông tin nội thất.` |

`annotator_id` do nhóm tự đặt, không cần tên thật. Một người chấm dùng `A01` cho các dòng của mình;
người thứ hai dùng `A02`. Có thể lập bảng đối chiếu người–mã riêng, không cần đưa tên vào dữ liệu nhãn.
Không đổi mã theo từng truy vấn.

Theo quy trình này, **mỗi nhãn đã chốt cần có reason** để có thể xem lại. Công cụ nhập hiện chỉ bắt buộc
điểm hợp lệ và mã người chấm; chưa kiểm tra reason có nội dung hay không.

`position` là thứ tự hiển thị đã xáo, không phải thứ hạng chất lượng. Dòng đầu không mặc nhiên đáng 3 điểm.

## 3. Xác định điều kiện bắt buộc

### 3.1. Gợi ý theo nhu cầu — `preferences`

Đối chiếu `description_vi` với `request_json` trong `queries.csv`.

| Trường trong request | Ý nghĩa khi chấm |
|---|---|
| `transaction_type` | Bắt buộc đúng mua bán (`sale`) hoặc cho thuê (`rent`) |
| `property_category` | Bắt buộc đúng loại BĐS đã chuẩn hóa |
| `price_period` | Bắt buộc đúng đơn vị giá: `total` là giá toàn bộ, `month` là đồng/tháng |
| `province_id` | Bắt buộc đúng tỉnh/thành nếu có |
| `district_id` | Bắt buộc đúng quận/huyện nếu có |
| `max_price` | Giá không vượt mức trần; bằng mức trần vẫn đạt |
| `min_area` | Diện tích không thấp hơn mức tối thiểu; bằng ngưỡng vẫn đạt |
| `min_bedrooms` | Số phòng ngủ không thấp hơn yêu cầu |
| `target_price` | Giá mong muốn để so độ phù hợp; không phải giá trần |
| `target_area` | Diện tích mong muốn; không phải diện tích tối thiểu |
| `query` | Ưu tiên nội dung trong bộ hiện tại, chẳng hạn có ban công hoặc hẻm xe hơi |

Ví dụ: giá mong muốn 10 triệu/tháng, trần 12 triệu/tháng. Tin 11 triệu **không vi phạm** ngân sách;
tin 12 triệu vẫn đạt trần; tin 12,5 triệu vi phạm. Tin 8 triệu không tự động tốt hơn tin 10 triệu:
cần xét thêm diện tích, nội dung và các nhu cầu khác.

Nếu request chỉ có tỉnh, không tự bổ sung yêu cầu phải đúng quận của tin dùng để tạo kịch bản.
Nếu mô tả và request khác nhau về điều kiện bắt buộc, để chờ rà soát thay vì tự chọn một cách hiểu.

### 3.2. Gợi ý tin tương tự — `similar`

Tin nguồn nằm trong `queries.csv`: `source_listing_key`, `source_title`, `source_description`,
`source_url`; mô tả nhu cầu ghi giá, diện tích và địa bàn tham chiếu.

Điều kiện bắt buộc theo engine hiện tại:

- Cùng mua bán/cho thuê, loại BĐS chuẩn hóa, đơn vị giá và tỉnh/thành với tin nguồn.
- Không phải chính tin nguồn hoặc cùng nhóm trùng của tin nguồn.
- Tin đáp ứng điều kiện đủ chuẩn và thời gian của snapshot.

**Không tự coi cùng quận, đúng giá, đúng diện tích hoặc đúng số phòng của tin nguồn là bắt buộc.**
Đây là các yếu tố so độ tương tự. Khác quận có thể giảm mức phù hợp, không tự động nhận 0.
Nếu hai tin khác mã nhưng có vẻ là cùng BĐS, ghi `NGHI_TRUNG` để kiểm tra; không tự sửa ID hoặc xóa dòng.

### 3.3. Thời điểm và trạng thái tin

Dùng `as_of` đã cố định của truy vấn và `max_age_days=90`, không dùng ngày đang ngồi chấm.
`last_seen_at` là thời điểm hệ thống quan sát gần nhất, **không phải ngày đăng** và không chứng minh tin còn trống.
Các kiểm tra ID, trạng thái đủ chuẩn và nhóm trùng đã được thực hiện khi tạo bộ dữ liệu;
người chấm không cần đoán thông tin kỹ thuật không có trong CSV.

Không hạ điểm chỉ vì link hiện tại đã mất hoặc hiện hiển thị thông tin khác snapshot.
Link chỉ là tham chiếu; nếu cần bổ sung bằng chứng, phải lưu và xác định lại phiên bản dữ liệu trước khi chấm tiếp.

## 4. Thang điểm chung 0–3

Thực hiện theo thứ tự: kiểm tra điều kiện bắt buộc → xem bằng chứng cho ưu tiên → chọn điểm tổng thể.
Không lấy trung bình các điểm con hoặc áp dụng chính các trọng số đang cần tối ưu để tạo nhãn.

| Grade | Điều kiện quyết định | Cách phân biệt |
|---:|---|---|
| **0** | Có bằng chứng vi phạm ít nhất một điều kiện bắt buộc; hoặc dù qua bộ lọc, tin không có giá trị thực tế cho nhu cầu | Không có lý do đáng kể để đề xuất tin này cho nhu cầu đã nêu |
| **1** | Đạt điều kiện bắt buộc và có một phần liên quan, nhưng đáp ứng ưu tiên yếu, có nhiều đánh đổi lớn | Có thể xem nếu thiếu lựa chọn; không phải đề xuất tốt |
| **2** | Đạt điều kiện bắt buộc, có bằng chứng đáp ứng đáng kể nhu cầu; vẫn có một hạn chế đáng kể hoặc một phần ưu tiên chưa rõ | Là lựa chọn đáng cân nhắc |
| **3** | Đạt điều kiện bắt buộc, đáp ứng rất tốt các ưu tiên chính hoặc rất tương tự tin nguồn; không có đánh đổi lớn đã biết | Có đủ bằng chứng để đề xuất ở nhóm đầu |

Không ép mỗi truy vấn phải có đủ bốn mức điểm, một số lượng nhãn 3 cố định, hoặc ít nhất một tin positive.
Có thể nhiều tin cùng điểm. Điểm 3 không có nghĩa hoàn hảo về mọi thuộc tính không liên quan đến nhu cầu.
Điểm 0 không dùng để thay cho “tôi chưa đọc”, “tôi chưa biết” hoặc “thông tin chưa đủ”.

### 4.1. Đánh giá các yếu tố cụ thể

| Yếu tố | Cách xem xét | Tránh |
|---|---|---|
| Giá | Đúng đơn vị; trong trần nếu có; mức phù hợp với giá mong muốn hoặc tin nguồn | Cho điểm cao chỉ vì rẻ nhất |
| Diện tích | Đạt tối thiểu; phù hợp quy mô mong muốn | Cho rằng diện tích càng lớn càng tốt |
| Vị trí | Đúng khu vực bắt buộc; với similar, xem mức tương đồng địa bàn có bằng chứng | Tự đo khoảng cách hoặc thời gian đi lại khi không có dữ liệu |
| Nội dung | Có thông tin trực tiếp về nội thất, ban công, hẻm xe hơi, loại công năng… theo nhu cầu | Coi trùng từ khóa hoặc tiêu đề dài là phù hợp |
| Cấu trúc | Số phòng, công năng, loại hình cụ thể phù hợp nhu cầu hoặc tin nguồn | Suy số phòng từ số tầng hoặc diện tích |
| Độ mới quan sát | Cân nhắc độ mới tại cùng `as_of` khi các tiêu chí chính tương đương | Để độ mới bù cho vi phạm bắt buộc |

Không có ngưỡng phần trăm chênh giá/diện tích bắt buộc trong hướng dẫn này.
Nếu nhóm cần quy ước định lượng, phải thống nhất trong vòng chấm thử và cố định trước chấm chính thức;
không thay ngưỡng sau khi thấy kết quả mô hình.

### 4.2. Tiêu chí theo loại BĐS

- **Nhà ở:** công năng để ở/kinh doanh, số phòng, nội thất, hẻm hoặc mặt tiền khi nhu cầu có nhắc.
- **Căn hộ:** số phòng, diện tích, nội thất, ban công, tiện ích được mô tả rõ.
- **Phòng trọ:** có gác, cửa sổ, máy lạnh, chung chủ, chỗ để xe theo ưu tiên.
- **Đất:** mục đích sử dụng được tin mô tả, diện tích, đường tiếp cận, thông tin giấy tờ theo ưu tiên.
- **Mặt bằng thương mại:** công năng kinh doanh/văn phòng/kho và điều kiện tiếp cận phù hợp.
  Cùng mã `COMMERCIAL` không đồng nghĩa văn phòng và kho thay thế được cho nhau.

Không thêm yêu cầu ngầm từ sở thích cá nhân. Thông tin như “sổ riêng” được chấm theo bằng chứng trong tin,
không được diễn giải thành kết luận rằng giấy tờ đã được xác minh.

## 5. Xử lý thông tin thiếu hoặc mâu thuẫn

Phân biệt ba tình huống:

| Tình huống | Cách xử lý |
|---|---|
| Có thông tin rõ ràng là không đáp ứng | Chấm theo thực tế; nếu là bắt buộc thì 0, nếu là ưu tiên thì cân nhắc mức giảm |
| Thiếu một ưu tiên nhưng phần còn lại đủ để đánh giá | Chấm mức có căn cứ; ghi rõ phần chưa biết, không kết luận là không có |
| Không đủ thông tin để xác định điều kiện bắt buộc hoặc mức phù hợp tổng thể | Giữ `grade` trống, ghi `CHO_RA_SOAT: …`, chưa đưa vào nhập nhãn cuối |

Ví dụ: nhu cầu có ưu tiên nội thất, tin không nhắc nội thất. Không được ghi “không có nội thất”.
Có thể chấm 2 nếu các yếu tố khác đáp ứng tốt và giải thích phần thiếu; không mặc định mọi tin thiếu nội thất đều là 2.
Nếu nội thất là ưu tiên cốt lõi duy nhất và không còn đủ căn cứ phân biệt phù hợp, đưa ra rà soát.

Nếu trường giá ghi 10 triệu/tháng nhưng mô tả rõ 20 triệu/tháng, hoặc diện tích mâu thuẫn lớn,
ghi `MAU_THUAN_DU_LIEU: …` và để trống điểm. Không tự chọn con số thuận tiện để chấm.
Tương tự, không tự ghép quận/phường khác tên thành cùng địa bàn khi chưa có ánh xạ xác nhận.

## 6. Viết reason có thể kiểm tra lại

Cấu trúc đề xuất:

> Đáp ứng: [điều kiện/ưu tiên có bằng chứng]. Hạn chế: [điểm lệch hoặc chưa rõ]. Kết luận: [vì sao chọn mức điểm].

Một đến ba câu là đủ. Nên ghi con số khi nó giải thích quyết định; không cần lặp toàn bộ tin.

| Mức | Reason minh họa |
|---|---|
| 3 | Đúng khu vực, 10 triệu/tháng gần mức mong muốn 10 triệu, 55 m² đạt tối thiểu 40 m². Mô tả ghi có ban công và nội thất; đáp ứng tốt ưu tiên chính. |
| 2 | Đúng khu vực, giá trong trần, diện tích phù hợp. Có ban công nhưng chưa có thông tin nội thất; vẫn đáng cân nhắc. |
| 1 | Đạt khu vực, ngân sách và diện tích tối thiểu. Mô tả ghi không có ban công và bàn giao trống, lệch cả hai ưu tiên chính; chỉ phù hợp yếu. |
| 0 | Giá 12,5 triệu/tháng vượt trần bắt buộc 12 triệu/tháng. |
| Chờ rà soát | CHO_RA_SOAT: yêu cầu ít nhất 2 phòng ngủ nhưng trường phòng ngủ trống và mô tả không nêu; chưa xác định điều kiện bắt buộc. |

Tránh ghi riêng “tốt”, “ổn”, “không thích”, “giống”, “model gợi ý cao” hoặc sao chép cùng một lý do cho mọi dòng.
`reason` là văn bản giải thích, hiện chưa được evaluator dùng để tính điểm hoặc tự học trọng số.

## 7. Ví dụ thực hành

**Các ví dụ dưới đây là giả định để giải thích thang điểm, không phải nhãn cho các tin thật trong bộ dữ liệu.**

### 7.1. Ví dụ preferences

Nhu cầu: thuê căn hộ tại Quận 7; tối đa 12 triệu đồng/tháng; ít nhất 40 m²;
giá mong muốn 10 triệu, diện tích mong muốn 50 m²; ưu tiên ban công và đầy đủ nội thất.

| Tin giả định | Điểm minh họa | Giải thích |
|---|---:|---|
| Quận 7, 10 triệu/tháng, 52 m², có ban công và đầy đủ nội thất | 3 | Đạt bắt buộc, gần các mục tiêu và có bằng chứng đáp ứng hai ưu tiên |
| Quận 7, 11 triệu/tháng, 50 m², có ban công, chưa rõ nội thất | 2 | Đáng cân nhắc nhưng thiếu bằng chứng cho một ưu tiên |
| Quận 7, 12 triệu/tháng, 40 m², không ban công, bàn giao trống | 1 | Đạt bắt buộc nhưng lệch nhiều ưu tiên |
| Quận 7, 13 triệu/tháng, 55 m², đầy đủ tiện ích | 0 | Vượt giá trần; tiện ích không bù cho vi phạm này |
| Quận 4, 9 triệu/tháng, 50 m², có ban công và nội thất | 0 | Sai quận bắt buộc |
| Quận 7, 10 triệu/tháng, diện tích mâu thuẫn 30/50 m² giữa các trường | Chờ rà soát | Chưa biết có đạt tối thiểu 40 m² hay không |

### 7.2. Ví dụ similar

Tin nguồn giả định: nhà bán tại TP.HCM, Quận Tân Phú, giá 5 tỷ, 60 m², 3 phòng ngủ,
hẻm xe hơi; không có yêu cầu cùng quận hoặc trần giá bổ sung.

| Tin giả định | Điểm minh họa | Giải thích |
|---|---:|---|
| Nhà bán Tân Phú, 5,1 tỷ, 62 m², 3 phòng ngủ, hẻm xe hơi | 3 | Gần tin nguồn trên các yếu tố chính |
| Nhà bán quận khác trong TP.HCM, 5 tỷ, 60 m², 3 phòng ngủ, hẻm xe hơi | 2 | Tương tự nhiều thuộc tính; khác địa bàn là điểm đánh đổi, không phải vi phạm bắt buộc |
| Nhà bán TP.HCM, 15 tỷ, 180 m², 7 phòng ngủ, công năng khác đáng kể | 0 hoặc 1 | 0 nếu không còn giá trị đề xuất; 1 nếu vẫn có một phần liên quan thực chất. Ghi rõ lý do và thống nhất trường hợp biên trong chấm thử |
| Căn hộ bán TP.HCM, 5 tỷ, 60 m², 3 phòng ngủ | 0 | Khác loại BĐS bắt buộc |
| Nhà cho thuê có hình thức tương tự | 0 | Khác thị trường mua bán/cho thuê |

Các mức trên minh họa cách lập luận. Không biến một ví dụ thành công thức cố định cho mọi nhu cầu.

## 8. Quy trình chấm từng truy vấn và xử lý ngoại lệ

1. Tra `query_id`, đọc nhu cầu và request; với similar đọc tin nguồn.
2. Ghi nhận điều kiện bắt buộc và ưu tiên, giải quyết mâu thuẫn của nhu cầu trước.
3. Đọc từng ứng viên độc lập theo cùng một tiêu chí. Có thể quay lại so các tin để giữ thang điểm nhất quán.
4. Nếu đủ căn cứ, điền `grade`, `annotator_id`, `reason`.
5. Nếu chưa đủ căn cứ, giữ điểm trống, điền mã người chấm và reason có tiền tố
   `CHO_RA_SOAT`, `MAU_THUAN_DU_LIEU` hoặc `NGHI_TRUNG` phù hợp.
6. Đọc lại cả truy vấn: các tin cùng điểm có mức phù hợp tương đương không, reason có khớp điểm không?
7. Chuyển sang truy vấn tiếp theo; nghỉ theo đợt để tránh chấm vội vì mệt.

Khi có ngoại lệ, người phụ trách kiểm tra snapshot/request và quyết định bằng bằng chứng.
Nếu chỉ cần đọc lại thông tin sẵn có, có thể chốt nhãn sau rà soát và ghi rõ lý do.
Nếu phải sửa dữ liệu, thay nhu cầu hoặc loại một cặp/truy vấn, cần phiên bản bộ dữ liệu mới
với CSV và JSONL khớp nhau; không chỉ xóa dòng CSV hoặc điền 0 để vượt kiểm tra.

Công cụ nhập yêu cầu chấm đủ các cặp của tập được đưa vào. Các dòng đang chờ rà soát sẽ làm nhập nhãn thất bại;
đây là dấu hiệu cần hoàn tất xử lý, không phải lý do tạo nhãn giả.
Trường hợp phát hiện ứng viên thật sự vi phạm bắt buộc: ghi điểm 0 khi bằng chứng rõ, đồng thời báo người phụ trách
để kiểm tra chất lượng bộ lọc/dữ liệu.

## 9. Nhiều người chấm và thống nhất tiêu chí

Trước khi chấm chính thức, cùng chọn khoảng 8–12 truy vấn tune phủ hai thị trường và hai phương thức.
Chấm độc lập, sau đó thảo luận các trường hợp bất đồng, đặc biệt ranh giới 0/1 và 2/3.
Chốt cách hiểu tiêu chí, lưu ngày và phiên bản hướng dẫn; chấm lại phần thử theo quy ước đã chốt.

Trong chấm chính thức, có thể giao hai người chấm độc lập 20–30% số cặp.
Giữ bản riêng cho từng người; không nhân đôi các dòng trong CSV nhập cuối.
Sau đó xem lại bằng chứng để thống nhất **một số nguyên** cho mỗi cặp; không lấy trung bình thành 1,5 hoặc 2,5.

Lưu nhãn gốc và lịch sử phân xử ở tệp riêng. Nếu nhãn cuối do hai người thống nhất,
có thể dùng mã `ADJ01` cho quyết định phân xử, giải thích mã này trong bảng quản lý người chấm.
Không ghi `A01` cho một quyết định đã bị người khác thay đổi mà không có dấu vết.

Đánh giá mức nhất quán trước phân xử: tỷ lệ trùng điểm, tỷ lệ lệch từ hai mức trở lên,
và có thể dùng weighted Cohen’s kappa nếu có công cụ. Nếu khác biệt nhiều, sửa cách hiểu tiêu chí
trước khi tiếp tục; không chọn nhãn giúp mô hình nào đạt điểm cao hơn.

## 10. Giữ tập test độc lập và tránh thiên lệch

- Dùng tập tune để xem kết quả và lựa chọn trọng số. Không dùng kết quả test để thử đi thử lại cấu hình.
- Có thể chấm test trước nếu người chấm không biết kết quả mô hình; giữ nhãn/kết quả test riêng khỏi người đang chọn trọng số.
- Không xem `pool_audit.json`, điểm mô hình hoặc cấu hình sinh ứng viên trước khi chấm.
- Không sao chép điểm của một tin từ truy vấn khác; nhu cầu khác có thể làm thay đổi điểm.
- Không tăng điểm chỉ vì đúng ngân sách, cùng quận hoặc tin quan sát gần đây; cần xét phù hợp tổng thể.
- Chấm hết các ứng viên trong phiếu, không chỉ 10 dòng đầu. Pool 40 tin không đồng nghĩa đánh giá ở K=40.
- Truy vấn ít hơn 10 ứng viên vẫn chấm theo cùng tiêu chí; không nâng điểm để bù ít tin.
  Khi báo cáo, tách nhóm ít ứng viên vì Precision@10 vẫn chia 10 khi trả thiếu kết quả.

Evaluator hiện coi điểm **1, 2 và 3 đều là positive** khi tính Precision/pooled Recall.
nDCG phân biệt các mức: gain tương ứng 0, 1, 3 và 7 cho nhãn 0, 1, 2 và 3.
Hãy chấm theo mức phù hợp thực tế, không điều chỉnh nhãn để thay đổi chỉ số.

## 11. Kiểm tra trước khi bàn giao

- Mọi cặp đã có đúng một grade nguyên 0–3; không còn ngoại lệ chưa giải quyết.
- Mỗi dòng có mã người chấm và reason có căn cứ; không có reason mâu thuẫn với điểm.
- Không sửa ID, nhu cầu, dữ liệu ứng viên hoặc số dòng của bảng nguồn.
- Không có nhãn giả, điểm trung bình thập phân, hoặc nhãn được đoán từ thứ tự hiển thị.
- Bản chấm của từng người và quyết định phân xử được lưu riêng khi có nhiều người tham gia.
- Tune và test vẫn tách riêng, cùng phiên bản dữ liệu và cùng thời điểm `as_of` với bộ truy vấn.

Sau đó dùng công cụ nhập nhãn theo [hướng dẫn bộ dữ liệu](BO_TRUY_VAN_DANH_GIA_GOI_Y.md#4-nhập-nhãn-và-đánh-giá).
Nhập nhãn và chạy evaluator chỉ tạo dữ liệu/báo cáo đánh giá; không tự đổi trọng số mô hình.
