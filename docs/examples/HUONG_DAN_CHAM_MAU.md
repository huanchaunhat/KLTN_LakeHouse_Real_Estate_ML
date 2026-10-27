# Ví dụ chấm một truy vấn thuê nhà

Đây là phiếu thực hành với 5 tin thật từ snapshot đã lưu. Chưa có nhãn của con người: mọi điểm đang là `null`. Không dùng phiếu này để công bố chất lượng mô hình.

## 1. Nhu cầu cần đánh giá

Tìm thuê nhà ở TP Hồ Chí Minh, tối đa **15 triệu đồng/tháng**, diện tích ít nhất **50 m²**, ít nhất **2 phòng ngủ**. Ưu tiên gần trường học. Chỉ đánh giá theo thông tin snapshot được cung cấp; không suy ra tiện ích từ điểm mô hình.

Các điều kiện giá/diện tích/phòng/giao dịch là bắt buộc. Gần trường học là ưu tiên. Nếu thông tin chưa đủ để chấm, giữ `null` và ghi chú để xem lại; không tự xem thiếu thông tin là nhãn 0.

## 2. Thang chấm

| Điểm | Ý nghĩa |
|---:|---|
| 0 | Không phù hợp nhu cầu |
| 1 | Phù hợp yếu, thiếu phần lớn thông tin đáp ứng ưu tiên |
| 2 | Phù hợp, có bằng chứng đáp ứng đáng kể nhu cầu |
| 3 | Rất phù hợp, là lựa chọn đáng cân nhắc hàng đầu |

Các tin dưới đây đã được xáo thứ tự, không hiển thị điểm hoặc thứ hạng của mô hình. Sau khi đọc, điền điểm vào đúng ID trong [labels_rent_to_grade.jsonl](labels_rent_to_grade.jsonl).

## 3. Các tin cần chấm

### Tin 1: NHÀ CHO THUÊ NGUYÊN CĂN NGAY TÂN BÌNH GIÁ RẺ 

- ID: `524b5745365f7347593976c7ed25ae92c886cab1fedd8c7970e1da446aa6a5cc`
- Giá: 10,000,000 VND/tháng
- Diện tích: 60 m²
- Phòng ngủ: 3; phòng tắm: 2
- Địa điểm: Phường 11, Quận Tân Bình, Tp Hồ Chí Minh
- Quan sát gần nhất: 2026-10-22T07:44:26.181572+00:00

Mô tả nguồn tại snapshot:

> NHÀ CHO THUÊ NGUYÊN CĂN NGAY TÂN BÌNH GIÁ RẺ 
> KC 2 TẦNG 
> Gồm 1 PK 3 PN 2 VS 
> nhà gần chợ Trần Văn Quang 
> gần trường học c2 c3 
> nhà trống giao  ngay

**Điểm của bạn: ___ / 3. Lý do: ___**

### Tin 2: Cho Thuê Nhà Nguyên Căn Gia Đình Ở Hóc Môn

- ID: `9e6f9c8c403af22cf5c0a794ebd81b73483cd8c3f214e4ce2d6ae9dbf913ae03`
- Giá: 6,000,000 VND/tháng
- Diện tích: 80 m²
- Phòng ngủ: 2; phòng tắm: 1
- Địa điểm: Xã Xuân Thới Đông, Huyện Hóc Môn, Tp Hồ Chí Minh
- Quan sát gần nhất: 2026-10-22T08:08:33.748626+00:00

Mô tả nguồn tại snapshot:

> Dự án: 
> Thông tin chi tiết: Cho Thuê Nhà Nguyên Căn Gia Đình Ở
> Gần trường cấp 1,2,3 và Đại Học Huflit
> Gần chợ , công viên
> Hẻm thông , xe hơi vào được

**Điểm của bạn: ___ / 3. Lý do: ___**

### Tin 3: Nhà cho thuê nguyên căn

- ID: `3d10ef4724e503f91e932399374830f5e371d838bb51b16e09f6239121be5a5d`
- Giá: 9,500,000 VND/tháng
- Diện tích: 60 m²
- Phòng ngủ: 3; phòng tắm: 2
- Địa điểm: Phường 18, Quận 4, Tp Hồ Chí Minh
- Quan sát gần nhất: 2026-10-22T08:07:16.037421+00:00

Mô tả nguồn tại snapshot:

> Nhà gần chợ, gần trường học. Khu dân cư yên tĩnh, an ninh, phù hợp cho gia đình

**Điểm của bạn: ___ / 3. Lý do: ___**

### Tin 4: cho thuê nhà quận 11

- ID: `44148f5ba9d62bd13e08462210545c00ec98a3e85b1be64eea818a288600e8f4`
- Giá: 14,000,000 VND/tháng
- Diện tích: 63.7999 m²
- Phòng ngủ: 2; phòng tắm: 2
- Địa điểm: Phường 9, Quận 11, Tp Hồ Chí Minh
- Quan sát gần nhất: 2026-10-22T08:12:07.613139+00:00

Mô tả nguồn tại snapshot:

> Hẻm rộng thoáng mát gần trung tâm trường học gần chợ

**Điểm của bạn: ___ / 3. Lý do: ___**

### Tin 5: Cho thuê nhà Hóc môn

- ID: `d28cbdf1015b9dd0ca2fd705fd08c6bddc1a475aa078d97c170a7b0745577a4a`
- Giá: 5,000,000 VND/tháng
- Diện tích: 80 m²
- Phòng ngủ: 3; phòng tắm: 1
- Địa điểm: Xã Tân Xuân, Huyện Hóc Môn, Tp Hồ Chí Minh
- Quan sát gần nhất: 2026-10-22T07:52:21.331149+00:00

Mô tả nguồn tại snapshot:

> Hàng hiếm nhanh còn kịp : Cho thuê nhà Tân Xuân 5, Hóc Môn, diện tích 4m x 22m, gồm 3 phòng ngủ, 1 phòng bếp, 1 nhà vệ sinh, 2 sân ( trước , sau ), hẻm xe hơi, có chỗ quay đầu, tiện ích : gần trường Đại học ngoại ngữ tin học (cs 2), trường tiểu học Mỹ Hòa, trường mẫu giáo , chợ,...
> Giá cho thuê. 5 tr. 
> Liên hệ. ***.

**Điểm của bạn: ___ / 3. Lý do: ___**

## 4. Điền file JSONL

Mở [labels_rent_to_grade.jsonl](labels_rent_to_grade.jsonl). Trong `relevance`, thay từng `null` bằng số nguyên 0, 1, 2 hoặc 3. Không sửa ID, request, dataset_version hoặc as_of của phiếu này. Không đặt dấu nháy quanh điểm.

Ví dụ cú pháp sau chỉ dùng ID giả để giải thích cách điền, không phải nhãn gợi ý cho 5 tin thật:

```json
"relevance": {"ID_A": 3, "ID_B": 0, "ID_C": 2}
```

Giữ nguyên một đối tượng JSON trên một dòng của `.jsonl`. Nếu chưa chấm đủ cả 5 tin, evaluator sẽ từ chối các giá trị `null`. Không dùng điểm giả để lách kiểm tra.

## 5. Đưa file vào đánh giá

Chạy từ thư mục gốc repository sau khi chấm đủ:

```bash
python3 -m recommendation.evaluate \
  --bundle artifacts/recommendation/models/20261023_v1 \
  --labels docs/examples/labels_rent_to_grade.jsonl \
  --k 3 \
  --output docs/examples/evaluation_rent_example.json
```

`--k 3` đánh giá ba kết quả đứng đầu; pool đang có năm tin để có thêm ứng viên đã chấm. Dùng `--k 10` sẽ không đủ nhãn nếu model trả thêm tin ngoài năm ID này.

Kết quả nằm trong nhóm `rent/tune`, gồm `precision@3`, `pooled_recall@3`, `ndcg@3` và số truy vấn được tính. Điểm > 0 được evaluator hiện tại xem là positive. Nếu không có positive thì recall/nDCG là null; đó không phải lỗi.

## 6. File chấm có làm mô hình học lại không?

**Không.** `evaluate.py` đọc bundle, chạy lại truy vấn tại cùng `as_of`, rồi so kết quả với nhãn. Nó chỉ ghi báo cáo, không đổi vector TF-IDF, trọng số hoặc model. `train.py` hiện nhận Parquet và không nhận file nhãn.

Để tinh chỉnh: dùng nhãn tune so các cấu hình riêng trên cùng catalog, chọn cấu hình rồi đánh giá tập test độc lập. Pipeline hiện chưa tự tìm trọng số tối ưu. Để học trọng số từ nhãn cần bổ sung bước tối ưu/ranking có giám sát.

Phiếu này lấy năm ứng viên từ model hiện tại, chỉ nhằm thực hành định dạng và lệnh chạy. Để so sánh công bằng nhiều mô hình, phải hợp nhất ứng viên từ các baseline và chấm thêm mọi kết quả Top-K mới. Không kết luận model tốt chỉ từ một phiếu này.

Dataset: `gold.ml_recommendation_features.delta_v0`. Thời điểm cố định: `2026-10-23T04:55:03+00:00`.
