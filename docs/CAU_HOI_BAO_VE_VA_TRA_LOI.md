# Câu hỏi hội đồng có thể hỏi và gợi ý trả lời

Tài liệu chuẩn bị cho buổi bảo vệ đồ án *Lakehouse dữ liệu bất động sản TP.HCM và mô hình dự đoán giá*. Mỗi câu gồm **ý chính cần nói** (ngắn, để nhớ) và **giải thích thêm** (khi hội đồng hỏi sâu). Số liệu lấy từ lần huấn luyện gần nhất (champion v16, dữ liệu Delta v30, 48.628 tin bán).

Mục lục:

1. Tổng quan và kiến trúc
2. Thu thập và xử lý dữ liệu (Bronze, Silver)
3. Star schema: dimension và fact (Gold)
4. Bảng đặc trưng và chống rò rỉ dữ liệu
5. Mô hình dự đoán giá
6. Đánh giá mô hình
7. Vận hành: Airflow, MLflow, API
8. Mô hình gợi ý
9. Hạn chế và hướng phát triển

---

## 1. Tổng quan và kiến trúc

**Câu 1. Đồ án giải quyết bài toán gì? Tại sao cần Lakehouse mà không chỉ crawl rồi train?**

Ý chính: Đồ án thu thập tin rao BĐS TP.HCM mỗi ngày, lưu lịch sử, và phục vụ ba bài toán: dự đoán giá bán, xu hướng giá theo khu vực, nguồn cung và thanh khoản. Chỉ crawl rồi train thì chỉ có một ảnh chụp tại một thời điểm, không có lịch sử giá, không biết tin nào bị gỡ, không tái lập được kết quả.

Giải thích thêm: Lakehouse cho phép lưu dữ liệu thô bất biến (replay được), có versioning nhờ Delta Lake, có catalog (Hive Metastore) để nhiều công cụ cùng đọc (dbt, Superset, Spark, Python). Mô hình ML chỉ là một "người dùng" của tầng Gold.

**Câu 2. Kiến trúc tổng thể gồm những gì?**

Ý chính: Medallion ba tầng trên MinIO (S3) với định dạng Delta Lake.

- Nguồn: API công khai của Nhà Tốt.
- Landing: JSON thô theo ngày crawl và run_id.
- Bronze: `raw_properties`, append-only, giữ nguyên payload.
- Silver: dbt ép kiểu, làm sạch, tách quarantine, tính lịch sử thay đổi của từng tin.
- Gold: star schema Kimball (5 dim, 3 fact), 3 mart tổng hợp, 2 bảng đặc trưng ML.
- Điều phối: Airflow DAG chạy hằng ngày. Tính toán: Spark 3.3 + Spark Thrift Server. BI: Superset. ML: MLflow registry, FastAPI phục vụ dự đoán.

**Câu 3. Tại sao chọn Delta Lake? Tại sao không dùng database quan hệ?**

Ý chính: Delta cho ACID trên object storage, time travel (mỗi lần train ghi lại Delta version để tái lập), MERGE cho incremental, và lưu được hàng chục nghìn tin × hàng trăm lần crawl với chi phí thấp. Database quan hệ không phù hợp để giữ JSON thô và lịch sử snapshot lớn, và không tách được tính toán khỏi lưu trữ.

**Câu 4. Tại sao dùng dbt thay vì viết Spark job?**

Ý chính: dbt quản lý phụ thuộc giữa các bảng, có test dữ liệu tích hợp (unique, not_null, relationships, test tự viết), có tài liệu lineage. Logic nghiệp vụ nằm trong SQL dễ đọc, dễ kiểm tra hơn code PySpark. Silver chỉ được lên Gold khi toàn bộ test Silver đạt (quality gate).

---

## 2. Thu thập và xử lý dữ liệu

**Câu 5. Dữ liệu lấy từ đâu, lấy thế nào, bao nhiêu?**

Ý chính: Crawl API Nhà Tốt theo 22 quận/huyện cũ của TP.HCM, cả bán và thuê. Bước 1 lấy danh sách ID, bước 2 lấy chi tiết từng tin. Mỗi ngày crawl lại cả tin cũ để theo dõi thay đổi giá. Hiện có khoảng 48.600 BĐS bán đủ điều kiện huấn luyện sau khi làm sạch.

Giải thích thêm: Retry với backoff khi gặp 429/5xx. Mọi phản hồi, kể cả lỗi, đều được ghi kèm `http_status`, nên tầng sau phân biệt được tin bị gỡ (404/410) với lỗi mạng. Crawl là idempotent nhờ cờ `_SUCCESS`, chạy lại không tạo bản sao.

**Câu 6. Tầng Silver làm sạch những gì?**

Ý chính: Ba việc.

1. Ép kiểu và trích thuộc tính từ JSON: diện tích, ngang, dài, số phòng, số tầng, mặt tiền, hẻm xe hơi, nở hậu, nội thất, hướng, pháp lý, dự án, môi giới, tọa độ.
2. Kiểm tra chất lượng: mỗi dòng có mảng `quality_errors` (giá âm, tọa độ ngoài miền, số phòng âm, ...). Dòng lỗi đi vào `listing_quarantine`, không bị xóa, không vào Gold.
3. Lịch sử thay đổi: so sánh `content_hash` và giá giữa hai lần crawl liên tiếp của cùng tin để gán `change_type`: NEW, PRICE_CHANGED, UPDATED, UNCHANGED.

**Câu 7. Outlier xử lý ra sao? Tại sao không xóa luôn?**

Ý chính: Gắn cờ, không xóa. Giá/m² hoặc diện tích nằm ngoài khoảng [p1, p99] **trong từng phân khúc** (loại giao dịch × loại BĐS) thì `is_price_outlier = true` kèm `outlier_reason`. Mart xu hướng và bảng ML lọc theo cờ, còn fact vẫn giữ đủ dữ liệu để truy vết.

Giải thích thêm: Tính theo phân khúc vì 1 tỷ/m² là bình thường với căn hộ quận 1 nhưng vô lý với đất Củ Chi. Ngưỡng để trong `vars` của dbt, đổi được mà không sửa SQL.

**Câu 8. Làm sao biết một tin đã bị gỡ (bán xong hoặc rút tin)?**

Ý chính: Hai bằng chứng, lấy mốc sớm hơn: nguồn trả 404/410, hoặc tin vắng mặt trong lần crawl **đầy đủ** kế tiếp. Lần crawl dở dang (ít hơn 50% số tin so với lần lớn nhất) không được dùng để kết luận, tránh báo gỡ oan khi crawler bị chặn.

**Câu 9. Tin trùng nhau (nhiều môi giới đăng cùng một căn) xử lý thế nào?**

Ý chính: Ở bảng ML, các tin trùng loại, phường, tọa độ làm tròn 4 chữ số, diện tích, số phòng, số tầng và giá được gộp lại, giữ bản thấy gần nhất, kèm `duplicate_count`. Nếu không gộp, cùng một căn có thể nằm ở cả train lẫn test và điểm đánh giá sẽ lạc quan giả tạo.

**Câu 10. Vấn đề địa giới hành chính sau sáp nhập 2025 ảnh hưởng gì?**

Ý chính: Nguồn vẫn dùng mã quận/huyện cũ. Tin ở Bình Dương, Bà Rịa - Vũng Tàu cũ bị xếp nhầm vào quận cũ của TP.HCM, và phường 11221 là nơi nguồn "đổ" những tin không ánh xạ được phường mới. Bảng ML loại các tin này bằng danh sách phường không tin cậy và regex địa danh trong tiêu đề/địa chỉ. Đây là xử lý dữ liệu dựa trên khảo sát thực tế, không phải giả định.

---

## 3. Star schema: dimension và fact

**Câu 11. Tại sao mô hình hóa Gold theo star schema của Kimball?**

Ý chính: Ba bài toán đều là câu hỏi phân tích dạng "measure theo chiều": giá/m² theo quận theo tuần, số tin gỡ theo loại BĐS. Star schema tách rõ *cái gì được đo* (fact) và *đo theo chiều nào* (dim), join đơn giản, Superset và ML đọc trực tiếp. Mỗi bảng Gold phải trả lời được ít nhất một câu hỏi của ba bài toán, không thì không đưa vào.

**Câu 12. Có những dimension nào, ý nghĩa gì?**

| Dimension | Grain | Ý nghĩa và thuộc tính phân tích |
|---|---|---|
| `dim_date` | 1 ngày | Trục thời gian, sinh bằng dbt_date |
| `dim_location` | (tỉnh, quận, phường) | Phân cấp địa lý. Có `district_type` (Quận/Huyện/Thành phố) và `area_zone` (Nội/Ngoại thành) để cắt xu hướng theo vùng |
| `dim_property_type` | (loại, loại con) | Phân khúc. Có `property_category` chuẩn hóa (HOUSE, APARTMENT, LAND, ROOM, COMMERCIAL) và `is_residential` |
| `dim_legal_status` | tình trạng pháp lý | Gom thành `legal_category` (Có sổ, Đang chờ sổ, Giấy tờ khác, Không có sổ) và cờ `has_certificate` |
| `dim_project` | dự án | Thuộc dự án hay không; dự án được gán vào quận có nhiều tin nhất vì một số tin ghi sai quận |

**Câu 13. Có những fact nào, tại sao cần tới ba?**

Ý chính: Ba fact ứng với ba loại fact Kimball, mỗi loại trả lời một kiểu câu hỏi khác nhau, cùng nối qua `listing_key`.

| Fact | Loại | Grain | Trả lời câu hỏi |
|---|---|---|---|
| `fct_listing_snapshot` | Periodic snapshot | 1 tin × 1 lần crawl | Giá từng tin qua thời gian, tăng hay giảm. Nguồn của mart xu hướng giá |
| `fct_listing` | Accumulating snapshot | 1 tin | Vòng đời của tin: giá đầu, giá hiện tại, số lần đổi giá, ngày đăng, ngày gỡ, `days_on_market`, `is_active`. Nguồn của bảng ML |
| `fct_listing_event` | Transaction | 1 sự kiện | NEW, PRICE_CHANGED, UPDATED, REMOVED. Nguồn của mart nguồn cung và thanh khoản |

Giải thích thêm: Nếu chỉ có snapshot thì đếm "tin mới tuần này" phải quét toàn bộ lịch sử. Nếu chỉ có accumulating thì mất chuỗi giá theo thời gian. Mỗi fact tối ưu cho một loại truy vấn.

**Câu 14. Quan hệ giữa dim và fact được đảm bảo thế nào?**

Ý chính: Surrogate key sinh bởi macro dùng chung (`xxhash64` trên thuộc tính tự nhiên, trả về BIGINT). Dim và fact gọi cùng macro nên key luôn khớp, không cần bước lookup. NULL ánh xạ về member "Không rõ" hoặc "Không thuộc dự án" nên join luôn là inner. dbt test `relationships` kiểm tra mọi khóa ngoại.

**Câu 15. Tại sao không dùng key tự tăng như sách giáo khoa?**

Ý chính: Gold được build lại toàn bộ mỗi ngày trên Spark, không có sequence. Hash key xác định (deterministic): cùng thuộc tính luôn ra cùng key, nên build lại không làm key đổi và không cần bảng ánh xạ.

**Câu 16. Ba mart tổng hợp khác gì ba fact?**

Ý chính: Mart là bảng đã tính sẵn cho dashboard, grain là kỳ × cấp địa lý × loại giao dịch × loại BĐS.

- `agg_price_trend_weekly/monthly`: median, p25, p75 giá/m², số tin, % thay đổi so kỳ trước.
- `agg_market_supply_weekly`: tin đang rao, tin mới, tin gỡ, tỷ lệ giảm giá, số ngày rao trung vị khi gỡ.

Điểm thiết kế quan trọng: mỗi tin chỉ đếm một lần mỗi kỳ, median của cấp quận/vùng/thành phố tính trực tiếp từ dữ liệu tin bằng GROUPING SETS chứ không lấy trung bình của median cấp dưới, và có cờ `is_reliable` khi mẫu dưới 10 tin.

**Câu 17. Tại sao fact snapshot là incremental còn các bảng khác build lại toàn bộ?**

Ý chính: Snapshot là bảng lớn nhất (mọi tin × mọi lần crawl) và chỉ thêm dòng mới, nên MERGE theo `observation_id` phân vùng theo ngày là đủ. Các fact còn lại cần nhìn toàn bộ lịch sử để kết luận (tin bị gỡ, giá đầu tiên) và kích thước nhỏ hơn nhiều, build lại đơn giản và luôn đúng.

---

## 4. Bảng đặc trưng và chống rò rỉ dữ liệu

**Câu 18. Bảng `ml_price_features` chọn dòng và cột theo nguyên tắc nào?**

Ý chính: Dòng: tin **bán**, trạng thái mới nhất, đủ điều kiện (`is_price_training_eligible`), không outlier, không thuộc phường không tin cậy, đã gộp trùng, bỏ nhóm văn phòng/mặt bằng vì giá "bán" của nhóm này thường là giá thuê hoặc sang nhượng. Cột: chỉ những gì người dùng nhập được khi hỏi giá.

**Câu 19. Rò rỉ dữ liệu (leakage) được chống ở đâu?**

Ý chính: Ba lớp.

1. Không đưa vào feature những thứ chỉ biết sau khi đăng: số ngày rao, số ảnh.
2. Không tính trước trong Gold các đặc trưng tổng hợp theo khu vực như giá trung vị quận. Chúng được tính **bên trong tập train** khi huấn luyện, out-of-fold.
3. Chia train/test theo thời gian đăng, test là 20% tin mới nhất, nên mô hình không nhìn thấy tương lai.

**Câu 20. Target là gì? Tại sao dùng log và giá/m²?**

Ý chính: Cây học `log(giá / diện_tích)`, lúc dự đoán cộng lại `log(diện_tích)` để trả về giá. Log vì giá lệch phải rất mạnh (từ vài trăm triệu tới hàng trăm tỷ) và sai số tương đối quan trọng hơn sai số tuyệt đối. Giá/m² vì nó ổn định theo khu vực hơn tổng giá, giúp cây học vị trí tốt hơn. Thử nghiệm cho thấy bước này giảm MAPE khoảng 0,25 điểm.

---

## 5. Mô hình dự đoán giá

**Câu 21. Tại sao chọn XGBoost?**

Ý chính: Chọn bằng thực nghiệm, không phải chọn trước. Năm thuật toán được so sánh trên cùng tập test theo thời gian: Ridge, Random Forest, HistGradientBoosting, XGBoost, CatBoost.

| Mô hình | MAPE test |
|---|---|
| Baseline (median giá/m² theo phường × loại) | 35,2% |
| Ridge | 26,8% |
| Random Forest | 21,0% |
| CatBoost | 20,9% |
| HistGradientBoosting | 19,9% |
| XGBoost | 19,8% |

XGBoost tốt nhất, sau khi tinh chỉnh siêu tham số xuống 19,3%. Về lý thuyết, gradient boosting phù hợp dữ liệu dạng bảng cỡ vài chục nghìn dòng, nhiều biến phân loại, nhiều giá trị thiếu, quan hệ phi tuyến giữa vị trí, diện tích và giá.

**Câu 22. Tại sao CatBoost không hơn, dù CatBoost nổi tiếng xử lý biến phân loại tốt?**

Ý chính: Thế mạnh của CatBoost là ordered target encoding cho biến phân loại nhiều mức. Pipeline đã tự làm target encoding cross-fitted cho phường, đường, dự án và thêm đặc trưng giá/m² theo khu vực, nên lợi thế đó bị triệt tiêu. CatBoost còn dùng cây đối xứng, kém linh hoạt hơn khi dữ liệu có tương tác phức tạp. Với dữ liệu này, phần sai số còn lại nằm ở dữ liệu (đất nông nghiệp, thổ cư một phần), không phải ở thuật toán.

**Câu 23. Tại sao không dùng mạng nơ-ron hoặc mô hình ngôn ngữ?**

Ý chính: Với dữ liệu dạng bảng dưới 100 nghìn dòng, các nghiên cứu so sánh đều cho thấy gradient boosting bằng hoặc hơn mạng nơ-ron, huấn luyện nhanh hơn, ít tham số cần chỉnh, chạy được trong container 3 GB RAM. Tiêu đề và mô tả tin có thể khai thác bằng NLP nhưng chưa làm, đó là hướng phát triển.

**Câu 24. Tiền xử lý đặc trưng cho mô hình gồm gì?**

Ý chính: Bốn nhóm trong một ColumnTransformer.

- Số và boolean: giữ nguyên, NaN để cây tự xử lý.
- Phân loại ít mức (hướng nhà, nội thất, pháp lý, ...): OrdinalEncoder, giá trị lạ thành NaN.
- Phân loại nhiều mức (phường, đường, dự án): TargetEncoder cross-fitted, giá của chính dòng đó không bao giờ dùng để mã hóa nó.
- Đặc trưng khu vực: trung bình có làm mượt của log giá/m² và số tin theo phường × loại, quận + đường × loại, dự án, quận × loại con; tính out-of-fold trên tập train. Công thức làm mượt: (tổng + k × prior) / (n + k), với prior là trung bình của loại BĐS, k = 5, nên nhóm ít tin dựa vào prior.

**Câu 25. Siêu tham số chọn thế nào?**

Ý chính: Optuna với sampler TPE, mục tiêu là MAPE trên tập valid (20% mới nhất của train, test không đụng tới). Trong notebook đã tìm rộng 150 lần thử rồi tìm hẹp 40 lần. Kết quả: khoảng 1.900 cây, learning rate 0,014, độ sâu 10, subsample 0,69, colsample 0,84. Pipeline sản xuất chạy lại tìm kiếm 30 lần mỗi lần train, lần thử đầu luôn là bộ tham số tốt nhất đã biết nên không bao giờ tệ hơn.

**Câu 26. Có thử tách mô hình riêng cho từng loại BĐS không?**

Ý chính: Có. Một mô hình chung cho 19,20%, ba mô hình riêng cho 19,21%. Căn hộ tốt hơn một chút nhưng đất tệ hơn vì ít dữ liệu. Giữ một mô hình chung vì đơn giản hơn và kết quả bằng nhau.

---

## 6. Đánh giá mô hình

**Câu 27. Dùng thước đo nào, tại sao?**

Ý chính: MAPE là thước đo chính vì người dùng quan tâm sai bao nhiêu phần trăm, và giá trải nhiều bậc độ lớn nên RMSE bị vài căn trăm tỷ chi phối. Báo thêm MdAPE (trung vị, ít nhạy ngoại lai), R² trên log giá, và MAPE theo từng loại BĐS.

**Câu 28. Kết quả cụ thể?**

| | MAPE |
|---|---|
| Baseline median theo phường × loại | 35,0% |
| Mô hình champion hiện tại (v16) | 19,3% |
| Căn hộ | khoảng 13% |
| Nhà ở | khoảng 19% |
| Đất | khoảng 29% |

**Câu 29. Tại sao đất sai nhiều?**

Ý chính: Giá đất phụ thuộc thông tin không có trong tin rao: tỷ lệ thổ cư, quy hoạch, đất nông nghiệp hay đất ở. Đất nông nghiệp có MAPE gần 60%, lô trên 200 m² thường chỉ một phần thổ cư. Đã thử lọc nhiễu, mô hình riêng cho đất, trích "thổ cư" từ mô tả bằng regex, mỗi cách cải thiện dưới 0,2 điểm tổng thể.

**Câu 30. Chia train/test thế nào? Tại sao không chia ngẫu nhiên?**

Ý chính: Theo thời gian đăng, test là 20% tin mới nhất, cắt tại ngày tròn. Mô hình sẽ dùng để định giá tin tương lai nên phải đánh giá trên tương lai. Đã đo thêm: chia ngẫu nhiên cho MAPE gần bằng hoặc thấp hơn một chút, chứng tỏ sau khi gộp trùng thì rò rỉ còn rất nhỏ.

**Câu 31. Làm sao biết kết quả không phải may mắn?**

Ý chính: Chạy 3 seed, độ lệch chuẩn MAPE dưới 0,1 điểm. So sánh với champion cũ luôn trên **cùng một tập test**. Cải tiến nào dưới 0,2 điểm thì coi là trong ngưỡng nhiễu và không đưa vào sản xuất.

**Câu 32. 19% có dùng được không?**

Ý chính: Đây là giá **rao**, không phải giá giao dịch, nên bản thân nhãn đã có nhiễu do người bán kê giá khác nhau. Các nghiên cứu định giá tự động trên dữ liệu rao bán tại Việt Nam và khu vực thường báo MAPE 15 đến 25%. Mô hình giảm sai số gần một nửa so với baseline, dùng được để tham khảo khoảng giá, không thay thẩm định.

---

## 7. Vận hành: Airflow, MLflow, API

**Câu 33. Pipeline chạy tự động thế nào?**

Ý chính: DAG hằng ngày: crawl ID, crawl chi tiết, nạp Bronze, dbt run Silver, dbt test Silver, dbt run Gold, dbt test Gold. Test Silver thất bại thì Gold không chạy. DAG huấn luyện chạy hằng tuần, kích hoạt bởi Dataset khi Gold test xong, hoặc chạy tay.

**Câu 34. MLflow dùng để làm gì?**

Ý chính: Ghi lại mỗi lần train: tham số, metric, Delta version của dữ liệu, bảng so sánh với baseline và champion, model artifact. Model đăng ký trong registry `price_model`, alias `champion` trỏ tới phiên bản đang phục vụ. API chỉ load theo alias nên đổi mô hình không cần sửa code.

**Câu 35. Mô hình mới được đưa vào sản xuất theo quy tắc nào?**

Ý chính: Quality gate: mô hình mới được gán `champion` chỉ khi MAPE trên tập test hôm nay không tệ hơn champion hiện tại chấm trên **cùng tập test đó**. Nhờ vậy mô hình kém không bao giờ tự động lên; đã có lần v4 bị từ chối đúng.

**Câu 36. API dự đoán hoạt động thế nào? Người dùng nhập sai thì sao?**

Ý chính: FastAPI load champion từ MLflow lúc khởi động, có endpoint `/predict`, `/options` trả danh sách giá trị hợp lệ cho form, `/model/reload` để nạp mô hình mới không cần restart. Hàm ép kiểu dùng chung với lúc train nên dữ liệu vào API được xử lý giống hệt dữ liệu train. Giá trị phân loại lạ thành NaN, cây vẫn dự đoán được.

---

## 8. Mô hình gợi ý

**Câu 37. Mô hình gợi ý làm gì và khác mô hình giá chỗ nào?**

Ý chính: Gợi ý dựa trên nội dung: từ một tin tìm Top-K tin tương tự, hoặc từ nhu cầu (mua/thuê, loại, khu vực, ngân sách, diện tích, số phòng, từ khóa) lọc cứng rồi xếp hạng theo TF-IDF nội dung, giá, diện tích, khoảng cách và độ mới. Không dự đoán gì, giá chỉ là ràng buộc. Chưa có dữ liệu hành vi người dùng nên không dùng collaborative filtering và không tạo nhãn giả.

**Câu 38. Đánh giá mô hình gợi ý thế nào khi không có nhãn?**

Ý chính: Kiểm tra ràng buộc (kết quả đúng loại, đúng thị trường, trong ngân sách), độ trễ, và đánh giá định tính. Nói thẳng đây là hạn chế: không có relevance metric thực sự khi chưa có click hoặc liên hệ của người dùng.

---

## 9. Hạn chế và hướng phát triển

**Câu 39. Hạn chế lớn nhất của đồ án?**

Ý chính, nói thẳng:

- Nhãn là giá rao, không phải giá giao dịch.
- Chỉ một nguồn dữ liệu, chỉ TP.HCM theo địa giới cũ.
- Xu hướng giá chỉ có ý nghĩa khi crawl đều nhiều tháng, chưa đủ dài.
- Tin bị gỡ chỉ là proxy của giao dịch.
- Mô hình đất còn yếu vì thiếu thông tin pháp lý chi tiết.
- Hạ tầng chạy trên một máy 8 GB, chưa có HA.

**Câu 40. Nếu có thêm thời gian sẽ làm gì?**

Ý chính: Thêm nguồn (Batdongsan, Mogi) để giảm thiên lệch nguồn; khai thác mô tả bằng NLP để lấy thổ cư, quy hoạch, hẻm; thêm đặc trưng ngoại vi (khoảng cách tới metro, trường, trung tâm); thu thập hành vi người dùng để cá nhân hóa gợi ý; giám sát drift và tự retrain khi MAPE trên dữ liệu mới vượt ngưỡng.

**Câu 41. Điểm đóng góp của sinh viên là gì, phần nào là dùng công cụ sẵn?**

Ý chính: Công cụ (Spark, Delta, dbt, Airflow, MLflow, XGBoost) là mã nguồn mở. Đóng góp của đồ án là: thiết kế pipeline end-to-end chạy ổn định hằng ngày trên máy cấu hình thấp; quy tắc nghiệp vụ cho dữ liệu rao bán (phát hiện gỡ tin, snapshot đầy đủ, gộp trùng, outlier theo phân khúc, xử lý sáp nhập địa giới); star schema với ba loại fact đúng mục đích; quy trình huấn luyện có chống leakage, đánh giá out-of-time và quality gate; bộ thí nghiệm có số liệu để biện minh từng lựa chọn.
