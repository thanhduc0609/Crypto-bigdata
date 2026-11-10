# Sườn báo cáo đồ án — bản để duyệt trước khi viết từng mục

Cập nhật giao diện 10/11/2026: ảnh chương 5 lấy tại `/thi-truong`; ảnh dự báo,
kiểm định và sai số chương 6 lấy tại `/du-bao-3-nam`. Hướng dẫn thao tác và góc
chụp: [RESEARCH_UI.md](RESEARCH_UI.md). Giữ cấu trúc đề cương; chưa viết các chương.

Ngày lập: 09/11/2026. Đây là **kế hoạch nội dung và minh chứng**, chưa phải các chương báo cáo đã viết hoàn chỉnh. Tên trường, học phần, giảng viên, thành viên, phân công và mẫu trình bày sẽ bổ sung theo thông tin thật của nhóm.

## A. Chốt phạm vi và tên đề tài

**Tên đề xuất:** Xây dựng hệ thống thu thập, xử lý và phân tích dữ liệu BTC-USDT từ OKX, kết hợp dự báo giá đóng cửa theo tháng trong 36 tháng.

- Bài toán chính theo đề: dữ liệu kinh tế → thu thập có nguồn → làm sạch → trực quan hóa → dự báo/đánh giá 3 năm.
- Đầu ra dài hạn: giá đóng cửa từng tháng, 36 tháng 11/2026–10/2029, có dải bất định. Dữ liệu huấn luyện chỉ dùng tháng hoàn tất tới 10/2026.
- Phân loại hướng giá +5 phút, phân tích giao dịch lớn, order flow và Futures là phần mở rộng. Không lấy accuracy +5 phút để chứng minh chất lượng dự báo 3 năm.
- Nêu dự án là hệ thống thử nghiệm xử lý dữ liệu theo kiến trúc Big Data; dung lượng thực đo chưa chứng minh năng lực ở quy mô công nghiệp.
- Không gọi mọi kết quả là tốt hoặc cam kết điểm số. Mô hình có thể thua baseline; báo cáo cần giải thích và chứng minh quy trình đúng.

## B. Bố cục toàn bộ tài liệu

1. Bìa theo mẫu của khoa; tên đề tài, môn học, lớp, giảng viên, nhóm.
2. Trang phân công và đóng góp thật của từng thành viên; không tự gán phần việc chưa thực hiện.
3. Lời cảm ơn/cam đoan nếu mẫu của khoa yêu cầu.
4. Tóm tắt: bài toán, dữ liệu, phương pháp, kết quả đo chính và giới hạn. Viết sau cùng.
5. Mục lục tự động; danh mục hình; danh mục bảng; danh mục từ viết tắt.
6. Tám chương bên dưới.
7. Tài liệu tham khảo và phụ lục.

Độ dài, font, giãn dòng và kiểu trích dẫn theo mẫu được cô/khoa cung cấp; chưa có mẫu thì chưa coi một định dạng tự chọn là quy định bắt buộc.

## C. Đối chiếu trực tiếp với yêu cầu của cô

| Tiêu chí | Nơi chứng minh trong báo cáo | Bằng chứng cần có |
|---|---|---|
| Chọn một loại dữ liệu quan tâm | 1.1–1.3, 3.1 | Lý do chọn dữ liệu kinh tế BTC-USDT và câu hỏi nghiên cứu |
| Thu thập, chỉ rõ nguồn Internet | 3.1–3.4 | OKX API/WebSocket, tham số, đơn vị, mốc thời gian, ngày thu thập, raw/manifest |
| Làm sạch dữ liệu | 4.2–4.5 | Quy tắc và thống kê trước/sau, ví dụ lỗi, dữ liệu thiếu/trùng/đến trễ, đối soát |
| Trực quan hóa | Chương 5 | Biểu đồ có trục, đơn vị, nguồn, thời gian và nhận xét dựa trên dữ liệu |
| Dự báo, đánh giá tương lai 3 năm | 6.1–6.5 | Horizon 36 tháng, baseline, chia thời gian, backtest, sai số từng năm và bất định |
| Nhóm tối đa 5 người | Trang phân công, 8.3 | Danh sách và đóng góp thật; xác nhận khi có thông tin nhóm |

## Chương 1. Tổng quan và xác định bài toán

### 1.1. Bối cảnh và lý do chọn đề tài
- Nêu đặc điểm dữ liệu giao dịch cập nhật liên tục, nhu cầu lưu trữ/phân tích lịch sử.
- Giải thích vì sao BTC-USDT thuộc dữ liệu kinh tế, vì sao chọn OKX; không đồng nhất USDT với USD.
- Tránh mở đầu bằng lời hứa dự báo thị trường chắc chắn hoặc sinh lời.

### 1.2. Mục tiêu và câu hỏi nghiên cứu
- Hệ thống có thu thập/lưu trữ/tổng hợp và truy vết được dữ liệu không?
- Làm sạch/đối soát giải quyết lỗi gì, có số liệu chứng minh không?
- Dữ liệu lịch sử thể hiện xu hướng, khối lượng và biến động nào?
- Phương pháp dự báo 36 tháng có tốt hơn baseline trên phép đánh giá đã định không?

### 1.3. Phạm vi và đầu ra
- BTC-USDT Spot là nguồn chính; lịch UTC; phân biệt lịch sử ngày/tháng và giao dịch/phút.
- Chỉ dự báo close tháng; không tạo OHLC tương lai giả.
- Phân biệt chức năng chính với phần mở rộng +5 phút/Futures.

### 1.4. Phương pháp thực hiện và cấu trúc báo cáo
- Tóm tắt các bước thực nghiệm; mỗi bước gắn một đầu ra có thể kiểm chứng.
- **Bảng cần có:** mục tiêu → đầu ra → tiêu chí hoàn thành.
- **Hoàn thành khi:** mọi mục tiêu trong phần mở đầu được trả lời lại ở Chương 8.

## Chương 2. Cơ sở lý thuyết và công nghệ sử dụng

### 2.1. Dữ liệu giao dịch, OHLCV và dữ liệu chuỗi thời gian
- Định nghĩa trade, nến, open/high/low/close, volume BTC, turnover USDT, timestamp.
- Phân biệt nến đang mở với nến đã đóng; tính liên tục theo lịch.

### 2.2. Kiến trúc xử lý dòng và xử lý lô
- Vai trò thật của Kafka, Spark Structured Streaming, Spark batch, S3/Parquet và PostgreSQL.
- Giải thích Bronze/Silver/Gold, checkpoint, offset, watermark, replay và upsert.
- Chỉ nêu đảm bảo đã kiểm chứng; không suy ra end-to-end exactly-once chỉ từ tên công nghệ.

### 2.3. Phân tích chuỗi thời gian và mô hình dự báo
- Baseline giữ giá cuối; log drift; damped trend; Ridge lợi suất; regularization và ensemble đã thử.
- Logistic Regression, XGBoost và HistGradientBoosting cho phần mở rộng 5 phút.
- Không thêm mô hình vào lý thuyết như thể đã thực nghiệm nếu chưa sử dụng.

### 2.4. Đánh giá và chống rò rỉ tương lai
- Chia tập theo thời gian, rolling-origin/nested validation; nhãn phải sẵn sàng tại thời điểm ra quyết định.
- Công thức MAE, RMSE, sMAPE, MASE; sai số log dùng đúng công thức code `sqrt(mean((log(y)-log(yhat))^2))`.
- Accuracy, balanced accuracy, precision/recall/F1, AUC, log loss/Brier cho phân loại.
- Phân biệt dải danh nghĩa, độ phủ và độ rộng; xác suất mô hình khác tỷ lệ dự báo đúng.
- **Bảng cần có:** công nghệ → vai trò → lý do chọn → giới hạn.

## Chương 3. Nguồn dữ liệu và thiết kế hệ thống

### 3.1. Nguồn và phương thức thu thập
- API/WebSocket OKX; endpoint, symbol, bar, pagination, rate limit/retry đã dùng, ngày thu thập.
- Đính nguồn tài liệu chính thức; không đưa khóa AWS hoặc mật khẩu vào báo cáo.

### 3.2. Các bộ dữ liệu và phạm vi thời gian
- Giao dịch streaming thực thu; không nhận là toàn bộ lịch sử giao dịch của sàn.
- Snapshot nến phút đã khóa: 154.351 phút liên tục, từ 25/07 đến 09/11/2026.
- Dài hạn: 3.223 ngày hợp lệ và 105 tháng hoàn tất; giải thích tháng đầu/cuối bị loại.
- DW đang cập nhật có thể lớn hơn snapshot; chụp số liệu cùng thời điểm để tránh mâu thuẫn.

### 3.3. Từ điển dữ liệu
- Bảng trường, kiểu, đơn vị, ý nghĩa, nguồn và quy tắc NULL.
- REST không có buy/sell volume/trade imbalance thật thì giữ NULL; không điền giả.

### 3.4. Kiến trúc tổng thể đúng với code đang chạy
- **Hình 3.1:** sơ đồ hai nhánh thu thập và các tầng lưu trữ/serving.
- Nhánh streaming: OKX → Python → Kafka → Spark → Bronze/speed/DW; batch → Silver/Gold.
- Nhánh dài hạn: OKX REST ngày → làm sạch → tháng hoàn tất → mô hình 36 tháng → snapshot/API/web. Nhánh này không bắt buộc đi qua Kafka/Spark.
- **Hình 3.2:** mô hình dữ liệu DW và khóa chính/duy nhất; vai trò bảng dự báo.

### 3.5. Thiết kế phiên bản và khả năng truy vết
- Manifest, checksum, nguồn raw, phiên xử lý, snapshot đóng băng và model artifact.
- Gold vận hành theo phiên riêng; không ghi đè snapshot nghiên cứu đã khóa.
- **Bảng cần có:** dataset → số dòng → khoảng thời gian → dung lượng → độ hạt → cập nhật lúc nào.

## Chương 4. Triển khai pipeline và làm sạch dữ liệu

### 4.1. Môi trường, cấu hình và thứ tự khởi động
- Python/Java/Docker, phiên bản thư viện đã chạy; địa chỉ local và các dịch vụ.
- File mẫu chỉ có placeholder; các script khởi động/dừng nằm trong repo.

### 4.2. Làm sạch giao dịch Bronze → Silver
- Kiểm tra symbol, giá/size hữu hạn và dương, timestamp, side, trade_id.
- Khử trùng lặp có thứ tự xác định; giữ lineage tới file nguồn và Kafka offset.
- **Bảng 4.1:** tổng vào = lỗi + trùng bị loại + số sạch.
- Dữ liệu thật có thể ghi nhận 0 lỗi/trùng; phân biệt với fixture cố tình tạo lỗi để kiểm thử.

### 4.3. Tổng hợp Silver → Gold
- Công thức OHLCV, VWAP, dòng mua/bán, trade count và trade value.
- Cách xử lý giao dịch cùng timestamp, nến chưa đóng và dữ liệu đến trễ.
- **Hình 4.1:** một phút cụ thể được tính lại từ các giao dịch đã thu thập.

### 4.4. Nến REST canonical và đối soát
- Quy tắc nguồn REST đã đóng ưu tiên hơn nến stream chưa đầy đủ.
- Khôi phục phút thiếu, kiểm tra OHLC, xác nhận đóng, tính liên tục, backup và upsert.
- Sai số làm tròn theo kiểu NUMERIC của DB khác với sai lệch dữ liệu có ý nghĩa.
- **Bảng 4.2:** trước/sau làm sạch trên cùng miền thời gian; phần mở rộng thời gian phải ghi riêng, không giả thành hiệu quả làm sạch.

### 4.5. Kiểm chứng nguồn và khả năng chạy lại
- Một trade thật: Kafka → file Bronze đã commit → Silver → phút Gold → lựa chọn canonical trong DW.
- Chỉ đọc file Spark đã commit, không nhận file orphan là dữ liệu đã công bố.
- Chạy lại không tạo khóa nến trùng; checkpoint được giữ.
- **Bằng chứng:** manifest, SHA, CSV trước/sau, audit thật và log kiểm thử.

## Chương 5. Phân tích dữ liệu và trực quan hóa

### 5.1. Tổng quan dữ liệu và chất lượng
- Giải thích số dòng/khoảng quan sát, phân bố thời gian, những khoảng thiếu và nguồn.
- **Ảnh đề xuất:** bảng trạng thái luồng và thống kê làm sạch trên web.

### 5.2. Giá và khối lượng theo thời gian
- Nến phút/giờ/ngày và nến tháng, giải thích phép tổng hợp và vai trò từng độ hạt.
- Nhận xét có mốc thời gian và số liệu cụ thể; phân biệt trục log với trục thường.
- **Ảnh đề xuất:** nến thị trường + volume; lịch sử close tháng.

### 5.3. Giao dịch lớn và dòng mua/bán
- Ngưỡng USDT, bộ lọc thời gian, pagination; tổng hợp tính trên toàn bộ tập lọc.
- Donut, volume, order flow 60 giây: giải thích khác sổ lệnh và khác mọi giao dịch của sàn.
- **Bảng cần có:** câu hỏi phân tích → bộ lọc → số liệu → nhận xét → giới hạn.

### 5.4. Nhận xét và giới hạn phân tích
- Phân biệt mô tả với nguyên nhân; không kết luận quan hệ nhân quả từ một biểu đồ.
- Mỗi hình có caption, đơn vị, múi giờ, nguồn, thời điểm chụp và một đoạn nhận xét.
- Không chỉ dán ảnh dashboard liên tiếp mà không trả lời câu hỏi nghiên cứu.

## Chương 6. Dự báo và đánh giá mô hình

### 6.1. Bài toán chính: dự báo giá close tháng cho 36 tháng
- Mục tiêu, dữ liệu tháng hoàn tất, cách tạo chuỗi và chân trời dự báo.
- Lịch sử 2018-02–2026-10; phân biệt backtest với dự báo 2026-11–2029-10 chưa có đủ giá thật.

### 6.2. Thí nghiệm gốc đã khóa
- Train/chọn phương pháp trước kỳ kiểm định; baseline, tham số, seed, snapshot.
- **Hình 6.1:** dòng thời gian chia tập.
- **Bảng 6.1:** các mô hình trên cùng test, MAE/RMSE/sMAPE và sai số năm thứ 1/2/3.
- Giữ kết quả gốc: MAE model được chọn 47.949,55 USDT, baseline 43.678,28 USDT; chưa chứng minh vượt baseline.

### 6.3. Đánh giá bổ sung rolling/nested
- Chốt protocol, chọn bằng validation bên trong từng mốc rồi dự báo đủ 36 tháng.
- Ba mốc train kết thúc 2023-01/07/10; trình bày cả mô hình được chọn và baseline.
- Các cửa sổ chồng lấn và outcome đã từng được xem: ghi rõ hồi cứu, không gọi là 3 test độc lập mới.
- Không đổi mô hình theo cột test rồi công bố lại như lựa chọn chưa nhìn tương lai.

### 6.4. Dải bất định và phân tích lỗi dài hạn
- Độ phủ đi cùng độ rộng và interval score; 100% độ phủ không phải 100% chính xác.
- Nguyên nhân đã chứng minh khác giả thuyết; giới hạn số tháng và biến giải thích.
- **Ảnh 6.2:** lịch sử/dự báo với đường phân cách và dải.
- **Ảnh 6.3:** backtest và baseline; **Bảng 6.2:** metric theo horizon 12/24/36 tháng.

### 6.5. Phần mở rộng: phân loại UP/NOT_UP sau 5 phút
- Nhãn, thời điểm nến đóng, warm-up, purge và tập đã khóa; không dùng làm thay thế mục 6.1–6.4.
- Bốn fold, tám ứng viên, baseline, metric từng lớp và confidence interval theo ngày.
- **Bảng 6.3:** cùng 17.274 mẫu, accuracy/BA/F1/AUC/log loss; confusion matrix.
- Incumbent accuracy 51,32%, BA 51,16%; Logistic nghiên cứu 51,42% và 51,36%. Lợi thế nhỏ, CI chứa 0, chưa triển khai thay model.
- Phân biệt metric lịch sử trong artifact, metric cohort mới và xác suất của một dự báo đang chạy.

### 6.6. Tổng kết kết quả dự báo
- Điều gì đã thử, cải thiện ở khía cạnh nào, điều gì chưa có bằng chứng.
- Không lấy kết quả thuận lợi duy nhất hoặc viết tỷ lệ đúng tương lai khi chưa có outcome.

## Chương 7. Kiểm thử, vận hành và demo

### 7.1. Chiến lược kiểm thử
- Unit, tích hợp DB/S3, luồng thật, UI/API, phục hồi và tải; fixture tách dữ liệu thật.
- Mỗi kết quả có ngày chạy, phạm vi, log và số test thực tế; không cộng lại các lần chạy trùng rồi nhận là test khác nhau.

### 7.2. Kết quả kiểm thử chức năng và đối soát
- Dữ liệu, canonical precedence, prediction outcome, cache/retry/fallback, checksum và cô lập test.
- **Bảng 7.1:** nhóm kiểm thử → số kiểm tra → kết quả → file minh chứng.

### 7.3. Dừng/khởi động lại và phục hồi
- Giữ checkpoint, Spark theo dõi mọi query, dịch vụ khởi động lại với backoff.
- Kiểm chứng backlog Kafka được xử lý sau sự cố trong phạm vi kiểm tra; không suy ra không mất mọi sự kiện từ sàn khi nguồn bị ngắt.
- **Hình 7.1:** trước/lúc sự cố/sau phục hồi; duplicate keys và offsets.

### 7.4. Tải và tài nguyên
- Thời lượng, số request, concurrency, p50/p95/max, lỗi, RAM/CPU đo thực tế.
- Tách khởi động chưa sẵn sàng với vận hành ổn định; giữ cả lượt thất bại và lượt sau sửa.
- Không coi bài thử demo vài phút là chứng minh tải lớn hay SLA chạy lâu.

### 7.5. Kịch bản demo và khả năng tái lập
- Cấu hình → khởi động → trạng thái luồng → dữ liệu/làm sạch → phân tích → dự báo/backtest → CSV.
- Khi thiếu Internet: chỉ dùng dữ liệu/snapshot đã có, giữ nhãn nguồn/cũ/mất kết nối; không giả live.
- **Bảng 7.2:** thao tác, kết quả mong đợi, bằng chứng, phương án xử lý khi lỗi.

## Chương 8. Kết luận và hướng phát triển

### 8.1. Kết quả đạt được theo từng yêu cầu của cô
- Trả lời lần lượt năm tiêu chí ở bảng đối chiếu, dẫn tới chương/hình/bảng minh chứng.

### 8.2. Giới hạn hiện tại
- Phạm vi dữ liệu, thời gian thu, quy mô thử nghiệm, chất lượng mô hình và dải bất định.
- Phân biệt cái đã kiểm tra với cái mới là hướng nghiên cứu; không dùng câu “hoàn hảo/100%” nếu thiếu bằng chứng.

### 8.3. Đóng góp thành viên và hướng phát triển
- Nêu phần việc thật, kiến thức nhóm tự bảo vệ được, những hướng cần dữ liệu/test mới.
- Hướng mở rộng phải gắn hạn chế cụ thể, không chỉ liệt kê thêm công nghệ.

## D. Tài liệu tham khảo và phụ lục

- Tài liệu chính thức OKX, Kafka, Spark, PostgreSQL, S3/Parquet và tài liệu phương pháp dự báo/đánh giá đã sử dụng.
- Tách tài liệu công nghệ với nguồn dữ liệu; ghi ngày truy cập khi mẫu yêu cầu.
- Phụ lục A: từ điển dữ liệu và schema.
- Phụ lục B: tham số, seed, phiên bản thư viện, manifest/hash.
- Phụ lục C: bảng metric đầy đủ, forecast CSV, độ phủ/độ rộng và dự đoán từng mẫu.
- Phụ lục D: hướng dẫn cài/chạy/dừng, xử lý lỗi, log kiểm thử và kịch bản demo.
- Không đưa .env, AWS key, mật khẩu DB hoặc ảnh terminal có secret vào phụ lục.

## E. Danh mục hình/bảng tối thiểu để chuẩn bị

| Mã dự kiến | Nội dung | Nguồn chuẩn bị |
|---|---|---|
| Hình 3.1 | Kiến trúc hai nhánh và vai trò từng thành phần | Sơ đồ khớp code, không chụp web thay sơ đồ |
| Hình 3.2 | Schema DW, khóa và quan hệ | SQL hiện tại |
| Bảng 3.1–3.2 | Quy mô dữ liệu và dictionary | Kiểm kê, manifest, từ điển dữ liệu |
| Bảng 4.1–4.2 | Trước/sau làm sạch và đối soát canonical | CSV kiểm kê/khôi phục, batch manifest |
| Hình 4.1 | Một trade/phút thật đi qua các chặng | Audit Kafka–S3–DW |
| Hình 5.1–5.3 | Nến/volume, giao dịch lớn, order flow | Ảnh web kèm bộ lọc và thời điểm |
| Hình 6.1–6.3 | Chia thời gian, forecast, backtest | Protocol và ảnh web |
| Bảng 6.1–6.4 | Baseline, metric dài hạn, từng năm, +5 phút | CSV/JSON thực nghiệm đã khóa |
| Bảng 7.1–7.2 | Kiểm thử và kịch bản demo | Log kết quả và hướng dẫn chạy |
| Hình 7.1 | Phục hồi sau dừng/chạy lại | Offsets, PID, DW và ảnh trạng thái |

Ảnh chỉ là minh họa. Các số liệu dùng phân tích phải truy ngược được về CSV/JSON nguồn và cùng phạm vi thời gian.

## F. Thứ tự viết từng phần nhỏ sau khi duyệt sườn

1. Chốt tên đề tài và mục 1.2–1.3 (mục tiêu/phạm vi), xác nhận mẫu của cô và thông tin nhóm.
2. Viết 3.1 nguồn dữ liệu → 3.2 quy mô → 3.3 dictionary → 3.4 kiến trúc.
3. Viết lần lượt 4.2 làm sạch, 4.3 tổng hợp, 4.4 canonical, 4.5 truy vết.
4. Chọn từng hình ở Chương 5 và viết nhận xét dựa trên số liệu đã chốt.
5. Viết 6.1–6.4 bài toán 3 năm trước; sau đó mới viết 6.5 phần mở rộng 5 phút.
6. Viết Chương 7 từ kết quả kiểm thử thật và kịch bản demo đã chạy.
7. Hoàn thiện lý thuyết liên quan, mở đầu, kết luận, tóm tắt, tham khảo và định dạng.

Mỗi lượt chỉ viết một mục nhỏ đã thống nhất, đưa đủ nguồn/hình/bảng cần thiết, rồi bạn xem lại trước khi chuyển mục khác. Chưa bắt đầu viết nội dung các chương trong lần lập sườn này.
