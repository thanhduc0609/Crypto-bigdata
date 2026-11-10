# Hướng dẫn giải thích và viết báo cáo dự án

Đây là khung báo cáo và kết quả hiện tại, **chưa phải báo cáo hoàn thành toàn bộ dự án**.
Ngày lập: 08/11/2026. Chỉ nêu đã hoàn thành khi có code, dữ liệu và minh chứng tương ứng.

Cập nhật ML 5 phút ngày 09/11/2026: đã thử 8 ứng viên trên snapshot canonical đã
khóa, test chính 17.274 mẫu. Logistic mới accuracy 51,42%, balanced accuracy
51,36%; incumbent tương ứng 51,32% và 51,16%. CI chênh lệch chứa 0 nên chưa đổi
model đang chạy. Dùng [kết quả và giới hạn đánh giá mới](SHORT_TERM_ML_EVALUATION.md)
khi viết phần ML 5 phút; các tỷ lệ ở cohort cũ không so trực tiếp với kỳ này.
Phần dự báo 36 tháng dưới đây giữ nguyên và là một bài toán riêng.

## 1. Phát biểu bài toán thống nhất

Tên gợi ý: **Thu thập, xử lý và phân tích dữ liệu BTC-USDT từ OKX; dự báo giá đóng cửa
tháng cho 36 tháng tiếp theo**.

Bài toán chính theo đề: chọn dữ liệu kinh tế; thu thập có nguồn; làm sạch; trực quan;
dự báo và đánh giá tương lai 3 năm. Streaming, phân tích giao dịch lớn và classifier
5 phút là phần mở rộng kỹ thuật, không thay thế đầu ra 36 tháng.

- Đầu vào dài hạn: nến ngày BTC-USDT trên OKX, lịch UTC, nhiều năm.
- Đơn vị quan sát để dự báo: tháng hoàn tất; mục tiêu là **giá đóng cửa cuối tháng**,
  không phải giá trung bình, cũng không phải đủ bốn giá OHLC tương lai.
- Đầu ra: 36 dự báo tháng, dải bất định, đánh giá trên dữ liệu quá khứ và phân tích giới hạn.
- Không tuyên bố hệ thống đưa ra mức giá chắc chắn hoặc đảm bảo hiệu quả giao dịch.

## 2. Vì sao giữ riêng 36 tháng?

Để đánh giá khả năng dự báo 3 năm mà không phải đợi 3 năm, giả lập thời điểm chỉ biết
dữ liệu đến 10/2023, dự báo 11/2023–10/2026, sau đó đối chiếu giá thực tế đã có.
36 tháng là lựa chọn thiết kế phù hợp với horizon 36 tháng, **không phải một điều kiện
chia tập do cô ghi trong đề**. Không chia ngẫu nhiên dữ liệu thời gian.

| Bước | Dữ liệu được phép dùng | Mục đích |
|---|---|---|
| Học/chọn phương pháp | 02/2018–10/2023, 69 tháng | Các fold validation đều nằm trong đoạn này |
| Chấm thí nghiệm | 11/2023–10/2026, 36 tháng | So dự báo đóng băng với giá thật; không cập nhật giữa chừng bằng giá test |
| Dự báo thực tế | Fit lại toàn bộ 02/2018–10/2026, 105 tháng | Cùng phương pháp đã chọn, tạo dự báo 11/2026–10/2029 |

69 tháng đầu **không phải đều là train của mọi fold**: validation lần lượt fit 24,
30, 33 tháng, rồi dự báo 36 tháng; các đoạn validation chồng lấn. Sau chọn phương
pháp, fit trên đủ 69 tháng trước test. Sau đánh giá mới fit lại trên đủ 105 tháng.
Không bỏ vĩnh viễn 36 tháng dữ liệu; không lấy 36 tháng tương lai chưa xảy ra để huấn luyện.

Tham khảo phương pháp: [Forecasting: Principles and Practice, mục 5.8 và 5.10](https://otexts.com/fpp3/toolbox.html#training-and-test-sets).

## 3. Vì sao hai biểu đồ khác nhau?

| Thuộc tính | Biểu đồ dài hạn | Biểu đồ thị trường trực tiếp |
|---|---|---|
| Dữ liệu | Tháng đã hoàn tất | Nến gần nhất, có thể có nến đang chạy |
| Độ chi tiết | Một điểm hoặc một nến = một tháng | Một nến = một phút, giờ hoặc ngày |
| Đường đóng cửa | Chỉ lấy close cuối mỗi tháng | Nến cho thấy open/high/low/close trong từng khoảng |
| Phạm vi | Nhiều năm quá khứ + 36 tháng dự báo | Một đoạn thị trường gần đây |
| Trục giá | Chọn logarithm hoặc thông thường | Biểu đồ nến giao dịch với độ phóng đại riêng |

Ví dụ: trong một tháng giá dao động từ 60.000 đến 80.000 nhưng cuối tháng đóng cửa
ở 70.000 USDT, đường tháng chỉ có điểm 70.000. Nến tháng giữ cả open/high/low/close.
Đó là phép tổng hợp, không phải lỗi hoặc một nguồn giá khác.

Giao diện đã thêm lựa chọn **nến tháng** để đối chiếu. Khi đổi sang nến tháng, chỉ
lịch sử có OHLC thật; tương lai vẫn là dự báo close và dải bất định. Không tạo OHLC
tương lai khi chưa có mô hình dự báo các biến đó.

Đã thống nhất request nến ngày live từ `1D` (ngày UTC+8) sang **`1Dutc`**, giống dữ liệu
dài hạn. Kết nối DB của web dùng timezone UTC. Nhãn thời gian giao dịch vẫn có thể
hiển thị giờ Việt Nam (UTC+7); đó là cách hiển thị, không đổi ranh giới ngày dữ liệu.
[Tài liệu OKX về bar và ranh giới ngày](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks).

## 4. Chất lượng dự báo có nghĩa gì?

Chất lượng là mức độ gần giá thật của các dự báo trên dữ liệu không dùng để fit/chọn
mô hình trong thí nghiệm. **Chương trình chạy đúng không đồng nghĩa dự báo tốt**.

- **MAE**: trung bình trị tuyệt đối của sai lệch, đơn vị USDT/BTC. MAE 47.949,55 nghĩa
  là mức lệch bình quân trên 36 giá đóng cửa tháng là khoảng 47.950 USDT, không phải
  từng tháng đều lệch bằng đó, không phải sai số năm 2029 đã biết và không phải % đúng.
- **RMSE**: cũng đơn vị USDT, phạt các sai lệch lớn mạnh hơn MAE.
- **sMAPE**: một thước đo sai số theo tỷ lệ, dùng mẫu số gồm cả giá thật và dự báo;
  không diễn giải thành accuracy bằng phép `100% - sMAPE`.
- **Baseline**: cách đơn giản để làm mốc; ở đây luôn dùng giá cuối của đoạn train
  cho cả 36 tháng tương lai. Model phức tạp cần chứng minh giá trị bổ sung so với mốc này.
- **Độ phủ khoảng**: bao nhiêu giá thật nằm trong dải. Phải xem cùng độ rộng và
  interval score; dải quá rộng có thể bao phủ nhiều giá mà không hữu ích.
- **RMSLE validation**: chỉ tiêu đã dùng để chọn phương pháp trước test, nhấn mạnh
  sai lệch trên thang log. Đây là tiêu chí lựa chọn được ghi rõ, khác MAE dùng diễn giải.

## 5. Kết quả hiện tại — không thay chỉ số để làm đẹp

| Phương pháp | MAE (USDT) | RMSE (USDT) | sMAPE (%) | Vai trò |
|---|---:|---:|---:|---|
| Giữ giá gần nhất | 43.678,28 | 48.140,71 | 73,13 | Baseline |
| Log drift | 29.486,35 | 35.109,59 | 43,90 | So sánh thí nghiệm |
| Damped log trend | 47.949,55 | 52.389,53 | 83,88 | Chọn bằng validation trước test |
| Ridge returns | 27.218,68 | 32.801,47 | 32,07 | Tốt nhất trong bảng test này, chưa được coi là winner trên một test độc lập mới |

Phương pháp được chọn có MAE cao hơn baseline khoảng **9,78%**. Điều này cho thấy
kết quả chọn từ các giai đoạn trước chưa khái quát tốt sang giai đoạn test sau.
Chưa xác định được nguyên nhân nhân quả; thay đổi trạng thái thị trường và ít mẫu
tháng là các giả thuyết cần khảo sát, không phải kết luận đã được chứng minh.

Ridge có MAE thấp hơn baseline khoảng **37,68% trên chính đoạn test này**. Có thể
trình bày đây là kết quả so sánh và hướng nghiên cứu đáng thử. Không được chuyển
Ridge thành model được chọn rồi nói rằng cùng đoạn test đó là phép đánh giá hoàn
toàn độc lập cho quyết định chuyển model. Kết quả test đã được nhìn và cần ghi
lịch sử thí nghiệm khi tiếp tục cải tiến.

Dải 95% của model được chọn chứa 36/36 giá thật, nhưng cần xem độ rộng trung bình
trong JSON đánh giá: khoảng **231.926,64 USDT**. 100% độ phủ trong 36 tháng không phải 100% dự báo đúng.
Dải dự báo ở cuối 10/2029 hiện xấp xỉ 6.319–651.998 USDT, cho thấy bất định rất lớn;
không nên chọn một mức giá ở giữa rồi tuyên bố chắc chắn sẽ xảy ra.

**Chưa biết accuracy của dự báo 11/2026–10/2029**, vì chưa có đủ giá thực tế trong
tương lai. Các số liệu trên đều là backtest quá khứ. Classifier +5 phút là bài toán
khác, dùng metric phân loại; không gộp accuracy đó vào chất lượng dự báo giá 3 năm.

## 6. Các cải thiện nên thực hiện tiếp theo

| Ưu tiên | Việc cần làm | Bằng chứng để ghi vào báo cáo |
|---|---|---|
| 1 | Đóng băng thí nghiệm hiện tại, lưu model/config/data/hash và so sánh cả baseline | Có thể tái lập; phân biệt exploratory với kiểm định độc lập |
| 2 | Đánh giá rolling/nested theo nhiều mốc quá khứ, chọn model chỉ bằng phần trước từng mốc | Bảng kết quả theo mốc và năm dự báo; nêu các cửa sổ chồng lấn, không coi là độc lập |
| 3 | Thử Ridge có regularization, ensemble và các mô hình chuỗi thời gian phù hợp | Cùng dữ liệu, horizon và ngân sách tuning; không chỉ giữ lần chạy tốt nhất |
| 4 | Kiểm tra đặc trưng volume, biến động, lợi suất ngày/tuần/tháng | Mỗi đặc trưng chỉ dùng dữ liệu sẵn có tại thời điểm dự báo; đánh giá thêm/bớt đặc trưng |
| 5 | Xem xét nguồn bổ sung hoặc biến giải thích có lịch sử đáng tin cậy | Ghi nguồn, đơn vị, thời điểm công bố; không ghép USD/USDT hoặc các sàn khác nhau thành một chuỗi giả |
| 6 | Đánh giá dải bằng độ phủ + độ rộng + interval score, hiệu chỉnh trên validation | Không mở rộng dải bằng test để đạt độ phủ đẹp; ghi rõ các giả định |
| 7 | Kiểm tra độ ổn định theo horizon và trạng thái thị trường | Báo cáo riêng năm thứ 1/2/3; ghi rõ điều kiện mô hình kém |

Không coi thêm LSTM/Transformer, tăng số cây hay kéo dài biểu đồ là bằng chứng chất
lượng tăng. Cần kết quả đánh giá tương ứng. Nhiều hàng dữ liệu ngày hơn không tạo
thêm nhiều chu kỳ thị trường độc lập; phải giữ đúng ranh giới thời gian khi đánh giá.

## 7. Khung báo cáo sau khi hoàn thành toàn bộ dự án

1. **Bài toán và yêu cầu**: mục tiêu chính 36 tháng, câu hỏi nghiên cứu, phạm vi; tách
   rõ phần trực tiếp, phân tích và mở rộng +5 phút/Futures.
2. **Nguồn và kiến trúc**: sơ đồ đúng code đã chạy, vai trò từng thành phần, nguồn dữ
   liệu và các nhánh thực tế. Không nói module dài hạn đi qua Kafka nếu thực tế tải REST.
3. **Thu thập và mô tả dữ liệu**: kỳ quan sát thật, số dòng/dung lượng/đơn vị, dictionary,
   giới hạn thời gian của sàn và dữ liệu trước/sau tổng hợp.
4. **Làm sạch và kiểm soát chất lượng**: số lượng lỗi/trùng/thiếu, quy tắc đối soát,
   ví dụ xử lý, kiểm idempotence và không nội suy ngầm.
5. **Phân tích và trực quan**: biểu đồ có câu hỏi, nhận xét, đơn vị, lịch UTC; giải thích
   được từ nến ngày đến nến tháng và đường close.
6. **Phương pháp dự báo**: mục tiêu, đặc trưng, chia thời gian, validation/test, baseline,
   tham số, dải bất định và cách tái lập.
7. **Thực nghiệm và đánh giá**: tất cả phương pháp, nhiều horizon, sai số, độ phủ/độ rộng,
   phân tích model kém; không báo kết quả tương lai như đã biết.
8. **Kiểm thử và vận hành**: test đã thực sự chạy, log, độ trễ, lỗi phục hồi, hướng dẫn
   chạy trên máy mới; không ghi “100% dự án pass” khi vẫn còn suite chưa qua.
9. **Kết luận và giới hạn**: mục tiêu đạt/chưa đạt, nguyên nhân đã chứng minh hoặc giả
   thuyết, hướng phát triển và phân công đóng góp thật.

## 8. Đoạn nhận xét có thể dùng làm bản nháp

> Nhóm thu thập dữ liệu ngày BTC-USDT từ OKX, làm sạch và tổng hợp thành 105 tháng
> hoàn tất. Bài toán dự báo giá đóng cửa tháng có horizon 36 tháng. Các phương pháp
> được chọn trên validation theo thời gian trước đoạn test 11/2023–10/2026. Phương
> pháp damped log trend được validation chọn nhưng có MAE test 47.949,55 USDT, cao
> hơn baseline 43.678,28 USDT khoảng 9,78%. Vì vậy, thí nghiệm này chưa chứng minh
> ưu thế dự báo so với baseline. Ridge cho sai số thấp hơn trong cùng phép so sánh,
> là hướng thử nghiệm tiếp theo; cần thiết kế đánh giá tránh dùng lại test để lựa
> chọn rồi công bố như kết quả độc lập. Kết quả tương lai được thể hiện kèm khoảng
> bất định và các giới hạn của dữ liệu cũng như phương pháp.

Đây là đoạn mô tả đúng **hiện trạng**, cần cập nhật sau mỗi thí nghiệm cuối cùng.
Đề của cô chưa cung cấp rubric điểm chi tiết hay ngưỡng accuracy; không thể cam kết
10 điểm chỉ từ một chỉ số. Mục tiêu là đủ yêu cầu, quy trình đúng, minh chứng tái
lập và bảo vệ được kết quả, kể cả những kết quả chưa như mong muốn.

## Minh chứng cho lần chỉnh phần giải thích

- Giữ nguyên snapshot và các dự báo `20261108T142006113873Z`; không sửa model hoặc
  các số đo thí nghiệm để làm đẹp bảng.
- Đã đối chiếu 9 nến ngày UTC trên web (30/10–07/11/2026) với nguồn Parquet dài hạn:
  OHLC và volume khớp. Kết nối PostgreSQL của web được kiểm tra trả timezone UTC.
- 24/24 test module dài hạn và 36/36 kiểm tra hồi quy web qua trong lần chỉnh này.
- Đã thử xem nến tháng, đổi trục logarithm/thông thường, bật/tắt dải và mở bảng so sánh.
- Có CSV bốn phương pháp và JSON chứa MAE, RMSE, sMAPE, độ phủ, độ rộng và interval score.

## Tài liệu phương pháp

- Hyndman & Athanasopoulos, [Forecasting: Principles and Practice, Chương 5](https://otexts.com/fpp3/toolbox.html): training/test, baseline, MAE/RMSE, rolling validation, khoảng dự báo và interval score.
- [OKX API documentation](https://www.okx.com/docs-v5/en/): lịch nến UTC, schema và cờ hoàn tất.
