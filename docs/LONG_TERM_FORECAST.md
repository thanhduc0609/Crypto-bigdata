# Dự báo giá đóng cửa BTC-USDT trong 36 tháng

Mục tiêu đồ án: từ lịch sử quan sát được, dự báo **giá đóng cửa cuối mỗi tháng UTC**,
đơn vị USDT/BTC, cho 36 tháng sau tháng cuối đầy đủ. Đây là bài toán hồi quy chuỗi
thời gian riêng; mô hình XGBoost phân loại hướng giá +5 phút vẫn giữ nguyên.

Giải thích dễ hiểu về cách chia 36 tháng, hai dạng biểu đồ, ý nghĩa metric, kết quả
hiện tại và khung báo cáo tổng thể nằm trong [REPORT_GUIDE.md](REPORT_GUIDE.md).

## Dữ liệu và nguồn

- Nguồn duy nhất: API công khai của OKX, `GET /api/v5/market/history-candles`.
- Tham số: `instId=BTC-USDT`, `bar=1Dutc`, `limit=100`; phân trang ngược bằng `after`.
- [Tài liệu nguồn OKX](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks-history).
- Lưu từng trang raw và thời điểm lấy; retry có giới hạn, phát hiện phân trang bị kẹt.
- Chỉ chấp nhận `confirm=1`, thời gian UTC đúng đầu ngày và ngày đã kết thúc.
- OHLC phải hữu hạn, dương và đúng hình học nến; volume/turnover không âm.
- Volume BTC ở vị trí 5, turnover USDT ở vị trí 7; không tự suy ra buy/sell volume.
- Bỏ trùng hoàn toàn; **dừng nếu cùng ngày có hai giá trị xung đột**.
- **Không nội suy ngày thiếu**. Thiếu ngày hoặc thiếu ngày hoàn tất mới nhất thì dừng,
  không thay snapshot đã công bố.
- Tổng hợp tháng: open đầu tháng, high/low cực trị, close cuối tháng, tổng volume/turnover.
  Chỉ dùng tháng đủ mọi ngày thật. Không đổi nến ngày thành hàng triệu nến phút giả.

Lần chạy 08/11/2026: 3.224 hàng raw, bỏ 1 nến chưa đóng, còn **3.223 nến ngày**
từ **11/01/2018 đến 07/11/2026**, 0 ngày thiếu, 0 dòng nội suy.
Tháng 01/2018 chỉ có 21/31 ngày và tháng 11/2026 mới có 7/30 ngày nên bị loại.
Còn **105 tháng liên tiếp, 02/2018–10/2026**. Không suy diễn rằng OKX cung cấp đủ
dữ liệu từ ngày bắt đầu yêu cầu 01/01/2017; phạm vi quan sát thật được ghi trong quality.json.

## Quy trình đánh giá chống rò rỉ

1. Giữ nguyên **36 tháng cuối** làm test, không đưa vào chọn mô hình.
2. Trên phần trước test: expanding-window validation có horizon **36 tháng**,
   ít nhất 24 tháng train, stride 6 tháng, có thêm origin cuối nếu cần.
3. Chọn mô hình theo RMSLE trung bình qua các fold validation. Các fold chồng lấn,
   **không phải các mẫu độc lập**. Các tham số phương pháp cố định trong code.
4. Đóng băng lựa chọn; dự báo **một đường đủ 36 tháng**, không cập nhật bằng actual
   trong test. So sánh cả bốn phương pháp nhưng không đổi winner theo kết quả test.
5. Sau khi ghi kết quả test, fit lại **cùng phương pháp đã chọn** trên toàn bộ 105
   tháng để tạo dự báo 11/2026–10/2029.

Với snapshot hiện tại: train trước test 02/2018–10/2023 (69 tháng), test
11/2023–10/2026 (36 tháng). Validation có 3 fold (24, 30, 33 tháng train, mỗi fold
36 tháng validation), tất cả kết thúc không muộn hơn 10/2023.

## Phương pháp

| Phương pháp | Mô tả |
|---|---|
| naive_last | Baseline: mọi tháng tương lai bằng giá tháng gần nhất |
| log_drift | Giá log cuối cộng trung bình lợi suất log train × số tháng |
| damped_log_trend | Fit đường thẳng trên log giá 24 tháng gần nhất; giảm ảnh hưởng xu hướng bằng hệ số 0,9 theo từng bước |
| ridge_returns | StandardScaler + Ridge alpha=10; dự báo đệ quy lợi suất log với các lag 1,2,3,6,12 tháng |

Không tăng độ phức tạp thành deep learning với chỉ khoảng 100 quan sát tháng.
Ridge chỉ có ít mẫu trong các fold sớm; đây là hạn chế phải nêu khi bảo vệ.

## Khoảng bất định

Tính residual log dự báo 1 bước bằng rolling origin trên **riêng train**. Center
residual và lấy mẫu circular block 3 tháng, cộng dồn nhiễu log qua horizon,
3.000 simulation, seed 42. Dải danh nghĩa 80%/95% lấy từ các phân vị mô phỏng.
Tính lại dải bằng toàn bộ lịch sử chỉ sau bước đánh giá để dự báo tương lai.

Đây là **khoảng xấp xỉ có điều kiện theo mô hình**, chưa được hiệu chỉnh độ phủ;
không bảo đảm bao trùm các cú sốc hoặc thay đổi chế độ thị trường. Báo cáo độ phủ
quan sát trên test nhưng không dùng test để chỉnh độ rộng. Dải rộng không chứng
minh model tốt. Trục biểu đồ log được ghi rõ để hiển thị các dải có phạm vi lớn.

## Kết quả thực nghiệm 08/11/2026

Validation chọn `damped_log_trend`, RMSLE trung bình 0,8807; baseline 1,0462.

| Phương pháp | MAE test (USDT) | RMSE test (USDT) | sMAPE (%) | RMSLE |
|---|---:|---:|---:|---:|
| Baseline naive_last | 43.678,28 | 48.140,71 | 73,13 | 0,8269 |
| log_drift | 29.486,35 | 35.109,59 | 43,90 | 0,5172 |
| damped_log_trend — chọn bằng validation | 47.949,55 | 52.389,53 | 83,88 | 0,9624 |
| ridge_returns | 27.218,68 | 32.801,47 | 32,07 | 0,3730 |

**Phương pháp được chọn chưa vượt baseline trên test.** Không đổi sang Ridge chỉ
vì đã nhìn thấy test tốt hơn: làm vậy sẽ dùng test để chọn mô hình. Muốn thử cải
tiến cần ghi rõ đó là thí nghiệm mới và có một đoạn đánh giá tương lai mới hoặc
thiết kế nested walk-forward phù hợp. Không coi các con số trên là cam kết dự báo.

Pipeline đã đáp ứng phạm vi dữ liệu nhiều năm, đầu ra 36 tháng và đánh giá đúng
horizon; **chưa đáp ứng mục tiêu chứng minh dự báo chính xác/bền vững**. Chỉ một
cửa sổ test dài hạn, không có biến vĩ mô, và lịch sử sàn ngắn là các giới hạn.

## Chạy lại

Từ thư mục repo, dùng Python môi trường dự án:

```powershell
.\.venv\Scripts\python.exe scripts/run_long_term.py
```

Ghi thêm snapshot bất biến lên S3 đang cấu hình (mỗi run có prefix riêng):

```powershell
.\.venv\Scripts\python.exe scripts/run_long_term.py --upload-s3
```

Tái lập từ raw đã lưu, không gọi OKX:

```powershell
.\.venv\Scripts\python.exe scripts/run_long_term.py --offline-from data/long_term/runs/<run-id>
```

Không cần key OKX để tải dữ liệu public. Key AWS chỉ cần khi dùng `--upload-s3`;
đọc từ môi trường hoặc `.env` đã ignore, không ghi vào Git.

Mỗi run lưu `raw_rows.json`, các trang raw nếu tải mới, `source.json`,
`daily.parquet`, `monthly.parquet`, `quality.json`, `forecast.json`, `forecast.csv`,
`backtest.csv`. JSON chứa SHA-256 của raw/dataset/config/implementation, commit gốc,
cờ có thay đổi chưa commit và phiên bản thư viện. Có thể tái dựng mô hình từ model
name, tham số đã ghi, lịch sử và phiên bản code; không tải pickle không tin cậy.

`data/long_term/current.json` chỉ được đổi bằng atomic replace sau khi toàn bộ run
thành công (và upload thành công nếu yêu cầu S3). Có khóa chống publish song song.
GET API không train, không gọi OKX/S3, kiểm checksum và chỉ đọc snapshot local.
Mặc định mỗi lần chạy mới tải lịch sử lại; chưa có scheduler tự động hoặc incremental
refresh cho module dài hạn này.

## Dashboard và API

- Trang chính có khối **Dự báo BTC trong 3 năm**, biểu đồ tương lai và kiểm định,
  số liệu baseline, bảng 36 tháng, nguồn và giới hạn.
- `GET /api/forecast/long-term`: kết quả + history + backtest + quality + provenance.
- `GET /api/forecast/long-term/export`: CSV 36 dòng dự báo, 80% và 95% bands.
- 404 nếu chưa có snapshot; 503 nếu snapshot sai checksum hoặc schema.
- Module này dùng Parquet/JSON local và snapshot S3 riêng; chưa có bảng PostgreSQL
  monthly hoặc tích hợp vào batch medallion cũ. Không giả định đã đi qua Kafka.

## Kiểm thử

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_long_term_forecast.py -v
node --check web/static/js/long_term.js
```

20 test kiểm ngày thiếu, nến lỗi/trùng/xung đột, tháng thiếu, năm nhuận, source cũ,
phân trang kẹt, chống rò rỉ holdout, đủ horizon, khoảng bất định tái lập, checksum,
CSV/API, đường dẫn sai và giữ snapshot cũ khi run lỗi. Đây không phải kết luận toàn
bộ các test cũ trong repo đã được sửa hoặc đã chạy lại.

## Minh chứng chạy thực tế và kiểm tra hồi quy

Snapshot đã công bố: `20261108T142006113873Z`. Các kiểm tra bổ sung đã qua:

- 20/20 test mới cho module dài hạn.
- Hồi quy: orderflow/worker 14/14; đánh giá dự báo 8/8; Futures 28/28;
  web terminal/API cũ 36/36. Tổng cộng 86 kiểm tra hồi quy trong phạm vi này.
- Chạy lại toàn pipeline từ raw vào thư mục riêng: forecast, backtest, validation,
  evaluation và các hash dataset/config/implementation khớp hoàn toàn.
- Đọc lại S3 thật: forecast JSON, raw JSON, daily Parquet (3.223 dòng) và monthly
  Parquet (105 dòng) khớp SHA-256 với file local.
- HTTP thật: trang chính, API dài hạn, CSV dài hạn, health và prediction cũ đều 200;
  API có 36 dự báo và CSV có 36 dòng dữ liệu.
- Đã mở trình duyệt, chuyển chế độ kiểm định/tương lai và mở bảng đủ 36 tháng.
- File model XGBoost +5 phút giữ SHA-256 ban đầu
  `6e53a2155df0919b4a24d3d70d90eb3f26beec2743ead0fa51355694aa39123b`.

GitHub đã được fetch và đối chiếu đầu công việc: `main` và `origin/main` cùng
commit `6e6b185`, không có commit mới để pull. Phần bổ sung hiện là thay đổi local,
chưa commit/push. Các lỗi test/S3/API khác trong báo cáo tổng thể trước vẫn là các
đầu việc riêng, không được tính là đã sửa trong bước dự báo 3 năm này.

Lần làm rõ giao diện/báo cáo tiếp theo: thêm xem nến tháng và trục giá thông thường,
giải thích 69/36/105 tháng, bảng so sánh và CSV đánh giá. Nến ngày live thống nhất
`1Dutc`, DB web dùng UTC; 9 nến OHLCV khớp nguồn dài hạn. 24/24 test module và 36/36
test web qua. Các dự báo và số đo model của snapshot trên được giữ nguyên.
