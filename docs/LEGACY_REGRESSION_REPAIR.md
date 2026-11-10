# Sửa ba bộ test legacy và bảo vệ việc chấm kết quả dự báo

Đầu việc hoàn thành ngày 08/11/2026. Phạm vi: `test_pipeline_e2e.py`,
`test_reconciliation.py`, `test_inference_reliability.py` và lỗi thực tế trong
`ml/predict.py` do các regression mới phát hiện. Chưa xử lý mục API cache/retry/fallback.

## 1. Nguyên nhân các bộ test cũ không qua

| Bộ test | Vấn đề cũ | Cách sửa |
|---|---|---|
| Pipeline | Import `PARTITION_MAP` không còn tồn tại; kiểm malformed payload bằng điều kiện viết lại trong test; fixture inference thiếu nguồn/đóng nến và dùng ngày cố định đã cũ | Gọi callback ingestion thật với Kafka producer giả; kiểm record thực sự được gửi; fixture synthetic có provenance, nến đã đóng, mốc phút UTC và clock kiểm soát |
| Reconciliation | Kỳ vọng stream volume lớn ghi đè REST; coi REST volume nhỏ hơn là dữ liệu cũ | Kiểm REST có quyền ưu tiên, độc lập thứ tự đến; stream giữ chất lượng theo trade_count/đóng nến; REST cùng nguồn được sửa theo bản cập nhật, không suy độ mới từ volume |
| Inference reliability | Kỳ vọng bỏ qua nến có `is_closed=False` trong khi quality gate hiện từ chối cả input; dùng source stream; sửa target thành quá khứ lệch giây | Kiểm cả chấp nhận và từ chối đúng hợp đồng; mô phỏng prediction clock trong quá khứ, giữ target đúng feature+5 phút; kiểm SQL thật chỉ chấm sau khi đến hạn |

Chuyển thành các `unittest.TestCase` độc lập, để một assertion hỏng không làm mất
bằng chứng về những tình huống còn lại. Giữ các hàm entrypoint cũ trả bool để các
runner trước vẫn gọi được. Không bỏ assertion để báo xanh và không sửa logic REST
canonical để chiều theo kỳ vọng volume cũ.

## 2. Các lỗi production được tái hiện, sau đó sửa

Sau khi cập nhật fixture và hợp đồng, chạy regression mới **trước khi sửa evaluator**:
pipeline 7/7, reconciliation 7/7; inference có 6 failure/subtest được tái hiện:

1. Có thể lấy nến `realtime_stream` trong DW làm giá actual dù chưa canonical REST.
2. Có thể lấy DataFrame actual thiếu trường `source`.
3. Có thể chấm giá actual NaN, Infinity, 0 hoặc âm.

Đã sửa `evaluate_unresolved_predictions`:

- Input actual phải có nguồn REST xác nhận rõ ràng (`historical_rest` hoặc
  `okx_rest`) và cờ đóng nến boolean đúng.
- Query actual từ DW thêm điều kiện `source='historical_rest'`, cùng timestamp
  chính xác và `is_closed=TRUE` như trước.
- Giá actual lấy từ DataFrame hoặc DW đều phải hữu hạn và lớn hơn 0. Bản ghi
  không hợp lệ không được dùng để ghi `actual_direction`/`is_correct`.
- Giữ nguyên kiểm tra target đã kết thúc (`target_timestamp + 60 seconds`),
  không dùng tolerance thời gian và không đổi horizon để test qua.

Đây là lỗi phát hiện bằng fixture trong DB test. Không suy diễn rằng dữ liệu live
trước đây chắc chắn đã bị chấm sai; không tự sửa hàng loạt lịch sử dự báo của người dùng.

## 3. PostgreSQL test riêng và kiểm tra an toàn

`test_support/postgres_sandbox.py` tự tạo DB mới, tên dạng:

```text
crypto_test_<suite>_<UTC timestamp>_<random>
```

- Không nhận tên DB live làm đích test. Kiểm identity và `current_database()` trên
  từng connection trước khi dùng.
- Khởi tạo schema thật trong DB mới; không chạy init/truncate trên DB ứng dụng.
- Mỗi test reset các fact trong đúng DB riêng, để không phụ thuộc thứ tự chạy.
- Các alias connection trong predict/sink/dataset được override trong tiến trình
  test và khôi phục sau suite. Web/producer/streaming ở tiến trình khác không bị patch.
- Marker sở hữu được kiểm trước khi drop. Chỉ drop DB tạo bởi instance này; không
  dùng force-drop hoặc kết thúc connection của ứng dụng.
- Sau khi đóng các connection, drop DB test và kiểm tra nó không còn trong catalog.
- Không lấy thêm dữ liệu thật từ OKX, không ghi S3 và không gửi record vào Kafka
  thật trong ba suite này. Các fixture đều được ghi rõ là synthetic.

Role PostgreSQL trong `.env` cần quyền tạo database. Nếu thiếu quyền hoặc không có
PostgreSQL, suite báo lỗi; không tự chuyển thành SKIP/PASS hoặc quay sang DB live.

Các kiểm tra helper offline xác minh: tên suite sai, thay đích sang DB live,
connection đi nhầm DB, marker khác chủ, context chưa mở và reset khi chưa active.

## 4. Kết quả cuối cùng

| Bộ kiểm tra | Kết quả |
|---|---|
| Pipeline regression | **7/7** |
| Canonical reconciliation | **7/7** |
| Inference reliability | **10/10** |
| PostgreSQL test-sandbox guards | **6/6** |
| Prediction evaluation cũ | **8/8** |
| Orderflow/worker cũ | **14/14** |
| Tổng trong phạm vi lần này | **52 kiểm tra qua, không skip** |

Ba lần chạy đối chiếu trước/trong/sau sửa tạo tổng cộng **9 DB test**; đã đọc
catalog PostgreSQL độc lập và xác nhận không còn DB nào trong 9 DB đó.
Toàn bộ 5 file artifact active giữ nguyên SHA-256, gồm model và manifest.

Lượt final: `20261108T153441Z-5f1c7bccca07`. Báo cáo và log giữ nguyên cả lần trước
sửa, lần trung gian và lần final để truy vết. Lần trung gian còn một assertion
giá do fixture có nhiều chữ số hơn NUMERIC warehouse lưu; fixture target đã đổi
về giá theo cent, vẫn giữ assertion so giá chính xác, không nới tolerance để bỏ lỗi.

Pipeline suite giữ kiểm tra query local dưới 100 ms như trước. Đây là phép đo trên
DB fixture nhỏ, không phải benchmark tải hoặc cam kết SLA toàn hệ thống.

## 5. Chạy lại

Từ thư mục repo, dùng Python của môi trường đã cài dependencies:

```powershell
python scripts/run_legacy_regressions.py
```

Hoặc chạy từng bộ:

```powershell
python tests/test_pipeline_e2e.py
python tests/test_reconciliation.py
python tests/test_inference_reliability.py
```

Kiểm tra helper và các unit regression liên quan:

```powershell
python -m unittest discover -s tests -p test_postgres_test_safety.py -v
python -m unittest discover -s tests -p test_prediction_evaluation.py -v
python -m unittest discover -s tests -p test_orderflow_and_worker.py -v
```

Trên terminal Windows ghi log, có thể đặt `PYTHONIOENCODING=utf-8` để giữ tiếng Việt.
Metadata từng DB ở `data/test_runs/postgres_<suite>/<db-name>/result.json`; summary
của runner ở `data/test_runs/legacy-reports/<run-id>/summary.json`. Không chứa password.

## 6. Áp dụng và giới hạn

Đã khởi động lại riêng web để worker dùng evaluator mới. Health trả 200, DB kết
nối và worker còn chạy. Không thay model, không huấn luyện lại và không tuyên bố
accuracy của model tăng lên nhờ sửa test.

Mặc dù file giữ tên `test_pipeline_e2e.py`, ingestion trong suite này dùng callback
thật với producer giả, features/model thật và PostgreSQL thật. Nó không kiểm một
phiên WS→Kafka→Spark→S3 thực tế. Full streaming medallion E2E là bài riêng chưa chạy
lại trong lượt này; kết quả ở đây không đồng nghĩa toàn bộ test dự án đã qua.

Các thay đổi vẫn ở local, chưa commit/push GitHub. Đầu việc tiếp theo theo thứ tự:
API nến gián đoạn, timeout/retry/cache và fallback có nguồn/timestamp rõ ràng.
