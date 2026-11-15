# Vận hành local, demo và phạm vi kiểm chứng

Docker đã thống nhất vào `docker-compose.yml`, project `crypto-bigdata`.
Máy đã chạy bộ cũ cần làm theo [chuyển dữ liệu Docker](DOCKER_SETUP.md) một lần.
Giữ các giá trị `*_VOLUME_NAME`/`*_IMAGE` mà công cụ chuyển ghi vào `.env` riêng.

Cập nhật giao diện 10/11/2026: `/` hoặc `/du-bao-3-nam` mở dự báo dài hạn;
`/thi-truong` mở thị trường trực tiếp. Hai trang có điều hướng chung.
Xem [thu phóng, bố cục và chụp ảnh minh chứng](RESEARCH_UI.md).

Các entry point và checkpoint mới đều nằm trong repo, dưới `scripts/` và
`data/runtime/`. Khi đổi địa chỉ S3, công cụ chuyển giữ offset đã commit để tạo
checkpoint cho đích mới; checkpoint cũ được giữ làm bằng chứng phục hồi. Xem
[cấu trúc S3](S3_LAYOUT.md), không khởi động bằng wrapper bên ngoài repo.

## Chạy trên máy hiện tại

Mở Docker Desktop, mở PowerShell tại thư mục repo:

```powershell
.\scripts\setup_env.ps1 -StartDemo -Streaming
# Lần gọi tiếp theo nhận ra các tiến trình đang chạy, không tạo bản trùng.
```

Truy cập `http://127.0.0.1:5001/`. Script chờ PostgreSQL/web sẵn sàng. Spark và
batch cần thêm thời gian làm nóng; trước thuyết trình nên chạy trước 3–5 phút và
xem các số đo thực tế ở bảng Luồng dữ liệu & chất lượng.

```powershell
# Dừng ứng dụng, giữ container, volume và checkpoint:
.\scripts\setup_env.ps1 -StopDemo
```

`setup_env.ps1 -StartDemo` không recreate các container sẵn có. Không xóa Kafka,
checkpoint hay volume để chữa lỗi nếu chưa có phương án backup/migration.
Volume Kafka trong compose dành cho lần tạo container mới; container cũ hiện có
được giữ nguyên. Kiểm tra phục hồi đã thực hiện trên runtime hiện tại, không phải
bài thử xóa/recreate broker hoặc mô phỏng hỏng ổ đĩa.

## Chuẩn bị môi trường khác

- Python 3.11; Java 8/11/17 phù hợp Spark 3.5; Docker Desktop và PowerShell.
- Bản đã kiểm chứng sử dụng Java 8 trên Windows. Thiết lập JAVA_HOME nếu tự tìm
  Java không chọn đúng; không cần hạ chính sách bảo mật hệ điều hành.
- Tạo venv mới khi chưa có và cài `requirements.txt` (phiên bản thực
  cài trên máy kiểm thử). `pip check` trên máy này không phát hiện dependency hỏng.
- Khi chưa có `.env`, sao chép `.env.example`, điền thông tin thật ở
  máy cá nhân. Không ghi đè `.env` đang dùng và không đưa khóa vào Git/báo cáo.
- Dùng `S3_PROJECT_PREFIX=crypto`. Bronze/Silver/Gold nằm trong cấu trúc chung
  của nhóm; [hướng dẫn S3](S3_LAYOUT.md) mô tả chuyển địa chỉ, checkpoint và writer
  riêng cho từng máy. Quyền AWS phải phù hợp bucket/prefix được nhóm cho phép.
- Các file nghiên cứu đã khôi phục vẫn bị gitignore. Repo có gói phiên bản ở
  `artifacts/research/`; chạy `python scripts/quality/research_artifacts.py restore`, rồi
  `python scripts/quality/research_artifacts.py verify` trước khi mở web trên bản clone mới.
  Gói không khôi phục DB live/checkpoint hoặc toàn bộ raw stream/audit trên S3.
- `CANONICAL_SNAPSHOT_STORAGE=local` trong env mẫu dùng bản canonical nghiên cứu
  đã kiểm checksum, độc lập với bucket S3 vận hành. Cấu hình cũ không đặt biến này
  vẫn giữ chính sách `inherit`. Không đổi URI nguồn/hash để ghép nhầm phiên.
- Muốn thu thập/nghiên cứu lại, tạo phiên mới; dữ liệu tải mới không được gọi là
  cùng snapshot đã công bố. Xem `artifacts/research/README.md`.
- Chưa kiểm thử trên một máy vật lý thứ hai; không tuyên bố mọi môi trường đều
  chạy được mà không cần kiểm tra Java, quyền AWS và cấu hình.

## Dịch vụ và nơi xem log

| Thành phần | Entry point | Vai trò |
|---|---|---|
| Web | scripts/local_service.py web | Flask, order flow trực tiếp và worker dự báo |
| Producer | scripts/local_service.py producer | OKX text ping/pong, ghi Kafka |
| Streaming | scripts/local_service.py spark | Bronze, speed layer và DW; giữ checkpoint |
| Batch | scripts/run_operational_batch.py --watch --interval 300 | Phiên Silver/Gold mới, đồng bộ DW |
| Monitor | scripts/monitor_demo.py --watch | Đo Kafka/commit offsets, DW và phiên batch |

Log/PID ở `data/runtime/`. Checkpoint cũ tương thích được dùng lại; nếu không có,
checkpoint mới ở `data/runtime/checkpoints/`. Ivy cache cũ chỉ là tối ưu tùy chọn;
có thể cấu hình SPARK_IVY_DIR, mặc định mới là `data/ivy`.

Web/producer/Spark có supervisor tự chạy lại tiến trình con, backoff 5–60 giây.
Spark dùng awaitAnyTermination để không che giấu sink DW bị lỗi sau một query
khác còn sống. Các phép đo checkpoint mới xác nhận tiến độ; PID còn tồn tại không
đủ chứng minh luồng còn nhận dữ liệu.

Batch chạy không chồng lấn. Mỗi lượt ghi vào `medallion-runs/<run-id>` riêng,
kiểm tra số Silver bằng tổng trade_count Gold, công bố manifest cuối cùng rồi
cập nhật last-success. Snapshot canonical của ML không bị ghi đè. Nến REST đã
đóng vẫn được ưu tiên hơn nến stream trong DW.

Đọc Bronze theo Spark commit log, bỏ orphan; cache có ETag/SHA và gộp đầu vào
local trước khi Spark làm sạch. Footer schema Spark cũ được bỏ khi thêm lineage,
tránh làm mất các cột nguồn trong lần Spark đọc kế tiếp.

## Đã kiểm chứng

- 201 test unit/hồi quy thuần, 24 kiểm tra DB legacy cô lập, 36 kiểm tra web/API,
  5 test Spark cleaning thật trên fixture riêng: 266 kiểm tra trong phạm vi.
- 18 kiểm tra luồng thật: Kafka record → committed Bronze → Silver lineage →
  Gold OHLCV → DW canonical/REST → outcome dự báo và API.
- Lượt soak đầu: 600 request, 2 lỗi đầu do web chưa mở cổng. Giữ lại log này.
  Sau thêm readiness: 600/600 request thành công trong 5 phút, 2 req/s, tối đa
  4 đồng thời; p95 khoảng 774 ms, max khoảng 2,59 s.
- Resource sampler: peak RSS các tiến trình demo khoảng 1.878 MiB; peak CPU quan
  sát khoảng 10,8% tổng máy. Khoảng lấy mẫu và PID sống qua hai mẫu hạn chế độ chính
  xác; không coi đây là giới hạn công suất hay bằng chứng không có memory leak.
- Crash Spark con: tự khởi động và xử lý backlog khoảng 103 s; producer vẫn ghi
  Kafka, không có khóa nến trùng. Crash producer: kết nối lại khoảng 18 s.
- Không giả lập ngắt mạng toàn máy, hỏng đĩa, chạy nhiều ngày hoặc tải công nghiệp.
  Không khẳng định đã thu đủ mọi giao dịch sàn trong lúc nguồn bị gián đoạn.
- UI kiểm tra desktop/mobile, khung nến, forecast/backtest, bands, lọc và CSV.
  Bản CSV mẫu qua nút trên trình duyệt khớp ngưỡng lọc.

## Các hạn chế không được bỏ khỏi báo cáo

- Dự báo 5 phút chưa chứng minh lợi thế ổn định; giữ model XGBoost đang chạy.
- Dự báo 36 tháng gốc thua baseline theo MAE. Nghiên cứu nested bổ sung thắng
  baseline trên 3 cửa sổ hồi cứu nhưng chúng chồng lấn và outcome đã từng được xem.
- Khi chọn lại trên toàn bộ 105 tháng, policy nghiên cứu chọn baseline; không
  chứng minh thị trường tương lai sẽ đi theo đường dự báo hoặc có lợi nhuận.
- Dải 95% là danh nghĩa, còn rất rộng; độ phủ 100% không phải accuracy 100%.
- Batch và lịch cục bộ chạy khi ứng dụng được khởi động, không phải dịch vụ cloud
  hay Windows task đã đăng ký tự bật sau reboot hệ điều hành.
- Một số JVM Windows có warning dọn JAR lúc kết thúc. Xem exit code, manifest và
  kiểm chứng đầu ra; không suy ra dữ liệu hỏng chỉ từ warning dọn file tạm.

## Lệnh kiểm tra khi cần

```powershell
.venv/Scripts/python.exe scripts/demo/monitor_demo.py
.venv/Scripts/python.exe scripts/demo/audit_demo_pipeline.py
.venv/Scripts/python.exe scripts/ml/verify_short_term_experiment.py data/ml_experiments/20261109T074901815206Z
```

Các crash test chỉ dùng khi chủ động kiểm thử, không chạy giữa buổi thuyết trình:

```powershell
.venv/Scripts/python.exe scripts/demo/check_demo_recovery.py spark
.venv/Scripts/python.exe scripts/demo/check_demo_recovery.py producer
```

Sườn báo cáo được bàn giao để duyệt trước. Chưa viết thay nội dung từng chương;
sau khi thống nhất sườn, thực hiện từng mục nhỏ với nguồn và minh chứng tương ứng.
