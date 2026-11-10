# Tiến độ cải thiện dự án

Quy tắc làm việc theo yêu cầu: mỗi lượt xử lý một đầu việc, kiểm chứng và báo kết
quả trước khi chuyển sang đầu việc khác. Không gộp các mục còn lại vào một lượt.

| Đầu việc | Trạng thái hiện tại |
|---|---|
| **Tích hợp và tái lập từ Git — 10/11/2026** | Gói 41 artifact có SHA-256; CLI restore/verify không ghi đè dữ liệu khác; canonical local tách S3 vận hành; bỏ runtime lock khỏi Git; README thống nhất, exporter portable và workflow CI Windows/Linux |
| **Giao diện hai trang — 10/11/2026** | `/du-bao-3-nam` và `/thi-truong`; zoom/pan/preset/tooltip/bàn phím, nến tháng thật và đường dự báo, ranh giới/bất định rõ, Futures tiếp tục gập |
| Dữ liệu nhiều năm và pipeline dự báo 36 tháng | Giữ thí nghiệm gốc; bổ sung nested 3 mốc đủ 36 tháng, giảm MAE hồi cứu 24,67%–36,59% so baseline; outcome đã xem và cửa sổ chồng lấn, chưa chứng minh tương lai |
| Làm rõ chia tập, biểu đồ và metric | Đã bổ sung giải thích, nến tháng, bảng so sánh và khung báo cáo |
| **Cô lập test S3** | **Hoàn thành 08/11/2026**: 28 test guard, 2 lượt Parquet và 5 kiểm tra registry trên AWS qua; vùng dùng chung giữ nguyên |
| **Sửa ba bộ test legacy chưa qua** | **Hoàn thành 08/11/2026**: 7/7 pipeline, 7/7 đối soát, 10/10 inference; sửa lỗi chấm actual; thêm 28 kiểm tra liên quan qua |
| **API nến gián đoạn, cache/retry/fallback** | **Bàn giao 09/11/2026**: 39/39 test nến + 36/36 kiểm tra web qua; timeout/retry hữu hạn, cache/gom request, fallback DW; sửa query giờ/ngày, hiển thị nguồn/cũ/thiếu |
| **Bằng chứng dữ liệu/làm sạch toàn hệ thống** | **Bàn giao 09/11/2026**: manifest S3/local/DW, đọc commit log, replay cleaning thật, đối soát độ chính xác số và lineage; còn các thiếu hụt dữ liệu phải xử lý riêng |
| **Xử lý phát hiện dữ liệu trước khi cải thiện ML** | **Bàn giao 09/11/2026**: snapshot REST 154.351 phút liên tục, xác nhận đóng nến từ raw; S3/DW khớp toàn bộ miền đã chốt, backup và phiên bản riêng; 100 kiểm tra trong phạm vi/hồi quy qua |
| **Đánh giá/cải thiện ML trên phiên đã khóa** | **Bàn giao 09/11/2026**: 8 ứng viên, 4 fold thời gian, test 17.274 mẫu; Logistic mới BA 51,36% so incumbent 51,16%; CI chứa 0, chỉ thắng 2/4 fold nên giữ live model; 49 test + 16 kiểm chứng qua |
| **Lịch batch S3, Windows, phục hồi và tải** | **Bàn giao 09/11/2026**: entry point trong repo, Silver/Gold theo phiên, monitor, supervisor/backoff, giữ checkpoint; 18 kiểm chứng luồng thật, 600/600 request sau readiness; crash Spark/producer tự phục hồi, không có khóa nến trùng |
| **Dashboard và bộ demo** | **Bàn giao 09/11/2026**: tiêu đề học thuật, Futures gập theo lựa chọn của người dùng, chất lượng/nguồn minh bạch, biểu đồ local không phụ thuộc CDN, lọc/CSV và ảnh demo; 266 kiểm tra trong phạm vi |
| **Sườn báo cáo** | Đã lập chi tiết chương/mục, hình/bảng, minh chứng và thứ tự viết. Chờ người dùng duyệt sườn, sau đó mới viết từng mục nhỏ; chưa nhận là báo cáo học thuật hoàn chỉnh |

Chi tiết ML vừa hoàn thành: [SHORT_TERM_ML_EVALUATION.md](SHORT_TERM_ML_EVALUATION.md).
Vận hành/demo: [LOCAL_DEMO_HANDOVER.md](LOCAL_DEMO_HANDOVER.md).
Phiên dữ liệu đã khóa: [CANONICAL_DATA_REPAIR.md](CANONICAL_DATA_REPAIR.md).
Kiểm kê trước khi sửa: [DATA_QUALITY_AUDIT.md](DATA_QUALITY_AUDIT.md).
Việc API trước đó: [CANDLES_API_RELIABILITY.md](CANDLES_API_RELIABILITY.md).
Việc legacy trước đó: [LEGACY_REGRESSION_REPAIR.md](LEGACY_REGRESSION_REPAIR.md).
Việc S3 trước đó: [S3_TEST_ISOLATION.md](S3_TEST_ISOLATION.md).
Kết quả model và hướng báo cáo: [REPORT_GUIDE.md](REPORT_GUIDE.md).
