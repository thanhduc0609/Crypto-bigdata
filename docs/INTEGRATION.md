# Tích hợp bản đồ án — 10/11/2026

Các thay đổi được phát triển từ `main` tại `6e6b185`. Khi fetch để tích hợp,
`origin/main` chưa có commit mới so với base này; không cần ghép thêm các nhánh
cá nhân khác hoặc ghi đè công việc của nhóm.

## Phạm vi bản tích hợp

- Bài toán 36 tháng, dữ liệu dài hạn, baseline, kiểm định và dải bất định.
- Canonical đã khóa, kiểm kê/làm sạch, đánh giá ML 5 phút và nghiên cứu hồi cứu.
- S3 test/DB test cô lập; API nến có timeout/retry/cache/fallback và nguồn rõ ràng.
- Streaming + batch theo phiên, local launcher, monitor và supervisor phục hồi.
- Hai trang học thuật, tương tác biểu đồ tháng, CSV và Futures gập mặc định.
- Gói 41 artifact có checksum đi cùng repo, restore/verify trên clone mới.
- Canonical nghiên cứu local độc lập với bucket vận hành; `.env` và pointer
  active được ignore. Tệp lock runtime được bỏ khỏi index nhưng không xóa file
  mà worker đang giữ.
- README thống nhất, exporter portable, smoke test có main guard/deadline và
  workflow offline Windows/Linux.

## Kiểm chứng trước push

| Phạm vi | Kết quả local |
|---|---|
| Unit/hồi quy offline theo nhóm | 252 test qua |
| Toán thu phóng/pan/preset/ranh giới | 6 test qua; các JS qua kiểm cú pháp |
| DB riêng: pipeline/canonical/inference | 24 kiểm tra qua, không skip |
| Web/API thị trường với dịch vụ thật | 36 kiểm tra qua |
| Tính toàn vẹn nghiên cứu ML đã khóa | 16 điều kiện qua |
| Checkout chỉ từ Git index, không `.env` | Restore thành công; hai trang và ba API nghiên cứu HTTP 200; 154.351 nến đúng checksum |
| Đóng gói | Exporter chạy được với output-dir tùy chọn, ZIP kiểm CRC/SHA |

CI trên GitHub xác minh lại checkout Windows/Linux. Kết quả CI cụ thể xem trong
tab Actions/pull request; bảng trên là kết quả local, không nhận thay kết quả CI.

## Lưu ý về byte của source/model

Thí nghiệm gốc gắn SHA-256 với byte của feature engineering và model/metadata.
Một số file gốc có CRLF; `.gitattributes` giữ đúng byte này trên các hệ điều hành,
thay vì đổi checksum cũ sau khi đã xem test. Các thay đổi chỉ về LF/CRLF trong
những file đó không đổi logic hoặc trọng số model. Hai dấu cách cuối dòng đã có
trong source feature engineering cũng được giữ vì cùng lý do.

## Cập nhật máy của thành viên

Theo quick-start trong README: cài dependency → restore/verify gói nghiên cứu →
điền `.env` riêng → mở Docker/khởi động demo → kiểm các phép đo mới. Máy đã có
snapshot khác sẽ được yêu cầu giữ riêng phiên, không bị restore ghi đè.

Phần viết Word từng mục, phân công thật, nhận xét hình và luyện bảo vệ vẫn do
nhóm hoàn thiện theo sườn đã lập. Mô hình dài hạn gốc vẫn thua baseline theo MAE;
các cửa sổ nghiên cứu bổ sung vẫn là hồi cứu chồng lấn. Tích hợp code không biến
các giới hạn này thành bảo đảm dự báo hoặc bảo đảm điểm số.
