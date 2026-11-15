# Snapshot nghiên cứu đi cùng mã nguồn

`academic-snapshot-20261109.zip` chứa 41 tệp đã khóa (khoảng 11 MB nén), kèm
`academic-snapshot-20261109.manifest.json` là danh mục, kích thước và SHA-256.
Gói này phục vụ tái lập bài đồ án, không chứa DB live hoặc toàn bộ raw stream.

| Nội dung | Phiên |
|---|---|
| Canonical 154.351 nến phút đã đóng + manifest | 20261109T063455Z-adb14c7395 |
| ML: protocol, selection, candidate, dự đoán và đánh giá | 20261109T074901815206Z |
| Dài hạn gốc: raw ngày, ngày/tháng sạch, forecast/backtest | 20261108T142006113873Z |
| Nghiên cứu dài hạn bổ sung | 20261109T082513613587Z |

## Khôi phục trên bản clone mới

Sau khi cài dependency:

```powershell
python scripts/quality/research_artifacts.py restore
python scripts/quality/research_artifacts.py verify
```

Lệnh chỉ dùng dữ liệu trong gói tin cậy đi kèm repo, không tải mạng hoặc train.
Nó kiểm hash archive, từng file, model và các source gắn với thí nghiệm. Nếu code/
model khác hoặc dữ liệu local trùng đường dẫn nhưng khác nội dung, lệnh dừng để
tránh trộn phiên; không có thao tác ép ghi đè. Dữ liệu được ghi trước pointer.

Đặt `CANONICAL_SNAPSHOT_STORAGE=local` trong `.env` (đã có ở cấu hình mẫu) để
đọc canonical local đã xác minh, trong khi `STORAGE_DESTINATION=s3` vẫn dùng
bucket/prefix riêng cho vận hành. URI S3 trong descriptor là thông tin nguồn gốc,
không cần đổi và không phải key. `.env` và descriptor active không được commit.

Giữ nguyên byte LF/CRLF của các source/model trong `.gitattributes`: SHA của
thí nghiệm gốc dựa trên byte. Đổi line ending cũng phải được coi là đổi phiên
code khi kiểm chứng; không cập nhật checksum của thí nghiệm cũ để che thay đổi.

## Tái lập và giới hạn

```powershell
python scripts/quality/check_project.py
```

Runner có kiểm chứng 16 điều kiện của ML đã công bố và không thay mô hình đang
dùng. Candidate `.joblib` chỉ được nạp khi chủ động chạy xác minh từ gói tin cậy.
Khôi phục file tự nó không deserialize candidate.

Backtest là dữ liệu quá khứ; các dự báo 36 tháng chưa có đủ giá thật tương lai
để chấm. Nghiên cứu bổ sung vẫn là hồi cứu, cửa sổ chồng lấn. Các hạn chế và
quyết định không thay model được giữ nguyên; không bảo đảm điểm số hay lợi nhuận.

Gói không seed PostgreSQL hoặc phục hồi checkpoint/S3 vận hành. Muốn đo luồng
live, khởi động dịch vụ, chờ dữ liệu mới và xem từng phép đo. Tạo phiên nghiên
cứu mới phải có descriptor/manifest riêng, không ghi đè snapshot đã công bố.

## Người bảo trì tạo phiên gói mới

```powershell
python scripts/research_artifacts.py create --archive artifacts/research/<phien-moi>.zip
```

Lệnh đọc các pointer đang active, chọn file theo allowlist, kiểm provenance và
quét giá trị credential trong `.env` hiện dùng trước khi đóng gói. Chỉ công bố
kết quả có nguồn và quyền sử dụng phù hợp. Không bỏ ignore toàn bộ thư mục data.
