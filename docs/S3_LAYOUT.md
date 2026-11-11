# Cấu trúc lưu trữ S3 của dự án

`local-runs` trước đây dùng để tách dữ liệu của từng phiên chạy trên máy cá nhân.
Dữ liệu vẫn ở S3. Cấu trúc hiện tại dùng `S3_PROJECT_PREFIX=crypto`, đưa các tầng
vào trực tiếp dưới `crypto/`, không có thư mục cá nhân bao quanh dự án.

```text
crypto/
  bronze/trades/
    streams/<writer-id>/          Giao dịch thô, nhật ký Spark riêng từng máy
    history/<migration-id>/       Giao dịch đã thu thập, đã kiểm chứng khi chuyển
  silver/trades/<batch-id>/       Dữ liệu làm sạch theo phiên xử lý
  gold/candles_1m/<batch-id>/      Kết quả tổng hợp theo phiên xử lý
  streaming/candles_1m/
    streams/<writer-id>/          Nến stream bổ trợ
    history/<migration-id>/       Lịch sử stream trước chuyển
  canonical-repairs/<snapshot>/   Nến REST đã đóng, dữ liệu nghiên cứu bất biến
  long_term/<snapshot>/          Dữ liệu và kết quả dài hạn
  models/                        Mô hình
  manifests/batches/<batch-id>/   Nguồn, checksum, số dòng và kiểm chứng đầu ra
  audit/                         Bằng chứng kiểm kê/đối soát
```

Các dataset cũ của nhóm ở tầng gốc được giữ nguyên. Các mục `streams` và `history`
là cách tổ chức file bên trong Bronze/Streaming. Chúng cần thiết vì hai máy không
được ghi chung một `_spark_metadata` với hai checkpoint riêng. Writer ID trung tính
được sinh một lần tại `data/runtime/storage-writer.json`; không sao chép file đó
hoặc checkpoint của máy khác. Những tên cũ chỉ được dùng để nhận diện nguồn khi
chuyển và trong bằng chứng lịch sử, không đặt cho phiên dữ liệu mới.

## Vận hành

Máy mới dùng `.env.example`, điền key/bucket của nhóm và giữ `S3_PROJECT_PREFIX=crypto`.
Sau khi kho chung đã chuyển, các máy khác chỉ cần pull mã mới và sửa `.env`
thành `S3_PROJECT_PREFIX=crypto`, bỏ dòng `S3_RUN_PREFIX` cũ; không chuyển lại kho.
Checkpoint cũ của chính máy đó được nhận diện để lấy Kafka offset. Nếu có nhiều
checkpoint, chọn `STREAM_RESUME_CHECKPOINT` trong `.env`. Không sao chép
`stream-bootstrap.json`, writer ID hay checkpoint từ máy khác.
Batch đọc riêng nhật ký của từng stream/history đã hoàn tất, rồi làm sạch và khử
trùng trade ID. Mỗi lượt Silver/Gold có ID riêng, manifest công bố sau khi kiểm
chứng. Không cộng các snapshot batch lặp lại thành số giao dịch mới.

`history` chỉ được đọc khi có `_READY.json` do công cụ chuyển công bố sau khi
kiểm checksum. Dữ liệu chưa được Spark commit không được tính là dữ liệu hoàn tất.
Test S3 vẫn bị giới hạn ở `crypto/tests/<suite>/<id>`; test không được dọn tầng gốc.

## Chuyển từ cấu trúc cũ

Mở PowerShell ở repo. Chỉ một thành viên thực hiện chuyển kho chung; các máy khác
dừng writer cũ để chốt nguồn. Điền đúng bucket/key đang dùng; không chia sẻ `.env`.

```powershell
./scripts/Stop-LocalDemo.ps1 -IncludeLegacy
.venv/Scripts/python.exe scripts/organize_s3.py prepare --source-prefix crypto/local-runs/<ten-cu>
.venv/Scripts/python.exe scripts/organize_s3.py copy
./scripts/Start-LocalDemo.ps1 -Streaming -Batch
# Chờ Silver/Gold mới thành công, sau đó đối soát chỉ đọc:
.venv/Scripts/python.exe scripts/audit_demo_pipeline.py
# Dùng đúng đường dẫn audit mới vừa tạo:
.venv/Scripts/python.exe scripts/organize_s3.py finish --audit data/demo_audits/<id>/pipeline-audit.json
```

Nếu một batch cũ còn hoàn tất trong lúc chuyển, dữ liệu của nó vẫn ở nguồn.
Sau khi các máy đã dừng hẳn, chạy `organize_s3.py refresh` rồi `organize_s3.py copy`
để bổ sung object mới. Các receipt đã kiểm chứng được dùng tiếp; `finish` vẫn
đọc lại đích và kiểm backup từng object trước khi xóa nguồn. Không sửa plan bằng tay.

`copy` chỉ tạo đích chưa có; nếu có bytes khác thì dừng, không ghi đè. Mọi object
được sao lưu nguyên byte tại `data/runtime/s3-layout/original-objects/` và kiểm
SHA-256 tại đích. Nhật ký Spark được đổi địa chỉ tham chiếu; các file dữ liệu,
model và manifest nghiên cứu đã khóa giữ nguyên nội dung/checksum.

Công cụ ghi `configs/s3_locations.json` để phân giải URI cũ trong bằng chứng bất
biến sang địa chỉ mới. Không sửa số liệu, train lại hoặc thay hash để đổi tên.
File này không chứa key; cần đi cùng mã nguồn để các thành viên đọc được snapshot
cũ. URI ghi trong manifest lịch sử vẫn là địa chỉ lúc thu thập.

Đích file sink mới dùng checkpoint mới trong repo. `stream-bootstrap.json` giữ
offset Kafka đã commit để nối tiếp, không nhảy thẳng tới `latest`. Lịch sử cũ vẫn
được batch đọc để tổng hợp đầy đủ; nến REST canonical vẫn có ưu tiên trong DW.
Mỗi máy giữ checkpoint riêng; nếu Kafka đã hết retention ở offset cần đọc, dừng
và phục hồi nguồn, không tắt kiểm tra mất dữ liệu để che lỗi.

`finish` chỉ xóa object nguồn có trong manifest đã chốt, sau audit mới đạt và sau
khi kiểm tra lại backup, hash đích, ETag nguồn. Có thể tiếp tục `finish` nếu bị ngắt.
Nguồn phát sinh thêm object hoặc thay bytes thì dừng để kiểm tra, không xóa rộng.
Thư mục backup và bản env trước chuyển có dữ liệu riêng, đã Git ignore; không đưa
vào Git, Word hoặc gói chia sẻ. Giữ backup ít nhất đến khi cả nhóm xác nhận chạy tốt.

Tham khảo [AWS về di chuyển object](https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html)
và [Spark về thay đổi file sink/checkpoint](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html).
