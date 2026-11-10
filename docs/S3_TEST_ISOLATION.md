# Cô lập các bài kiểm thử S3

Việc hoàn thành ngày 08/11/2026: loại bỏ khả năng các bài test đã xác định ghi/xóa
nhầm vào đường dẫn S3 dùng chung. Đây là **một đầu việc riêng**, không phải kết luận
rằng các bộ test còn lại hoặc toàn bộ luồng medallion đã được sửa xong.

## Vấn đề trước khi sửa

- `tests/test_s3.py` chạy thao tác mạng/Spark ngay khi import và append fixture vào
  `S3_RAW_PATH`/`S3_PROCESSED_PATH`, có thể trùng dữ liệu đang chạy.
- `tests/test_model_registry.py` upload phiên bản mặc định lên `crypto/models/v2.0.0`
  và tạo lại manifest trong thư mục artifact active.
- `tests/test_streaming_medallion_e2e.py` xóa mọi object ở `crypto/bronze/trades/` và
  `crypto/silver/trades/` trước khi chạy. Test cũng dùng checkpoint cố định.

## Cách sửa

Helper `test_support/s3_sandbox.py` cấp prefix mới cho từng lần chạy:

```text
crypto/tests/<suite>/<UTC timestamp>-<UUID>/
```

Không nhận prefix sản xuất từ biến môi trường hoặc tham số tùy ý. Mỗi run có một
workspace local riêng dưới `data/test_runs/` (đã ignore Git).

1. Kiểm tra prefix chưa có object; từ chối nhận vùng đã có dữ liệu.
2. Tạo marker sở hữu có điều kiện `IfNoneMatch=*`, chứa suite/run ID/token.
3. Client kiểm bucket và **đúng prefix của run** trước các thao tác đọc, ghi, liệt kê,
   upload/download hoặc xóa. Không cung cấp proxy cho thao tác tùy ý như delete bucket.
4. Từ chối đường dẫn tuyệt đối, `..`, URI, ký tự escape và thay đổi identity sau khởi tạo.
5. Một batch xóa có dù chỉ một key ngoài phạm vi sẽ bị từ chối trước khi gọi SDK.
6. Khi kết thúc: xác minh marker, liệt kê các trang, kiểm tất cả key trước khi xóa,
   xóa theo batch tối đa 1.000 key, bỏ marker cuối cùng và xác minh danh sách hiện hành rỗng.
7. Không dọn nếu marker khác chủ sở hữu. Nếu AWS báo lỗi dọn, ghi `cleanup_failed`,
   giữ bằng chứng local; không tuyên bố đã dọn thành công. Lỗi dọn không che lỗi gốc của test.

Đây là guard trong ứng dụng, không thay thế IAM. Cleanup chỉ xử lý object hiện hành
trong prefix test; không purge lịch sử version ở bucket bật versioning, không đổi
chính sách bucket, quyền truy cập hoặc lifecycle.

## Thay đổi ở ba bài test

| Bài test | Hành vi mới |
|---|---|
| `test_s3.py` | Chỉ chạy khi gọi hàm/CLI; tạo 2 fixture Parquet có `is_test=true`, kiểm bytes và dữ liệu đọc lại trong prefix riêng |
| `test_model_registry.py` | Copy artifact vào workspace của run; upload phiên bản/prefix riêng; download không activate; thử làm hỏng object test rồi xác minh production downloader thực sự từ chối checksum sai |
| `test_streaming_medallion_e2e.py` | Bỏ toàn bộ bước xóa Bronze/Silver cũ; cấp đường dẫn layer/checkpoint riêng; boto3 chỉ kiểm prefix run; chặn chạy nếu chưa chọn DB và topic test riêng |

Test registry kiểm SHA-256 của **mọi file** trực tiếp trong `ml/artifacts` trước/sau,
bao gồm manifest active. Không ghi vào artifact active để upload.

Test kết nối giờ kiểm boto3 + Parquet, **không kiểm Spark S3A**. Spark S3A thuộc bài
medallion đầy đủ. Không nên dùng kết quả test kết nối để thay cho chứng minh luồng Spark.

Các override đường dẫn medallion chỉ có hiệu lực trong context của tiến trình test;
được khôi phục sau thành công hoặc lỗi, không sửa `.env` hoặc cấu hình dự án trên đĩa.

## Cách chạy

Kiểm tra guard offline, không gọi AWS:

```powershell
python -m unittest discover -s tests -p test_s3_test_safety.py -v
```

Hai bài kiểm tra AWS thật, dùng cấu hình bucket/credential hiện có:

```powershell
python tests/test_s3.py
python tests/test_model_registry.py
```

Mặc định tự dọn object test hiện hành. Dùng `--keep` nếu chủ động muốn giữ **riêng
prefix của lần chạy đó** để xem bằng chứng. Metadata kết quả nằm tại:

```text
data/test_runs/<suite>/<run-id>/result.json
```

Import ba module test không khởi tạo Spark, không kết nối AWS và không ghi fixture.
Helper từ chối chạy AWS nếu chưa cấu hình rõ `S3_BUCKET_NAME`.

## Điều kiện riêng của full medallion E2E

Full test có ghi Kafka và PostgreSQL, nên phải chuẩn bị DB đã khởi tạo schema và
topic dành riêng cho kiểm thử, rồi cấu hình:

```powershell
$env:TEST_POSTGRES_DB = 'crypto_test_<ten_db_test>'
$env:TEST_KAFKA_TOPIC = 'crypto-tests-<ten_topic_test>'
python tests/test_streaming_medallion_e2e.py
```

Đây là mẫu đặt tên, không phải lệnh tự tạo DB/topic. Khi thiếu điều kiện, chương
trình dừng trước khi tạo prefix S3 hoặc lấy dữ liệu giao dịch. Test không tự dọn
DB/topic để tránh phát sinh thao tác quản trị ngoài phạm vi.

**Full medallion E2E chưa được chạy lại trong đầu việc này.** Các assertion/schema
legacy của bài đó cần được đánh giá trong đầu việc kiểm thử toàn luồng sau. Không
đổi hoặc bỏ assertion để biến kết quả chưa kiểm chứng thành PASS.

## Minh chứng ngày 08/11/2026

| Kiểm tra | Kết quả |
|---|---|
| Guard offline | **28/28 test qua** |
| AWS thật: Parquet fixture | **2/2 lượt ghi–đọc qua** |
| AWS thật: model registry | **5/5 kiểm tra qua**, gồm từ chối artifact bị sửa trên S3 |
| Dọn hai prefix test | Danh sách object hiện hành còn **0** ở cả hai prefix |
| S3 Bronze dùng chung | 4 object trước/sau, hash inventory key/ETag/size giữ nguyên |
| S3 Silver dùng chung | 2 object trước/sau, hash inventory giữ nguyên |
| S3 models dùng chung | 5 object trước/sau, hash inventory giữ nguyên |
| Artifact active trên máy | Toàn bộ 5 file giữ nguyên SHA-256 |

Hai run AWS:

- Connection: `20261108T151019Z-814d58399963456ab62189246e08f9d4`.
- Model registry: `20261108T151024Z-d906c7f0118247ae9799e2e8a1f4e6af`.

Unit test xác minh các trường hợp: prefix gốc/production, bucket khác, đường dẫn
thoát phạm vi, batch xóa trộn key, thay identity, vùng có dữ liệu trước, marker sai,
listing sai phạm vi, hơn 1.000 object, lỗi AWS, lỗi bài test, chế độ giữ dữ liệu,
khôi phục cấu hình, import an toàn và gate hạ tầng medallion.

Gói minh chứng gồm log, JSON kết quả trước/sau và các file code liên quan. Không
chứa `.env`, key AWS, token hoặc mật khẩu. Các thay đổi hiện ở local, chưa push GitHub.
