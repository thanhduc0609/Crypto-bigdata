# Docker thống nhất và chuyển dữ liệu cũ

Chỉ dùng `docker-compose.yml`, project `crypto-bigdata`. Web/Python/Spark vẫn chạy
trên máy host; Docker cung cấp PostgreSQL 16, Kafka và pgAdmin tùy chọn. Việc gộp
không thay đổi dữ liệu nghiên cứu, mô hình, metric hay tiêu chí đánh giá đồ án.

## Máy mới

Làm phần cài đặt trong README: Python 3.11, Java, Docker Desktop, venv, thư viện,
khôi phục/verify gói nghiên cứu và điền `.env` từ `.env.example`. Sau đó:

```powershell
.\scripts\setup_env.ps1 -StartDemo                  # Web + monitor, không thu thập mới
.\scripts\setup_env.ps1 -StartDemo -Streaming -Batch # Toàn luồng vận hành
```

Không phải chạy lại cài đặt, thu thập lịch sử, train model hoặc toàn bộ test mỗi
lần mở web. Streaming chạy liên tục. Xem `data/runtime/logs/` khi một bước thất
bại; không chờ hai tiếng chỉ vì terminal chưa kết thúc. Readiness Docker có
giới hạn chờ 180 giây; trạng thái luồng phải xem thêm `/api/system/status`.

## Máy từng chạy Docker cũ

Không dùng `docker compose up` trực tiếp để chuyển môi trường cũ: Compose có thể
khởi tạo volume mới, khiến nhìn như mất dữ liệu. Launcher chính phát hiện tên
container cũ và chặn trường hợp này trước khi tạo database.

```powershell
.venv/Scripts/python.exe scripts/demo/docker_environment.py status
.venv/Scripts/python.exe scripts/demo/docker_environment.py migrate
```

Nếu có cả hai bộ cũ, công cụ không tự chọn dữ liệu. Chọn bộ chứa dữ liệu nhóm
muốn tiếp tục (kiểm tra bằng pgAdmin/psql trước), ví dụ:

```powershell
.venv/Scripts/python.exe scripts/demo/docker_environment.py migrate --source-project codex-crypto-local
# Hoặc: --source-project crypto-bigdata
```

Bộ không được chọn phải được dừng rõ ràng trước khi chuyển. Công cụ chỉ nhận các
container tên cũ, có đúng label project/service; không tác động dự án Docker khác.
Thông tin DB/user/password trong `.env` phải khớp nguồn. Không thay mật khẩu hay
user trong `.env` để mong thay được tài khoản đã khởi tạo trong volume cũ.

Quy trình chuyển:

1. Dừng các tiến trình ứng dụng thuộc repo, giữ checkpoint.
2. Đo số dòng mọi bảng nghiệp vụ, gồm schema `warehouse`, và Kafka end offsets.
3. Dừng nguồn; sao lưu thư mục PostgreSQL và log Kafka thật bằng tar giữ quyền
   sở hữu. Kafka cũ có thể nằm trong writable layer thay vì volume được khai báo.
4. Tạo volume mới riêng, phục hồi bản sao và dùng đúng image nguồn, tránh tự
   nâng/hạ phiên bản. Tài khoản/mật khẩu và cấu hình S3 được giữ nguyên.
5. Khởi động project thống nhất, đối chiếu số dòng và Kafka offsets.
6. Ghi manifest `verified` chỉ khi các phép đối chiếu khớp.

Volume cũ không bị xóa. Nếu bộ cũ cũng có project `crypto-bigdata`, các container
đó phải nhường label cho bộ mới; chúng chỉ bị xóa sau khi sao lưu hoàn tất. Dữ
liệu của bộ không chọn được giữ trong volume gốc và archive trước khi nhường tên.
pgAdmin cũ được sao lưu cấu hình riêng, không tự trộn tài khoản/server registrations.

Sau chuyển, chạy lại `.\scripts\setup_env.ps1 -StartDemo -Streaming -Batch`, xem hai trang web,
kiểm tra DB/luồng. Sau khi xác nhận có thể bỏ container nguồn còn đang dừng:

```powershell
.venv/Scripts/python.exe scripts/demo/docker_environment.py retire --manifest data/runtime/docker-migration/<run-id>/manifest.json
```

Lệnh retire kiểm tra target, checksum backup và nguồn đã dừng trước khi xóa
container; không xóa volume. Nếu còn bộ cũ không được chọn, giữ nó dừng để đối
chiếu dữ liệu; không chạy hai bộ trên cùng cổng. Sau khi nhóm quyết định dữ liệu
không còn cần, mới xử lý riêng bộ đó, không xóa hàng loạt.

## Backup và xử lý khi chuyển dở

Backup nằm ở `data/runtime/docker-migration/<run-id>/`, được Git ignore. Có
`postgres.tar`, `kafka.tar`, SHA-256 trong `manifest.json`, ảnh chụp `.env` trước
chuyển ở `environment.before`. **Thư mục này có dữ liệu riêng và credential trong
bản sao env; không đưa vào Git, báo cáo hoặc gói chia sẻ.**

Nếu chuyển thất bại, giữ toàn bộ container/volume/archive, dừng ứng dụng và đọc
`status` trong manifest. Không chạy migrate nhiều lần hoặc xóa checkpoint. Nếu
nguồn vẫn còn container, dừng bộ mới trước khi khởi động lại nguồn; khôi phục env
từ `environment.before` sau khi sao lưu env hiện tại. Nếu container nguồn đã nhường
label, phục hồi tar vào volume mới bằng đúng image nguồn; không giải nén đè lên
volume đang chạy. Đây là chuyển local có dừng dịch vụ, không phải HA/zero downtime.

Giữ `POSTGRES_VOLUME_NAME`, `KAFKA_VOLUME_NAME` và `*_IMAGE` công cụ ghi vào `.env`
trên máy đã chuyển. Máy mới dùng mặc định riêng, không sao chép tên volume local
của người khác. Không dùng `down -v`, `volume prune` hoặc reset Kafka để chữa lỗi.

## pgAdmin tùy chọn

Điền `PGADMIN_EMAIL` và `PGADMIN_PASSWORD` riêng trong `.env`, sau đó:

```powershell
.\scripts\setup_env.ps1 -StartDemo -Admin
```

Mở http://127.0.0.1:5050. Đăng ký server host **`postgres`**, port **5432**, database,
user/password theo `.env`. Từ ứng dụng trên host vẫn dùng `127.0.0.1:5433`.
Mặc định chỉ PostgreSQL/Kafka chạy, không tải pgAdmin để xem web. Docker ports chỉ
bind localhost. `.\scripts\setup_env.ps1 -StopDemo` dừng app; muốn dừng cả hạ tầng nhưng giữ dữ liệu:

```powershell
.venv/Scripts/python.exe scripts/demo/docker_environment.py stop
```

## Những phần cần giữ khi dọn dự án

Giữ `tests/`, `test_support/`, tài liệu/bằng chứng, model và gói nghiên cứu đã khóa.
Cache `__pycache__`, log, venv, dữ liệu chạy và key đã được Git ignore. Hai cấu hình
Docker và hai template env cũ được thay bằng một Compose và `.env.example` duy nhất.
Không cần xóa test để chạy nhanh; launcher không chạy test hoặc huấn luyện.
