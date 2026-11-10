# Giao diện nghiên cứu và phân tích — 10/11/2026

## Hai trang, cùng một ứng dụng

| URL local | Nội dung |
|---|---|
| http://127.0.0.1:5001/ hoặc /du-bao-3-nam | Lịch sử tháng, dự báo 36 tháng, kiểm định, baseline, sai số, bất định và CSV |
| http://127.0.0.1:5001/thi-truong | Nến phút/giờ/ngày, volume, dòng mua/bán, giao dịch lớn, phân loại +5 phút |

Thanh điều hướng chuyển giữa hai URL. Trang dự báo không tải JavaScript thị trường
hoặc liên tục gọi các API nến/Futures. Cả hai đọc cùng backend; không nhân đôi kho dữ
liệu. Luồng dữ liệu & chất lượng là khối thu gọn chung, mở để xem phép đo thật.
Futures là phần mở rộng gập mặc định, được giữ trong trang thị trường.

## Cách dùng biểu đồ tháng

- Lăn chuột trong vùng vẽ để zoom quanh vị trí con trỏ; kéo ngang để xem thời gian
  khác. Ctrl/Cmd + lăn chuột vẫn dành cho chức năng zoom của trình duyệt.
- **Toàn bộ:** 105 tháng lịch sử + 36 tháng dự báo của snapshot hiện tại.
- **5 năm gần nhất:** 60 tháng lịch sử hoàn tất, kết thúc ở tháng chốt dữ liệu.
  Đây là 5 năm của snapshot, không tính theo ngày đồng hồ. Dự báo ở ngoài bên phải
  được báo rõ dưới biểu đồ.
- **Lịch sử gần đây + 36 tháng dự báo:** 24 tháng lịch sử cuối cùng + toàn bộ tương lai.
- **Khôi phục góc nhìn:** trở về toàn bộ dữ liệu của chế độ đang chọn.
- Nút +/− dùng trên điện thoại; chạm để xem tooltip, kéo ngang một ngón tay;
  vẫn có thể cuộn trang theo chiều dọc. Chưa triển khai cử chỉ chụm hai ngón.
- Bàn phím khi biểu đồ có focus: +/− zoom, ←/→ di chuyển, Home khôi phục.
- Đổi kiểu lịch sử, trục giá, bật/tắt dải không xóa góc nhìn. Dự báo và kiểm định
  giữ góc nhìn riêng trong phiên trang. Làm mới cùng snapshot giữ góc nhìn;
  tải lại trang bắt đầu ở góc nhìn toàn bộ.
- Tooltip thể hiện tháng UTC, dữ liệu thật/dự báo, giá và dải 80%/95%.
  Chỉ nến lịch sử có OHLC; không tạo nến tương lai giả.
- Đường phân cách nằm đúng giữa tháng lịch sử cuối và tháng dự báo đầu tiên.
  Khi ranh giới ngoài màn hình, dòng trạng thái ghi rõ hướng; không vẽ ranh giới giả.
- Kiểm định có đúng 36 tháng quá khứ. Hai preset dành cho lịch sử/tương lai bị vô
  hiệu trong chế độ này; vẫn dùng zoom/pan/reset và xem baseline màu xám.
- Trục giá tự khớp vùng đang xem, gồm dải nếu đang bật. Cần giữ cùng trục và góc
  nhìn khi so sánh ảnh; trục log và tuyến tính thể hiện khoảng cách khác nhau.

## Chụp minh chứng để đưa vào Word

1. Trang dự báo: chụp tổng quan, giữ nguồn, khoảng thời gian và quy trình chia tập.
2. Chọn lịch sử gần đây + 36 tháng, trục log, bật dải; chụp biểu đồ với chú giải,
   ranh giới, đơn vị và phần sai số bên dưới. Nêu đây là dự báo close tháng.
3. Chọn kiểm định; chụp cùng baseline và mở **Chất lượng dự báo và so sánh mô hình**.
4. Mở nghiên cứu bổ sung để chụp 3 mốc hồi cứu; ghi rõ chúng chồng lấn và đã được xem.
5. Trang thị trường: chụp nến + volume, giao dịch lớn kèm bộ lọc và nguồn/thời điểm.
6. Mở trạng thái hệ thống để chụp các chặng xử lý, tránh suy luận từ nến trực tiếp.

Giữ số liệu mô hình thua baseline và giới hạn bất định trong báo cáo. Không dùng
độ phủ 95% để gọi là chính xác 95%. Không cần mở Futures nếu không trình bày phần
mở rộng; đây là thay đổi bố cục đã được người dùng duyệt, không thay đổi đánh giá.
Không chụp khóa AWS, `.env`, terminal có thông tin đăng nhập hoặc đường dẫn bí mật.

## Kiểm thử phạm vi thay đổi

```powershell
node --test tests/long_term_viewport.test.cjs
.venv/Scripts/python.exe -m unittest tests.test_research_pages tests.test_demo_runtime tests.test_long_term_forecast tests.test_long_term_research -q
# Các API thị trường cần DB và dịch vụ ngoài:
.venv/Scripts/python.exe tests/test_web_ui_refactor.py
```

Thay đổi này chỉ là trình bày/điều hướng/thu phóng. Các dataset đã khóa, mô hình,
ngưỡng, kết quả kiểm định và quyết định chưa thay mô hình đang chạy được giữ nguyên.
Kiểm tra UI trên desktop và kích thước điện thoại không thay thế kiểm thử mọi
thiết bị vật lý hoặc thử tải dài hạn.
