# Đánh giá ML trên snapshot canonical đã khóa

Bàn giao 09/11/2026, run `20261109T074901815206Z`. Chỉ đánh giá phân loại tăng/không
tăng sau 5 phút; mô hình dự báo giá 36 tháng không đổi.

## Dữ liệu và protocol

Snapshot `20261109T063455Z-adb14c7395`: 154.351 nến phút REST đã đóng, liên tục từ
25/07 tới 09/11/2026. SHA Parquet:
`50a2a30a67c4e8d5280a5d44334ed153f28320a15d481b7649e440947b205dd5`.
119 phút lịch sử + 5 nhãn cuối bị loại, còn 154.227 mẫu cùng cohort cho mọi ứng viên.

`configs/short_term_experiment.json` khóa trước khi tính điểm: bốn tuần validation
rời nhau, expanding train; 8 ứng viên và 5 ngưỡng. Chọn bằng balanced accuracy
trung bình validation, không chọn lại bằng test. StandardScaler fit train; Platt
calibration dùng 7 ngày cuối train riêng và purge nhãn ở cả hai ranh giới.

Mốc nến `t`, ra quyết định khi nến đóng `t+1`, so giá đóng nến `t+5` (quan sát tại
`t+6`). Mọi nhãn fit/validation phải sẵn sàng **trước** ranh giới kế tiếp. Test
24/10–04/11 có 17.274 mẫu. 05–09/11 từng được kiểm tra trong các lần trước nên chỉ
là chẩn đoán phụ. Đây là đánh giá hồi cứu đã chốt quy trình, chưa phải tiến cứu mù.

## Thay đổi kỹ thuật

- `ml/locked_dataset.py`: đọc snapshot đúng checksum, liên tục, REST, đóng nến;
  `ml/dataset.py` ưu tiên snapshot thay archive cũ và dừng khi snapshot lỗi.
- `ml/short_term_features.py`: đặc trưng EWM trong ngữ cảnh 120 nến giống đường
  DW inference; thêm 42 đặc trưng tỷ lệ/lợi suất, log-volume, hình nến, biến động,
  lịch UTC và VWAP từ turnover thật. Không điền giả order flow REST.
- `ml/short_term_experiment.py`: tách select/evaluate, lưu model nghiên cứu riêng,
  SHA model/code, runtime, dự đoán từng mẫu, confusion matrix, metric từng lớp,
  calibration và paired day bootstrap. Không dùng trainer ghi thẳng live artifact.
- Parity trên 32 cửa sổ thật đạt; sai lệch xác suất tối đa 5,64e-10. Hợp đồng là
  120 nến; chưa đổi worker legacy hay fallback sử dụng ngữ cảnh ít hơn.

## Kết quả và quyết định

Logistic Regression C=0.1, ngưỡng 0.5 được chọn; validation BA 51,885%, baseline
đảo chiều 5 phút 51,806%; chỉ thắng baseline tốt nhất 2/4 fold.

| Phương án trên cùng test | Accuracy | Balanced accuracy | Macro F1 | AUC | Log loss |
|---|---|---|---|---|---|
| Majority từ train | 50,25% | 50,00% | 33,45% | 0,5000 | 0,693148 |
| Đảo chiều 5 phút | 50,81% | 50,81% | 50,81% | Không có xác suất | Không áp dụng |
| Incumbent XGBoost v2.0.0 | 51,32% | 51,16% | 45,73% | 0,5231 | 0,692672 |
| Logistic chuẩn hóa | 51,42% | 51,36% | 50,83% | 0,5189 | 0,692595 |

Chênh lệch BA so incumbent +0,206 điểm phần trăm, CI xấp xỉ 95% theo 12 cụm ngày
[-0,837; +1,190]; so baseline đảo chiều +0,552, CI [-0,780; +1,848]. Có 2.000 lần
bootstrap ghép cặp; nhãn chồng lấp và ít cụm ngày hạn chế kết luận. AUC model mới
thấp hơn incumbent. 05–09/11: model mới BA 51,31%, incumbent 52,25%.

**Chưa đạt promotion gate** vì số fold thắng và CI. Giữ nguyên model v2.0.0 đang
chạy; artifact ứng viên là nghiên cứu offline. Không hạ điều kiện sau khi xem test.
Chuỗi số liệu cũ 56,49%/53,55% dùng cohort khác, không so trực tiếp với kết quả này.

## Kiểm chứng và tái lập

49/49 kiểm thử: 27 mới ML + 8 outcome/input + 14 orderflow/worker. 16/16 kiểm chứng
bàn giao: nhãn nguồn, tính metric từ confusion counts, tái tạo xác suất, biên thời
gian, checksum code/model, giữ nguyên canonical pointer và artifact hiện hành.
Đây là kiểm tra trong phạm vi, không khẳng định toàn repo đã chạy test đầy đủ.

```powershell
.venv\Scripts\python.exe -m unittest tests.test_short_term_experiment tests.test_prediction_evaluation tests.test_orderflow_and_worker -v
.venv\Scripts\python.exe scripts/verify_short_term_experiment.py data/ml_experiments/20261109T074901815206Z
# Phiên nghiên cứu mới, không phải kiểm định độc lập nếu vẫn dùng lại cùng test:
.venv\Scripts\python.exe scripts/run_short_term_experiment.py select
.venv\Scripts\python.exe scripts/run_short_term_experiment.py evaluate --run data/ml_experiments/<run-id>
```

Mỗi run lưu `protocol.json`, `provenance.json`, `selection.json`, mốc mở holdout,
`candidate.joblib`, CSV/Parquet predictions, metric và kiểm chứng. Joblib chỉ nạp
từ nguồn tin cậy. Dependencies chính đã dùng: Python 3.11, numpy 2.4.6, pandas
2.3.3, sklearn 1.9.1, xgboost 3.2.0; phiên bản chi tiết trong provenance.

Bundle bàn giao chứa code, snapshot Parquet, model, raw metric và log; không kèm
`.env`/khóa. Báo cáo tiếng Việt và CSV ở thư mục outputs của workspace.

Nghiên cứu tiếp theo cần holdout tương lai mới, nhiều chế độ thị trường và đánh
giá đúng bài toán. Muốn tuyên bố giao dịch sinh lời phải thêm spread/phí/trượt giá.
Không tinh chỉnh theo các ngày test vừa xem rồi coi việc chấm lại là độc lập.
