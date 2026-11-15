"""Export Vietnamese report and an allowlisted reproducibility bundle; no secrets."""
import argparse
import json
import shutil
import sys
import zipfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
from ml.realtime.short_term_experiment import ROOT,sha


def table(frame):
    columns=list(frame.columns)
    lines=["| "+" | ".join(columns)+" |","|"+"|".join("---" for _ in columns)+"|"]
    lines += ["| "+" | ".join(str(v) for v in row)+" |" for row in frame.itertuples(index=False,name=None)]
    return "\n".join(lines)


def export(run,output):
    run=Path(run).resolve();output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    read=lambda name:json.loads((run/name).read_text(encoding="utf-8"))
    evaluation=read("evaluation.json");selection=read("selection.json");dataset=read("dataset.json")
    verification=read("delivery_verification.json")
    if not verification["passed"]:raise ValueError("Verification required before export")
    name=selection["selected"]["name"]
    scores=pd.read_csv(run/"primary_test_scores.csv")
    comparison=scores[["name","samples","accuracy","balanced_accuracy","macro_f1","recall_up","roc_auc","log_loss","brier"]].copy()
    comparison.to_csv(output/"So-sanh-ML-tren-tap-kiem-dinh.csv",index=False,encoding="utf-8-sig")
    shown=comparison.drop(columns="samples").copy()
    for col in ["accuracy","balanced_accuracy","macro_f1","recall_up"]:shown[col]=shown[col].map(lambda x:f"{100*x:.2f}%")
    for col in ["roc_auc","log_loss","brier"]:shown[col]=shown[col].map(lambda x:f"{x:.6f}" if pd.notna(x) else "Không áp dụng")
    validation=pd.read_csv(run/"validation_ranking.csv").drop_duplicates("name")
    validation.to_csv(output/"So-sanh-cac-phuong-an-ML-validation.csv",index=False,encoding="utf-8-sig")
    cv=pd.read_csv(run/"validation_scores.csv")
    folds=cv[(cv.name==name)&(cv.threshold==selection["threshold"])][["fold","samples","accuracy","balanced_accuracy","log_loss"]].copy()
    folds["baseline_BA_tot_nhat"]=cv[cv.name.isin(read("protocol.json")["baselines"])].groupby("fold").balanced_accuracy.max().to_numpy()
    for col in ["accuracy","balanced_accuracy","baseline_BA_tot_nhat"]:folds[col]=folds[col].map(lambda x:f"{x*100:.3f}%")
    intervals=evaluation["results"]["primary_test"]["paired_day_intervals"]
    ci=table(pd.DataFrame([dict(doi_chieu=k,chenh_lech_diem_phan_tram=f"{v['point']*100:+.3f}",
            CI_95_diem_phan_tram=f"[{v['lower_95']*100:+.3f}; {v['upper_95']*100:+.3f}]") for k,v in intervals.items()]))
    daily=pd.read_csv(run/"primary_test_daily_scores.csv")
    daily.to_csv(output/"Danh-gia-ML-theo-ngay.csv",index=False,encoding="utf-8-sig")
    checks=table(pd.DataFrame([dict(dieu_kien=k,ket_qua="Đạt" if v else "Chưa đạt") for k,v in evaluation["promotion_checks"].items()]))
    vr=validation[["name","threshold","balanced_accuracy","log_loss","brier"]].copy()
    vr.balanced_accuracy=vr.balanced_accuracy.map(lambda x:f"{x*100:.3f}%")
    report=rf"""# Đánh giá và cải thiện ML trên phiên dữ liệu đã khóa

Ngày bàn giao: 09/11/2026. Phiên thí nghiệm: `{run.name}`.

## Kết luận có thể sử dụng trong báo cáo

Đã đánh giá 8 phương án trên cùng dữ liệu canonical và hoàn thiện quy trình chọn
model theo thời gian. Phương án được chọn trước khi mở test là Logistic Regression
`C=0.1`, 42 đặc trưng chuẩn hóa, ngưỡng UP=0.5. Trên 17.274 mẫu test, accuracy đạt
**51,42%**, balanced accuracy đạt **51,36%**. Model XGBoost v2.0.0 đang dùng lần lượt
đạt **51,32%** và **51,16%** trên đúng các mẫu đó.

Đây là cải thiện nhỏ về điểm ước lượng; chưa chứng minh được lợi thế bền vững.
Khoảng tin cậy chênh lệch vẫn chứa 0 và model chỉ thắng baseline tốt nhất ở 2/4
giai đoạn validation. **Không thay model đang chạy.** Artifact nghiên cứu được lưu
riêng; trọng số/metadata model hiện hành, pointer canonical và pointer dự báo
36 tháng giữ nguyên qua kiểm tra SHA-256. Việc này hoàn thiện đánh giá khoa học,
không phải bằng chứng dự báo thị trường chính xác cao hoặc bảo đảm điểm đồ án.

## 1. Bài toán và dữ liệu

- Đây là **phân loại tăng/không tăng sau 5 phút**, độc lập với dự báo giá 36 tháng.
- Nến phút có mốc đầu `t`; lúc `t+1` nến đóng mới có thể ra dự báo. Nhãn UP khi
  `close(t+5)>close(t)`; giá bằng nhau thuộc NOT_UP. Nến mục tiêu đóng lúc `t+6`,
  đúng 5 phút sau thời điểm ra quyết định. Múi giờ toàn bộ đánh giá: UTC.
- Snapshot `{dataset['descriptor']['run_id']}`: **154.351 nến phút**, từ
  25/07/2026 01:59 tới 09/11/2026 06:29, liên tục, REST, xác nhận đóng.
- SHA-256 Parquet: `{dataset['descriptor']['gold_sha256']}`.
- Sau 119 nến làm lịch sử và 5 nến cuối chưa đủ nhãn còn **154.227 mẫu**.
  Đây là khoảng 107 ngày dữ liệu phút, không đại diện cho nhiều năm thị trường.
- Nguồn chính xác và raw REST được truy vết trong hồ sơ khôi phục canonical.
  Không bịa buy/sell volume, trade count hoặc trade imbalance cho REST. Đặc trưng
  VWAP quan sát được tính riêng từ **turnover USDT / volume BTC**, không điền
  ngược vào các trường microstructure đang NULL.
- Bộ nạp ML ưu tiên snapshot đã kích hoạt trước archive cũ; checksum lỗi sẽ dừng,
  không âm thầm chuyển sang DW đang thay đổi hoặc tự backfill.

## 2. Đã cải thiện những gì

1. Khóa dataset, protocol, seed, phiên bản thư viện, mã nguồn và artifact bằng SHA.
2. Thay cách huấn luyện EWM trên toàn lịch sử bằng cửa sổ 120 nến cho thí nghiệm,
   khớp ngữ cảnh 120 nến của đường suy luận từ DW. Kiểm tra đối chiếu công thức
   cũ và đặc trưng mới trên 32 cửa sổ dữ liệu thật: đạt; sai lệch xác suất tối đa
   `{evaluation['feature_parity']['max_prediction_difference']:.3g}`.
3. Thử đặc trưng tỷ lệ/lợi suất, khoảng cách tới MA, biến động, log-volume,
   hình dạng nến, thời gian UTC. Tránh lệ thuộc mức giá tuyệt đối; log-volume xử lý
   trường hợp volume trước bằng 0; RSI của chuỗi hoàn toàn phẳng có giá trị trung tính.
   Tên nội bộ `stationary` chỉ nhóm đặc trưng chuẩn hóa, không khẳng định đã chứng
   minh tính dừng thống kê. Chặn giá trị ngoài biên cố định, không fit theo test.
4. So sánh Logistic Regression, XGBoost thường/regularized và HistGradientBoosting;
   thử Platt calibration trên 7 ngày quá khứ tách khỏi fit; không chọn nó chỉ vì
   một chỉ số test thuận lợi. StandardScaler chỉ fit trên train.
5. Báo accuracy, balanced accuracy, precision/recall/F1 từng lớp, macro F1, AUC,
   log loss, Brier, reliability bins, confusion matrix và sai số theo ngày.

So sánh nhiều phương án thay cả model và đặc trưng; không được diễn giải đây là
một ablation thuần túy chứng minh riêng 42 đặc trưng gây ra toàn bộ cải thiện.

## 3. Chia tập và chống rò rỉ

Train mở rộng, validation là bốn tuần rời nhau:

| Fold | Validation, đầu gồm/cuối loại | Train chỉ dùng nhãn đã đóng trước |
|---|---|---|
| 1 | 31/08–07/09/2026 | 31/08/2026 00:00 UTC |
| 2 | 14/09–21/09/2026 | 14/09/2026 00:00 UTC |
| 3 | 28/09–05/10/2026 | 28/09/2026 00:00 UTC |
| 4 | 12/10–19/10/2026 | 12/10/2026 00:00 UTC |

Mọi fit/validation/calibration yêu cầu `label_available_at < boundary`, gồm cả thời
điểm đóng nến mục tiêu. Không xáo trộn. Ngưỡng 0.45/0.475/0.5/0.525/0.55 và 8 ứng viên
đã ghi trước; chọn theo balanced accuracy trung bình, hòa thì log loss rồi tên.
Không dùng test để chọn số cây, tham số, đặc trưng hoặc ngưỡng.

Fit cuối có **130.796 mẫu**, từ 25/07 03:58 tới 23/10 23:53; nhãn cuối đóng 23:59.
Test cuối **24/10 00:00–04/11 23:53**, nhãn cuối đóng 04/11 23:59: **17.274 mẫu**,
8.593 UP và 8.681 NOT_UP; có 24 nhãn giá bằng nhau. Sáu phút cuối bị loại để nhãn
không vượt sang biên tiếp theo. Artifact incumbent tạo ngày 23/10, kỳ train/test
cũ kết thúc trước cửa sổ so sánh này.

Ngày **05–09/11** từng được xem trong kiểm tra luồng trước: giữ làm chẩn đoán phụ,
không dùng chọn model và không gọi là test hoàn toàn mới. Ngay cả cửa sổ chính cũng
là phép kiểm định hồi cứu đã khóa trước thí nghiệm này, không phải một thử nghiệm
mù tiến cứu trên dữ liệu chưa từng tồn tại. Không thể dùng lịch sử một lần để bảo
đảm mọi tác giả trước đây chưa từng thấy nó.

## 4. Kết quả validation

Mỗi hàng dưới dùng ngưỡng tốt nhất được chọn **chỉ trong validation** cho ứng viên đó.

{table(vr)}

Chi tiết phương án được chọn:

{table(folds)}

Baseline validation tốt nhất là đảo chiều 5 phút, BA trung bình **51,806%**;
ứng viên đạt **51,885%**, chỉ hơn khoảng **0,079 điểm phần trăm**. Không coi sự
chênh lệch nhỏ này là kết luận chắc chắn; việc chọn giữa nhiều ứng viên cũng tạo
lạc quan trong validation, vì vậy cần tập test cuối riêng.

## 5. Kết quả test trên cùng 17.274 mẫu

{table(shown)}

Balanced accuracy là trung bình recall của UP và NOT_UP; đoán một lớp duy nhất đạt
50% khi cả hai lớp hiện diện. Macro F1 cho hai lớp trọng số bằng nhau. AUC đo khả
năng xếp hạng xác suất; model mới có AUC **0,5189**, thấp hơn incumbent **0,5231**,
nên không được nói model mới cải thiện mọi khía cạnh.

Log loss và Brier càng thấp càng tốt. Model mới tốt hơn xác suất cố định học từ
train nhưng mức hơn nhỏ. ECE 10 bins khoảng **0,536 điểm phần trăm**; ECE thấp không
đủ chứng minh dự báo hữu ích vì xác suất thường gần 50%. 98% xác suất nằm khoảng
44,81%–57,27%. Bin 0,6–0,7 chỉ có 58 mẫu, bin 0,7–0,8 có 2 mẫu, không đủ kết luận
về dự đoán tự tin cao. Quy tắc momentum/mean-reversion là tín hiệu cứng, không có
xác suất hiệu chỉnh nên để trống AUC/log loss/Brier.

Confusion matrix model mới, hàng=thực tế [NOT_UP, UP], cột=dự đoán [NOT_UP, UP]:

| | Dự đoán NOT_UP | Dự đoán UP |
|---|---|---|
| Thực tế NOT_UP | 5.383 | 3.298 |
| Thực tế UP | 5.094 | 3.499 |

Recall UP tăng từ **19,33%** lên **40,72%**, đổi lại recall NOT_UP giảm từ **82,99%**
xuống **62,01%**. Đây là đánh đổi giữa hai lớp, không phải tăng chính xác tuyệt đối.

## 6. Bất định và ngày model yếu

Bootstrap ghép cặp **2.000 lần theo 12 ngày UTC**, cùng ngày được lấy lại cho cả hai
model, giữ các mẫu phút trong ngày đi cùng nhau:

{ci}

Cả hai khoảng chứa 0. Các nhãn 5 phút chồng lấp không phải 17.274 quan sát độc lập;
12 cụm ngày còn ít và ngày liền nhau có thể phụ thuộc. Đây là khoảng tin cậy xấp
xỉ, chưa phải bằng chứng ý nghĩa thống kê mạnh hoặc lợi thế giao dịch sau phí.

- 30/10: BA **48,33%**, kém mức 50% của đoán một lớp.
- 28/10: BA **49,93%**; 01/11: BA **49,92%**. Riêng 01/11 accuracy **47,92%**
  cho thấy accuracy chịu ảnh hưởng tỷ lệ lớp.
- 31/10: BA **54,56%**, tốt hơn các ngày trên. Kết quả phụ thuộc giai đoạn.
- Có thể liên quan biến đổi chế độ thị trường, tín hiệu OHLCV yếu ở chân trời
  5 phút và độ dài lịch sử ngắn; đây là giả thuyết cần nghiên cứu, không phải
  nguyên nhân đã được chứng minh. Không dùng các ngày test yếu này để tinh chỉnh
  rồi chấm lại như thể test mới.
- Chẩn đoán phụ 05–09/11: 6.145 mẫu, model mới BA **51,31%**, incumbent **52,25%**,
  baseline đảo chiều 5 phút **51,63%**. Ứng viên không thắng trên mọi giai đoạn.

## 7. Quyết định triển khai

Điều kiện đã ghi trong protocol trước khi chạy:

{checks}

Ứng viên không đạt 2 điều kiện: số fold thắng và khoảng tin cậy chênh lệch dương.
Không hạ ngưỡng sau khi xem test. Model nghiên cứu ở `candidate.joblib`; không gọi
trainer legacy vốn ghi thẳng vào artifact đang chạy. Không thay worker, model S3
registry, DB hoặc kết quả 36 tháng trong lần thí nghiệm này. Kiểm tra parity là
cho ngữ cảnh **120 nến**; đường fallback legacy dùng ít nến hơn không được xem là
đã tự động chuyển sang hợp đồng mới.

## 8. Minh chứng và tái lập

- **49/49 kiểm thử trong phạm vi qua**: 27 kiểm tra mới ML + 8 outcome/input quality
  + 14 orderflow/worker. Không diễn giải thành toàn bộ test repo đã qua.
- **16/16 kiểm chứng bàn giao qua**: nhãn tính lại từ giá nguồn, metric tính lại từ
  confusion counts, dự đoán từ model đã lưu khớp, các biên thời gian/purge đúng,
  model và dữ liệu giữ checksum. Kiểm tra này đọc lại kết quả đã mở, không chọn lại.
- Bundle đính kèm có protocol, code SHA, runtime, CSV, dự đoán từng mẫu, model nghiên
  cứu, artifact incumbent, Parquet đã khóa và log. Dùng đúng thư viện trong
  `provenance.json`; bit-for-bit có thể khác giữa phần cứng/thư viện, nên đối chiếu
  bằng sai số số học đã quy định. Chỉ nạp joblib do pipeline tin cậy này tạo.
- Dùng kết quả đã lưu để viết báo cáo; nếu chạy lại để tái lập thì ghi rõ cùng
  snapshot và cùng test đã xem, không coi đó là lần kiểm định độc lập mới.

```powershell
# Từ thư mục repo, đọc/kiểm chứng phiên đã chạy, không fit lại
.venv\Scripts\python.exe scripts/ml/verify_short_term_experiment.py data/ml_experiments/{run.name}
# Quy trình thí nghiệm cho một phiên mới: select trước, evaluate sau
.venv\Scripts\python.exe scripts/ml/run_short_term_experiment.py select
.venv\Scripts\python.exe scripts/ml/run_short_term_experiment.py evaluate --run data/ml_experiments/<run-id>
```

## 9. Hướng nghiên cứu ML còn lại

1. Thu thêm dữ liệu tương lai sau snapshot và nhiều chế độ thị trường; đóng băng
   một holdout mới trước vòng điều chỉnh tiếp theo. Không tối ưu trên test đã mở.
2. Nghiên cứu nhãn có vùng trung tính theo biên độ/chi phí, hoặc thêm dữ liệu thực
   về order flow/biến số khác. Đây là bài toán mới, phải công bố và đánh giá lại,
   không thay nhãn âm thầm để làm đẹp accuracy.
3. Nếu muốn nói tới giao dịch, cần backtest có spread, phí, trượt giá và ràng buộc
   thời điểm giao dịch; kết quả phân loại hiện tại chưa chứng minh lợi nhuận.
4. Chỉ thay model khi bằng chứng ngoài mẫu đủ ổn định. Với đồ án, có thể trình bày
   trung thực kết quả âm/nhỏ, nguyên nhân giả định và các cải tiến đã thử.
5. Dự báo 36 tháng vẫn là đầu việc riêng. Các metric ở đây không chứng minh yêu
   cầu dự báo 3 năm đã đạt chất lượng mong muốn.
"""
    report_path=output/"Bao-cao-danh-gia-cai-thien-ML.md"
    report_path.write_text(report,encoding="utf-8")
    shutil.copy2(run/"evaluation.json",output/"Ket-qua-danh-gia-ML.json")
    shutil.copy2(run/"delivery_verification.json",output/"Kiem-chung-danh-gia-ML.json")
    archive=output/"Minh-chung-danh-gia-cai-thien-ML.zip"
    sources=read("provenance.json")["code_sha256"]
    source_files=list(sources)+["ml/dataset.py","scripts/ml/run_short_term_experiment.py",
        "scripts/ml/verify_short_term_experiment.py","scripts/ml/export_short_term_experiment.py",
        "tests/test_short_term_experiment.py","docs/SHORT_TERM_ML_EVALUATION.md","storage/canonical_snapshot.py",
        "docs/REPORT_GUIDE.md","docs/PROJECT_IMPROVEMENT_STATUS.md"]
    manifest={}
    with zipfile.ZipFile(archive,"w",zipfile.ZIP_DEFLATED) as bundle:
        def add(file,destination):
            bundle.write(file,destination);manifest[destination]=sha(file)
        for file in run.iterdir():
            if file.is_file() and file.suffix in (".json",".csv",".parquet",".joblib"):add(file,"experiment/"+file.name)
        for file in source_files:
            target = ROOT / file
            if not target.is_file():
                cand = ROOT / file.replace("ml/", "ml/realtime/").replace("scripts/", "scripts/ml/")
                if cand.is_file():
                    target = cand
            add(target, "source/" + file)
        for file in (ROOT/"ml/artifacts").iterdir():
            if file.is_file() and file.suffix==".json":add(file,"incumbent/"+file.name)
        add(ROOT/"configs/canonical_snapshot.json","source/configs/canonical_snapshot.json")
        parquet=ROOT/dataset["descriptor"]["local_gold_path"]/"symbol=BTC-USDT/canonical.parquet"
        add(parquet,"source/"+parquet.relative_to(ROOT).as_posix())
        for file in ["short-term-ml-selection.log","short-term-ml-evaluation.log","short-term-ml-tests.log","short-term-ml-verification.log"]:
            log_file = ROOT.parent / "local-run" / file
            if log_file.is_file():
                add(log_file, "logs/" + file)
        add(report_path,report_path.name)
        bundle.writestr("bundle_sha256.json",json.dumps(manifest,indent=2))
    with zipfile.ZipFile(archive) as bundle:
        if bundle.testzip() is not None:raise ValueError("Broken archive")
    print(json.dumps(dict(report=str(report_path),archive=str(archive),files=len(manifest),archive_sha256=sha(archive)),indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("run",type=Path);parser.add_argument("output",type=Path)
    args=parser.parse_args();export(args.run,args.output)
