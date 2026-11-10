"""Render completed audit evidence into Vietnamese Markdown and UTF-8 CSV."""
import argparse
import csv
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from processing.quality.inventory import sha256


def csv_file(path, rows, columns=None):
    rows = list(rows)
    columns = columns or list(dict.fromkeys(key for row in rows for key in row))
    with Path(path).open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def render(run_path, output):
    run_path, output = Path(run_path).resolve(), Path(output).resolve()
    data = json.loads((run_path / "run.json").read_text(encoding="utf-8"))
    assert data["state"] == "completed"
    assert all(sha256(ROOT / name) == digest for name, digest in data["source_sha256"].items()), "Implementation differs from audited version"
    assert sha256(run_path / "s3_inventory.json") == data["s3_inventory_sha256"]
    assert sha256(run_path / "downloaded_objects.json") == data["snapshot_objects_sha256"]
    output.mkdir(parents=True, exist_ok=True)
    datasets = data["s3"]["datasets"]
    inventory = []
    for item in datasets:
        quality = item.get("quality", {})
        interval = quality.get("continuity", {})
        inventory.append(dict(dataset=item["dataset"], scope=item["scope"], parquet_files=item["parquet_files"],
            bytes=item["bytes"], physical_rows=item["physical_rows"], committed_rows=item["logical_committed_rows"],
            unreferenced_files=item.get("unreferenced_files", 0), first_utc=interval.get("first"), last_utc=interval.get("last"),
            invalid_rows=quality.get("invalid_rows"), duplicate_rows=quality.get("duplicate_rows"),
            missing_between_bounds=interval.get("missing"), unknown_closed_rows=quality.get("unknown_closed_rows"),
            scan_mode=quality.get("profile_status", "footer_only_or_trade_replay")))
    csv_file(output / "Kiem-ke-dataset-S3.csv", inventory)
    manifest = json.loads((run_path / "s3_inventory.json").read_text(encoding="utf-8"))
    csv_file(output / "Kiem-ke-object-S3.csv", manifest["objects"])
    csv_file(output / "Kiem-ke-file-local.csv", data["local_files"])
    before_after = [dict(dataset=name, before=p["raw_rows"], rejected=p["rejected_rows"],
                         duplicates_removed=p["duplicate_rows_removed"], after=p["cleaned_rows"],
                         meaning="Production cleaning replay on frozen source; not an S3 publish")
                    for name, p in data["trade_replays"].items()]
    daily = data["daily_replay"]
    q = daily["quality"]
    before_after.append(dict(dataset="OKX daily / " + daily["run_id"], before=q["raw_rows"],
                            rejected=sum(q["rejected_rows"].values()), duplicates_removed=q["duplicate_rows_removed"],
                            after=q["valid_daily_rows"], meaning="Replayed raw rows equal published daily/monthly Parquet"))
    csv_file(output / "Thong-ke-truoc-sau-lam-sach.csv", before_after)
    gaps = []
    for item in datasets:
        for gap in item.get("quality", {}).get("continuity", {}).get("gaps", []):
            gaps.append(dict(dataset=item["dataset"], **gap))
    for symbol, profile in data["postgres"]["candles_by_symbol"].items():
        gaps.extend(dict(dataset="PostgreSQL / " + symbol, **g) for g in profile["continuity"]["gaps"])
    csv_file(output / "Cac-khoang-thieu-nen.csv", gaps, ["dataset", "start", "end", "missing"])
    fields = [
        ("trade_id", "Mã giao dịch", "String", "Khóa khử trùng giao dịch; với pipeline hiện tại chỉ BTC-USDT"),
        ("symbol", "Cặp giao dịch", "BTC-USDT", "BTC là tài sản cơ sở, USDT là tài sản báo giá"),
        ("price", "Giá khớp", "USDT/BTC", "Phải hữu hạn và > 0"),
        ("size / volume / volume_btc", "Khối lượng", "BTC", "Giao dịch size > 0; nến cho phép volume = 0"),
        ("side", "Phía chủ động của giao dịch", "buy/sell", "Không phải danh tính người mua/bán hay sổ lệnh"),
        ("timestamp / raw_timestamp", "Thời gian sự kiện gốc", "Unix millisecond UTC", "Chia 1000 để có giây"),
        ("trade_time", "Thời gian khớp", "Timestamp UTC", "Từ raw_timestamp, không thay bằng giờ nhận Kafka"),
        ("kafka_timestamp", "Giờ Kafka ghi nhận", "Timestamp UTC", "Dùng theo dõi thu nhận, có thể khác event time"),
        ("kafka_topic / partition / offset", "Vị trí bản ghi Kafka", "Topic + số nguyên", "Offset chỉ có nghĩa trong cùng topic/partition"),
        ("window_start / window_end", "Cửa sổ nến phút", "UTC; [start, end)", "Đúng mốc phút, độ dài 60 giây"),
        ("open/high/low/close", "Giá mở/cao/thấp/đóng nến", "USDT/BTC", "Low <= Open,Close <= High; mọi giá > 0"),
        ("trade_value / total_trade_value / turnover_usdt", "Doanh số", "USDT", "Giao dịch = price × size; doanh số REST lấy từ nguồn, không tự suy đoán khi thiếu"),
        ("vwap", "Giá bình quân theo khối lượng", "USDT/BTC", "Doanh số / khối lượng khi đủ thông tin"),
        ("trade_count", "Số giao dịch đóng góp", "Số giao dịch", "REST có thể không cung cấp; NULL không phải 0"),
        ("buy_volume / sell_volume", "Khối lượng theo phía chủ động", "BTC", "REST có thể NULL; không chia đôi volume để điền"),
        ("trade_imbalance", "Cán cân khối lượng", "[-1,1]", "(buy_volume - sell_volume)/(buy_volume + sell_volume) khi có đủ dữ liệu"),
        ("source", "Nguồn dữ liệu", "historical_rest/realtime_stream…", "Phân biệt quyền ưu tiên REST với streaming"),
        ("is_closed / confirm", "Xác nhận nến hoàn tất", "True/False/NULL hoặc 1/0", "NULL là chưa biết; không mặc định thành True trong audit"),
        ("_source_object / _source_sha256", "Truy vết audit", "S3 key / SHA-256", "Từ dòng sau làm sạch quay lại file nguồn cố định; không phải lineage có sẵn cho mọi bản ghi lịch sử"),
    ]
    csv_file(output / "Tu-dien-du-lieu.csv", [dict(field=a, meaning=b, unit=c, rule=d) for a,b,c,d in fields])
    primary=data["s3"].get("effective_canonical_dataset","crypto/gold/candles_1m")
    gold = next(x for x in datasets if x["dataset"] == primary)
    gq = gold["quality"]
    db = data["postgres"]
    dp = db["candles_by_symbol"]["BTC-USDT"]
    rows = ["# Kiểm kê dữ liệu và thống kê trước/sau làm sạch", "",
            f"Bàn giao ngày 09/11/2026. Run: `{data['run_id']}`.",
            f"S3 được kiểm kê trong khoảng **{data['s3']['inventory_window'][0]} → {data['s3']['inventory_window'][1]}**.",
            f"PostgreSQL là snapshot riêng tại **{db['snapshot_utc']}**. Tất cả các mốc bên dưới là UTC.", "",
            "## 1. Kết quả chính", "",
            "Đã kiểm kê object S3 hiện hữu, đếm dòng Parquet, đọc log commit Spark, kiểm tra dữ liệu",
            "nghiệp vụ đã commit, kiểm kê file local và chụp PostgreSQL bằng transaction read-only/repeatable-read.",
            "Đã chạy lại **hàm làm sạch production thật** trên nguồn Bronze cố định và đối chiếu lại",
            "raw → daily → monthly của snapshot dự báo đã xuất bản. Không ghi/xóa S3 hoặc sửa DW.", "",
            f"- S3: **{data['s3']['objects']:,} object**, **{data['s3']['bytes']:,} byte**; gồm cả checkpoint, staging và audit.",
            f"- Có **{data['s3']['objects_after_cutoff']} object sau cutoff** trong listing; không đưa nội dung các object đó vào snapshot phân tích.",
            f"- Gold canonical được chọn: `{primary}`; **{gq['rows']:,} nến phút**, thiếu **{gq['continuity']['missing']:,} phút** giữa hai đầu.",
            f"- Gold canonical được chọn có **{gq.get('unknown_closed_rows',0):,} dòng không rõ cờ đóng nến**; không được tự coi là nến đã đóng.",
            f"- DW hiện có **{dp['rows']:,} nến**; đây là dữ liệu khác phạm vi Gold dùng chung, không cộng hai nguồn thành một tập.",
            f"- Dữ liệu dài hạn: **{q['raw_rows']:,} → {q['valid_daily_rows']:,} nến ngày**, tổng hợp **{daily['complete_months']} tháng đầy đủ**.", "",
            "Số liệu là bằng chứng tại thời điểm snapshot. Live tiếp tục chạy nên số dòng hiện tại có thể tăng.",
            "Không phát hiện lỗi số học/OHLC trong phần được quét không đồng nghĩa dữ liệu đủ mọi thời điểm",
            "hoặc đủ điều kiện huấn luyện. Các thiếu hụt và giới hạn được ghi riêng bên dưới.", "",
            "Tần suất theo cấu hình: giao dịch đến liên tục, Spark trigger 10 giây, nến phút có bước 60 giây;",
            "dữ liệu dài hạn dùng nến ngày UTC và tổng hợp tháng lịch. Đây là cadence cấu hình/độ hạt,",
            "không chứng minh hệ thống đã thu nhận liên tục. Batch Silver/Gold của phiên chạy chưa có bằng chứng lịch tự động.", "",
            "## 2. Kiểm kê S3 theo vai trò", "",
            "| Vùng | Object | Byte | File Parquet |", "|---|---:|---:|---:|"]
    for scope, counts in data["s3"]["scopes"].items():
        rows.append(f"| {scope} | {counts['objects']:,} | {counts['bytes']:,} | {counts['parquet_files']:,} |")
    rows += ["", "`shared`: dữ liệu dùng chung; `active_run`: prefix phiên chạy đang cấu hình;",
             "`staging`: bản trung gian; `test_or_audit`: kiểm thử cũ; `checkpoint`: trạng thái Spark;",
             "`forecast_snapshot`: snapshot dự báo; `model`: artifact model. Không cộng lẫn các vai trò.", "",
             "| Dataset | Vai trò | Dòng vật lý | Dòng đã commit/listing | File chưa được log ghi nhận |",
             "|---|---|---:|---:|---:|"]
    for item in datasets:
        rows.append(f"| `{item['dataset']}` | {item['scope']} | {item['physical_rows']:,} | {item['logical_committed_rows']:,} | {item.get('unreferenced_files',0)} |")
    rows += ["", "Các dataset có `_spark_metadata` dùng compact log mới nhất và các batch nối tiếp",
             "để xác định file đã commit. File không có trong log có thể chưa commit tại mốc snapshot",
             "hoặc là file thừa; **không tự gọi là dữ liệu hỏng và không tự xóa**. Dataset không có log",
             "được đánh dấu `directory_listing`; `_SUCCESS` một mình không chứng minh toàn bộ lịch sử xử lý.", "",
             "Staging chỉ kiểm footer/count/checksum; không đánh đồng với dữ liệu production đã publish.",
             "`same_parquet_bytes_as` trong JSON chỉ ra tập file có cùng byte/checksum; đây không phải",
             "lý do cộng thêm dữ liệu. Inventory chỉ xét phiên bản object hiện tại, không gồm version cũ.", "",
             "## 3. Bảng trước/sau làm sạch có thể đưa vào báo cáo", "",
             "| Nguồn cố định | Trước | Loại không hợp lệ/chưa đóng | Loại trùng | Sau |",
             "|---|---:|---:|---:|---:|"]
    for item in before_after:
        rows.append(f"| `{item['dataset']}` | {item['before']:,} | {item['rejected']:,} | {item['duplicates_removed']:,} | {item['after']:,} |")
    rows += ["", "Phép đối chiếu: **Trước = Loại không hợp lệ + Loại trùng + Sau**. Lý do lỗi",
             "chi tiết có hai dạng: `primary` mỗi dòng chỉ tính một lần; `overlapping` cho biết tất cả",
             "lỗi của dòng đó nên không được cộng thành tổng dòng lỗi.", "",
             "Dữ liệu thật không có lỗi/trùng theo các quy tắc này thì ghi đúng là 0; không chèn",
             "dòng lỗi giả để làm đẹp bảng. Khả năng bắt lỗi được kiểm thử riêng bằng fixture synthetic.",
             "Đối với Gold lịch sử 129.613 nến: đây là profile đầu ra đang lưu. Chưa có bằng chứng raw cùng",
             "phiên bao phủ toàn bộ tập này để tái lập số dòng từng bị loại; không được suy ngược rejected=0",
             "chỉ vì đầu ra hiện không phát hiện giá/OHLC sai.", ""]
    for name, profile in data["trade_replays"].items():
        comparison = profile["stored_silver_comparison"]
        rows += [f"### `{name}`", "",
                 f"- Khoảng event time: {profile['first_event']} → {profile['last_event']} UTC.",
                 f"- Mã giao dịch xung đột: {profile['conflicting_trade_ids']}; trường hợp không đủ khóa phân thắng khi dedup: {profile['ambiguous_dedup_ties']}.",
                 f"- Lỗi giá trị doanh số phát sinh sau tính toán: {profile['derived_trade_value_non_finite']}."]
        if comparison.get("status") == "absent":
            rows += ["- **Chưa có Silver đã publish trong prefix này.** Sau làm sạch ở bảng trên là replay local có lưu",
                     "  input/output Parquet và lineage, không phải tuyên bố batch Silver/Gold đang tự động chạy trên S3."]
        else:
            rows += [f"- Silver lưu sẵn: {comparison['stored_rows']} dòng; khớp {comparison['common_trade_ids']} trade_id,",
                     f"  khác giá trị trên {comparison.get('mismatching_common_trade_ids','chưa đo')} trade_id trong các trường đã đối chiếu."]
        rows += [""]
    rows += ["### Dữ liệu dự báo dài hạn", "",
             f"Run gốc `{daily['run_id']}`; source as_of **{daily['source']['as_of']}**.",
             f"Phạm vi nến ngày: **{q['first_day']} → {q['last_day']}**; thiếu {q['missing_daily_count']} ngày",
             "giữa hai đầu; không nội suy. Replay dùng giờ chốt của nguồn, không dùng giờ hiện tại để đổi kết quả cũ.",
             "Có một nến chưa đóng được loại. Hai tháng biên chưa đủ ngày được loại khỏi **tổng hợp tháng**:"]
    rows += [f"- {item['month']}: có {item['observed_days']}/{item['expected_days']} ngày." for item in daily["excluded_months"]]
    rows += ["", "Từ nến ngày sang tháng là phép tổng hợp, không phải hàng nghìn dòng bị mất do làm sạch.",
             "Daily/monthly tái tạo khớp snapshot đã publish; hai file Parquet trên S3 được đối chiếu SHA-256 với local.",
             "Mốc nguồn cũ không tự trở thành dữ liệu cập nhật đến ngày kiểm kê; chưa tải thêm ngày mới trong đầu việc này.", "",
             "## 4. Chất lượng và khoảng thiếu", "",
             "| Nguồn nến | Dòng | OHLC/thời gian/kiểu số sai | Trùng timestamp | Phút thiếu giữa hai đầu | Cờ đóng nến NULL |",
             "|---|---:|---:|---:|---:|---:|"]
    for item in datasets:
        quality = item.get("quality", {})
        if quality.get("profile_status") == "full_scan":
            rows.append(f"| `{item['dataset']}` | {quality['rows']:,} | {quality['invalid_rows']} | {quality['duplicate_rows']} | {quality['continuity']['missing']:,} | {quality.get('unknown_closed_rows','không có cột')} |")
    rows.append(f"| PostgreSQL BTC-USDT | {dp['rows']:,} | {dp['invalid_rows']} | {dp['duplicate_rows']} | {dp['continuity']['missing']:,} | {dp.get('unknown_closed_rows',0)} |")
    rows += ["", "Phút thiếu = số ô phút UTC mong đợi giữa min/max trừ số timestamp hợp lệ duy nhất.",
             "Đó không phải tỷ lệ API hoạt động hay phần trăm toàn thị trường đã thu thập. Với dữ liệu",
             "trade, phút không quan sát được giao dịch có thể do không thu nhận hoặc không có giao dịch;",
             "không suy diễn rằng toàn sàn không giao dịch. Không tạo nến giả bằng forward-fill.", "",
             f"Trong 24 giờ đã kết thúc ở mốc DW: có **{dp['last_24h_closed_minute_grid']['observed']}/1440**",
             f"timestamp phút, thiếu **{dp['last_24h_closed_minute_grid']['missing']}**; đây là hiện diện timestamp,",
             f"chưa khẳng định mọi nến đều finalized. Có **{dp.get('unclosed_rows',0)} nến chưa đóng**, trong đó",
             f"**{dp['unclosed_older_than_5m']}** nến đã cũ hơn 5 phút mà chưa đóng.", "",
             "NULL ở trade_count/buy_volume/sell_volume của REST là thông tin không được nguồn cung cấp,",
             "không phải tự động là bản ghi sai. Ngược lại, NULL ở is_closed nghĩa là thiếu bằng chứng xác nhận",
             "hoàn tất; cần đối soát nguồn trước khi đưa vào bộ huấn luyện yêu cầu nến đã đóng.", "",
             "## 5. PostgreSQL và đối soát", "",
             "| Bảng | Dòng | Dung lượng bảng + index (byte) | Min / Max sự kiện UTC |", "|---|---:|---:|---|"]
    for name, item in db["tables"].items():
        rows.append(f"| `{name}` | {item.get('rows','không có')} | {item.get('relation_bytes','—')} | {item.get('first_event','—')} / {item.get('last_event','—')} |")
    rows += ["", "Dung lượng `pg_total_relation_size` gồm index/TOAST và không thể so trực tiếp với file Parquet nén.",
             "`fact_crypto_trades` trong nhánh streaming chỉ nhận giao dịch lớn ≥100.000 USDT, không phải",
             "mọi giao dịch Bronze. Chênh lệch số dòng giữa hai bảng không được gọi là tỷ lệ mất dữ liệu.", "",
             "| Lake → DW | Khóa chung | Khác vượt độ chính xác DW | Chỉ khác do làm tròn | Chỉ có ở lake | Chỉ có ở DW |",
             "|---|---:|---:|---:|---:|---:|"]
    for name, item in data["lake_dw_reconciliation"].items():
        rows.append(f"| `{name}` | {item['common_keys']} | {item['mismatching_common_keys']} | {item['rounding_only_common_keys']} | {item['lake_only_keys']} | {item['warehouse_only_keys']} |")
    rows += ["", "Chỉ đối chiếu cùng `(symbol, window_start)`. Đọc numeric_scale trực tiếp từ schema DW tại snapshot;",
             "dung sai là nửa đơn vị lưu trữ NUMERIC cộng 4 ULP do số thực. Doanh số DW scale=4, không so nó",
             "như float chính xác vô hạn. JSON vẫn giữ cả số khác ở độ chính xác thô và số chỉ lệch do làm tròn.",
             "Không có khóa chung thì **chưa đủ bằng chứng đồng bộ**. Khác nhau có thể do thời điểm chụp,",
             "REST canonical cập nhật lại nến hoặc streaming còn thiếu giao dịch; audit không tự ghi đè để ép khớp.", "",
             "## 6. Quy tắc làm sạch và khả năng truy vết", "",
             "```mermaid", "flowchart LR", " A[Manifest S3 + ETag + SHA-256] --> B[Đọc log commit Spark]",
             " B --> C[Snapshot Bronze cục bộ]", " C --> D[Hàm transform production]",
             " D --> E[Rejected + duplicate counters]", " D --> F[Cleaned Parquet + lineage]",
             " G[Snapshot DW read-only] --> H[Đối soát khóa và OHLCV]", " F --> I[Bảng trước/sau]", "```", "",
             "Trade được giữ khi có trade_id, đúng BTC-USDT, price/size hữu hạn >0, timestamp >0 và side buy/sell.",
             "Khử trùng trade_id theo raw_timestamp mới nhất rồi Kafka offset cao nhất. Audit ghi nhận cả trường hợp",
             "khóa sắp xếp còn hòa mà giá trị xung đột; không tự tuyên bố lựa chọn là duy nhất nếu thiếu khóa phân thắng.",
             "Thời gian được chuẩn hóa UTC. Đây là các quy tắc hiện có, không phải bằng chứng rằng mọi lỗi ngữ nghĩa đều bị chặn.", "",
             "Mỗi dòng replay giữ `_source_object`, `_source_sha256`, trade_id và Kafka partition/offset khi có.",
             "Có thể nối kết quả → dòng nguồn → object → SHA-256 → run audit → phiên bản code/config.",
             "Lineage mới này **không khôi phục lịch sử phiên thu thập/ETL đã thiếu cho mọi bản ghi cũ**.",
             "Quarantine đã lưu **trong audit local**; batch production trước đó chỉ đếm/loại lỗi và chưa có",
             "quarantine bền vững trên S3 kèm manifest cho từng phiên. Chưa thiết lập lịch Silver/Gold trong đầu việc này.", "",
             "## 7. Các việc còn phải xử lý từ bằng chứng này", "",
             "1. Đối soát/backfill các khoảng nến phút thiếu bằng REST; chỉ cập nhật nguồn chuẩn sau khi kiểm geometry,",
             "   xác nhận đóng nến và quy tắc canonical. Không lấp số liệu bằng nến giả.",
             "2. Xác minh cờ đóng nến bị NULL trong Gold lịch sử từ nguồn raw/REST; không đổi hàng loạt NULL thành True.",
             "3. Xuất bản Silver/Gold cho phiên chạy sau khi replay đã kiểm chứng; bổ sung manifest, quarantine bền vững,",
             "   lịch batch và trạng thái từng phiên. Hoàn thành riêng đầu việc vận hành này.",
             "4. Xác định tập nến canonical thống nhất giữa S3 và DW; báo cáo miền thời gian khác nhau trước khi so số dòng.",
             "5. Sau khi khóa phiên bản dữ liệu phù hợp, tiếp tục đánh giá ML nhiều giai đoạn; kiểm kê/làm sạch tốt",
             "   không tự làm MAE/accuracy tốt hơn và không bảo đảm điểm tuyệt đối.", "",
             "## 8. Tái lập và giới hạn", "",
             "```powershell", "python scripts/run_data_quality_audit.py --snapshot-only",
             "python scripts/run_data_quality_audit.py --from-snapshot data/quality_audits/<snapshot-run>",
             "python scripts/render_data_quality_report.py --run data/quality_audits/<completed-run> --output <report-directory>",
             "python -m unittest discover -s tests -p test_data_quality_audit.py -v",
             "python -m unittest discover -s tests -p test_quality_trade_replay.py -v", "```", "",
             "Cần Python/Arrow/Pandas, Spark/Java và cấu hình S3/PostgreSQL hiện có; không đưa secret vào báo cáo.",
             "Lần chạy mới có ID riêng; khi reuse snapshot sẽ kiểm lại SHA-256 từng file. GET có IfMatch ETag,",
             "nhưng ETag không được coi là SHA-256. S3 listing nhiều object không phải snapshot transaction nguyên tử;",
             "các file được cố định theo manifest/cutoff và kiểm trước khi đọc. DB là snapshot ở thời điểm riêng.",
             "Không quét version S3 cũ. Local inventory không cộng các file sinh ra của chính audit/checkpoint vào dữ liệu nguồn.",
             "Bản inventory và replay này không xuất bản lại model, không sửa dữ liệu nguồn, không hoàn thành các đầu việc ML/batch tiếp theo.", ""]
    rows += ["## 9. File bàn giao để dùng khi viết báo cáo", "",
             "- `Thong-ke-truoc-sau-lam-sach.csv`: bảng số dòng và phép cân bằng trước/sau.",
             "- `Kiem-ke-dataset-S3.csv`, `Kiem-ke-object-S3.csv`, `Kiem-ke-file-local.csv`: quy mô, vị trí và checksum.",
             "- `Cac-khoang-thieu-nen.csv`: đầy đủ các khoảng thiếu trong phạm vi đã kiểm tra.",
             "- `Tu-dien-du-lieu.csv`: ý nghĩa trường, đơn vị BTC/USDT và quy tắc sử dụng NULL.",
             "- `Ket-qua-kiem-ke-va-lam-sach.json`: toàn bộ số đo, nguồn/config hash và thời điểm snapshot.",
             "- Bộ ZIP minh chứng: mã nguồn, log test, input/output replay, ví dụ lineage và nến loại ở nguồn ngày.", ""]
    (output / "Bao-cao-kiem-ke-va-lam-sach.md").write_text("\n".join(rows), encoding="utf-8")
    (output / "Ket-qua-kiem-ke-va-lam-sach.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(render(args.run, args.output))
