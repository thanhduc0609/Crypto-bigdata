/* Measured operational status and frozen research evidence; never fake liveness. */
(() => {
    const node = (tag, text, cls) => { const e = document.createElement(tag); if (text !== undefined) e.textContent = text; if (cls) e.className = cls; return e; };
    const number = x => Number.isFinite(x) ? x.toLocaleString('vi-VN') : '—';
    const percent = x => Number.isFinite(x) ? (100 * x).toFixed(2).replace('.', ',') + '%' : '—';
    const batchLabels = {running:'Đang xử lý',success:'Hoàn tất',failed:'Lượt gần nhất bị lỗi',no_data:'Không có dữ liệu mới'};
    const utc = x => { const d = new Date(x); return x && !Number.isNaN(d.valueOf()) ? d.toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : '—'; };
    const card = (title, status, lines, fresh) => {
        const e = node('article', undefined, 'evidence-stage');
        e.append(node('h3', title));
        e.append(node('span', fresh && status === 'verified' ? 'Đã kiểm chứng' : status === 'error' ? 'Lỗi phép đo' : 'Chưa có phép đo mới', fresh && status === 'verified' ? 'evidence-tag' : 'evidence-tag muted'));
        lines.forEach(line => e.append(node('p', line)));
        return e;
    };
    async function pipeline() {
        const box = document.getElementById('pipeline-stages');
        const caption = document.getElementById('pipeline-measured');
        if (!box || !caption) return;
        try {
            const r = await fetch('/api/system/status', {signal: AbortSignal.timeout(8000)});
            if (!r.ok) throw Error('status');
            const data = await r.json(); const s = data.stages || {}; const fresh = data.status === 'current';
            caption.dataset.state = fresh ? 'current' : 'stale';
            caption.textContent = fresh ? 'Đo lúc ' + utc(data.checked_at) : 'Chưa có phép đo mới — không xác nhận hệ thống trực tuyến';
            const k = s.kafka || {}, b = s.bronze || {}, w = s.warehouse || {}, g = s.batch || {}, q = s.scheduler || {};
            const recent = t => t && (Date.now() - Date.parse(t)) < 180000;
            box.replaceChildren(
                card('1 · Kafka', k.status, ['Cuối log: ' + number(k.end_offsets ? Object.values(k.end_offsets).reduce((a, v) => a + v, 0) : null),
                    'Bản ghi mới giữa hai phép đo: ' + number(k.new_records_since_previous_measurement)], fresh),
                card('2 · Spark → Bronze', b.status, ['Batch đã commit: ' + number(b.committed_batch), 'Chậm so Kafka: ' + number(b.lag_records) + ' bản ghi'], fresh && recent(b.checked_at)),
                card('3 · Silver → Gold', g.status, ['Giao dịch sạch: ' + number(g.output?.silver_rows), 'Nến tổng hợp: ' + number(g.output?.gold_rows), 'Hoàn tất: ' + utc(g.completed_at)], fresh && g.status !== 'stale'),
                card('4 · PostgreSQL', w.status, ['Nến đang lưu: ' + number(w.candle_rows), 'Khóa nến trùng: ' + number(w.duplicate_candle_keys), 'Nến mới nhất: ' + utc(w.latest_candle)], fresh && recent(w.latest_candle)),
                card('5 · Dự báo 5 phút', w.status, ['Đã ghi: ' + number(w.predictions), 'Đã có kết quả đối chiếu: ' + number(w.evaluated_predictions), 'Nến đầu vào mới nhất: ' + utc(w.latest_prediction_feature)], fresh && recent(w.latest_prediction_feature)),
                card('6 · Lịch batch', q.status, ['Chu kỳ: ' + number(q.interval_seconds) + ' giây', 'Lượt hiện tại: ' + (batchLabels[g.current_status] || 'chưa xác minh')], fresh)
            );
            const detail = document.getElementById('pipeline-data-quality');
            const c = g.cleaning;
            detail.replaceChildren(node('p', 'Dữ liệu ML đã khóa: 154.351 nến REST đóng, liên tục từ 25/07 đến 09/11/2026. Đây là dữ liệu nghiên cứu cố định, khác số nến DW đang tiếp tục cập nhật.'));
            if (c) detail.append(node('p', `Phiên batch gần nhất: ${number(c.total)} dòng đầu vào → ${number(c.valid)} giao dịch sạch; loại ${number(c.rejected)} dòng không hợp lệ và ${number(c.duplicates)} dòng trùng. Số 0 nghĩa không phát hiện lỗi tương ứng trong phiên này.`));
            detail.append(node('p', 'Gold từ giao dịch là kết quả tổng hợp trong thời gian đã thu thập, không cam kết đủ toàn bộ giao dịch của sàn. Nến REST chuẩn được ưu tiên trong DW.'));
        } catch (_) {
            caption.dataset.state = 'error';
            caption.textContent = 'Không đọc được phép đo hệ thống. Hãy kiểm tra kết nối và dịch vụ monitor.';
            box.replaceChildren(node('p', 'Trạng thái luồng hiện chưa được xác minh.'));
        }
    }
    async function review() {
        const box = document.getElementById('ml-review-content');
        if (!box) return;
        try {
            const r = await fetch('/api/ml/review', {signal: AbortSignal.timeout(8000)});
            if (!r.ok) throw Error('review');
            const d = await r.json();
            box.replaceChildren(node('p', `Đang dùng ${d.active_model}. Kiểm định hồi cứu trên ${number(d.cohort.rows)} mẫu, cùng tập dữ liệu cho các phương án.`));
            const t = node('table', undefined, 'terminal-table');
            const head = node('tr'); ['Phương án', 'Accuracy', 'Balanced accuracy'].forEach(v => head.append(node('th', v))); t.append(head);
            [['Đoán lớp phổ biến từ train', d.majority_baseline], [d.active_model, d.active_metrics], ['Logistic nghiên cứu mới', d.candidate_metrics]].forEach(([label, m]) => {
                const row = node('tr'); [label, percent(m.accuracy), percent(m.balanced_accuracy)].forEach(v => row.append(node('td', v))); t.append(row);
            }); box.append(t);
            box.append(node('p', 'Ứng viên mới chỉ hơn nhẹ, thắng baseline tốt nhất ở 2/4 fold; khoảng tin cậy chênh lệch chứa 0 nên chưa thay mô hình đang chạy. Xác suất dự báo không phải tỷ lệ thắng giao dịch.'));
        } catch (_) { box.replaceChildren(node('p', 'Chưa đọc được kết quả đánh giá ML đã xác minh trên máy này.')); }
    }
    async function longTermResearch() {
        const box = document.getElementById('lt-research-content');
        if (!box) return;
        try {
            const r = await fetch('/api/forecast/long-term/research', {signal: AbortSignal.timeout(8000)});
            if (!r.ok) throw Error('research'); const d = await r.json();
            box.replaceChildren(node('p', 'Nghiên cứu bổ sung chọn mô hình bằng validation bên trong từng mốc, rồi dự báo đủ 36 tháng phía sau. Ba cửa sổ chồng lấn và dữ liệu đã từng được xem: đây là đánh giá hồi cứu, không phải ba kiểm định độc lập mới.'));
            const wrap = node('div', undefined, 'table-container'); const table = node('table', undefined, 'terminal-table');
            const head = node('tr'); ['Mốc kết thúc train', 'Phương án được chọn', 'MAE (USDT)', 'MAE baseline', 'Giảm MAE'].forEach(v => head.append(node('th', v))); table.append(head);
            const price = v => new Intl.NumberFormat('vi-VN', {maximumFractionDigits: 2}).format(v);
            const modelNames = {anchored_ensemble:'Kết hợp baseline + Ridge',naive_last:'Giữ giá cuối (baseline)',
                ridge_strong:'Ridge regularization mạnh',ridge_returns:'Ridge lợi suất',log_drift:'Xu hướng lợi suất log',damped_log_trend:'Xu hướng log giảm dần'};
            d.outer_folds.forEach(f => {
                const row = node('tr'); [f.train_end, modelNames[f.selected_model] || f.selected_model, price(f.selected.mae_usdt), price(f.baseline.mae_usdt), percent(1-f.selected.mae_usdt/f.baseline.mae_usdt)].forEach(v => row.append(node('td', v))); table.append(row);
            }); wrap.append(table);box.append(wrap);
            box.append(node('p', 'Cả 3 mốc chọn phương án kết hợp baseline và Ridge mạnh. Trên toàn bộ 105 tháng, validation lại chọn baseline giữ giá cuối. Vì vậy lợi thế quá khứ không được xem là bảo đảm dự báo tương lai.'));
            box.append(node('p', 'Dải 95% có độ phủ 100% ở các cửa sổ trên nhưng rất rộng, trung bình khoảng 212.741–286.702 USDT; không diễn giải thành dự báo đúng 100%. Bản thí nghiệm gốc trên biểu đồ vẫn được giữ nguyên.'));
        } catch (_) { box.replaceChildren(node('p', 'Chưa có kết quả dài hạn bổ sung đã xác minh.')); }
    }
    document.getElementById('btn-refresh')?.addEventListener('click', pipeline);
    pipeline(); review(); longTermResearch(); setInterval(pipeline, 30000);
})();
