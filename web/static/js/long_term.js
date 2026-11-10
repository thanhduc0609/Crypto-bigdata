/* Independent read-only monthly forecast; never triggers training from the UI. */
(function () {
    'use strict';
    const el = id => document.getElementById(id);
    const fmt = n => new Intl.NumberFormat('vi-VN', {maximumFractionDigits: 2}).format(n);
    let data, activeView = 'forecast';
    if (!el('lt-chart')) return;
    const renderer = window.createLongTermChart(el, fmt);
    function draw(view) {
        activeView = view;
        document.querySelectorAll('.lt-view').forEach(b => {
            b.classList.toggle('active', b.dataset.view === view);
            b.setAttribute('aria-pressed', b.dataset.view === view ? 'true' : 'false');
        });
        renderer.draw(view);
    }
    function paragraph(parent, text) {
        const n = document.createElement('p'); n.textContent = text; parent.appendChild(n);
    }
    async function load() {
        try {
            const response = await fetch('/api/forecast/long-term', {signal: AbortSignal.timeout(15000)});
            const result = await response.json();
            if (!response.ok) throw new Error(result.message || 'Không thể đọc dự báo dài hạn');
            data = result; renderer.setData(data);
            el('lt-status').textContent = `${data.model_label} · Dữ liệu thật từ OKX · Snapshot ${data.generated_at.slice(0, 10)}`;
            el('lt-content').hidden = false;
            const summary = el('lt-summary'); summary.replaceChildren();
            [`${fmt(data.data_quality.valid_daily_rows)} nến ngày`, `${data.history.length} tháng đầy đủ`,
                `Lịch sử: ${data.history[0].month} → ${data.as_of_month}`, `Dự báo: ${data.forecast[0].month} → ${data.forecast.at(-1).month}`]
                .forEach(text => { const n = document.createElement('span'); n.textContent = text; summary.appendChild(n); });
            const ev = data.evaluation;
            const report = data.reporting, split = el('lt-split'); split.replaceChildren();
            [`Học và chọn phương pháp: ${ev.train_start} → ${ev.train_end} (${report.selection_months} tháng).`,
                `Thử dự báo quá khứ: ${ev.test_start} → ${ev.test_end} (${ev.holdout_months} tháng, đã có giá thật để chấm).`,
                `Sau đánh giá: fit lại toàn bộ ${report.refit_months} tháng, dự báo ${data.forecast[0].month} → ${data.forecast.at(-1).month}.`]
                .forEach(text => {const item = document.createElement('li'); item.textContent=text; split.appendChild(item);});
            const change = report.mae_change_vs_baseline_pct;
            el('lt-evaluation').textContent = `Trong 36 tháng quá khứ, dự báo lệch trung bình ${fmt(ev.selected.mae_usdt)} USDT mỗi tháng (MAE); baseline lệch ${fmt(ev.baseline.mae_usdt)} USDT. ` +
                (change === null ? 'Baseline có MAE bằng 0; không tính tỷ lệ thay đổi.' : change > 0
                    ? `Sai số của phương pháp được chọn cao hơn baseline ${fmt(change)}%.`
                    : change < 0 ? `Sai số thấp hơn baseline ${fmt(-change)}% trong đoạn này.` : 'Sai số bằng baseline trong đoạn này.');
            const comparison = el('lt-comparison'); comparison.replaceChildren();
            report.comparisons.forEach(row => {
                const tr = document.createElement('tr');
                [row.label, row.selected_before_test ? 'Chọn bằng validation' : row.model === 'naive_last' ? 'Baseline' : 'So sánh thí nghiệm',
                    fmt(row.mae_usdt),fmt(row.rmse_usdt),fmt(row.smape_pct)].forEach((text,i) => {
                    const td=document.createElement('td'); td.textContent=text; if(i>=2)td.className='num-col tabular'; tr.appendChild(td);
                }); comparison.appendChild(tr);
            });
            const band = report.intervals.find(row => row.nominal_level === 95);
            el('lt-interval-quality').textContent = `Dải danh nghĩa 95% chứa ${fmt(band.observed_coverage_pct)}% giá thật trong test, nhưng rộng trung bình ${fmt(band.mean_width_usdt)} USDT. Cần xem cả độ phủ lẫn độ rộng; đây không phải tỷ lệ dự báo đúng.`;
            const table = el('lt-table'); table.replaceChildren();
            data.forecast.forEach(row => {
                const tr = document.createElement('tr');
                [row.month, fmt(row.predicted), `${fmt(row.lower_80)} – ${fmt(row.upper_80)}`, `${fmt(row.lower_95)} – ${fmt(row.upper_95)}`].forEach((value, i) => {
                    const td = document.createElement('td'); td.textContent = value;
                    if (i) td.className = 'num-col tabular'; tr.appendChild(td);
                }); table.appendChild(tr);
            });
            const method = el('lt-method'); method.replaceChildren();
            paragraph(method, `Nguồn OKX public REST: nến 1Dutc từ ${data.data_quality.first_day} đến ${data.data_quality.last_day}; tổng hợp tháng UTC, không nội suy ngày thiếu, loại tháng chưa đủ ngày.`);
            paragraph(method, `Chọn mô hình bằng rolling validation ${data.validation.horizon_months} tháng, chỉ dùng dữ liệu đến ${data.validation.latest_month_available_for_selection}. Giữ riêng 36 tháng cuối để đánh giá, sau đó fit lại cùng mô hình trên toàn bộ lịch sử để dự báo tương lai.`);
            data.validation.candidates.forEach(c => paragraph(method, `${c.label}: RMSLE validation ${fmt(c.mean_rmsle)} (${c.folds.length} fold).`));
            data.limitations.forEach(text => paragraph(method, text));
            document.querySelectorAll('.lt-view').forEach(b => b.classList.toggle('active', b.dataset.view === activeView));
            draw(activeView);
        } catch (error) { el('lt-status').textContent = error.message; }
    }
    document.querySelectorAll('.lt-view').forEach(button => button.addEventListener('click', () => {
        document.querySelectorAll('.lt-view').forEach(b => b.classList.toggle('active', b === button)); draw(button.dataset.view);
    }));
    ['lt-style','lt-scale','lt-bands'].forEach(id => el(id).addEventListener('change', () => draw(activeView)));
    el('btn-refresh')?.addEventListener('click', load);
    load();
}());
