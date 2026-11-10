/* SVG monthly chart: clipped bands, true time boundary, pointer/keyboard navigation. */
window.createLongTermChart = function (el, fmt) {
    'use strict';
    const V = window.LongTermViewport, chart = el('lt-chart'), tip = el('lt-tooltip');
    let data, view = 'forecast', model, drag, frame, hover;
    const ranges = {}, presets = {forecast: 'all', backtest: 'all'};
    const ns = 'http://www.w3.org/2000/svg';
    function node(tag, attrs = {}, text) {
        const n = document.createElementNS(ns, tag);
        Object.entries(attrs).forEach(([k, v]) => n.setAttribute(k, v));
        if (text !== undefined) n.textContent = text;
        return n;
    }
    function queue() { if (!frame) frame = requestAnimationFrame(() => { frame = null; draw(); }); }
    function setRange(range, preset = '') {
        if (!model) return;
        ranges[view] = V.clamp(range, model.months.length); presets[view] = preset; queue();
    }
    function hideTip() { tip.hidden = true; if (hover) hover.setAttribute('visibility', 'hidden'); }
    function draw(nextView) {
        if (!data) return;
        if (nextView) view = nextView;
        hideTip(); chart.replaceChildren();
        const test = view === 'backtest', pred = test ? data.backtest : data.forecast;
        document.querySelector('.lt-baseline-key').hidden = !test;
        chart.setAttribute('aria-label', test ? 'Biểu đồ tương tác kiểm định 36 tháng: thực tế, dự báo và baseline' : 'Biểu đồ tương tác lịch sử và dự báo giá đóng cửa BTC từng tháng');
        const histMap = new Map(data.history.map(r => [r.month, r]));
        const actual = test ? pred.map(r => ({...histMap.get(r.month), month: r.month, close: r.actual})) : data.history;
        const months = test ? pred.map(r => r.month) : actual.map(r => r.month).concat(pred.map(r => r.month));
        const indices = new Map(months.map((month, i) => [month, i]));
        const range = ranges[view] = V.clamp(ranges[view] || {start: 0, end: months.length - 1}, months.length);
        const width = Math.max(280, chart.clientWidth), height = width < 600 ? 350 : 430;
        chart.setAttribute('viewBox', `0 0 ${width} ${height}`);
        const left = width < 600 ? 58 : 78, right = width - 20, top = 30, bottom = height - 44;
        const x = i => left + (i - range.start) / (range.end - range.start) * (right - left);
        const candles = el('lt-style').value === 'candles', log = el('lt-scale').value === 'log', bands = el('lt-bands').checked;
        document.querySelector('.lt-band-key').hidden = !bands;
        const near = row => indices.get(row.month) >= Math.floor(range.start) && indices.get(row.month) <= Math.ceil(range.end);
        let values = actual.filter(near).flatMap(r => candles ? [r.low, r.high, r.close] : [r.close]);
        values = values.concat(pred.filter(near).flatMap(r => bands ? [r.predicted, r.lower_95, r.upper_95] : [r.predicted]));
        if (test) values.push(...pred.filter(near).map(r => r.baseline));
        values = values.filter(v => Number.isFinite(v) && (!log || v > 0));
        const transform = p => log ? Math.log10(p) : p;
        const low = Math.min(...values.map(transform)), high = Math.max(...values.map(transform));
        const pad = (high - low || Math.abs(high) * .1 || 1) * .08;
        const lo = log ? low - pad : Math.max(0, low - pad), hi = high + pad;
        const y = p => bottom - (transform(p) - lo) / (hi - lo) * (bottom - top);
        const add = n => chart.appendChild(n);
        const defs = node('defs'), clip = node('clipPath', {id: 'lt-plot-clip'});
        clip.append(node('rect', {x: left, y: top, width: right - left, height: bottom - top})); defs.append(clip); add(defs);
        add(node('text', {x: left, y: 17, fill: '#64748b', 'font-size': 11}, `USDT · ${log ? 'logarithm' : 'tuyến tính'}`));
        for (let i = 0; i <= 4; i++) {
            const level = lo + (hi - lo) * i / 4, price = log ? 10 ** level : level, at = y(price);
            add(node('line', {x1: left, x2: right, y1: at, y2: at, stroke: '#e6edf3'}));
            add(node('text', {x: left - 10, y: at + 4, 'text-anchor': 'end', fill: '#64748b', 'font-size': 11},
                new Intl.NumberFormat('vi-VN', {notation: 'compact', maximumFractionDigits: 1}).format(price)));
        }
        const ticks = width < 600 ? 3 : 6, seen = new Set();
        for (let t = 0; t < ticks; t++) {
            const i = Math.round(range.start + (range.end - range.start) * t / (ticks - 1));
            if (seen.has(i)) continue; seen.add(i);
            add(node('text', {x: Math.max(left + 22, Math.min(right - 22, x(i))), y: bottom + 25, 'text-anchor': 'middle', fill: '#64748b', 'font-size': 11}, months[i]));
        }
        const plot = node('g', {'clip-path': 'url(#lt-plot-clip)'}); add(plot);
        if (!test) {
            const bx = x(actual.length - .5);
            plot.append(node('rect', {x: Math.max(left, bx), y: top, width: Math.max(0, right - Math.max(left, bx)), height: bottom - top, fill: '#fffaf0'}));
        }
        (bands ? [95, 80] : []).forEach(level => {
            const upper = pred.map(r => `${x(indices.get(r.month))},${y(r['upper_' + level])}`);
            const lower = pred.slice().reverse().map(r => `${x(indices.get(r.month))},${y(r['lower_' + level])}`);
            plot.append(node('polygon', {points: upper.concat(lower).join(' '), fill: level === 95 ? '#dce8f7' : '#adc8ec', opacity: .7}));
        });
        const line = (rows, key, color, dashed) => plot.append(node('polyline', {
            points: rows.map(r => `${x(indices.get(r.month))},${y(r[key])}`).join(' '), fill: 'none', stroke: color,
            'stroke-width': 2.2, 'stroke-dasharray': dashed ? '6 4' : 'none', 'stroke-linejoin': 'round'}));
        if (candles && actual.every(r => [r.open, r.high, r.low, r.close].every(Number.isFinite))) {
            const bodyWidth = Math.max(2, Math.min(14, (right - left) / (range.end - range.start + 1) * .6));
            actual.filter(near).forEach(r => {
                const color = r.close >= r.open ? '#16806a' : '#bc4757', at = x(indices.get(r.month));
                plot.append(node('line', {x1: at, x2: at, y1: y(r.high), y2: y(r.low), stroke: color}));
                plot.append(node('rect', {x: at - bodyWidth / 2, y: Math.min(y(r.open), y(r.close)), width: bodyWidth,
                    height: Math.max(1.5, Math.abs(y(r.open) - y(r.close))), fill: color}));
            });
        } else line(actual, 'close', '#174c86', false);
        if (test) line(pred, 'baseline', '#8290a4', true);
        line(pred, 'predicted', '#b76b08', true);
        pred.filter(near).forEach(r => plot.append(node('circle', {cx: x(indices.get(r.month)), cy: y(r.predicted), r: 2.8, fill: '#b76b08'})));
        const boundaryBox = el('lt-boundary-status');
        if (!test) {
            const boundary = actual.length - .5, state = V.boundary(range, actual.length), bx = x(boundary);
            boundaryBox.textContent = state === 'past-left' ? `← Toàn bộ vùng xem là dự báo (từ ${pred[0].month})` : state === 'future-right'
                ? `Đang xem lịch sử · Dự báo từ ${pred[0].month} ở bên phải →` : `Lịch sử đến ${data.as_of_month} | Dự báo từ ${pred[0].month}`;
            if (state === 'visible') {
                plot.append(node('line', {x1: bx, x2: bx, y1: top, y2: bottom, stroke: '#687b93', 'stroke-dasharray': '4 4', 'data-boundary': 'forecast'}));
                add(node('text', {x: Math.max(left + 5, Math.min(right - 90, bx + 6)), y: top + 15, fill: '#82602e', 'font-size': 11}, 'Bắt đầu dự báo'));
            }
        } else boundaryBox.textContent = 'Kiểm định quá khứ · Toàn bộ giá thực tế đã được quan sát';
        if (!bands) boundaryBox.textContent += ' · Dải bất định đang ẩn';
        hover = node('line', {y1: top, y2: bottom, stroke: '#71839b', 'stroke-dasharray': '3 3', visibility: 'hidden', 'pointer-events': 'none'}); plot.append(hover);
        el('lt-range-status').textContent = `${months[Math.ceil(range.start)]} → ${months[Math.floor(range.end)]} · ${Math.floor(range.end) - Math.ceil(range.start) + 1}/${months.length} tháng`;
        document.querySelectorAll('.lt-range').forEach(b => {
            b.disabled = test && b.dataset.range !== 'all';
            b.title = b.disabled ? 'Kiểm định có đúng 36 tháng quá khứ; dùng thu phóng để xem chi tiết.' : '';
            b.classList.toggle('active', b.dataset.range === presets[view]); b.setAttribute('aria-pressed', b.dataset.range === presets[view] ? 'true' : 'false');
        });
        el('lt-zoom-in').disabled = range.end - range.start <= 5;
        el('lt-zoom-out').disabled = range.end - range.start >= months.length - 1;
        el('lt-chart-caption').textContent = (test
            ? (candles ? 'Nến: OHLC thực tế; ' : 'Đường xanh: giá đóng cửa thực tế; ') + 'cam: dự báo đóng băng trước 36 tháng kiểm định; xám: baseline giữ nguyên giá. Không học từ đoạn kiểm định.'
            : `Tháng cuối đầy đủ: ${data.as_of_month}. Vùng tương lai ${pred[0].month} → ${pred.at(-1).month} chỉ là dự báo, chưa có giá thật để chấm.`)
            + (candles ? ' Chỉ lịch sử có nến OHLC; tương lai chỉ dự báo giá đóng cửa.' : ' Mỗi điểm lịch sử là giá đóng cửa cuối tháng.')
            + (bands ? ' Dải xanh: bất định danh nghĩa 80% (đậm), 95% (nhạt).' : ' Dải bất định đang ẩn.')
            + (log ? ' Trục log so sánh tỷ lệ thay đổi.' : ' Trục tuyến tính so sánh số USDT.');
        model = {test, pred, actual, months, indices, range, x, y, left, right, top, bottom, width, height, bands};
    }
    function position(event) {
        const rect = chart.getBoundingClientRect();
        return {x: (event.clientX - rect.left) * model.width / rect.width, y: (event.clientY - rect.top) * model.height / rect.height};
    }
    function showTip(event) {
        if (!model || drag?.moved) return;
        const p = position(event), m = model;
        if (p.x < m.left || p.x > m.right || p.y < m.top || p.y > m.bottom) return hideTip();
        const index = Math.max(0, Math.min(m.months.length - 1, Math.round(m.range.start + (p.x - m.left) / (m.right - m.left) * (m.range.end - m.range.start))));
        const month = m.months[index], a = m.actual.find(r => r.month === month), f = m.pred.find(r => r.month === month);
        tip.replaceChildren();
        const row = (text, cls) => { const n = document.createElement('div'); n.textContent = text; if (cls) n.className = cls; tip.append(n); };
        row(`${month} · UTC`, 'tooltip-heading'); row(m.test ? 'Kiểm định quá khứ' : a ? 'Dữ liệu lịch sử' : 'Dự báo tương lai', 'tooltip-type');
        if (a) {
            row(`Đóng cửa thật: ${fmt(a.close)} USDT`);
            if (el('lt-style').value === 'candles') { row(`Mở: ${fmt(a.open)} · Cao: ${fmt(a.high)}`); row(`Thấp: ${fmt(a.low)}`); }
        }
        if (f) { row(`Dự báo: ${fmt(f.predicted)} USDT`); if (m.test) row(`Baseline: ${fmt(f.baseline)} USDT`);
            row(`80%: ${fmt(f.lower_80)} – ${fmt(f.upper_80)}`); row(`95%: ${fmt(f.lower_95)} – ${fmt(f.upper_95)}`);
            if (!m.bands) row('Dải trên biểu đồ đang ẩn', 'tooltip-type'); }
        tip.hidden = false;
        const at = m.x(index); hover.setAttribute('x1', at); hover.setAttribute('x2', at); hover.setAttribute('visibility', 'visible');
        tip.style.left = Math.max(5, Math.min(chart.clientWidth - tip.offsetWidth - 5, p.x + 14)) + 'px';
        tip.style.top = Math.max(5, Math.min(chart.clientHeight - tip.offsetHeight - 5, p.y + 14)) + 'px';
    }
    chart.addEventListener('wheel', e => {
        if (!model || e.ctrlKey || e.metaKey) return;
        const p = position(e); if (p.x < model.left || p.x > model.right || p.y < model.top || p.y > model.bottom) return;
        e.preventDefault();
        const delta = e.deltaY * (e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? model.height : 1);
        setRange(V.zoom(ranges[view], model.months.length, Math.exp(Math.max(-240, Math.min(240, delta)) * .0025), (p.x - model.left) / (model.right - model.left)));
    }, {passive: false});
    chart.addEventListener('pointerdown', e => {
        if (!model || (e.pointerType === 'mouse' && e.button !== 0)) return;
        const p = position(e); if (p.x < model.left || p.x > model.right || p.y < model.top || p.y > model.bottom) return;
        drag = {id: e.pointerId, startX: e.clientX, range: {...ranges[view]}, moved: false};
        chart.setPointerCapture(e.pointerId); chart.classList.add('dragging'); showTip(e);
    });
    chart.addEventListener('pointermove', e => {
        if (drag && drag.id === e.pointerId) {
            const delta = e.clientX - drag.startX;
            if (Math.abs(delta) > 3) drag.moved = true;
            if (drag.moved) setRange(V.pan(drag.range, model.months.length, -delta / (model.right - model.left) * (drag.range.end - drag.range.start)));
        } else showTip(e);
    });
    function release(e) { if (drag?.id === e.pointerId) { if (chart.hasPointerCapture(e.pointerId)) chart.releasePointerCapture(e.pointerId); drag = null; chart.classList.remove('dragging'); } }
    chart.addEventListener('pointerup', release); chart.addEventListener('pointercancel', release); chart.addEventListener('lostpointercapture', () => {drag = null; chart.classList.remove('dragging');});
    chart.addEventListener('pointerleave', () => { if (!drag) hideTip(); });
    chart.addEventListener('keydown', e => {
        if (!model) return;
        if (['+', '=', '-', 'ArrowLeft', 'ArrowRight', 'Home'].includes(e.key)) e.preventDefault(); else return;
        if (e.key === 'Home') return setRange(V.preset('all', data.history.length, data.forecast.length, model.test), 'all');
        const current = ranges[view];
        setRange(e.key.startsWith('Arrow') ? V.pan(current, model.months.length, (e.key === 'ArrowRight' ? 1 : -1) * (current.end - current.start) * .1)
            : V.zoom(current, model.months.length, e.key === '-' ? 1.3 : 1 / 1.3));
    });
    document.querySelectorAll('.lt-range').forEach(b => b.addEventListener('click', () => { if (model) setRange(V.preset(b.dataset.range, data.history.length, data.forecast.length, model.test), b.dataset.range); }));
    el('lt-reset').addEventListener('click', () => { if (model) setRange(V.preset('all', data.history.length, data.forecast.length, model.test), 'all'); });
    [['lt-zoom-in', 1 / 1.3], ['lt-zoom-out', 1.3]].forEach(([id, factor]) => el(id).addEventListener('click', () => { if (model) setRange(V.zoom(ranges[view], model.months.length, factor)); }));
    new ResizeObserver(queue).observe(chart.parentElement);
    return {setData(next) { if (data?.generated_at !== next.generated_at) { delete ranges.forecast; delete ranges.backtest; presets.forecast = presets.backtest = 'all'; } data = next; }, draw};
};
