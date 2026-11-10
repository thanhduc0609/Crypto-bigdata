/* Pure index-space navigation. No data/model mutation and no dependencies. */
(function (root, factory) {
    if (typeof module === 'object' && module.exports) module.exports = factory();
    else root.LongTermViewport = factory();
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    function clamp(range, count) {
        const maximum = Math.max(0, count - 1);
        const span = Math.min(maximum, Math.max(Math.min(5, maximum), range.end - range.start));
        const start = Math.max(0, Math.min(maximum - span, range.start));
        return {start, end: start + span};
    }
    function zoom(range, count, factor, anchor = .5) {
        anchor = Math.max(0, Math.min(1, anchor));
        const span = range.end - range.start;
        const target = Math.min(count - 1, Math.max(Math.min(5, count - 1), span * factor));
        const start = range.start + span * anchor - target * anchor;
        return clamp({start, end: start + target}, count);
    }
    function pan(range, count, delta) {
        return clamp({start: range.start + delta, end: range.end + delta}, count);
    }
    function preset(name, historyCount, forecastCount, backtest = false) {
        const count = backtest ? forecastCount : historyCount + forecastCount;
        if (backtest || name === 'all') return clamp({start: 0, end: count - 1}, count);
        if (name === 'five-years') return clamp({start: Math.max(0, historyCount - 60), end: historyCount - 1}, count);
        return clamp({start: Math.max(0, historyCount - 24), end: count - 1}, count);
    }
    function boundary(range, historyCount) {
        const index = historyCount - .5;
        return index < range.start ? 'past-left' : index > range.end ? 'future-right' : 'visible';
    }
    return {clamp, zoom, pan, preset, boundary};
}));
