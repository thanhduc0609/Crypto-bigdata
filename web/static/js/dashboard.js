/**
 * =============================================================================
 * Crypto Big Data - Web Analytics & ML Prediction Client
 * =============================================================================
 * Kiến trúc tối giản, hướng dữ liệu thật:
 *   - Gom 3 tab: Tổng quan, Giao dịch lớn, Dự báo
 *   - Chỉ nạp và refresh dữ liệu cho tab đang hoạt động
 *   - Quản lý hủy request cũ bằng AbortController, tránh race condition
 *   - Hiển thị nguồn dữ liệu và thời điểm cập nhật thực tế
 *   - Không tạo nến giả, không làm tròn số 0 làm mất dữ liệu null
 * =============================================================================
 */

(function () {
    "use strict";

    // Global application state
    const state = {
        activeTab: "tab-overview",
        timeframe: "1m",               // "1m", "1h", "1d"
        showVwap: true,
        tradesTimeRange: "all",        // "1h", "24h", "7d", "all"
        tradesMinValue: 10000,
        tradesPage: 1,
        tradesPageSize: 25,
        tradesTotalPages: 1,
        predPage: 1,
        predPageSize: 10,
        predTotalPages: 1,
        
        // Network & polling
        abortController: null,
        pollingTimer: null,
        orderflowTimer: null,
        workerTimer: null,
        pollingIntervalMs: 15000,
        isFetching: false,

        // Chart components
        chart: null,
        candleSeries: null,
        volumeSeries: null,
        vwapSeries: null,
        rawCandles: [],
        hasFittedChart: false
    };

    // DOM Elements cache
    const el = {
        // Top Header
        provenanceSource: document.getElementById("provenance-source"),
        provenanceDataTs: document.getElementById("provenance-data-ts"),
        provenanceFetchTs: document.getElementById("provenance-fetch-ts"),
        provenanceStatus: document.getElementById("provenance-status"),
        btnRefresh: document.getElementById("btn-refresh"),
        btnRefreshText: document.getElementById("btn-refresh-text"),
        
        // Navigation Tabs
        navTabs: document.querySelectorAll(".nav-tab"),
        tabPanes: document.querySelectorAll(".tab-pane"),

        // Tab 1: Overview
        ovPrice: document.getElementById("ov-price"),
        ovChange: document.getElementById("ov-change"),
        ovHigh24h: document.getElementById("ov-high-24h"),
        ovLow24h: document.getElementById("ov-low-24h"),
        ovVol24h: document.getElementById("ov-vol-24h"),
        ovTurnover24h: document.getElementById("ov-turnover-24h"),
        tfButtons: document.querySelectorAll(".tf-btn"),
        toggleVwap: document.getElementById("toggle-vwap"),
        chartContainer: document.getElementById("tv-chart"),
        
        // Candle inspect bar
        ciTime: document.getElementById("ci-time"),
        ciOpen: document.getElementById("ci-open"),
        ciHigh: document.getElementById("ci-high"),
        ciLow: document.getElementById("ci-low"),
        ciClose: document.getElementById("ci-close"),
        ciVol: document.getElementById("ci-vol"),
        ciVwap: document.getElementById("ci-vwap"),
        ciClosed: document.getElementById("ci-closed"),
        
        // Order flow 60s
        ofRow: document.getElementById("orderflow-row"),
        ofBuyVol: document.getElementById("of-buy-vol"),
        ofSellVol: document.getElementById("of-sell-vol"),
        ofImbalance: document.getElementById("of-imbalance"),
        ofNotice: document.getElementById("of-notice"),
        ofStatusBadge: document.getElementById("of-status-badge"),
        ofBuyPct: document.getElementById("of-buy-pct"),
        ofSellPct: document.getElementById("of-sell-pct"),
        ofBarBuy: document.getElementById("of-bar-buy"),
        ofBarSell: document.getElementById("of-bar-sell"),
        ofTotalVol: document.getElementById("of-total-vol"),
        ofTradeCount: document.getElementById("of-trade-count"),
        ofCoverage: document.getElementById("of-coverage"),
        candlesTableBody: document.getElementById("candles-table-body"),

        // Tab 2: Large Trades
        tradesTimeRange: document.getElementById("trades-time-range"),
        tradesMinValue: document.getElementById("trades-min-value"),
        btnExportTrades: document.getElementById("btn-export-trades"),
        tradesSummaryCount: document.getElementById("trades-summary-count"),
        tradesSummaryValue: document.getElementById("trades-summary-value"),
        largeTradesBody: document.getElementById("large-trades-body"),
        btnTradesPrev: document.getElementById("btn-trades-prev"),
        btnTradesNext: document.getElementById("btn-trades-next"),
        tradesPageIndicator: document.getElementById("trades-page-indicator"),

        // Tab 3: Prediction & Worker
        workerStatusBadge: document.getElementById("worker-status-badge"),
        workerLastCheck: document.getElementById("worker-last-check"),
        workerLastSuccess: document.getElementById("worker-last-success"),
        workerCycleStatus: document.getElementById("worker-cycle-status"),
        workerCounts: document.getElementById("worker-counts"),
        predDirBadge: document.getElementById("pred-dir-badge"),
        predProbUp: document.getElementById("pred-prob-up"),
        predProbNotUp: document.getElementById("pred-prob-notup"),
        predBasePrice: document.getElementById("pred-base-price"),
        predFeatureTime: document.getElementById("pred-feature-time"),
        predTargetTime: document.getElementById("pred-target-time"),
        predEvalStatus: document.getElementById("pred-eval-status"),
        btnTriggerPred: document.getElementById("btn-trigger-pred"),
        predTriggerFeedback: document.getElementById("pred-trigger-feedback"),
        predHistoryBody: document.getElementById("pred-history-body"),
        btnPredPrev: document.getElementById("btn-pred-prev"),
        btnPredNext: document.getElementById("btn-pred-next"),
        predPageIndicator: document.getElementById("pred-page-indicator"),
        modelInfoContent: document.getElementById("model-info-content")
    };

    // =========================================================================
    // UTILITY FORMATTERS (GIỜ VIỆT NAM - UTC+7)
    // =========================================================================

    function formatNumber(val, decimals = 2) {
        if (val === null || val === undefined || isNaN(val)) return "—";
        return Number(val).toLocaleString("en-US", {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        });
    }

    function formatCryptoSize(val) {
        if (val === null || val === undefined || isNaN(val)) return "—";
        const num = Number(val);
        if (num === 0) return "0.0000";
        if (num < 0.0001) return num.toFixed(6);
        if (num < 1) return num.toFixed(4);
        return num.toFixed(4);
    }

    /**
     * Định dạng thời gian sang Giờ Việt Nam (Asia/Ho_Chi_Minh - UTC+7)
     * Format chuẩn: YYYY-MM-DD HH:mm:ss
     */
    function formatVnTime(isoStringOrTs, includeDate = true) {
        if (!isoStringOrTs) return "—";
        try {
            const date = typeof isoStringOrTs === "number"
                ? new Date(isoStringOrTs * 1000)
                : new Date(isoStringOrTs);
            if (isNaN(date.getTime())) return String(isoStringOrTs);

            return new Intl.DateTimeFormat("sv-SE", {
                timeZone: "Asia/Ho_Chi_Minh",
                dateStyle: includeDate ? "short" : undefined,
                timeStyle: "medium"
            }).format(date);
        } catch (e) {
            return String(isoStringOrTs);
        }
    }

    // Gán để các hàm gọi cũ formatUtcTime đều tự động chuyển sang giờ Việt Nam
    const formatUtcTime = formatVnTime;

    /**
     * Lấy thời điểm hiện tại định dạng Giờ Việt Nam
     */
    function getNowVnString() {
        try {
            return new Intl.DateTimeFormat("sv-SE", {
                timeZone: "Asia/Ho_Chi_Minh",
                dateStyle: "short",
                timeStyle: "medium"
            }).format(new Date());
        } catch (e) {
            return new Date().toLocaleTimeString();
        }
    }

    const getNowUtcString = getNowVnString;

    function updateProvenance(sourceName, dataTimestamp, isStale = false) {
        if (el.provenanceSource) {
            el.provenanceSource.textContent = sourceName === "live_okx"
                ? "Nguồn: OKX REST Trực tiếp"
                : "Nguồn: PostgreSQL Data Warehouse";
        }

        if (el.provenanceDataTs) {
            el.provenanceDataTs.textContent = dataTimestamp ? formatVnTime(dataTimestamp) : "—";
        }

        if (el.provenanceFetchTs) {
            el.provenanceFetchTs.textContent = getNowVnString();
        }

        if (el.provenanceStatus) {
            if (isStale) {
                el.provenanceStatus.textContent = "Dữ liệu cũ — cập nhật thất bại";
                el.provenanceStatus.className = "status-indicator stale";
            } else {
                el.provenanceStatus.textContent = "";
                el.provenanceStatus.className = "status-indicator";
            }
        }
    }

    // =========================================================================
    // INITIALIZATION & TAB SWITCHING
    // =========================================================================

    function init() {
        setupEventListeners();
        initChart();
        fetchActiveTabData();
        startPollingTimer();
    }

    function setupEventListeners() {
        // Tab switching
        el.navTabs.forEach(tab => {
            tab.addEventListener("click", () => {
                const targetTabId = tab.getAttribute("data-tab");
                if (targetTabId === state.activeTab) return;

                // Update tab styles
                el.navTabs.forEach(t => t.classList.remove("active"));
                el.tabPanes.forEach(p => p.classList.remove("active"));

                tab.classList.add("active");
                const targetPane = document.getElementById(targetTabId);
                if (targetPane) targetPane.classList.add("active");

                state.activeTab = targetTabId;

                // Abort old request and reset timer
                abortInFlightRequest();
                fetchActiveTabData();
                restartPollingTimer();

                // Resize chart if overview tab was activated
                if (targetTabId === "tab-overview" && state.chart) {
                    setTimeout(() => {
                        if (el.chartContainer) {
                            state.chart.applyOptions({ width: el.chartContainer.clientWidth });
                        }
                    }, 50);
                }
            });
        });

        // Manual refresh button
        el.btnRefresh.addEventListener("click", () => {
            abortInFlightRequest();
            fetchActiveTabData();
        });

        // Timeframe selector (Tab 1)
        el.tfButtons.forEach(btn => {
            btn.addEventListener("click", () => {
                el.tfButtons.forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                state.timeframe = btn.getAttribute("data-tf");
                fetchOverviewAndCandles();
            });
        });

        // VWAP Toggle
        el.toggleVwap.addEventListener("change", (e) => {
            state.showVwap = e.target.checked;
            updateVwapSeriesVisibility();
        });

        // Window resize handler for chart
        window.addEventListener("resize", () => {
            if (state.chart && el.chartContainer && state.activeTab === "tab-overview") {
                state.chart.applyOptions({ width: el.chartContainer.clientWidth });
            }
        });

        // Large Trades Filters (Tab 2)
        el.tradesTimeRange.addEventListener("change", (e) => {
            state.tradesTimeRange = e.target.value;
            state.tradesPage = 1;
            fetchLargeTrades();
        });

        el.tradesMinValue.addEventListener("change", (e) => {
            state.tradesMinValue = Number(e.target.value);
            state.tradesPage = 1;
            fetchLargeTrades();
        });

        el.btnTradesPrev.addEventListener("click", () => {
            if (state.tradesPage > 1) {
                state.tradesPage--;
                fetchLargeTrades();
            }
        });

        el.btnTradesNext.addEventListener("click", () => {
            if (state.tradesPage < state.tradesTotalPages) {
                state.tradesPage++;
                fetchLargeTrades();
            }
        });

        el.btnExportTrades.addEventListener("click", () => {
            const url = `/api/trades/large/export?time_range=${state.tradesTimeRange}&min_value=${state.tradesMinValue}`;
            window.location.href = url;
        });

        // Prediction Controls (Tab 3)
        el.btnTriggerPred.addEventListener("click", triggerNewPrediction);

        el.btnPredPrev.addEventListener("click", () => {
            if (state.predPage > 1) {
                state.predPage--;
                fetchPredictionHistory();
            }
        });

        el.btnPredNext.addEventListener("click", () => {
            if (state.predPage < state.predTotalPages) {
                state.predPage++;
                fetchPredictionHistory();
            }
        });
    }

    function abortInFlightRequest() {
        if (state.abortController) {
            state.abortController.abort();
            state.abortController = null;
        }
        state.isFetching = false;
        el.btnRefresh.disabled = false;
        el.btnRefreshText.textContent = "Làm mới";
    }

    function startPollingTimer() {
        if (state.pollingTimer) clearInterval(state.pollingTimer);
        state.pollingTimer = setInterval(() => {
            fetchActiveTabData(true);
        }, state.pollingIntervalMs);

        // Realtime Orderflow 1.5s interval (khi ở Tab 1)
        if (state.orderflowTimer) clearInterval(state.orderflowTimer);
        state.orderflowTimer = setInterval(() => {
            if (state.activeTab === "tab-overview") {
                fetchRealtimeOrderflow();
            }
        }, 1500);

        // ML Worker Status 10s interval (khi ở Tab 3)
        if (state.workerTimer) clearInterval(state.workerTimer);
        state.workerTimer = setInterval(() => {
            if (state.activeTab === "tab-prediction") {
                fetchPredictionWorkerStatus();
            }
        }, 10000);
    }

    function restartPollingTimer() {
        startPollingTimer();
    }

    function fetchActiveTabData(isBackground = false) {
        if (state.isFetching && isBackground) return;

        if (state.activeTab === "tab-overview") {
            fetchOverviewAndCandles();
        } else if (state.activeTab === "tab-trades") {
            fetchLargeTrades();
        } else if (state.activeTab === "tab-prediction") {
            fetchPredictionData();
        }
    }

    // =========================================================================
    // TAB 1: OVERVIEW & CANDLES CHART LOGIC
    // =========================================================================

    function initChart() {
        if (!el.chartContainer || typeof LightweightCharts === "undefined") return;

        state.chart = LightweightCharts.createChart(el.chartContainer, {
            width: el.chartContainer.clientWidth,
            height: 420,
            layout: {
                background: { color: "#ffffff" },
                textColor: "#475569",
                fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
            },
            grid: {
                vertLines: { color: "#f1f5f9" },
                horzLines: { color: "#f1f5f9" }
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal
            },
            localization: {
                locale: "vi-VN",
                timeFormatter: (timestamp) => {
                    const d = new Date(timestamp * 1000);
                    return new Intl.DateTimeFormat("sv-SE", {
                        timeZone: "Asia/Ho_Chi_Minh",
                        dateStyle: "short",
                        timeStyle: "medium"
                    }).format(d);
                }
            },
            rightPriceScale: {
                borderColor: "#e2e8f0",
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.25
                }
            },
            timeScale: {
                borderColor: "#e2e8f0",
                timeVisible: true,
                secondsVisible: false,
                rightOffset: 5,
                tickMarkFormatter: (time) => {
                    const d = new Date(time * 1000);
                    return new Intl.DateTimeFormat("en-GB", {
                        timeZone: "Asia/Ho_Chi_Minh",
                        hour: "2-digit",
                        minute: "2-digit",
                        hour12: false
                    }).format(d);
                }
            }
        });

        // 1. Candlestick Series
        state.candleSeries = state.chart.addCandlestickSeries({
            upColor: "#16a34a",
            downColor: "#dc2626",
            borderUpColor: "#16a34a",
            borderDownColor: "#dc2626",
            wickUpColor: "#16a34a",
            wickDownColor: "#dc2626"
        });

        // 2. Volume Series (Independent bottom scale)
        state.volumeSeries = state.chart.addHistogramSeries({
            priceFormat: { type: "volume" },
            priceScaleId: "", // Separate overlay scale
            lastValueVisible: false,
            priceLineVisible: false,
            scaleMargins: {
                top: 0.8,
                bottom: 0
            }
        });

        // 3. VWAP Line Series
        state.vwapSeries = state.chart.addLineSeries({
            color: "#2563eb",
            lineWidth: 2,
            priceScaleId: "right",
            title: "VWAP"
        });

        // Crosshair move listener: inspect candle data
        state.chart.subscribeCrosshairMove(param => {
            if (!param || !param.time || !state.rawCandles.length) {
                // If not hovering, display the latest candle
                if (state.rawCandles.length > 0) {
                    displayCandleInspection(state.rawCandles[state.rawCandles.length - 1]);
                }
                return;
            }

            const currentCandle = state.rawCandles.find(c => c.time === param.time);
            if (currentCandle) {
                displayCandleInspection(currentCandle);
            }
        });
    }

    async function fetchOverviewAndCandles() {
        state.isFetching = true;
        el.btnRefresh.disabled = true;
        el.btnRefreshText.textContent = "Đang tải...";
        state.abortController = new AbortController();
        const signal = state.abortController.signal;

        try {
            // Concurrent fetch of overview ticker and candle bars from pinned OKX REST source
            const [overviewRes, candlesRes] = await Promise.all([
                fetch(`/api/overview?source=live`, { signal }),
                fetch(`/api/candles?source=live&timeframe=${state.timeframe}&limit=150`, { signal })
            ]);

            const overviewData = await overviewRes.json();
            const candlesData = await candlesRes.json();

            // 1. Render Overview
            if (overviewRes.ok && overviewData.status === "success") {
                el.ovPrice.textContent = formatNumber(overviewData.current_price, 2);

                if (overviewData.change_24h !== null && overviewData.change_24h !== undefined) {
                    const chg = overviewData.change_24h;
                    el.ovChange.textContent = (chg > 0 ? "+" : "") + chg.toFixed(2) + "%";
                    el.ovChange.className = "change-badge " + (chg >= 0 ? "up" : "down");
                } else {
                    el.ovChange.textContent = "— (Chưa đủ 24h)";
                    el.ovChange.className = "change-badge";
                }

                el.ovHigh24h.textContent = overviewData.high_24h ? formatNumber(overviewData.high_24h, 2) : "—";
                el.ovLow24h.textContent = overviewData.low_24h ? formatNumber(overviewData.low_24h, 2) : "—";
                el.ovVol24h.textContent = overviewData.volume_24h ? formatNumber(overviewData.volume_24h, 2) : "—";
                el.ovTurnover24h.textContent = overviewData.turnover_24h ? formatNumber(overviewData.turnover_24h, 2) : "—";

                updateProvenance(overviewData.source, overviewData.data_timestamp, false);
            } else if (overviewData.status === "empty") {
                el.ovPrice.textContent = "—";
                el.ovChange.textContent = "—";
                updateProvenance("live_okx", null, false);
            } else {
                updateProvenance("live_okx", null, true);
            }

            // 2. Render Candles & Chart
            if (candlesRes.ok && candlesData.status === "success" && candlesData.candles) {
                state.rawCandles = candlesData.candles;
                renderCandlesChart(candlesData.candles);
                renderCandlesTable(candlesData.candles);

                if (candlesData.candles.length > 0) {
                    displayCandleInspection(candlesData.candles[candlesData.candles.length - 1]);
                }
                // Đồng thời cập nhật cán cân realtime 60s
                fetchRealtimeOrderflow();
            } else {
                renderCandlesTable([]);
                fetchRealtimeOrderflow();
            }
        } catch (err) {
            if (err.name !== "AbortError") {
                console.error("Lỗi tải Tổng quan:", err);
                updateProvenance("live_okx", null, true);
            }
        } finally {
            state.isFetching = false;
            el.btnRefresh.disabled = false;
            el.btnRefreshText.textContent = "Làm mới";
        }
    }

    function renderCandlesChart(candles) {
        if (!state.chart || !candles.length) return;

        const chartCandles = [];
        const chartVolumes = [];
        const chartVwaps = [];

        candles.forEach(c => {
            chartCandles.push({
                time: c.time,
                open: c.open,
                high: c.high,
                low: c.low,
                close: c.close
            });

            chartVolumes.push({
                time: c.time,
                value: c.volume,
                color: c.close >= c.open ? "rgba(22, 163, 74, 0.4)" : "rgba(220, 38, 38, 0.4)"
            });

            // Only add VWAP point if present and valid
            if (c.vwap !== null && c.vwap !== undefined && !isNaN(c.vwap)) {
                chartVwaps.push({
                    time: c.time,
                    value: c.vwap
                });
            }
        });

        state.candleSeries.setData(chartCandles);
        state.volumeSeries.setData(chartVolumes);

        if (state.showVwap) {
            state.vwapSeries.setData(chartVwaps);
        } else {
            state.vwapSeries.setData([]);
        }

        if (!state.hasFittedChart && chartCandles.length > 0) {
            state.chart.timeScale().fitContent();
            state.hasFittedChart = true;
        }
    }

    function updateVwapSeriesVisibility() {
        if (!state.vwapSeries || !state.rawCandles.length) return;
        if (state.showVwap) {
            const chartVwaps = state.rawCandles
                .filter(c => c.vwap !== null && c.vwap !== undefined && !isNaN(c.vwap))
                .map(c => ({ time: c.time, value: c.vwap }));
            state.vwapSeries.setData(chartVwaps);
        } else {
            state.vwapSeries.setData([]);
        }
    }

    function displayCandleInspection(c) {
        if (!c) return;

        el.ciTime.textContent = formatVnTime(c.time);
        el.ciOpen.textContent = formatNumber(c.open, 2);
        el.ciHigh.textContent = formatNumber(c.high, 2);
        el.ciLow.textContent = formatNumber(c.low, 2);
        el.ciClose.textContent = formatNumber(c.close, 2);
        el.ciVol.textContent = formatCryptoSize(c.volume) + " BTC";
        el.ciVwap.textContent = c.vwap !== null ? formatNumber(c.vwap, 2) + " USDT" : "—";
        el.ciClosed.textContent = c.is_closed ? "Đã đóng" : "Đang mở";
    }

    /**
     * Cập nhật Cán cân Mua/Bán thời gian thực (Cửa sổ trượt 60 giây)
     * Đọc từ /api/orderflow/realtime (luồng giao dịch thật từ WebSocket OKX)
     */
    async function fetchRealtimeOrderflow() {
        if (!el.ofRow) return;
        try {
            const resp = await fetch("/api/orderflow/realtime");
            if (!resp.ok) return;
            const data = await resp.json();

            // 1. Khối lượng
            const buyVol = data.buy_volume !== null && data.buy_volume !== undefined ? Number(data.buy_volume) : 0;
            const sellVol = data.sell_volume !== null && data.sell_volume !== undefined ? Number(data.sell_volume) : 0;
            const totalVol = data.total_volume !== null && data.total_volume !== undefined ? Number(data.total_volume) : 0;

            el.ofBuyVol.textContent = formatCryptoSize(buyVol) + " BTC";
            el.ofSellVol.textContent = formatCryptoSize(sellVol) + " BTC";
            if (el.ofTotalVol) el.ofTotalVol.textContent = formatCryptoSize(totalVol) + " BTC";
            if (el.ofTradeCount) el.ofTradeCount.textContent = formatNumber(data.trade_count || 0, 0);
            if (el.ofCoverage) el.ofCoverage.textContent = (data.coverage_seconds || 0) + "s / 60s";

            // 2. Thanh tỉ lệ & Imbalance (Xử lý chặt chẽ khi volume = 0: Không fake 50/50)
            if (totalVol === 0 || data.imbalance === null) {
                if (el.ofBuyPct) el.ofBuyPct.textContent = "—%";
                if (el.ofSellPct) el.ofSellPct.textContent = "—%";
                if (el.ofBarBuy) el.ofBarBuy.style.width = "50%";
                if (el.ofBarSell) el.ofBarSell.style.width = "50%";
                el.ofImbalance.textContent = "Không xác định (0 volume)";
                el.ofImbalance.className = "of-value text-muted";
            } else {
                const buyPct = (data.buy_ratio * 100).toFixed(1);
                const sellPct = (data.sell_ratio * 100).toFixed(1);
                if (el.ofBuyPct) el.ofBuyPct.textContent = buyPct + "%";
                if (el.ofSellPct) el.ofSellPct.textContent = sellPct + "%";
                if (el.ofBarBuy) el.ofBarBuy.style.width = buyPct + "%";
                if (el.ofBarSell) el.ofBarSell.style.width = sellPct + "%";

                const imb = Number(data.imbalance);
                const imbPct = (imb * 100).toFixed(2);
                el.ofImbalance.textContent = (imb > 0 ? "+" : "") + imbPct + "%";
                el.ofImbalance.className = "of-value " + (imb >= 0 ? "text-buy" : "text-sell");
            }

            // 3. Trạng thái kết nối & dữ liệu
            if (el.ofStatusBadge) {
                const stateMap = {
                    "active": { text: "Đang hoạt động", cls: "active" },
                    "warming_up": { text: "Đang tích lũy (khởi động)", cls: "warming_up" },
                    "no_trades": { text: "Không có giao dịch trong 60s", cls: "no_trades" },
                    "stale": { text: "Dữ liệu cũ (ngưng kết nối)", cls: "stale" },
                    "disconnected": { text: "Mất kết nối", cls: "disconnected" }
                };
                const st = stateMap[data.state] || { text: data.state || "Không rõ", cls: "idle" };
                el.ofStatusBadge.textContent = st.text;
                el.ofStatusBadge.className = "status-badge " + st.cls;
            }
        } catch (e) {
            console.error("Lỗi cập nhật cán cân realtime:", e);
        }
    }

    function renderCandlesTable(candles) {
        if (!el.candlesTableBody) return;
        if (!candles || candles.length === 0) {
            el.candlesTableBody.innerHTML = `<tr><td colspan="9" class="empty-cell">Chưa có dữ liệu nến</td></tr>`;
            return;
        }

        // Show latest 50 in reverse chronological order
        const displayList = [...candles].reverse().slice(0, 50);
        let html = "";
        displayList.forEach(c => {
            html += `
                <tr>
                    <td>${formatUtcTime(c.time)}</td>
                    <td class="num-col">${formatNumber(c.open, 2)}</td>
                    <td class="num-col">${formatNumber(c.high, 2)}</td>
                    <td class="num-col">${formatNumber(c.low, 2)}</td>
                    <td class="num-col">${formatNumber(c.close, 2)}</td>
                    <td class="num-col">${formatCryptoSize(c.volume)}</td>
                    <td class="num-col">${c.total_trade_value ? formatNumber(c.total_trade_value, 2) : "—"}</td>
                    <td class="num-col">${c.vwap ? formatNumber(c.vwap, 2) : "—"}</td>
                    <td>${c.is_closed ? "Đã đóng" : "<span class='text-muted'>Đang mở</span>"}</td>
                </tr>
            `;
        });
        el.candlesTableBody.innerHTML = html;
    }

    // =========================================================================
    // TAB 2: LARGE TRADES LOGIC
    // =========================================================================

    async function fetchLargeTrades() {
        state.isFetching = true;
        el.btnRefresh.disabled = true;
        el.btnRefreshText.textContent = "Đang tải...";
        state.abortController = new AbortController();
        const signal = state.abortController.signal;

        try {
            // Tab 2 luôn lấy từ nguồn cố định PostgreSQL DW với bộ lọc người dùng chọn
            const url = `/api/trades/large?time_range=${state.tradesTimeRange}&min_value=${state.tradesMinValue}&page=${state.tradesPage}&page_size=${state.tradesPageSize}`;
            const res = await fetch(url, { signal });
            const data = await res.json();

            if (res.ok && data.status === "success") {
                el.tradesSummaryCount.textContent = formatNumber(data.total_count, 0) + " lệnh";
                el.tradesSummaryValue.textContent = formatNumber(data.total_value, 2) + " USDT";

                state.tradesTotalPages = data.total_pages || 1;
                el.tradesPageIndicator.textContent = `Trang ${data.page} / ${state.tradesTotalPages}`;
                el.btnTradesPrev.disabled = (data.page <= 1);
                el.btnTradesNext.disabled = (data.page >= state.tradesTotalPages);

                renderLargeTradesTable(data.trades);
                updateProvenance("postgres_dw", data.data_timestamp, false);
            } else {
                renderLargeTradesTable([]);
                updateProvenance("postgres_dw", null, true);
            }
        } catch (err) {
            if (err.name !== "AbortError") {
                console.error("Lỗi tải giao dịch lớn:", err);
                updateProvenance("postgres_dw", null, true);
            }
        } finally {
            state.isFetching = false;
            el.btnRefresh.disabled = false;
            el.btnRefreshText.textContent = "Làm mới";
        }
    }

    function renderLargeTradesTable(trades) {
        if (!el.largeTradesBody) return;
        if (!trades || trades.length === 0) {
            el.largeTradesBody.innerHTML = `<tr><td colspan="6" class="empty-cell">Không có giao dịch lớn nào thỏa điều kiện lọc</td></tr>`;
            return;
        }

        let html = "";
        trades.forEach(t => {
            const isBuy = (t.side || "").toUpperCase() === "BUY";
            const sideClass = isBuy ? "buy" : "sell";
            const sideText = isBuy ? "MUA" : "BÁN";

            html += `
                <tr>
                    <td>${formatUtcTime(t.trade_time)}</td>
                    <td><span class="side-badge ${sideClass}">${sideText}</span></td>
                    <td class="num-col">${formatNumber(t.price, 2)}</td>
                    <td class="num-col">${formatCryptoSize(t.size)}</td>
                    <td class="num-col"><strong>${formatNumber(t.trade_value, 2)}</strong></td>
                    <td><code>${String(t.trade_id)}</code></td>
                </tr>
            `;
        });
        el.largeTradesBody.innerHTML = html;
    }

    // =========================================================================
    // TAB 3: MACHINE LEARNING PREDICTION LOGIC
    // =========================================================================

    async function fetchPredictionData() {
        state.isFetching = true;
        el.btnRefresh.disabled = true;
        el.btnRefreshText.textContent = "Đang tải...";
        state.abortController = new AbortController();
        const signal = state.abortController.signal;

        try {
            await Promise.all([
                fetchPredictionWorkerStatus(signal),
                fetchLatestPrediction(signal),
                fetchPredictionHistory(signal),
                fetchModelMetadata(signal)
            ]);
        } catch (err) {
            if (err.name !== "AbortError") {
                console.error("Lỗi tải dữ liệu dự báo:", err);
            }
        } finally {
            state.isFetching = false;
            el.btnRefresh.disabled = false;
            el.btnRefreshText.textContent = "Làm mới";
        }
    }

    async function fetchPredictionWorkerStatus(signal) {
        if (!el.workerStatusBadge) return;
        try {
            const res = await fetch("/api/ml/status", { signal });
            const data = await res.json();
            if (res.ok && data.status === "success" && data.worker) {
                const w = data.worker;
                const stateLabels = {
                    "running": { text: "Đang chạy", cls: "running" },
                    "idle": { text: "Chờ chu kỳ", cls: "idle" },
                    "stopped": { text: "Đã dừng", cls: "disconnected" }
                };
                const st = stateLabels[w.state] || { text: w.state || "Không rõ", cls: "idle" };
                el.workerStatusBadge.textContent = st.text;
                el.workerStatusBadge.className = "status-badge " + st.cls;

                if (el.workerLastCheck) el.workerLastCheck.textContent = w.last_check_at ? formatVnTime(w.last_check_at) : "Chưa kiểm tra";
                if (el.workerLastSuccess) el.workerLastSuccess.textContent = w.last_success_at ? formatVnTime(w.last_success_at) : "Chưa có";
                
                if (el.workerCycleStatus) {
                    if (w.last_rejection_code) {
                        el.workerCycleStatus.innerHTML = `<span class="text-sell">Từ chối: <strong>${w.last_rejection_code}</strong></span>`;
                    } else if (w.last_success_at) {
                        el.workerCycleStatus.innerHTML = `<span class="text-buy">Dự báo thành công</span>`;
                    } else {
                        el.workerCycleStatus.textContent = "Bình thường";
                    }
                }

                if (el.workerCounts) {
                    el.workerCounts.textContent = `${w.total_predictions || 0} dự báo / ${w.total_evaluations || 0} đối chiếu`;
                }
            }
        } catch (e) {
            if (e.name !== "AbortError") console.error("Lỗi lấy trạng thái ML worker:", e);
        }
    }

    async function fetchLatestPrediction(signal) {
        try {
            const res = await fetch("/api/ml/prediction", { signal });
            const data = await res.json();

            if (res.ok && data.status === "success" && data.prediction) {
                const p = data.prediction;
                const isUp = (p.predicted_direction === 1);

                el.predDirBadge.textContent = isUp ? "Tăng" : "Không tăng";
                el.predDirBadge.className = "pred-direction-badge " + (isUp ? "up" : "not-up");

                el.predProbUp.textContent = p.probability_up ? (p.probability_up * 100).toFixed(2) + "%" : "—";
                el.predProbNotUp.textContent = p.probability_not_up ? (p.probability_not_up * 100).toFixed(2) + "%" : "—";
                el.predBasePrice.textContent = p.current_price ? formatNumber(p.current_price, 2) + " USDT" : "—";
                el.predFeatureTime.textContent = formatUtcTime(p.feature_timestamp);
                el.predTargetTime.textContent = formatUtcTime(p.target_timestamp);

                // Render evaluation state
                if (p.eval_status === "PENDING_TARGET") {
                    el.predEvalStatus.innerHTML = `<span class="eval-text pending">Chờ thời điểm đối chiếu (+5m)</span>`;
                } else if (p.eval_status === "MISSING_ACTUAL") {
                    el.predEvalStatus.innerHTML = `<span class="eval-text missing">Chờ nến đối chiếu</span>`;
                } else if (p.is_correct === true) {
                    el.predEvalStatus.innerHTML = `<span class="eval-text correct">Chính xác (${formatNumber(p.actual_future_price, 2)} USDT)</span>`;
                } else if (p.is_correct === false) {
                    el.predEvalStatus.innerHTML = `<span class="eval-text incorrect">Sai (${formatNumber(p.actual_future_price, 2)} USDT)</span>`;
                } else {
                    el.predEvalStatus.textContent = "—";
                }

                updateProvenance("postgres_dw", p.feature_timestamp, false);
            } else {
                el.predDirBadge.textContent = "—";
                el.predDirBadge.className = "pred-direction-badge";
                el.predProbUp.textContent = "—";
                el.predProbNotUp.textContent = "—";
                el.predBasePrice.textContent = "—";
                el.predFeatureTime.textContent = "—";
                el.predTargetTime.textContent = "—";
                el.predEvalStatus.textContent = "Chưa có dự báo trong CSDL";
                updateProvenance("postgres_dw", null, false);
            }
        } catch (e) {
            if (e.name !== "AbortError") throw e;
        }
    }

    async function fetchPredictionHistory(signal) {
        try {
            const res = await fetch(`/api/ml/history?page=${state.predPage}&page_size=${state.predPageSize}`, { signal });
            const data = await res.json();

            if (res.ok && data.status === "success") {
                state.predTotalPages = data.total_pages || 1;
                el.predPageIndicator.textContent = `Trang ${data.page} / ${state.predTotalPages}`;
                el.btnPredPrev.disabled = (data.page <= 1);
                el.btnPredNext.disabled = (data.page >= state.predTotalPages);

                renderPredictionHistoryTable(data.history);
            } else {
                renderPredictionHistoryTable([]);
            }
        } catch (e) {
            if (e.name !== "AbortError") throw e;
        }
    }

    function renderPredictionHistoryTable(history) {
        if (!el.predHistoryBody) return;
        if (!history || history.length === 0) {
            el.predHistoryBody.innerHTML = `<tr><td colspan="8" class="empty-cell">Chưa có lịch sử dự báo</td></tr>`;
            return;
        }

        let html = "";
        history.forEach(item => {
            const isUp = (item.predicted_direction === 1);
            const dirHtml = `<strong class="${isUp ? 'text-buy' : 'text-muted'}">${isUp ? 'Tăng' : 'Không tăng'}</strong>`;

            let evalHtml = "—";
            if (item.eval_status === "PENDING_TARGET") {
                evalHtml = `<span class="eval-text pending">Chờ đối chiếu</span>`;
            } else if (item.eval_status === "MISSING_ACTUAL") {
                evalHtml = `<span class="eval-text missing">Thiếu nến</span>`;
            } else if (item.is_correct === true) {
                evalHtml = `<span class="eval-text correct">Đúng</span>`;
            } else if (item.is_correct === false) {
                evalHtml = `<span class="eval-text incorrect">Sai</span>`;
            }

            const actualDir = item.actual_direction === 1 ? "Tăng" : (item.actual_direction === 0 ? "Không tăng" : "—");

            html += `
                <tr>
                    <td>${formatUtcTime(item.feature_timestamp)}</td>
                    <td class="num-col">${item.current_price ? formatNumber(item.current_price, 2) : "—"}</td>
                    <td>${dirHtml}</td>
                    <td class="num-col">${item.probability_up ? (item.probability_up * 100).toFixed(2) + "%" : "—"}</td>
                    <td>${formatUtcTime(item.target_timestamp)}</td>
                    <td class="num-col">${item.actual_future_price ? formatNumber(item.actual_future_price, 2) : "—"}</td>
                    <td>${actualDir}</td>
                    <td>${evalHtml}</td>
                </tr>
            `;
        });
        el.predHistoryBody.innerHTML = html;
    }

    async function fetchModelMetadata(signal) {
        try {
            const res = await fetch("/api/ml/metadata", { signal });
            const data = await res.json();

            if (res.ok && data.status === "success") {
                const metrics = data.metrics || {};
                const topFeatures = (data.feature_importance || []).slice(0, 5);

                let featHtml = topFeatures.map(f => `<li><code>${f.feature}</code>: ${(f.importance * 100).toFixed(2)}%</li>`).join("");

                el.modelInfoContent.innerHTML = `
                    <table class="model-meta-table">
                        <tbody>
                            <tr><td>Tên mô hình:</td><td><strong>${data.model_name}</strong></td></tr>
                            <tr><td>Phiên bản:</td><td><code>${data.model_version}</code></td></tr>
                            <tr><td>Thuật toán:</td><td>${data.model_type}</td></tr>
                            <tr><td>Số lượng đặc trưng:</td><td><strong>${data.n_features} đặc trưng</strong></td></tr>
                            <tr><td>Chu kỳ dự báo:</td><td>${data.prediction_horizon_minutes} phút</td></tr>
                            <tr><td>Độ chính xác (Accuracy):</td><td>${metrics.accuracy ? (metrics.accuracy * 100).toFixed(2) + "%" : "—"}</td></tr>
                            <tr><td>F1-Score:</td><td>${metrics.f1_score ? metrics.f1_score.toFixed(4) : "—"}</td></tr>
                            <tr><td>ROC-AUC:</td><td>${metrics.roc_auc ? metrics.roc_auc.toFixed(4) : "—"}</td></tr>
                            <tr><td>Top đặc trưng quan trọng:</td><td><ul style="padding-left:18px;">${featHtml || "—"}</ul></td></tr>
                        </tbody>
                    </table>
                `;
            }
        } catch (e) {
            if (e.name !== "AbortError") throw e;
        }
    }

    async function triggerNewPrediction() {
        el.btnTriggerPred.disabled = true;
        el.btnTriggerPred.textContent = "Đang chạy suy luận...";
        el.predTriggerFeedback.className = "feedback-msg active text-muted";
        el.predTriggerFeedback.textContent = "Đang trích xuất 15 đặc trưng và thực thi suy luận từ mô hình XGBoost...";

        try {
            const res = await fetch("/api/ml/trigger_prediction", { method: "POST" });
            const data = await res.json();

            if (res.ok && data.status === "success") {
                el.predTriggerFeedback.className = "feedback-msg active text-buy";
                el.predTriggerFeedback.textContent = "Chu kỳ hoàn tất! Dự báo thành công và đã ghi nhận vào Data Warehouse.";
                await fetchPredictionWorkerStatus();
                await fetchLatestPrediction();
                await fetchPredictionHistory();
            } else {
                el.predTriggerFeedback.className = "feedback-msg active text-sell";
                const code = data.rejection_code ? `[${data.rejection_code}] ` : "";
                el.predTriggerFeedback.textContent = `${code}${data.message || "Từ chối suy luận."}`;
                await fetchPredictionWorkerStatus();
            }
        } catch (err) {
            el.predTriggerFeedback.className = "feedback-msg active text-sell";
            el.predTriggerFeedback.textContent = "Lỗi kết nối khi gửi yêu cầu suy luận: " + err.message;
        } finally {
            el.btnTriggerPred.disabled = false;
            el.btnTriggerPred.textContent = "Chạy dự báo mới";
            setTimeout(() => {
                el.predTriggerFeedback.classList.remove("active");
            }, 6000);
        }
    }

    // Initialize application when DOM is ready
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
