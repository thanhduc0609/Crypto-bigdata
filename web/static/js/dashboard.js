/**
 * =============================================================================
 * Crypto Big Data - Clean Light Trading Terminal Client
 * =============================================================================
 * Modular architecture:
 *   - ApiClient: Abortable fetch client with error handling
 *   - TimeUtils: Standardized UTC+7 Vietnam time formatting & countdowns
 *   - TVChartManager: TradingView Lightweight Charts (Candles, Volume, VWAP, PriceLines)
 *   - PredictionPlanView: Hero card for XGBoost +5m prediction & Futures Plan
 *   - OrderFlowView: 60s sliding window order flow imbalance
 *   - VolumeChartView: Pure SVG recent candle volume histogram
 *   - DonutChartView: Pure SVG breakdown of large trades (>= $100K)
 *   - LargeTradesView: Filterable, paginated large trades table & CSV export
 *   - PredictionHistoryView: Paginated historical evaluation
 *   - ModelMetadataView: Artifact-driven model specs & manual worker trigger
 *   - PollingScheduler: Coordinated polling with document visibility pause
 * =============================================================================
 */

(function () {
    "use strict";

    // Application State
    const state = {
        timeframe: "1m",               // "1m", "1h", "1d"
        showVwap: true,
        tradesTimeRange: "all",        // "1h", "24h", "7d", "all"
        tradesMinValue: 100000,
        tradesPage: 1,
        tradesPageSize: 25,
        tradesTotalPages: 1,
        predPage: 1,
        predPageSize: 10,
        predTotalPages: 1,

        // Chart Price Lines (Entry, SL, TP)
        priceLines: [],

        // Prediction & Target Countdown
        targetTimestamp: null,
        countdownTimer: null,

        // Active poll timers
        pollingTimer: null,
        orderflowTimer: null,
        isPollingActive: true
    };

    // DOM Elements Cache
    const el = {
        // Area A: Header
        ovPrice: document.getElementById("ov-price"),
        ovChange: document.getElementById("ov-change"),
        headerBasis: document.getElementById("header-basis"),
        provenanceFetchTs: document.getElementById("provenance-fetch-ts"),
        btnRefresh: document.getElementById("btn-refresh"),
        btnRefreshText: document.getElementById("btn-refresh-text"),
        connectionAlert: document.getElementById("connection-alert"),
        connectionAlertText: document.getElementById("connection-alert-text"),

        // Area B: Chart Controls
        tfButtons: document.querySelectorAll(".tf-btn"),
        toggleVwap: document.getElementById("toggle-vwap"),
        chartContainer: document.getElementById("tv-chart"),
        chartErrorOverlay: document.getElementById("chart-error-overlay"),
        ciTime: document.getElementById("ci-time"),
        ciOpen: document.getElementById("ci-open"),
        ciHigh: document.getElementById("ci-high"),
        ciLow: document.getElementById("ci-low"),
        ciClose: document.getElementById("ci-close"),
        ciVol: document.getElementById("ci-vol"),
        ciVwap: document.getElementById("ci-vwap"),

        // Area B: Prediction Block
        predDirBadge: document.getElementById("pred-dir-badge"),
        predProbUp: document.getElementById("pred-prob-up"),
        predProbNotup: document.getElementById("pred-prob-notup"),
        predProbBarUp: document.getElementById("pred-prob-bar-up"),
        predProbBarNotup: document.getElementById("pred-prob-bar-notup"),
        predProbDesc: document.getElementById("pred-prob-desc"),
        predFeatureTime: document.getElementById("pred-feature-time"),
        predTargetTime: document.getElementById("pred-target-time"),
        predValidityStatus: document.getElementById("pred-validity-status"),

        // Area B: Futures Plan Block
        planActionBadge: document.getElementById("plan-action-badge"),
        planFuturesPrice: document.getElementById("plan-futures-price"),
        planEntry: document.getElementById("plan-entry"),
        planSl: document.getElementById("plan-sl"),
        planTp: document.getElementById("plan-tp"),
        planRr: document.getElementById("plan-rr"),
        planReasonText: document.getElementById("plan-reason-text"),

        // Area C: Order Flow
        ofStatusBadge: document.getElementById("of-status-badge"),
        ofBuyPct: document.getElementById("of-buy-pct"),
        ofSellPct: document.getElementById("of-sell-pct"),
        ofBuyVol: document.getElementById("of-buy-vol"),
        ofSellVol: document.getElementById("of-sell-vol"),
        ofBarBuy: document.getElementById("of-bar-buy"),
        ofBarSell: document.getElementById("of-bar-sell"),
        ofImbalance: document.getElementById("of-imbalance"),
        ofTradeCount: document.getElementById("of-trade-count"),
        ofCoverage: document.getElementById("of-coverage"),

        // Area C: Volume Chart SVG
        volumeBarSvg: document.getElementById("volume-bar-svg"),
        volumeChartTf: document.getElementById("volume-chart-tf"),
        volStatsTotal: document.getElementById("vol-stats-total"),

        // Area C: Donut Chart SVG
        donutSvg: document.getElementById("donut-svg"),
        donutBuyVal: document.getElementById("donut-buy-val"),
        donutBuyPct: document.getElementById("donut-buy-pct"),
        donutSellVal: document.getElementById("donut-sell-val"),
        donutSellPct: document.getElementById("donut-sell-pct"),
        donutFilterDesc: document.getElementById("donut-filter-desc"),

        // Area D: Large Trades
        tradesSummaryCount: document.getElementById("trades-summary-count"),
        tradesSummaryValue: document.getElementById("trades-summary-value"),
        tradesTimeRange: document.getElementById("trades-time-range"),
        tradesMinValue: document.getElementById("trades-min-value"),
        btnExportTrades: document.getElementById("btn-export-trades"),
        largeTradesBody: document.getElementById("large-trades-body"),
        btnTradesPrev: document.getElementById("btn-trades-prev"),
        btnTradesNext: document.getElementById("btn-trades-next"),
        tradesPageIndicator: document.getElementById("trades-page-indicator"),

        // Area D: Prediction History
        tablePredHistoryBody: document.getElementById("pred-history-body"),
        btnPredPrev: document.getElementById("btn-pred-prev"),
        btnPredNext: document.getElementById("btn-pred-next"),
        predPageIndicator: document.getElementById("pred-page-indicator"),

        // Area D: Worker & Model Metadata
        workerStatusBadge: document.getElementById("worker-status-badge"),
        workerLastCheck: document.getElementById("worker-last-check"),
        workerCycleStatus: document.getElementById("worker-cycle-status"),
        btnTriggerPred: document.getElementById("btn-trigger-pred"),
        predTriggerFeedback: document.getElementById("pred-trigger-feedback"),
        modelInfoContent: document.getElementById("model-info-content")
    };

    // =========================================================================
    // 1. TIME UTILITIES (UTC+7 Vietnam Standard Time)
    // =========================================================================
    const TimeUtils = {
        formatDateTimeVN(isoStr) {
            if (!isoStr) return "—";
            try {
                const date = new Date(isoStr);
                if (isNaN(date.getTime())) return String(isoStr);
                return new Intl.DateTimeFormat("vi-VN", {
                    timeZone: "Asia/Ho_Chi_Minh",
                    year: "numeric",
                    month: "2-digit",
                    day: "2-digit",
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                    hour12: false
                }).format(date);
            } catch (e) {
                return String(isoStr);
            }
        },

        formatTimeOnlyVN(isoStr) {
            if (!isoStr) return "—";
            try {
                const date = new Date(isoStr);
                if (isNaN(date.getTime())) return "—";
                return new Intl.DateTimeFormat("vi-VN", {
                    timeZone: "Asia/Ho_Chi_Minh",
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                    hour12: false
                }).format(date);
            } catch (e) {
                return "—";
            }
        },

        formatUnixVN(unixSec) {
            if (!unixSec || isNaN(unixSec)) return "—";
            try {
                const date = new Date(Number(unixSec) * 1000);
                if (isNaN(date.getTime())) return "—";
                return new Intl.DateTimeFormat("vi-VN", {
                    timeZone: "Asia/Ho_Chi_Minh",
                    year: "numeric",
                    month: "2-digit",
                    day: "2-digit",
                    hour: "2-digit",
                    minute: "2-digit",
                    hour12: false
                }).format(date);
            } catch (e) {
                return "—";
            }
        }
    };

    // Format Number Utilities
    const NumUtils = {
        formatUSD(num) {
            if (num === null || num === undefined || isNaN(num)) return "—";
            return new Intl.NumberFormat("en-US", {
                minimumFractionDigits: 1,
                maximumFractionDigits: 1
            }).format(num);
        },

        formatIntegerUSD(num) {
            if (num === null || num === undefined || isNaN(num)) return "—";
            return new Intl.NumberFormat("en-US", {
                maximumFractionDigits: 0
            }).format(num);
        },

        formatBTC(num) {
            if (num === null || num === undefined || isNaN(num)) return "—";
            return new Intl.NumberFormat("en-US", {
                minimumFractionDigits: 4,
                maximumFractionDigits: 4
            }).format(num);
        },

        formatCompact(num) {
            if (num === null || num === undefined || isNaN(num)) return "—";
            if (num >= 1e6) return (num / 1e6).toFixed(2) + "M";
            if (num >= 1e3) return (num / 1e3).toFixed(1) + "K";
            return num.toFixed(1);
        }
    };

    // =========================================================================
    // 2. API CLIENT (Fetch with AbortController)
    // =========================================================================
    const ApiClient = {
        activeControllers: new Map(),

        async fetchJson(url, key = "default") {
            if (this.activeControllers.has(key)) {
                this.activeControllers.get(key).abort();
            }
            const controller = new AbortController();
            this.activeControllers.set(key, controller);

            try {
                const resp = await fetch(url, { signal: controller.signal });
                if (!resp.ok) {
                    throw new Error(`HTTP ${resp.status}`);
                }
                const data = await resp.json();
                this.activeControllers.delete(key);
                return data;
            } catch (err) {
                if (err.name === "AbortError") {
                    return null;
                }
                this.activeControllers.delete(key);
                throw err;
            }
        }
    };

    // =========================================================================
    // 3. TRADINGVIEW LIGHTWEIGHT CHARTS MANAGER (Light Mode)
    // =========================================================================
    const TVChartManager = {
        chart: null,
        candleSeries: null,
        volumeSeries: null,
        vwapSeries: null,
        rawCandles: [],
        hasFitted: false,

        init() {
            if (!el.chartContainer) return;
            if (typeof LightweightCharts === "undefined") {
                this.showError("Không tải được thư viện biểu đồ Lightweight Charts.");
                return;
            }

            try {
                const initialWidth = el.chartContainer.clientWidth || 700;
                const initialHeight = el.chartContainer.clientHeight || 480;

                this.chart = LightweightCharts.createChart(el.chartContainer, {
                    width: initialWidth,
                    height: initialHeight,
                    layout: {
                        background: { color: "#ffffff" },
                        textColor: "#64748b"
                    },
                    grid: {
                        vertLines: { color: "#f1f5f9" },
                        horzLines: { color: "#f1f5f9" }
                    },
                    crosshair: {
                        mode: LightweightCharts.CrosshairMode.Normal,
                        vertLine: { color: "#94a3b8", width: 1, style: 2 },
                        horzLine: { color: "#94a3b8", width: 1, style: 2 }
                    },
                    rightPriceScale: {
                        borderColor: "#e2e8f0",
                        scaleMargins: { top: 0.1, bottom: 0.22 }
                    },
                    timeScale: {
                        borderColor: "#e2e8f0",
                        timeVisible: true,
                        secondsVisible: false
                    }
                });

                // Candlestick Series: Green up, Red down
                this.candleSeries = this.chart.addCandlestickSeries({
                    upColor: "#16a34a",
                    downColor: "#dc2626",
                    borderUpColor: "#16a34a",
                    borderDownColor: "#dc2626",
                    wickUpColor: "#16a34a",
                    wickDownColor: "#dc2626"
                });

                // Volume Series
                this.volumeSeries = this.chart.addHistogramSeries({
                    color: "rgba(37, 99, 235, 0.35)",
                    priceFormat: { type: "volume" },
                    priceScaleId: "",
                    scaleMargins: { top: 0.8, bottom: 0 }
                });

                // VWAP Line Series
                this.vwapSeries = this.chart.addLineSeries({
                    color: "#d97706",
                    lineWidth: 2,
                    priceLineVisible: false
                });

                // Crosshair Move Listener
                this.chart.subscribeCrosshairMove((param) => {
                    this.handleCrosshairMove(param);
                });

                // Auto Resize with ResizeObserver & Window Resize
                const ro = new ResizeObserver((entries) => {
                    if (entries.length && this.chart && el.chartContainer) {
                        const rect = entries[0].contentRect;
                        if (rect.width > 0 && rect.height > 0) {
                            this.chart.applyOptions({ width: rect.width, height: rect.height });
                        }
                    }
                });
                ro.observe(el.chartContainer);

                window.addEventListener("resize", () => {
                    if (this.chart && el.chartContainer) {
                        const w = el.chartContainer.clientWidth;
                        const h = el.chartContainer.clientHeight || 480;
                        if (w > 0) {
                            this.chart.applyOptions({ width: w, height: h });
                        }
                    }
                });

            } catch (e) {
                console.error("[TVChart] Init failed:", e);
                this.showError("Khởi tạo biểu đồ thất bại: " + e.message);
            }
        },

        showError(msg) {
            if (el.chartErrorOverlay) {
                el.chartErrorOverlay.textContent = msg;
                el.chartErrorOverlay.style.display = "flex";
            }
        },

        clearError() {
            if (el.chartErrorOverlay) {
                el.chartErrorOverlay.style.display = "none";
            }
        },

        updateData(candles) {
            if (!this.candleSeries || !candles || !candles.length) return;
            this.clearError();
            this.rawCandles = candles;

            const candleData = [];
            const volumeData = [];
            const vwapData = [];

            // Sort chronologically ascending, robust to both `time` and `timestamp`
            const sorted = [...candles].sort((a, b) => {
                const ta = a.time !== undefined ? a.time : (a.timestamp || 0);
                const tb = b.time !== undefined ? b.time : (b.timestamp || 0);
                return ta - tb;
            });

            for (const c of sorted) {
                const t = c.time !== undefined ? c.time : c.timestamp;
                if (!t) continue;

                candleData.push({
                    time: t,
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close
                });

                const isUp = c.close >= c.open;
                volumeData.push({
                    time: t,
                    value: c.volume || 0,
                    color: isUp ? "rgba(22, 163, 74, 0.4)" : "rgba(220, 38, 38, 0.4)"
                });

                if (c.vwap !== null && c.vwap !== undefined && c.vwap > 0) {
                    vwapData.push({
                        time: t,
                        value: c.vwap
                    });
                }
            }

            if (candleData.length > 0) {
                this.candleSeries.setData(candleData);
                this.volumeSeries.setData(volumeData);

                if (state.showVwap && this.vwapSeries) {
                    this.vwapSeries.setData(vwapData);
                } else if (this.vwapSeries) {
                    this.vwapSeries.setData([]);
                }

                if (!this.hasFitted) {
                    this.chart.timeScale().fitContent();
                    this.hasFitted = true;
                }

                // Update top inspect bar with latest candle
                const latest = sorted[sorted.length - 1];
                this.updateInspectBar(latest);
            }
        },

        updateInspectBar(c) {
            if (!c) return;
            const tVal = c.time !== undefined ? c.time : c.timestamp;
            if (el.ciTime) el.ciTime.textContent = TimeUtils.formatUnixVN(tVal);
            if (el.ciOpen) el.ciOpen.textContent = NumUtils.formatUSD(c.open);
            if (el.ciHigh) el.ciHigh.textContent = NumUtils.formatUSD(c.high);
            if (el.ciLow) el.ciLow.textContent = NumUtils.formatUSD(c.low);
            if (el.ciClose) el.ciClose.textContent = NumUtils.formatUSD(c.close);
            if (el.ciVol) el.ciVol.textContent = NumUtils.formatBTC(c.volume) + " BTC";
            if (el.ciVwap) el.ciVwap.textContent = c.vwap ? NumUtils.formatUSD(c.vwap) : "—";
        },

        handleCrosshairMove(param) {
            if (!param || !param.time || !param.seriesData) {
                return;
            }
            const cData = param.seriesData.get(this.candleSeries);
            if (cData) {
                const volData = param.seriesData.get(this.volumeSeries);
                const vwapData = param.seriesData.get(this.vwapSeries);
                this.updateInspectBar({
                    time: param.time,
                    open: cData.open,
                    high: cData.high,
                    low: cData.low,
                    close: cData.close,
                    volume: volData ? volData.value : null,
                    vwap: vwapData ? vwapData.value : null
                });
            }
        },

        updatePriceLines(plan) {
            if (!this.candleSeries) return;

            // Remove existing lines
            for (const line of state.priceLines) {
                try {
                    this.candleSeries.removePriceLine(line);
                } catch (e) {}
            }
            state.priceLines = [];

            // Only draw when there is an active LONG or SHORT plan
            if (!plan || (plan.action !== "LONG" && plan.action !== "SHORT")) {
                return;
            }

            if (plan.entry_price) {
                state.priceLines.push(this.candleSeries.createPriceLine({
                    price: plan.entry_price,
                    color: "#2563eb",
                    lineWidth: 2,
                    lineStyle: 0,
                    axisLabelVisible: true,
                    title: "Entry (Futures)"
                }));
            }

            if (plan.stop_loss) {
                state.priceLines.push(this.candleSeries.createPriceLine({
                    price: plan.stop_loss,
                    color: "#dc2626",
                    lineWidth: 1,
                    lineStyle: 2,
                    axisLabelVisible: true,
                    title: "SL"
                }));
            }

            if (plan.take_profit) {
                state.priceLines.push(this.candleSeries.createPriceLine({
                    price: plan.take_profit,
                    color: "#16a34a",
                    lineWidth: 1,
                    lineStyle: 2,
                    axisLabelVisible: true,
                    title: "TP (1:1.5)"
                }));
            }
        }
    };

    // =========================================================================
    // 4. PREDICTION & FUTURES PLAN VIEW (HERO PANEL)
    // =========================================================================
    const PredictionPlanView = {
        renderPrediction(pred) {
            if (!pred) {
                this.renderEmptyPrediction();
                return;
            }

            const dir = pred.direction_label;
            const probUp = pred.probability_up;
            const probNotUp = pred.probability_not_up;
            const targetTs = pred.target_timestamp;

            state.targetTimestamp = targetTs;
            this.startCountdown(targetTs);

            // Direction Hero Badge
            if (dir === "UP") {
                el.predDirBadge.textContent = "TĂNG (UP)";
                el.predDirBadge.className = "direction-badge badge-up";
            } else if (dir === "NOT_UP") {
                el.predDirBadge.textContent = "KHÔNG TĂNG (NOT_UP)";
                el.predDirBadge.className = "direction-badge badge-notup";
            } else {
                el.predDirBadge.textContent = "CHỜ DỮ LIỆU";
                el.predDirBadge.className = "direction-badge badge-neutral";
            }

            // Probabilities & Dual Bar
            if (probUp !== null && probNotUp !== null) {
                const upPct = (probUp * 100).toFixed(1);
                const notUpPct = (probNotUp * 100).toFixed(1);

                el.predProbUp.textContent = upPct + "%";
                el.predProbNotup.textContent = notUpPct + "%";
                el.predProbBarUp.style.width = upPct + "%";
                el.predProbBarNotup.style.width = notUpPct + "%";

                if (probUp >= 0.48 && probUp <= 0.52) {
                    el.predProbDesc.textContent = "Xác suất mô hình sát ngưỡng 50%, hai lớp chưa phân tách rõ.";
                } else {
                    el.predProbDesc.textContent = "Mô hình phân loại nhị phân Spot BTC-USDT. Xác suất không phải tỷ lệ thắng giao dịch.";
                }
            } else {
                el.predProbUp.textContent = "—%";
                el.predProbNotup.textContent = "—%";
                el.predProbBarUp.style.width = "50%";
                el.predProbBarNotup.style.width = "50%";
            }

            // Timestamps
            el.predFeatureTime.textContent = TimeUtils.formatTimeOnlyVN(pred.feature_timestamp);
            el.predTargetTime.textContent = TimeUtils.formatTimeOnlyVN(pred.target_timestamp);

            // Validity Status
            const now = new Date().getTime();
            const targetTime = targetTs ? new Date(targetTs).getTime() : 0;
            if (targetTime > now) {
                el.predValidityStatus.textContent = "Còn hiệu lực";
                el.predValidityStatus.className = "kv-val buy-text font-bold";
            } else {
                el.predValidityStatus.textContent = "Hết hiệu lực mục tiêu";
                el.predValidityStatus.className = "kv-val sell-text font-bold";
            }
        },

        renderEmptyPrediction() {
            el.predDirBadge.textContent = "CHỜ DỮ LIỆU";
            el.predDirBadge.className = "direction-badge badge-neutral";
            el.predProbUp.textContent = "—%";
            el.predProbNotup.textContent = "—%";
            el.predFeatureTime.textContent = "—";
            el.predTargetTime.textContent = "—";
            el.predValidityStatus.textContent = "Chưa có dữ liệu";
            el.predValidityStatus.className = "kv-val text-muted";
        },

        renderFuturesPlan(plan) {
            if (!plan) return;

            const action = plan.action || "CHỜ";
            if (action === "LONG") {
                el.planActionBadge.textContent = "MUA (LONG)";
                el.planActionBadge.className = "plan-action-badge badge-up";
            } else if (action === "SHORT") {
                el.planActionBadge.textContent = "BÁN (SHORT)";
                el.planActionBadge.className = "plan-action-badge badge-notup";
            } else {
                el.planActionBadge.textContent = "CHỜ (WAIT)";
                el.planActionBadge.className = "plan-action-badge badge-wait";
            }

            // Market Data & Basis
            const mkt = plan.market_data || {};
            if (mkt.futures_price) {
                el.planFuturesPrice.textContent = NumUtils.formatUSD(mkt.futures_price) + " USDT";
            } else {
                el.planFuturesPrice.textContent = "—";
            }

            if (mkt.basis_usd !== null && mkt.basis_usd !== undefined) {
                const prefix = mkt.basis_usd > 0 ? "+" : "";
                el.headerBasis.textContent = `${prefix}${mkt.basis_usd.toFixed(2)} USDT`;
                el.headerBasis.className = mkt.basis_usd >= 0 ? "basis-val tabular buy-text" : "basis-val tabular sell-text";
            } else {
                el.headerBasis.textContent = "—";
            }

            // Levels: Entry, SL, TP, R:R
            if (action === "LONG" || action === "SHORT") {
                el.planEntry.textContent = NumUtils.formatUSD(plan.entry_price);
                el.planSl.textContent = NumUtils.formatUSD(plan.stop_loss);
                el.planTp.textContent = NumUtils.formatUSD(plan.take_profit);
                el.planRr.textContent = plan.risk_reward_ratio || "1:1.5";
            } else {
                el.planEntry.textContent = "—";
                el.planSl.textContent = "—";
                el.planTp.textContent = "—";
                el.planRr.textContent = "1:1.5";
            }

            // Reasoning Text
            el.planReasonText.textContent = plan.reason || "Giữ vị thế quan sát thị trường.";

            // Draw or clear price lines on chart
            TVChartManager.updatePriceLines(plan);
        },

        startCountdown(targetIsoStr) {
            if (state.countdownTimer) {
                clearInterval(state.countdownTimer);
                state.countdownTimer = null;
            }

            if (!targetIsoStr) {
                return;
            }

            const targetTime = new Date(targetIsoStr).getTime();
            const updateValidity = () => {
                const now = new Date().getTime();
                const diffMs = targetTime - now;

                if (diffMs <= 0) {
                    if (el.predValidityStatus) {
                        el.predValidityStatus.textContent = "Hết hiệu lực mục tiêu";
                        el.predValidityStatus.className = "kv-val sell-text font-bold";
                    }
                    clearInterval(state.countdownTimer);
                    state.countdownTimer = null;
                }
            };

            updateValidity();
            state.countdownTimer = setInterval(updateValidity, 1000);
        }
    };

    // =========================================================================
    // 5. ORDER FLOW VIEW (60s Sliding Window)
    // =========================================================================
    const OrderFlowView = {
        render(data) {
            if (!data) return;

            const st = data.status || data.state;
            el.ofStatusBadge.textContent = data.status_text || st;

            if (st === "active") {
                el.ofStatusBadge.className = "micro-badge buy-text";
            } else if (st === "warming_up") {
                el.ofStatusBadge.className = "micro-badge wait-text";
            } else {
                el.ofStatusBadge.className = "micro-badge sell-text";
            }

            if (data.total_volume > 0 && data.buy_ratio !== null) {
                const buyPct = (data.buy_ratio * 100).toFixed(1);
                const sellPct = (data.sell_ratio * 100).toFixed(1);

                el.ofBuyPct.textContent = buyPct + "%";
                el.ofSellPct.textContent = sellPct + "%";
                el.ofBuyVol.textContent = NumUtils.formatBTC(data.buy_volume) + " BTC";
                el.ofSellVol.textContent = NumUtils.formatBTC(data.sell_volume) + " BTC";

                el.ofBarBuy.style.width = buyPct + "%";
                el.ofBarSell.style.width = sellPct + "%";

                const imb = data.imbalance;
                const prefix = imb > 0 ? "+" : "";
                el.ofImbalance.textContent = `${prefix}${(imb * 100).toFixed(1)}%`;
                el.ofImbalance.className = imb >= 0 ? "am-val tabular font-bold buy-text" : "am-val tabular font-bold sell-text";
            } else {
                el.ofBuyPct.textContent = "—%";
                el.ofSellPct.textContent = "—%";
                el.ofBuyVol.textContent = "— BTC";
                el.ofSellVol.textContent = "— BTC";
                el.ofBarBuy.style.width = "50%";
                el.ofBarSell.style.width = "50%";
                el.ofImbalance.textContent = "—";
                el.ofImbalance.className = "am-val tabular font-bold";
            }

            el.ofTradeCount.textContent = data.trade_count !== undefined ? String(data.trade_count) : "—";
            el.ofCoverage.textContent = `${data.coverage_seconds || 0}s / 60s`;
        }
    };

    // =========================================================================
    // 6. VOLUME BAR CHART VIEW (Pure SVG Histogram)
    // =========================================================================
    const VolumeChartView = {
        render(candles) {
            if (!el.volumeBarSvg || !candles || !candles.length) return;

            // Take last 30 candles
            const recent = candles.slice(-30);
            const totalVol = recent.reduce((sum, c) => sum + (c.volume || 0), 0);
            if (el.volStatsTotal) {
                el.volStatsTotal.textContent = NumUtils.formatBTC(totalVol) + " BTC";
            }

            const maxVol = Math.max(...recent.map(c => c.volume || 0), 0.001);
            const svgWidth = 360;
            const svgHeight = 130;
            const paddingBottom = 10;
            const usableHeight = svgHeight - paddingBottom;
            const barCount = recent.length;
            const barSpacing = svgWidth / barCount;
            const barWidth = Math.max(2, barSpacing * 0.7);

            let svgHtml = "";

            recent.forEach((c, i) => {
                const vol = c.volume || 0;
                const barHeight = Math.max(2, (vol / maxVol) * usableHeight);
                const x = i * barSpacing + (barSpacing - barWidth) / 2;
                const y = svgHeight - paddingBottom - barHeight;
                const isUp = c.close >= c.open;
                const color = isUp ? "#16a34a" : "#dc2626";
                const tVal = c.time !== undefined ? c.time : c.timestamp;
                const timeStr = tVal ? TimeUtils.formatTimeOnlyVN(new Date(tVal * 1000).toISOString()) : "—";

                svgHtml += `
                    <rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barHeight.toFixed(1)}"
                          fill="${color}" opacity="0.85" rx="1">
                        <title>${timeStr} | Vol: ${vol.toFixed(4)} BTC</title>
                    </rect>
                `;
            });

            // Baseline
            svgHtml += `<line x1="0" y1="${svgHeight - paddingBottom}" x2="${svgWidth}" y2="${svgHeight - paddingBottom}" stroke="#e2e8f0" stroke-width="1" />`;

            el.volumeBarSvg.innerHTML = svgHtml;
        }
    };

    // =========================================================================
    // 7. DONUT CHART VIEW (Pure SVG Large Trades Breakdown)
    // =========================================================================
    const DonutChartView = {
        render(breakdown) {
            if (!el.donutSvg) return;

            if (!breakdown || breakdown.total_value <= 0) {
                // Empty circle
                el.donutSvg.innerHTML = `
                    <circle cx="80" cy="80" r="50" fill="none" stroke="#e2e8f0" stroke-width="16" />
                    <text x="80" y="85" text-anchor="middle" fill="#64748b" font-size="12" font-family="sans-serif">0 giao dịch</text>
                `;
                el.donutBuyVal.textContent = "—";
                el.donutBuyPct.textContent = "(0%)";
                el.donutSellVal.textContent = "—";
                el.donutSellPct.textContent = "(0%)";
                return;
            }

            const total = breakdown.total_value;
            const buyVal = breakdown.buy_value;
            const sellVal = breakdown.sell_value;
            const buyPct = breakdown.buy_pct;
            const sellPct = breakdown.sell_pct;

            // SVG Donut calculation: Circumference = 2 * PI * r
            const r = 50;
            const circumference = 2 * Math.PI * r;
            const buyStroke = (buyPct / 100) * circumference;
            const sellStroke = (sellPct / 100) * circumference;

            // Buy slice starts at -90deg (top: stroke-dashoffset = 0)
            const sellOffset = -buyStroke;

            const totalText = "$" + NumUtils.formatCompact(total);

            el.donutSvg.innerHTML = `
                <!-- Background track -->
                <circle cx="80" cy="80" r="${r}" fill="none" stroke="#e2e8f0" stroke-width="16" />
                
                <!-- Buy slice -->
                <circle cx="80" cy="80" r="${r}" fill="none" stroke="#16a34a" stroke-width="16"
                        stroke-dasharray="${buyStroke} ${circumference}"
                        stroke-dashoffset="0"
                        transform="rotate(-90 80 80)">
                    <title>Mua: $${NumUtils.formatIntegerUSD(buyVal)} (${buyPct}%)</title>
                </circle>

                <!-- Sell slice -->
                <circle cx="80" cy="80" r="${r}" fill="none" stroke="#dc2626" stroke-width="16"
                        stroke-dasharray="${sellStroke} ${circumference}"
                        stroke-dashoffset="${sellOffset}"
                        transform="rotate(-90 80 80)">
                    <title>Bán: $${NumUtils.formatIntegerUSD(sellVal)} (${sellPct}%)</title>
                </circle>

                <!-- Center Hole Text -->
                <text x="80" y="78" text-anchor="middle" fill="#0f172a" font-size="13" font-weight="700" font-family="monospace">${totalText}</text>
                <text x="80" y="93" text-anchor="middle" fill="#64748b" font-size="9" font-family="sans-serif">TỔNG USDT</text>
            `;

            el.donutBuyVal.textContent = "$" + NumUtils.formatCompact(buyVal);
            el.donutBuyPct.textContent = `(${buyPct}%)`;
            el.donutSellVal.textContent = "$" + NumUtils.formatCompact(sellVal);
            el.donutSellPct.textContent = `(${sellPct}%)`;
        }
    };

    // =========================================================================
    // 8. LARGE TRADES VIEW (Paginated Table & Filters)
    // =========================================================================
    const LargeTradesView = {
        async fetchAndRender() {
            try {
                const url = `/api/trades/large?time_range=${state.tradesTimeRange}&min_value=${state.tradesMinValue}&page=${state.tradesPage}&page_size=${state.tradesPageSize}`;
                const data = await ApiClient.fetchJson(url, "large_trades");
                if (!data) return;

                state.tradesTotalPages = data.total_pages || 1;

                // Summary
                el.tradesSummaryCount.textContent = data.total_count ? String(data.total_count) : "0";
                el.tradesSummaryValue.textContent = NumUtils.formatIntegerUSD(data.total_value) + " USDT";

                // Donut Chart
                DonutChartView.render(data.breakdown);

                // Table rows
                const trades = data.trades || [];
                if (!trades.length) {
                    el.largeTradesBody.innerHTML = `<tr><td colspan="6" class="empty-cell">Không có giao dịch lớn nào thỏa mãn bộ lọc hiện tại.</td></tr>`;
                } else {
                    let html = "";
                    for (const t of trades) {
                        const isBuy = t.side === "BUY";
                        const sideBadge = isBuy
                            ? `<span class="change-tag change-up">MUA</span>`
                            : `<span class="change-tag change-down">BÁN</span>`;

                        html += `
                            <tr>
                                <td>${TimeUtils.formatDateTimeVN(t.trade_time)}</td>
                                <td>${sideBadge}</td>
                                <td class="num-col">${NumUtils.formatUSD(t.price)}</td>
                                <td class="num-col">${NumUtils.formatBTC(t.size)}</td>
                                <td class="num-col font-bold">${NumUtils.formatIntegerUSD(t.trade_value)}</td>
                                <td><code class="text-muted">${t.trade_id}</code></td>
                            </tr>
                        `;
                    }
                    el.largeTradesBody.innerHTML = html;
                }

                // Pagination
                el.tradesPageIndicator.textContent = `Trang ${state.tradesPage} / ${state.tradesTotalPages}`;
                el.btnTradesPrev.disabled = state.tradesPage <= 1;
                el.btnTradesNext.disabled = state.tradesPage >= state.tradesTotalPages;

            } catch (err) {
                console.error("[LargeTrades] Fetch error:", err);
            }
        }
    };

    // =========================================================================
    // 9. PREDICTION HISTORY VIEW
    // =========================================================================
    const PredictionHistoryView = {
        async fetchAndRender() {
            try {
                const url = `/api/ml/history?page=${state.predPage}&page_size=${state.predPageSize}`;
                const data = await ApiClient.fetchJson(url, "pred_history");
                if (!data) return;

                state.predTotalPages = data.total_pages || 1;
                const history = data.history || [];

                if (!history.length) {
                    el.tablePredHistoryBody.innerHTML = `<tr><td colspan="8" class="empty-cell">Chưa có lịch sử dự báo nào trong cơ sở dữ liệu.</td></tr>`;
                } else {
                    let html = "";
                    for (const h of history) {
                        const isUp = h.direction_label === "UP";
                        const predBadge = isUp
                            ? `<span class="change-tag change-up">TĂNG</span>`
                            : `<span class="change-tag change-down">KHÔNG TĂNG</span>`;

                        let evalHtml = `<span class="text-muted">—</span>`;
                        if (h.eval_status === "PENDING_TARGET") {
                            evalHtml = `<span class="micro-badge wait-text">Đang chờ mục tiêu</span>`;
                        } else if (h.is_correct === true) {
                            evalHtml = `<span class="change-tag change-up">CHÍNH XÁC</span>`;
                        } else if (h.is_correct === false) {
                            evalHtml = `<span class="change-tag change-down">SAI</span>`;
                        }

                        const actualDir = h.actual_direction_label
                            ? (h.actual_direction_label === "UP" ? "TĂNG" : "KHÔNG TĂNG")
                            : "—";

                        html += `
                            <tr>
                                <td>${TimeUtils.formatDateTimeVN(h.feature_timestamp)}</td>
                                <td class="num-col">${NumUtils.formatUSD(h.current_price)}</td>
                                <td>${predBadge}</td>
                                <td class="num-col font-bold">${h.probability_up ? (h.probability_up * 100).toFixed(1) + "%" : "—"}</td>
                                <td>${TimeUtils.formatDateTimeVN(h.target_timestamp)}</td>
                                <td class="num-col">${NumUtils.formatUSD(h.actual_future_price)}</td>
                                <td>${actualDir}</td>
                                <td>${evalHtml}</td>
                            </tr>
                        `;
                    }
                    el.tablePredHistoryBody.innerHTML = html;
                }

                el.predPageIndicator.textContent = `Trang ${state.predPage} / ${state.predTotalPages}`;
                el.btnPredPrev.disabled = state.predPage <= 1;
                el.btnPredNext.disabled = state.predPage >= state.predTotalPages;

            } catch (err) {
                console.error("[PredHistory] Fetch error:", err);
            }
        }
    };

    // =========================================================================
    // 10. MODEL METADATA & WORKER CONTROLS
    // =========================================================================
    const ModelMetadataView = {
        async fetchAndRender() {
            try {
                const [metaData, statusData] = await Promise.all([
                    ApiClient.fetchJson("/api/ml/metadata", "ml_meta"),
                    ApiClient.fetchJson("/api/ml/status", "ml_status")
                ]);

                if (statusData && statusData.worker) {
                    const w = statusData.worker;
                    el.workerStatusBadge.textContent = w.is_running ? "Đang chạy" : "Tạm dừng";
                    el.workerLastCheck.textContent = TimeUtils.formatTimeOnlyVN(w.last_cycle_time);
                    el.workerCycleStatus.textContent = w.last_cycle_status || "Bình thường";
                }

                if (metaData) {
                    const m = metaData.metrics || {};
                    let fiHtml = "";
                    if (metaData.feature_importance && metaData.feature_importance.length) {
                        fiHtml = metaData.feature_importance.slice(0, 6).map(f => 
                            `<code>${f.feature}</code>: ${(f.importance * 100).toFixed(1)}%`
                        ).join(" · ");
                    }

                    el.modelInfoContent.innerHTML = `
                        <div style="display: flex; flex-direction: column; gap: 8px;">
                            <div>
                                <strong>Mô hình:</strong> ${metaData.model_name || "btc_xgboost_direction"} 
                                (v${metaData.model_version || "2.0.0"} · ${metaData.model_type || "XGBClassifier"} · Horizon: +${metaData.prediction_horizon_minutes || 5}m)
                            </div>
                            <div>
                                <strong>Kết quả kiểm định thật (Artifact Test Set):</strong>
                                Accuracy: <strong class="tabular">${m.accuracy ? (m.accuracy * 100).toFixed(2) + "%" : "49.98%"}</strong> | 
                                Baseline: <strong class="tabular">${m.baseline_accuracy ? (m.baseline_accuracy * 100).toFixed(2) + "%" : "50.29%"}</strong> | 
                                Lift: <strong class="tabular">${m.baseline_lift !== undefined ? (m.baseline_lift * 100).toFixed(2) + "%" : "-0.61%"}</strong> | 
                                ROC AUC: <strong class="tabular">${m.roc_auc ? m.roc_auc.toFixed(4) : "0.5053"}</strong>
                            </div>
                            <div>
                                <strong>Số đặc trưng nến/vi cấu trúc:</strong> ${metaData.n_features || 15} canonical features.
                            </div>
                            ${fiHtml ? `<div><strong>Đặc trưng quan trọng hàng đầu:</strong> ${fiHtml}</div>` : ""}
                        </div>
                    `;
                }
            } catch (err) {
                console.error("[ModelMetadata] Fetch error:", err);
            }
        },

        async triggerPrediction() {
            if (el.btnTriggerPred) el.btnTriggerPred.disabled = true;
            el.predTriggerFeedback.style.display = "block";
            el.predTriggerFeedback.textContent = "Đang kích hoạt chu kỳ suy luận...";
            el.predTriggerFeedback.className = "feedback-msg wait-text";

            try {
                const resp = await fetch("/api/ml/trigger_prediction", { method: "POST" });
                const data = await resp.json();

                if (resp.status === 200) {
                    el.predTriggerFeedback.textContent = "Chu kỳ suy luận hoàn tất thành công!";
                    el.predTriggerFeedback.className = "feedback-msg buy-text";
                    TerminalController.fetchCoreMarketAndPlan();
                    PredictionHistoryView.fetchAndRender();
                } else if (resp.status === 429) {
                    el.predTriggerFeedback.textContent = "Chu kỳ khác đang chạy đồng thời, vui lòng đợi.";
                    el.predTriggerFeedback.className = "feedback-msg wait-text";
                } else {
                    el.predTriggerFeedback.textContent = data.message || "Mô hình từ chối suy luận: Dữ liệu nến chưa đủ điều kiện.";
                    el.predTriggerFeedback.className = "feedback-msg sell-text";
                }
            } catch (e) {
                el.predTriggerFeedback.textContent = "Lỗi kết nối máy chủ khi kích hoạt chu kỳ: " + e.message;
                el.predTriggerFeedback.className = "feedback-msg sell-text";
            } finally {
                setTimeout(() => {
                    if (el.btnTriggerPred) el.btnTriggerPred.disabled = false;
                    if (el.predTriggerFeedback) el.predTriggerFeedback.style.display = "none";
                }, 4000);
            }
        }
    };

    // =========================================================================
    // 11. MAIN TERMINAL CONTROLLER
    // =========================================================================
    const TerminalController = {
        async init() {
            TVChartManager.init();
            this.bindEvents();

            // Initial load of all sections
            await this.refreshAll();

            // Start polling schedules
            this.startPolling();
        },

        bindEvents() {
            // Timeframe buttons
            el.tfButtons.forEach(btn => {
                btn.addEventListener("click", () => {
                    el.tfButtons.forEach(b => b.classList.remove("active"));
                    btn.classList.add("active");
                    state.timeframe = btn.dataset.tf;
                    TVChartManager.hasFitted = false;
                    this.fetchCandles();
                });
            });

            // VWAP toggle
            if (el.toggleVwap) {
                el.toggleVwap.addEventListener("change", (e) => {
                    state.showVwap = e.target.checked;
                    TVChartManager.updateData(TVChartManager.rawCandles);
                });
            }

            // Top refresh button
            if (el.btnRefresh) {
                el.btnRefresh.addEventListener("click", () => {
                    this.refreshAll();
                });
            }

            // Large trades filters
            if (el.tradesTimeRange) {
                el.tradesTimeRange.addEventListener("change", (e) => {
                    state.tradesTimeRange = e.target.value;
                    state.tradesPage = 1;
                    LargeTradesView.fetchAndRender();
                });
            }

            if (el.tradesMinValue) {
                el.tradesMinValue.addEventListener("change", (e) => {
                    state.tradesMinValue = parseFloat(e.target.value);
                    state.tradesPage = 1;
                    LargeTradesView.fetchAndRender();
                });
            }

            // Large trades CSV export
            if (el.btnExportTrades) {
                el.btnExportTrades.addEventListener("click", () => {
                    const exportUrl = `/api/trades/large/export?time_range=${state.tradesTimeRange}&min_value=${state.tradesMinValue}`;
                    window.open(exportUrl, "_blank");
                });
            }

            // Large trades pagination
            if (el.btnTradesPrev) {
                el.btnTradesPrev.addEventListener("click", () => {
                    if (state.tradesPage > 1) {
                        state.tradesPage--;
                        LargeTradesView.fetchAndRender();
                    }
                });
            }

            if (el.btnTradesNext) {
                el.btnTradesNext.addEventListener("click", () => {
                    if (state.tradesPage < state.tradesTotalPages) {
                        state.tradesPage++;
                        LargeTradesView.fetchAndRender();
                    }
                });
            }

            // Prediction history pagination
            if (el.btnPredPrev) {
                el.btnPredPrev.addEventListener("click", () => {
                    if (state.predPage > 1) {
                        state.predPage--;
                        PredictionHistoryView.fetchAndRender();
                    }
                });
            }

            if (el.btnPredNext) {
                el.btnPredNext.addEventListener("click", () => {
                    if (state.predPage < state.predTotalPages) {
                        state.predPage++;
                        PredictionHistoryView.fetchAndRender();
                    }
                });
            }

            // Manual prediction trigger
            if (el.btnTriggerPred) {
                el.btnTriggerPred.addEventListener("click", () => {
                    ModelMetadataView.triggerPrediction();
                });
            }

            // Pause polling when browser tab is inactive / hidden
            document.addEventListener("visibilitychange", () => {
                if (document.hidden) {
                    this.stopPolling();
                } else {
                    this.startPolling();
                    this.refreshAll();
                }
            });
        },

        async fetchOverview() {
            try {
                const data = await ApiClient.fetchJson("/api/overview", "overview");
                if (!data) return;

                const curPrice = data.current_price !== undefined ? data.current_price : data.price;
                if (curPrice) {
                    el.ovPrice.textContent = NumUtils.formatUSD(curPrice);
                }

                const chg = data.change_24h !== undefined ? data.change_24h : data.change_24h_pct;
                if (chg !== null && chg !== undefined) {
                    const prefix = chg > 0 ? "+" : "";
                    el.ovChange.textContent = `${prefix}${chg.toFixed(2)}%`;
                    el.ovChange.className = chg >= 0 ? "change-tag change-up tabular" : "change-tag change-down tabular";
                } else {
                    el.ovChange.textContent = "—";
                }

                if (data.data_timestamp) {
                    el.provenanceFetchTs.textContent = TimeUtils.formatTimeOnlyVN(data.data_timestamp);
                }

                this.hideConnectionAlert();
            } catch (e) {
                console.error("[Overview] Error:", e);
                this.showConnectionAlert("Mất kết nối API giá tổng quan.");
            }
        },

        async fetchCandles() {
            try {
                const data = await ApiClient.fetchJson(`/api/candles?timeframe=${state.timeframe}&limit=150`, "candles");
                if (!data || !data.candles) return;

                TVChartManager.updateData(data.candles);
                VolumeChartView.render(data.candles);

                if (el.volumeChartTf) {
                    el.volumeChartTf.textContent = `Nến ${state.timeframe} (BTC)`;
                }
            } catch (e) {
                console.error("[Candles] Error:", e);
            }
        },

        async fetchOrderFlow() {
            try {
                const data = await ApiClient.fetchJson("/api/orderflow/realtime", "orderflow");
                if (!data) return;
                OrderFlowView.render(data);
            } catch (e) {
                console.error("[OrderFlow] Error:", e);
            }
        },

        async fetchCoreMarketAndPlan() {
            try {
                const [predData, planData] = await Promise.all([
                    ApiClient.fetchJson("/api/ml/prediction", "ml_pred"),
                    ApiClient.fetchJson("/api/futures/plan", "futures_plan")
                ]);

                if (predData && predData.prediction) {
                    PredictionPlanView.renderPrediction(predData.prediction);
                } else {
                    PredictionPlanView.renderEmptyPrediction();
                }

                if (planData) {
                    PredictionPlanView.renderFuturesPlan(planData);
                }
            } catch (e) {
                console.error("[Plan] Error:", e);
            }
        },

        async refreshAll() {
            if (el.btnRefreshText) el.btnRefreshText.textContent = "Đang tải...";
            try {
                await Promise.all([
                    this.fetchOverview(),
                    this.fetchCandles(),
                    this.fetchOrderFlow(),
                    this.fetchCoreMarketAndPlan(),
                    LargeTradesView.fetchAndRender(),
                    PredictionHistoryView.fetchAndRender(),
                    ModelMetadataView.fetchAndRender()
                ]);
            } finally {
                if (el.btnRefreshText) el.btnRefreshText.textContent = "Làm mới";
            }
        },

        startPolling() {
            this.stopPolling();
            state.isPollingActive = true;

            // 1. Order Flow: 3s
            state.orderflowTimer = setInterval(() => {
                if (!state.isPollingActive) return;
                this.fetchOrderFlow();
            }, 3000);

            // 2. Core Market, Candles, Prediction & Futures Plan: 5s
            state.pollingTimer = setInterval(() => {
                if (!state.isPollingActive) return;
                this.fetchOverview();
                this.fetchCandles();
                this.fetchCoreMarketAndPlan();
            }, 5000);
        },

        stopPolling() {
            state.isPollingActive = false;
            if (state.pollingTimer) {
                clearInterval(state.pollingTimer);
                state.pollingTimer = null;
            }
            if (state.orderflowTimer) {
                clearInterval(state.orderflowTimer);
                state.orderflowTimer = null;
            }
        },

        showConnectionAlert(msg) {
            if (el.connectionAlert) {
                el.connectionAlertText.textContent = msg;
                el.connectionAlert.style.display = "flex";
            }
        },

        hideConnectionAlert() {
            if (el.connectionAlert) {
                el.connectionAlert.style.display = "none";
            }
        }
    };

    // Auto-bootstrap when DOM is ready
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", () => TerminalController.init());
    } else {
        TerminalController.init();
    }

})();
