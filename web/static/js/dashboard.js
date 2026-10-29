/**
 * =============================================================================
 * Crypto Big Data - Quant Analytics & Trading Platform (Frontend Controller)
 * =============================================================================
 */

document.addEventListener("DOMContentLoaded", () => {
    // -------------------------------------------------------------------------
    // 1. APPLICATION STATE
    // -------------------------------------------------------------------------
    // 1. APPLICATION STATE
    // -------------------------------------------------------------------------
    const state = {
        activeTab: "tab-candlestick",
        symbol: "BTC-USDT",
        timeframe: "1m",
        candleSource: "live", // 'live' or 'dw'
        autoRefresh: true,
        refreshIntervalMs: 3000,
        timerId: null,
        
        // TradingView chart objects
        chart: null,
        candleSeries: null,
        volumeSeries: null,
        vwapSeries: null,
        vwapUpperSeries: null,
        vwapLowerSeries: null,
        lastCandleTime: null,
        lastCandleCount: 0,
        
        // Whale radar filter
        whaleTier: "all",
        whaleSymbol: "",
        lastWhaleId: null,

        // Data Warehouse Explorer state
        dwTable: "vw_whale_trades",
        dwSymbol: "ALL",
        dwLimit: 50,
        dwOffset: 0,
        dwTotalCount: 0,

        // Chart.js instances - kept persistent, updated in-place
        analyticsCharts: {
            whale: null,
            turnover: null,
            orderflow: null
        }
    };

    // -------------------------------------------------------------------------
    // 2. DOM CACHED ELEMENTS
    // -------------------------------------------------------------------------
    const el = {
        clock: document.getElementById("current-clock"),
        dwStatusDot: document.getElementById("dw-status-dot"),
        dwStatusText: document.getElementById("dw-status"),
        streamStatus: document.getElementById("stream-status"),
        autoRefreshToggle: document.getElementById("auto-refresh-toggle"),
        btnManualRefresh: document.getElementById("btn-manual-refresh"),
        
        // Tabs
        tabBtns: document.querySelectorAll(".nav-tab-btn"),
        tabPanes: document.querySelectorAll(".tab-pane"),

        // Ticker strip
        tickerCards: document.querySelectorAll(".ticker-card"),

        // Tab 1: Candlestick & VWAP
        activeSymName: document.getElementById("active-symbol-name"),
        timeframeBtns: document.querySelectorAll(".tf-btn"),
        srcBtnLive: document.getElementById("src-btn-live"),
        srcBtnDw: document.getElementById("src-btn-dw"),
        chartContainer: document.getElementById("tv-chart-container"),
        chartLoader: document.getElementById("chart-loader"),
        chartLoaderText: document.getElementById("chart-loader-text"),
        statOpen: document.getElementById("stat-open"),
        statHigh: document.getElementById("stat-high"),
        statLow: document.getElementById("stat-low"),
        statClose: document.getElementById("stat-close"),
        statVwap: document.getElementById("stat-vwap"),
        statVwapUpper: document.getElementById("stat-vwap-upper"),
        statVwapLower: document.getElementById("stat-vwap-lower"),
        statParkinsonVol: document.getElementById("stat-parkinson-vol"),
        statCvd: document.getElementById("stat-cvd"),
        statVol: document.getElementById("stat-vol"),
        statVwapDiff: document.getElementById("stat-vwap-diff"),
        vwapDeviationPill: document.getElementById("vwap-deviation-pill"),
        recentCandlesTbody: document.getElementById("recent-candles-tbody"),

        // Tab 2: Order Flow
        ofCardBuyVol: document.getElementById("of-card-buy-vol"),
        ofCardBuyVal: document.getElementById("of-card-buy-val"),
        ofCardBuyPct: document.getElementById("of-card-buy-pct"),
        ofCardSellVol: document.getElementById("of-card-sell-vol"),
        ofCardSellVal: document.getElementById("of-card-sell-val"),
        ofCardSellPct: document.getElementById("of-card-sell-pct"),
        ofCardNetDelta: document.getElementById("of-card-net-delta"),
        ofCardNetVol: document.getElementById("of-card-net-vol"),
        ofCardImbalance: document.getElementById("of-card-imbalance"),
        ofCardSentiment: document.getElementById("of-card-sentiment"),
        sentimentBadge: document.getElementById("sentiment-badge"),
        buyRatioText: document.getElementById("buy-ratio-text"),
        sellRatioText: document.getElementById("sell-ratio-text"),
        imbalanceVal: document.getElementById("imbalance-val"),
        gaugeFillBuy: document.getElementById("gauge-fill-buy"),
        gaugeFillSell: document.getElementById("gauge-fill-sell"),
        ofBuyVol: document.getElementById("of-buy-vol"),
        ofSellVol: document.getElementById("of-sell-vol"),
        ofBuyVal: document.getElementById("of-buy-val"),
        ofSellVal: document.getElementById("of-sell-val"),
        ofSummaryTbody: document.getElementById("of-summary-tbody"),
        ofAbsorptionBadge: document.getElementById("of-absorption-badge"),
        ofAbsorptionDesc: document.getElementById("of-absorption-desc"),
        ofShockBadge: document.getElementById("of-shock-badge"),
        ofShockVal: document.getElementById("of-shock-val"),
        ofCvdPill: document.getElementById("of-cvd-pill"),

        // Tab 3: Whales
        whaleKpiTurnover: document.getElementById("whale-kpi-turnover"),
        whaleKpiMaxTrade: document.getElementById("whale-kpi-max-trade"),
        whaleKpiMaxSym: document.getElementById("whale-kpi-max-sym"),
        whaleKpiTotalCount: document.getElementById("whale-kpi-total-count"),
        whaleKpiRatio: document.getElementById("whale-kpi-ratio"),
        whaleKpiSentiment: document.getElementById("whale-kpi-sentiment"),
        whaleTableBody: document.getElementById("whale-table-body"),
        whaleCount: document.getElementById("whale-count"),
        tierFilterBtns: document.querySelectorAll(".tier-filter-btn"),
        whaleSymbolFilter: document.getElementById("whale-symbol-filter"),
        whaleTelegramPreview: document.getElementById("whale-telegram-preview"),
        btnCopyTelegramAlert: document.getElementById("btn-copy-telegram-alert"),
        whaleAudioToggle: document.getElementById("whale-audio-toggle"),

        // Tab 4: Scalping & Derivatives
        quantPredictionsTbody: document.getElementById("quant-predictions-tbody"),
        derivFunding: document.getElementById("deriv-funding"),
        derivFundingState: document.getElementById("deriv-funding-state"),
        derivApy: document.getElementById("deriv-apy"),
        derivOiUsd: document.getElementById("deriv-oi-usd"),
        derivOiCoins: document.getElementById("deriv-oi-coins"),
        btnTestWebhook: document.getElementById("btn-test-webhook"),
        webhookJsonPreview: document.getElementById("webhook-json-preview"),
        webhookSymBadge: document.getElementById("webhook-sym-badge"),

        // Tab 5: Data Warehouse Explorer
        dwKpiSize: document.getElementById("dw-kpi-size"),
        dwKpiTrades: document.getElementById("dw-kpi-trades"),
        dwKpiCandles: document.getElementById("dw-kpi-candles"),
        dwKpiWhales: document.getElementById("dw-kpi-whales"),
        dwViewBtns: document.querySelectorAll(".dw-view-btn"),
        dwFilterSymbol: document.getElementById("dw-filter-symbol"),
        dwLimitSelect: document.getElementById("dw-limit-select"),
        btnExportCsv: document.getElementById("btn-export-csv"),
        dwTableHead: document.getElementById("dw-table-head"),
        dwTableBody: document.getElementById("dw-table-body"),
        dwPaginationInfo: document.getElementById("dw-pagination-info"),
        dwPageIndicator: document.getElementById("dw-page-indicator"),
        btnDwPrev: document.getElementById("btn-dw-prev"),
        btnDwNext: document.getElementById("btn-dw-next"),

        // Tab 6: Telemetry
        dwTelemetrySize: document.getElementById("dw-telemetry-size")
    };

    // -------------------------------------------------------------------------
    // 3. TAB NAVIGATION
    // -------------------------------------------------------------------------
    function initTabs() {
        el.tabBtns.forEach(btn => {
            btn.addEventListener("click", () => {
                const targetTab = btn.getAttribute("data-tab");
                if (targetTab && targetTab !== state.activeTab) {
                    state.activeTab = targetTab;
                    
                    // Update tab buttons
                    el.tabBtns.forEach(b => b.classList.remove("active"));
                    btn.classList.add("active");

                    // Update panes
                    el.tabPanes.forEach(p => p.classList.remove("active"));
                    const activePane = document.getElementById(targetTab);
                    if (activePane) activePane.classList.add("active");

                    // Handle lazy initialization per tab
                    if (targetTab === "tab-candlestick") {
                        if (state.chart) {
                            setTimeout(() => {
                                state.chart.applyOptions({
                                    width: el.chartContainer.clientWidth,
                                    height: el.chartContainer.clientHeight
                                });
                            }, 50);
                        }
                        fetchCandles(false);
                    } else if (targetTab === "tab-orderflow") {
                        fetchOrderFlow();
                        fetchOrderFlowCharts();
                    } else if (targetTab === "tab-whales") {
                        fetchWhales();
                        fetchWhaleStats();
                    } else if (targetTab === "tab-signals") {
                        fetchAllScalpingSignals();
                        fetchDerivatives();
                        fetchMlPrediction();
                        fetchMlEvaluation();
                        fetchMlHistory();
                    } else if (targetTab === "tab-warehouse") {
                        fetchDwStats();
                        fetchDwQuery(false);
                    } else if (targetTab === "tab-pipeline") {
                        fetchPipelineTelemetry();
                    }
                }
            });
        });
    }

    // -------------------------------------------------------------------------
    // 3.5 THEME MANAGEMENT (LIGHT / WHITE & DARK)
    // -------------------------------------------------------------------------
    function getThemeColors() {
        const isDark = document.documentElement.getAttribute("data-theme") === "dark";
        return {
            isDark: isDark,
            chartBg: isDark ? "#080b11" : "#ffffff",
            textColor: isDark ? "#94a3b8" : "#475569",
            gridColor: isDark ? "rgba(255, 255, 255, 0.04)" : "rgba(0, 0, 0, 0.05)",
            borderColor: isDark ? "rgba(255, 255, 255, 0.08)" : "rgba(0, 0, 0, 0.08)",
            crosshairColor: isDark ? "rgba(0, 210, 255, 0.4)" : "rgba(2, 132, 199, 0.5)",
            upColor: isDark ? "#00f090" : "#10b981",
            downColor: isDark ? "#ff3b69" : "#ef4444",
            volUpColor: isDark ? "rgba(0, 240, 144, 0.45)" : "rgba(16, 185, 129, 0.5)",
            volDownColor: isDark ? "rgba(255, 59, 105, 0.45)" : "rgba(239, 68, 68, 0.5)",
            vwapColor: isDark ? "#00d2ff" : "#0284c7",
            vwapUpperColor: isDark ? "rgba(0, 210, 255, 0.45)" : "rgba(2, 132, 199, 0.65)",
            vwapLowerColor: isDark ? "rgba(168, 85, 247, 0.45)" : "rgba(124, 58, 237, 0.65)"
        };
    }

    function applyChartTheme(theme) {
        const tc = getThemeColors();
        if (state.chart) {
            state.chart.applyOptions({
                layout: {
                    background: { color: tc.chartBg },
                    textColor: tc.textColor
                },
                grid: {
                    vertLines: { color: tc.gridColor },
                    horzLines: { color: tc.gridColor }
                },
                crosshair: {
                    vertLine: { color: tc.crosshairColor },
                    horzLine: { color: tc.crosshairColor }
                },
                rightPriceScale: { borderColor: tc.borderColor },
                timeScale: { borderColor: tc.borderColor }
            });
            if (state.candleSeries) {
                state.candleSeries.applyOptions({
                    upColor: tc.upColor,
                    downColor: tc.downColor,
                    borderUpColor: tc.upColor,
                    borderDownColor: tc.downColor,
                    wickUpColor: tc.upColor,
                    wickDownColor: tc.downColor
                });
            }
            if (state.vwapSeries) state.vwapSeries.applyOptions({ color: tc.vwapColor });
            if (state.vwapUpperSeries) state.vwapUpperSeries.applyOptions({ color: tc.vwapUpperColor });
            if (state.vwapLowerSeries) state.vwapLowerSeries.applyOptions({ color: tc.vwapLowerColor });
        }

        // Re-render current candles to update volume colors
        if (state.lastCandleTime) {
            fetchCandles(false);
        }

        // Update Chart.js instances if present
        if (state.analyticsCharts.whale) {
            state.analyticsCharts.whale.data.datasets[0].borderColor = tc.isDark ? "#080b11" : "#ffffff";
            if (state.analyticsCharts.whale.options?.plugins?.legend?.labels) {
                state.analyticsCharts.whale.options.plugins.legend.labels.color = tc.textColor;
            }
            state.analyticsCharts.whale.update('none');
        }
        if (state.analyticsCharts.orderflow) {
            const sx = state.analyticsCharts.orderflow.options?.scales?.x;
            const sy = state.analyticsCharts.orderflow.options?.scales?.y;
            if (sx) {
                sx.ticks.color = tc.textColor;
                sx.grid.color = tc.gridColor;
            }
            if (sy) {
                sy.ticks.color = tc.textColor;
            }
            if (state.analyticsCharts.orderflow.options?.plugins?.legend?.labels) {
                state.analyticsCharts.orderflow.options.plugins.legend.labels.color = tc.textColor;
            }
            state.analyticsCharts.orderflow.update('none');
        }
        if (state.analyticsCharts.turnover) {
            const sx = state.analyticsCharts.turnover.options?.scales?.x;
            const sy = state.analyticsCharts.turnover.options?.scales?.y;
            if (sx) sx.ticks.color = tc.textColor;
            if (sy) {
                sy.ticks.color = tc.textColor;
                sy.grid.color = tc.gridColor;
            }
            state.analyticsCharts.turnover.update('none');
        }
    }

    function initTheme() {
        const savedTheme = localStorage.getItem("crypto_app_theme") || "light";
        document.documentElement.setAttribute("data-theme", savedTheme);

        const btnToggle = document.getElementById("theme-toggle-btn");
        const iconSpan = document.getElementById("theme-icon");

        function updateBtn(theme) {
            if (!iconSpan) return;
            if (theme === "dark") {
                iconSpan.textContent = "☀️";
                if (btnToggle) btnToggle.title = "Chuyển sang giao diện Sáng (White Theme)";
            } else {
                iconSpan.textContent = "🌙";
                if (btnToggle) btnToggle.title = "Chuyển sang giao diện Tối (Dark Theme)";
            }
        }

        updateBtn(savedTheme);

        if (btnToggle) {
            btnToggle.addEventListener("click", () => {
                const curTheme = document.documentElement.getAttribute("data-theme") || "light";
                const newTheme = curTheme === "dark" ? "light" : "dark";
                document.documentElement.setAttribute("data-theme", newTheme);
                localStorage.setItem("crypto_app_theme", newTheme);
                updateBtn(newTheme);
                applyChartTheme(newTheme);
            });
        }
    }

    // -------------------------------------------------------------------------
    // 4. TRADINGVIEW LIGHTWEIGHT CHARTS SETUP
    // -------------------------------------------------------------------------
    function initChart() {
        if (!window.LightweightCharts) {
            console.error("LightweightCharts library not loaded.");
            return;
        }

        const width = el.chartContainer.clientWidth || 800;
        const height = el.chartContainer.clientHeight || 460;
        const tc = getThemeColors();

        state.chart = LightweightCharts.createChart(el.chartContainer, {
            width: width,
            height: height,
            layout: {
                background: { color: tc.chartBg },
                textColor: tc.textColor,
                fontSize: 11,
                fontFamily: "'JetBrains Mono', monospace"
            },
            grid: {
                vertLines: { color: tc.gridColor },
                horzLines: { color: tc.gridColor }
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    color: tc.crosshairColor,
                    width: 1,
                    style: LightweightCharts.LineStyle.Dashed
                },
                horzLine: {
                    color: tc.crosshairColor,
                    width: 1,
                    style: LightweightCharts.LineStyle.Dashed
                }
            },
            rightPriceScale: {
                borderColor: tc.borderColor,
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.25
                }
            },
            timeScale: {
                borderColor: tc.borderColor,
                timeVisible: true,
                secondsVisible: false
            }
        });

        // 1. Candlestick series
        state.candleSeries = state.chart.addCandlestickSeries({
            upColor: tc.upColor,
            downColor: tc.downColor,
            borderUpColor: tc.upColor,
            borderDownColor: tc.downColor,
            wickUpColor: tc.upColor,
            wickDownColor: tc.downColor
        });

        // 2. Volume series
        state.volumeSeries = state.chart.addHistogramSeries({
            priceFormat: { type: "volume" },
            priceScaleId: "",
            scaleMargins: {
                top: 0.82,
                bottom: 0
            }
        });

        // 3. VWAP line series
        state.vwapSeries = state.chart.addLineSeries({
            color: tc.vwapColor,
            lineWidth: 2,
            title: "VWAP",
            priceLineVisible: false
        });

        // 4. VWAP Upper Band (+2σ)
        state.vwapUpperSeries = state.chart.addLineSeries({
            color: tc.vwapUpperColor,
            lineWidth: 1,
            lineStyle: LightweightCharts.LineStyle.Dashed,
            title: "+2σ Band",
            priceLineVisible: false
        });

        // 5. VWAP Lower Band (-2σ)
        state.vwapLowerSeries = state.chart.addLineSeries({
            color: tc.vwapLowerColor,
            lineWidth: 1,
            lineStyle: LightweightCharts.LineStyle.Dashed,
            title: "-2σ Band",
            priceLineVisible: false
        });

        // Crosshair listener for quick stats
        state.chart.subscribeCrosshairMove(param => {
            if (!param || !param.time || !param.seriesData.get(state.candleSeries)) {
                return;
            }
            const cData = param.seriesData.get(state.candleSeries);
            const vData = param.seriesData.get(state.volumeSeries);
            const wData = param.seriesData.get(state.vwapSeries);
            const uData = state.vwapUpperSeries ? param.seriesData.get(state.vwapUpperSeries) : null;
            const lData = state.vwapLowerSeries ? param.seriesData.get(state.vwapLowerSeries) : null;

            if (cData) {
                updateQuickStats({
                    open: cData.open,
                    high: cData.high,
                    low: cData.low,
                    close: cData.close,
                    volume: vData ? vData.value : null,
                    vwap: wData ? wData.value : null,
                    upper_band_2: uData ? uData.value : undefined,
                    lower_band_2: lData ? lData.value : undefined
                });
            }
        });

        window.addEventListener("resize", () => {
            if (state.chart) {
                state.chart.applyOptions({
                    width: el.chartContainer.clientWidth,
                    height: el.chartContainer.clientHeight
                });
            }
        });
    }

    // -------------------------------------------------------------------------
    // 5. CORE API FETCHING FUNCTIONS (IN-PLACE, ZERO-JUMP)
    // -------------------------------------------------------------------------

    // A. Health check
    async function checkHealth() {
        try {
            const res = await fetch("/api/health");
            const data = await res.json();
            if (data.database_connected) {
                el.dwStatusDot.className = "status-dot online";
                el.dwStatusText.textContent = "CONNECTED (5433)";
                el.dwStatusText.style.color = "var(--accent-green)";
            } else {
                el.dwStatusDot.className = "status-dot offline";
                el.dwStatusText.textContent = "OFFLINE";
                el.dwStatusText.style.color = "var(--accent-red)";
            }

            if (data.table_counts) {
                if (el.dwKpiTrades && data.table_counts.fact_crypto_trades) {
                    el.dwKpiTrades.textContent = data.table_counts.fact_crypto_trades.toLocaleString();
                }
                if (el.dwKpiCandles && data.table_counts.fact_crypto_1m_candles) {
                    el.dwKpiCandles.textContent = data.table_counts.fact_crypto_1m_candles.toLocaleString();
                }
                if (el.dwKpiWhales && data.table_counts.vw_whale_trades) {
                    el.dwKpiWhales.textContent = data.table_counts.vw_whale_trades.toLocaleString();
                }
            }
            if (data.database_size && el.dwKpiSize) {
                el.dwKpiSize.textContent = data.database_size;
            }
        } catch (e) {
            el.dwStatusDot.className = "status-dot offline";
            el.dwStatusText.textContent = "OFFLINE";
        }
    }

    // B. Overview Tickers (BTC-USDT Chuyên biệt)
    async function fetchOverview() {
        try {
            const res = await fetch("/api/overview");
            const data = await res.json();

            const symMap = {
                "BTC-USDT": { pId: "btc-price", cId: "btc-change", vId: "btc-vol", wId: "btc-vwap" }
            };

            for (const [sym, info] of Object.entries(data)) {
                const map = symMap[sym];
                if (!map) continue;

                const priceEl = document.getElementById(map.pId);
                const changeEl = document.getElementById(map.cId);
                const volEl = document.getElementById(map.vId);
                const vwapEl = document.getElementById(map.wId);

                if (priceEl) priceEl.textContent = formatCurrency(info.price);
                if (changeEl) {
                    const isPos = info.change_24h >= 0;
                    changeEl.textContent = `${isPos ? "+" : ""}${info.change_24h}%`;
                    changeEl.className = `price-change ${isPos ? "positive" : "negative"}`;
                }
                if (volEl) volEl.textContent = `$${(info.volume_usd_24h / 1e6).toFixed(1)}M`;
                if (vwapEl) vwapEl.textContent = formatCurrency(info.vwap);
            }
        } catch (e) {
            console.error("Error in fetchOverview:", e);
        }
    }

    // C. Fetch Candlesticks & VWAP (Smooth update without viewport snapping)
    async function fetchCandles(showLoader = false) {
        if (showLoader && el.chartLoader) {
            el.chartLoaderText.textContent = state.candleSource === "dw" 
                ? "Đang truy vấn PostgreSQL Data Warehouse..." 
                : "Đang tải nến thời gian thực sàn OKX...";
            el.chartLoader.classList.remove("hidden");
        }

        try {
            const url = `/api/candles?symbol=${state.symbol}&timeframe=${state.timeframe}&source=${state.candleSource}&limit=120`;
            const res = await fetch(url);
            const data = await res.json();

            if (data.candles && data.candles.length > 0) {
                const candleData = [];
                const volumeData = [];
                const vwapData = [];
                const vwapUpperData = [];
                const vwapLowerData = [];
                const tc = getThemeColors();

                data.candles.forEach(c => {
                    candleData.push({
                        time: c.time,
                        open: c.open,
                        high: c.high,
                        low: c.low,
                        close: c.close
                    });

                    volumeData.push({
                        time: c.time,
                        value: c.volume,
                        color: c.close >= c.open ? tc.volUpColor : tc.volDownColor
                    });

                    vwapData.push({
                        time: c.time,
                        value: c.vwap
                    });

                    vwapUpperData.push({
                        time: c.time,
                        value: c.upper_band_2 || c.vwap
                    });

                    vwapLowerData.push({
                        time: c.time,
                        value: c.lower_band_2 || c.vwap
                    });
                });

                const lastCandle = data.candles[data.candles.length - 1];
                const lastCandlePoint = candleData[candleData.length - 1];
                const lastVol = volumeData[volumeData.length - 1];
                const lastVwap = vwapData[vwapData.length - 1];
                const lastVwapUpper = vwapUpperData[vwapUpperData.length - 1];
                const lastVwapLower = vwapLowerData[vwapLowerData.length - 1];

                // If first render or explicit filter switch, replace full dataset
                if (showLoader || !state.lastCandleTime || state.lastCandleCount !== candleData.length) {
                    state.candleSeries.setData(candleData);
                    state.volumeSeries.setData(volumeData);
                    state.vwapSeries.setData(vwapData);
                    if (state.vwapUpperSeries) state.vwapUpperSeries.setData(vwapUpperData);
                    if (state.vwapLowerSeries) state.vwapLowerSeries.setData(vwapLowerData);
                    state.lastCandleTime = lastCandlePoint.time;
                    state.lastCandleCount = candleData.length;
                } else {
                    // Micro-update only the current ticking candle to preserve scroll/zoom state
                    state.candleSeries.update(lastCandlePoint);
                    state.volumeSeries.update(lastVol);
                    state.vwapSeries.update(lastVwap);
                    if (state.vwapUpperSeries) state.vwapUpperSeries.update(lastVwapUpper);
                    if (state.vwapLowerSeries) state.vwapLowerSeries.update(lastVwapLower);
                }

                // Update OHLC ribbon with latest rich candle metrics
                updateQuickStats(lastCandle);

                // Update VWAP spread / deviation
                if (lastCandle.close && lastVwap && lastVwap.value) {
                    const diffPct = ((lastCandle.close - lastVwap.value) / lastVwap.value) * 100;
                    if (el.statVwapDiff) {
                        el.statVwapDiff.textContent = `${diffPct >= 0 ? "+" : ""}${diffPct.toFixed(2)}%`;
                        el.statVwapDiff.className = `stat-val ${diffPct >= 0 ? "high" : "low"}`;
                    }
                    if (el.vwapDeviationPill) {
                        if (diffPct > 0.3) {
                            el.vwapDeviationPill.textContent = "Vùng Cao (Premium / Overbought)";
                            el.vwapDeviationPill.className = "vwap-status-pill premium";
                        } else if (diffPct < -0.3) {
                            el.vwapDeviationPill.textContent = "Vùng Thấp (Discount / Oversold)";
                            el.vwapDeviationPill.className = "vwap-status-pill discount";
                        } else {
                            el.vwapDeviationPill.textContent = "Vùng Cân Bằng (Fair Value)";
                            el.vwapDeviationPill.className = "vwap-status-pill";
                        }
                    }
                }

                // Render Recent 10 Candles Table (11 Columns)
                if (el.recentCandlesTbody && data.candles) {
                    const recent10 = data.candles.slice(-10).reverse();
                    let rowsHtml = "";
                    recent10.forEach(c => {
                        const dateStr = new Date(c.time * 1000).toISOString().replace("T", " ").slice(11, 19);
                        const isUp = c.close >= c.open;
                        const closeColor = isUp ? "var(--accent-green)" : "var(--accent-red)";
                        const imbColor = (c.imbalance || 0) >= 0 ? "var(--accent-green)" : "var(--accent-red)";
                        const cvdVal = Number(c.cvd || 0);
                        const cvdColor = cvdVal >= 0 ? "var(--accent-green)" : "var(--accent-red)";
                        const lowerBandStr = c.lower_band_2 ? formatCurrency(c.lower_band_2) : "---";
                        const upperBandStr = c.upper_band_2 ? formatCurrency(c.upper_band_2) : "---";

                        rowsHtml += `
                            <tr>
                                <td><span style="font-family:var(--font-mono); color:var(--text-muted);">${dateStr}</span></td>
                                <td>${formatCurrency(c.open)}</td>
                                <td style="color:var(--accent-green);">${formatCurrency(c.high)}</td>
                                <td style="color:var(--accent-red);">${formatCurrency(c.low)}</td>
                                <td style="color:${closeColor}; font-weight:700;">${formatCurrency(c.close)}</td>
                                <td>${Number(c.volume || 0).toFixed(2)}</td>
                                <td style="color:var(--accent-cyan); font-weight:600;">${formatCurrency(c.vwap)}</td>
                                <td style="font-size:11px; color:var(--text-muted);">${lowerBandStr} / ${upperBandStr}</td>
                                <td style="color:${cvdColor}; font-weight:600;">${cvdVal >= 0 ? "+" : ""}${cvdVal.toFixed(2)}</td>
                                <td style="color:${imbColor}; font-weight:600;">${(c.imbalance !== undefined && c.imbalance !== null) ? (c.imbalance >= 0 ? "+" : "") + Number(c.imbalance).toFixed(2) : "0.00"}</td>
                                <td>$${Math.round(c.trade_value || 0).toLocaleString()}</td>
                            </tr>
                        `;
                    });
                    el.recentCandlesTbody.innerHTML = rowsHtml;
                }
            }
        } catch (e) {
            console.error("Error in fetchCandles:", e);
        } finally {
            if (showLoader && el.chartLoader) {
                el.chartLoader.classList.add("hidden");
            }
        }
    }

    // D. Fetch Order Flow & Imbalance (Real-time dynamic gauge & KPI cards)
    async function fetchOrderFlow() {
        try {
            const src = state.candleSource || "live";
            const res = await fetch(`/api/orderflow?symbol=${state.symbol}&source=${src}`);
            if (!res.ok) return;
            const data = await res.json();

            if (el.buyRatioText) el.buyRatioText.textContent = `${data.buy_ratio_pct}%`;
            if (el.sellRatioText) el.sellRatioText.textContent = `${data.sell_ratio_pct}%`;
            if (el.imbalanceVal) {
                const sign = data.imbalance > 0 ? "+" : "";
                el.imbalanceVal.textContent = `${sign}${Number(data.imbalance || 0).toFixed(2)}`;
                el.imbalanceVal.style.color = data.imbalance >= 0.1 ? "var(--accent-green)" : (data.imbalance <= -0.1 ? "var(--accent-red)" : "var(--accent-cyan)");
            }

            if (el.gaugeFillBuy) el.gaugeFillBuy.style.width = `${data.buy_ratio_pct}%`;
            if (el.gaugeFillSell) el.gaugeFillSell.style.width = `${data.sell_ratio_pct}%`;

            const buyVolFormatted = data.buy_volume < 1 ? Number(data.buy_volume || 0).toFixed(4) : Number(data.buy_volume || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
            const sellVolFormatted = data.sell_volume < 1 ? Number(data.sell_volume || 0).toFixed(4) : Number(data.sell_volume || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});

            if (el.ofBuyVol) el.ofBuyVol.textContent = buyVolFormatted;
            if (el.ofSellVol) el.ofSellVol.textContent = sellVolFormatted;
            if (el.ofBuyVal) el.ofBuyVal.textContent = `$${Math.round(data.buy_value_usd || 0).toLocaleString()}`;
            if (el.ofSellVal) el.ofSellVal.textContent = `$${Math.round(data.sell_value_usd || 0).toLocaleString()}`;

            // Tab 2 Top KPI Cards
            if (el.ofCardBuyVol) el.ofCardBuyVol.textContent = buyVolFormatted;
            if (el.ofCardBuyVal) el.ofCardBuyVal.textContent = `$${Math.round(data.buy_value_usd || 0).toLocaleString()} USD`;
            if (el.ofCardBuyPct) el.ofCardBuyPct.textContent = `${data.buy_ratio_pct}%`;

            if (el.ofCardSellVol) el.ofCardSellVol.textContent = sellVolFormatted;
            if (el.ofCardSellVal) el.ofCardSellVal.textContent = `$${Math.round(data.sell_value_usd || 0).toLocaleString()} USD`;
            if (el.ofCardSellPct) el.ofCardSellPct.textContent = `${data.sell_ratio_pct}%`;

            const netDeltaUsd = (data.net_delta_usd !== undefined) ? data.net_delta_usd : ((data.buy_value_usd || 0) - (data.sell_value_usd || 0));
            const netDeltaVol = (data.net_delta_coin !== undefined) ? data.net_delta_coin : ((data.buy_volume || 0) - (data.sell_volume || 0));
            if (el.ofCardNetDelta) {
                const sign = netDeltaUsd >= 0 ? "+" : "";
                el.ofCardNetDelta.textContent = `${sign}$${Math.round(Math.abs(netDeltaUsd)).toLocaleString()}`;
                el.ofCardNetDelta.style.color = netDeltaUsd >= 0 ? "var(--accent-green)" : "var(--accent-red)";
            }
            if (el.ofCardNetVol) {
                const sign = netDeltaVol >= 0 ? "+" : "";
                el.ofCardNetVol.textContent = `${sign}${netDeltaVol.toFixed(4)} Coin Delta`;
            }

            if (el.ofCardImbalance) {
                const sign = data.imbalance >= 0 ? "+" : "";
                el.ofCardImbalance.textContent = `${sign}${Number(data.imbalance || 0).toFixed(4)}`;
                el.ofCardImbalance.style.color = data.imbalance >= 0.1 ? "var(--accent-green)" : (data.imbalance <= -0.1 ? "var(--accent-red)" : "var(--accent-cyan)");
            }
            if (el.ofCardSentiment) {
                el.ofCardSentiment.textContent = data.sentiment;
            }

            // Sentiment badge style
            if (el.sentimentBadge) {
                el.sentimentBadge.textContent = data.sentiment;
                if (data.imbalance >= 0.2) {
                    el.sentimentBadge.className = "sentiment-badge bullish";
                } else if (data.imbalance <= -0.2) {
                    el.sentimentBadge.className = "sentiment-badge bearish";
                } else {
                    el.sentimentBadge.className = "sentiment-badge";
                }
            }

            // Streaming Anomaly & Absorption Detector Strip (Module 2)
            if (el.ofAbsorptionBadge && el.ofAbsorptionDesc) {
                if (data.absorption_detected === "BULLISH_ABSORPTION") {
                    el.ofAbsorptionBadge.className = "anomaly-badge danger";
                    el.ofAbsorptionBadge.textContent = "BULL ABSORPTION";
                    el.ofAbsorptionDesc.textContent = "Tường mua ẩn hấp thụ lực bán xả (Iceberg Buyer)";
                } else if (data.absorption_detected === "BEARISH_ABSORPTION") {
                    el.ofAbsorptionBadge.className = "anomaly-badge danger";
                    el.ofAbsorptionBadge.textContent = "BEAR ABSORPTION";
                    el.ofAbsorptionDesc.textContent = "Tường bán ẩn hấp thụ lực mua gom (Iceberg Seller)";
                } else {
                    el.ofAbsorptionBadge.className = "anomaly-badge normal";
                    el.ofAbsorptionBadge.textContent = "NORMAL";
                    el.ofAbsorptionDesc.textContent = "Cung cầu khớp lệnh tự nhiên";
                }
            }

            if (el.ofShockBadge && el.ofShockVal) {
                const zscore = Number(data.volume_shock_zscore || 0);
                el.ofShockVal.textContent = `Z: ${zscore >= 0 ? '+' : ''}${zscore.toFixed(2)}`;
                if (zscore >= 3.0) {
                    el.ofShockBadge.className = "anomaly-badge danger";
                    el.ofShockBadge.textContent = "EXTREME SHOCK";
                } else if (zscore >= 2.0) {
                    el.ofShockBadge.className = "anomaly-badge warning";
                    el.ofShockBadge.textContent = "VOLUME SPIKE";
                } else {
                    el.ofShockBadge.className = "anomaly-badge normal";
                    el.ofShockBadge.textContent = "NORMAL";
                }
            }

            if (el.ofCvdPill) {
                const cvd = Number(data.cumulative_volume_delta || 0);
                el.ofCvdPill.textContent = `${cvd >= 0 ? '+' : ''}${cvd.toFixed(4)} Coin`;
                el.ofCvdPill.className = `cvd-pill ${cvd >= 0 ? "positive" : "negative"}`;
            }
        } catch (e) {
            console.error("Error in fetchOrderFlow:", e);
        }
    }

    // E. Fetch Derivatives
    async function fetchDerivatives() {
        try {
            const res = await fetch(`/api/derivatives?symbol=${state.symbol}`);
            const data = await res.json();
            const deriv = data[state.symbol];

            if (deriv) {
                if (el.derivFunding) el.derivFunding.textContent = `${deriv.funding_rate_pct.toFixed(4)}%`;
                if (el.derivFundingState) el.derivFundingState.textContent = deriv.funding_state;
                if (el.derivApy) el.derivApy.textContent = `${deriv.projected_apy_pct.toFixed(2)}%`;
                if (el.derivOiUsd) el.derivOiUsd.textContent = `$${(deriv.open_interest_usd / 1e6).toFixed(2)}M`;
                if (el.derivOiCoins) el.derivOiCoins.textContent = `${deriv.open_interest_coins.toLocaleString()} contracts`;
            }
        } catch (e) {
            console.error("Error in fetchDerivatives:", e);
        }
    }

    // F. Fetch Whale Radar (Live & Historical) - Atomic DOM replace
    async function fetchWhales() {
        try {
            const sym = state.whaleSymbol || state.symbol || "";
            const res = await fetch(`/api/whales?symbol=${sym}&tier=${state.whaleTier}&limit=30`);
            const data = await res.json();

            if (data.whales && data.whales.length > 0) {
                if (el.whaleCount) el.whaleCount.textContent = `${data.whales.length} Lệnh lớn`;
                
                let buyWhales = 0;
                let sellWhales = 0;
                let rowsHtml = "";

                data.whales.forEach(w => {
                    let tierClass = "tier-large";
                    if (w.tier.includes("Mega")) tierClass = "tier-mega";
                    else if (w.tier.includes("Big") || w.tier.includes("Whale (")) tierClass = "tier-whale";

                    const isBuy = w.side.toLowerCase() === "buy";
                    if (isBuy) buyWhales++;
                    else sellWhales++;

                    const sideClass = isBuy ? "side-buy" : "side-sell";
                    const sideLabel = isBuy ? "BUY" : "SELL";

                    rowsHtml += `
                        <tr>
                            <td><strong>${w.symbol}</strong> <span style="color:var(--text-muted); font-size:11px; margin-left:6px;">${w.time.slice(-8)}</span></td>
                            <td><span class="badge-tag ${sideClass}">${sideLabel}</span></td>
                            <td style="font-family:var(--font-mono); font-weight:600;">${formatCurrency(w.price)}</td>
                            <td style="font-family:var(--font-mono);">${Number(w.size).toFixed(3)}</td>
                            <td style="font-family:var(--font-mono); color: #ffffff; font-weight:700;">$${Math.round(w.trade_value).toLocaleString()}</td>
                            <td><span class="tier-tag ${tierClass}">${w.tier.split(' ')[0]}</span></td>
                            <td><span style="font-size:10px; color:var(--text-muted);">${w.source === 'postgres_dw' ? '🏛️ DW' : '⚡ Stream'}</span></td>
                        </tr>
                    `;
                });
                if (el.whaleTableBody) el.whaleTableBody.innerHTML = rowsHtml;

                if (el.whaleKpiRatio) el.whaleKpiRatio.textContent = `${buyWhales} Mua / ${sellWhales} Bán`;
                if (el.whaleKpiSentiment) {
                    if (buyWhales > sellWhales) {
                        el.whaleKpiSentiment.textContent = "Cá Mập Gom Hàng (Accumulation)";
                        el.whaleKpiSentiment.style.color = "var(--accent-green)";
                    } else if (sellWhales > buyWhales) {
                        el.whaleKpiSentiment.textContent = "Cá Mập Xả Hàng (Distribution)";
                        el.whaleKpiSentiment.style.color = "var(--accent-red)";
                    } else {
                        el.whaleKpiSentiment.textContent = "Cân Bằng Dòng Tiền";
                        el.whaleKpiSentiment.style.color = "var(--accent-cyan)";
                    }
                }

                // Audio chirp & new whale detection
                const topWhale = data.whales[0];
                const topWhaleKey = `${topWhale.symbol}_${topWhale.time}_${topWhale.trade_value}`;
                if (state.lastWhaleId && state.lastWhaleId !== topWhaleKey) {
                    if (topWhale.tier && topWhale.tier.includes("Mega")) {
                        if (el.whaleAudioToggle && el.whaleAudioToggle.checked) {
                            playWhaleChirp();
                        }
                    }
                }
                state.lastWhaleId = topWhaleKey;

                // Update Telegram / Discord Instant Alert Preview (Module 5.1)
                if (el.whaleTelegramPreview && data.telegram_alert) {
                    el.whaleTelegramPreview.textContent = data.telegram_alert;
                }
            } else {
                if (el.whaleTableBody) el.whaleTableBody.innerHTML = `<tr><td colspan="7" class="empty-state">Không có lệnh cá mập nào khớp bộ lọc</td></tr>`;
            }
        } catch (e) {
            console.error("Error in fetchWhales:", e);
        }
    }

    // Web Audio synthesizer for whale alerts (Zero external audio file dependency)
    function playWhaleChirp() {
        try {
            const AudioCtx = window.AudioContext || window.webkitAudioContext;
            if (!AudioCtx) return;
            const ctx = new AudioCtx();
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.type = "sine";
            osc.frequency.setValueAtTime(880, ctx.currentTime);
            osc.frequency.exponentialRampToValueAtTime(1760, ctx.currentTime + 0.15);
            gain.gain.setValueAtTime(0.25, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.2);
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.start();
            osc.stop(ctx.currentTime + 0.22);
        } catch (e) {
            console.warn("Whale audio alert failed:", e);
        }
    }

    // -------------------------------------------------------------------------
    // 6. TAB 2: DATA WAREHOUSE EXPLORER LOGIC
    // -------------------------------------------------------------------------

    // A. Fetch DW Stats & KPIs
    async function fetchDwStats() {
        try {
            const res = await fetch("/api/warehouse/stats");
            const data = await res.json();
            if (data.connected) {
                if (el.dwKpiSize) el.dwKpiSize.textContent = data.database_size;
                if (el.dwTelemetrySize) el.dwTelemetrySize.textContent = data.database_size;
                if (el.dwKpiWhales && data.whale_summary) {
                    el.dwKpiWhales.textContent = data.whale_summary.total_whale_trades.toLocaleString();
                }
                if (el.dwKpiTrades && data.trades_summary) {
                    el.dwKpiTrades.textContent = data.trades_summary.total_trades.toLocaleString();
                }
            }
        } catch (e) {
            console.error("Error in fetchDwStats:", e);
        }
    }

    // B. Fetch DW Dynamic Table Query (Zero Height Collapse on Refresh)
    async function fetchDwQuery(isBackground = false) {
        // Only display inline loading state if user initiated an explicit filter/table switch
        if (!isBackground) {
            el.dwTableBody.innerHTML = `<tr><td class="empty-state">Đang truy vấn bảng [${state.dwTable}]...</td></tr>`;
        }

        try {
            const url = `/api/warehouse/query?table=${state.dwTable}&symbol=${state.dwSymbol}&limit=${state.dwLimit}&offset=${state.dwOffset}`;
            const res = await fetch(url);
            const data = await res.json();

            if (data.error) {
                if (!isBackground) {
                    el.dwTableBody.innerHTML = `<tr><td class="empty-state" style="color:var(--accent-red);">${data.error}</td></tr>`;
                }
                return;
            }

            state.dwTotalCount = data.total_count || 0;

            // Build table header HTML
            let headHtml = "<tr>";
            data.columns.forEach(col => {
                headHtml += `<th>${col.replace(/_/g, " ").toUpperCase()}</th>`;
            });
            headHtml += "</tr>";
            if (el.dwTableHead.innerHTML !== headHtml) {
                el.dwTableHead.innerHTML = headHtml;
            }

            // Build table rows HTML in memory
            let rowsHtml = "";
            if (data.rows.length === 0) {
                rowsHtml = `<tr><td colspan="${data.columns.length}" class="empty-state">Không có bản ghi nào.</td></tr>`;
            } else {
                data.rows.forEach(row => {
                    let rowHtml = "<tr>";
                    row.forEach((cellVal, idx) => {
                        const colName = data.columns[idx];
                        let cellContent = "";

                        if (colName === "side") {
                            const isBuy = String(cellVal).toLowerCase() === "buy";
                            cellContent = `<span class="${isBuy ? 'side-buy' : 'side-sell'}">${String(cellVal).toUpperCase()}</span>`;
                        } else if (colName === "whale_tier") {
                            let tClass = "tier-large";
                            if (String(cellVal).includes("Mega")) tClass = "tier-mega";
                            else if (String(cellVal).includes("Whale")) tClass = "tier-whale";
                            cellContent = `<span class="tier-tag ${tClass}">${cellVal}</span>`;
                        } else if (typeof cellVal === "number") {
                            if (colName.includes("usd") || colName.includes("price") || colName.includes("turnover") || colName.includes("vwap")) {
                                cellContent = formatCurrency(cellVal);
                            } else if (colName.includes("funding_rate")) {
                                cellContent = (cellVal >= 0 ? "+" : "") + (cellVal * 100).toFixed(4) + "%";
                            } else if (colName.includes("pct") || colName.includes("ratio") || colName.includes("imbalance")) {
                                cellContent = cellVal.toFixed(2) + "%";
                            } else {
                                cellContent = cellVal.toLocaleString();
                            }
                        } else {
                            cellContent = cellVal !== null ? cellVal : "-";
                        }
                        rowHtml += `<td>${cellContent}</td>`;
                    });
                    rowHtml += "</tr>";
                    rowsHtml += rowHtml;
                });
            }

            // Single atomic DOM update prevents table collapse
            el.dwTableBody.innerHTML = rowsHtml;

            // Pagination info
            const startRecord = state.dwTotalCount === 0 ? 0 : state.dwOffset + 1;
            const endRecord = Math.min(state.dwOffset + data.rows.length, state.dwTotalCount);
            if (el.dwPaginationInfo) {
                el.dwPaginationInfo.textContent = `Hiển thị ${startRecord} - ${endRecord} / ${state.dwTotalCount.toLocaleString()} bản ghi`;
            }

            const currentPage = Math.floor(state.dwOffset / state.dwLimit) + 1;
            const totalPages = Math.ceil(state.dwTotalCount / state.dwLimit) || 1;
            if (el.dwPageIndicator) {
                el.dwPageIndicator.textContent = `Trang ${currentPage} / ${totalPages}`;
            }

            if (el.btnDwPrev) el.btnDwPrev.disabled = state.dwOffset <= 0;
            if (el.btnDwNext) el.btnDwNext.disabled = endRecord >= state.dwTotalCount;

        } catch (e) {
            if (!isBackground) {
                el.dwTableBody.innerHTML = `<tr><td class="empty-state" style="color:var(--accent-red);">Lỗi kết nối truy vấn Data Warehouse.</td></tr>`;
            }
        }
    }

    // -------------------------------------------------------------------------
    // 7. STREAMING ANALYSIS MODULES (TABS 2, 3, 4)
    // -------------------------------------------------------------------------

    // A. Tab 3: Whale Radar Stats & Tier Breakdown Donut Chart
    async function fetchWhaleStats() {
        try {
            const [statsRes, metricsRes] = await Promise.all([
                fetch("/api/warehouse/stats").then(r => r.json()).catch(() => null),
                fetch("/api/analytics/metrics").then(r => r.json()).catch(() => null)
            ]);

            if (statsRes && statsRes.whale_summary) {
                const ws = statsRes.whale_summary;
                if (el.whaleKpiTurnover) el.whaleKpiTurnover.textContent = `$${(ws.total_turnover_usd / 1e6).toFixed(1)}M`;
                if (el.whaleKpiMaxTrade) el.whaleKpiMaxTrade.textContent = `$${(ws.max_single_trade_usd / 1e6).toFixed(2)}M`;
                if (el.whaleKpiTotalCount) el.whaleKpiTotalCount.textContent = ws.total_whale_trades.toLocaleString();
            }

            if (metricsRes) {
                const ctxWhale = document.getElementById("chart-whale-distribution");
                if (ctxWhale && window.Chart) {
                    const whaleTiers = Object.keys(metricsRes.whale_distribution || {});
                    const whaleVals = Object.values(metricsRes.whale_distribution || {}).map(v => v.turnover_usd);
                    const hasWhaleData = whaleTiers.length > 0;
                    const labels = hasWhaleData ? whaleTiers : ["Mega Whale (>= $2M)", "Big Whale ($1M - $2M)", "Whale ($500K - $1M)"];
                    const values = hasWhaleData ? whaleVals : [3200000, 1850000, 950000];
                    const bgColors = ["#ffb703", "#ff3b69", "#00d2ff"];

                    if (state.analyticsCharts.whale) {
                        state.analyticsCharts.whale.data.labels = labels;
                        state.analyticsCharts.whale.data.datasets[0].data = values;
                        state.analyticsCharts.whale.data.datasets[0].backgroundColor = bgColors;
                        state.analyticsCharts.whale.update('none');
                    } else {
                        const tc = getThemeColors();
                        state.analyticsCharts.whale = new Chart(ctxWhale, {
                            type: "doughnut",
                            data: {
                                labels: labels,
                                datasets: [{
                                    data: values,
                                    backgroundColor: bgColors,
                                    borderColor: tc.isDark ? "#080b11" : "#ffffff",
                                    borderWidth: 2
                                }]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: {
                                    legend: { position: "right", labels: { color: tc.textColor, font: { family: "'JetBrains Mono'" } } }
                                }
                            }
                        });
                    }
                }
            }
        } catch (e) {
            console.error("Error in fetchWhaleStats:", e);
        }
    }

    // B. Tab 2: Order Flow Charts (Buy vs Sell Ratio & Turnover) & DW View Table
    async function fetchOrderFlowCharts() {
        try {
            const res = await fetch("/api/analytics/metrics");
            const data = await res.json();

            // Chart 1: Order Flow Ratio (Stacked Bar Chart)
            const ctxRatio = document.getElementById("chart-orderflow-ratio");
            if (ctxRatio && window.Chart) {
                const btcOf = data.orderflow_pressure?.["BTC-USDT"] || (data.orderflow_pressure && Object.values(data.orderflow_pressure)[0]) || {};
                const buyVol = Number(btcOf.buy_volume || 58);
                const sellVol = Number(btcOf.sell_volume || 42);
                const tot = (buyVol + sellVol) || 1;
                const buyPct = Math.round((buyVol / tot) * 100);
                const sellPct = 100 - buyPct;
                const labels = ["BTC-USDT"];
                const buyData = [buyPct];
                const sellData = [sellPct];

                if (state.analyticsCharts.orderflow) {
                    state.analyticsCharts.orderflow.data.labels = labels;
                    state.analyticsCharts.orderflow.data.datasets[0].data = buyData;
                    state.analyticsCharts.orderflow.data.datasets[1].data = sellData;
                    state.analyticsCharts.orderflow.update('none');
                } else {
                    const tc = getThemeColors();
                    state.analyticsCharts.orderflow = new Chart(ctxRatio, {
                        type: "bar",
                        data: {
                            labels: labels,
                            datasets: [
                                { label: "% Mua Chủ Động", data: buyData, backgroundColor: tc.upColor },
                                { label: "% Bán Chủ Động", data: sellData, backgroundColor: tc.downColor }
                            ]
                        },
                        options: {
                            indexAxis: "y",
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                x: { stacked: true, max: 100, ticks: { color: tc.textColor }, grid: { color: tc.gridColor } },
                                y: { stacked: true, ticks: { color: tc.textColor }, grid: { display: false } }
                            },
                            plugins: {
                                legend: { labels: { color: tc.textColor } }
                            }
                        }
                    });
                }
            }

            // Chart 2: BTC-USDT Turnover (Bar Chart)
            const ctxTurnover = document.getElementById("chart-asset-turnover");
            if (ctxTurnover && window.Chart) {
                const btcComp = data.symbols_comparison?.["BTC-USDT"] || (data.symbols_comparison && Object.values(data.symbols_comparison)[0]) || {};
                const turnoverVal = Number(btcComp.turnover_usd || 95301);
                const labels = ["BTC-USDT"];
                const values = [turnoverVal];

                if (state.analyticsCharts.turnover) {
                    state.analyticsCharts.turnover.data.labels = labels;
                    state.analyticsCharts.turnover.data.datasets[0].data = values;
                    state.analyticsCharts.turnover.update('none');
                } else {
                    const tc = getThemeColors();
                    state.analyticsCharts.turnover = new Chart(ctxTurnover, {
                        type: "bar",
                        data: {
                            labels: labels,
                            datasets: [{
                                label: "Turnover (USD)",
                                data: values,
                                backgroundColor: ["#f7931a"],
                                borderRadius: 6
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: { legend: { display: false } },
                            scales: {
                                x: { ticks: { color: tc.textColor }, grid: { display: false } },
                                y: { ticks: { color: tc.textColor }, grid: { color: tc.gridColor } }
                            }
                        }
                    });
                }
            }

            // Order Flow Table: of-summary-tbody
            const ofTbody = document.getElementById("of-summary-tbody");
            if (ofTbody && data.orderflow_pressure) {
                let html = "";
                for (const [sym, of] of Object.entries(data.orderflow_pressure)) {
                    if (sym !== "BTC-USDT") continue;
                    const imbColor = (of.imbalance || 0) >= 0 ? "var(--accent-green)" : "var(--accent-red)";
                    html += `
                        <tr>
                            <td><strong>${sym}</strong></td>
                            <td style="color:var(--accent-green); font-weight:600;">${Number(of.buy_volume || 0).toFixed(4)}</td>
                            <td style="color:var(--accent-red); font-weight:600;">${Number(of.sell_volume || 0).toFixed(4)}</td>
                            <td>${Number(of.buy_to_sell_ratio || 1).toFixed(2)} : 1</td>
                            <td style="color:${imbColor}; font-weight:700;">${(of.imbalance !== undefined && of.imbalance !== null) ? (of.imbalance >= 0 ? "+" : "") + Number(of.imbalance).toFixed(4) : "0.0000"}</td>
                            <td><span class="sentiment-badge ${(of.imbalance || 0) >= 0.1 ? 'bullish' : ((of.imbalance || 0) <= -0.1 ? 'bearish' : '')}">${of.sentiment || 'Neutral'}</span></td>
                        </tr>
                    `;
                }
                ofTbody.innerHTML = html;
            }
        } catch (e) {
            console.error("Error in fetchOrderFlowCharts:", e);
        }
    }

    // C. Tab 4: Scalping Decisions Hub & Live BTC-USDT Signals Matrix
    async function fetchAllScalpingSignals() {
        const tbody = document.getElementById("quant-predictions-tbody");

        try {
            const symbols = ["BTC-USDT"];
            const signalPromises = symbols.map(s =>
                fetch(`/api/signals/realtime?symbol=${s}`).then(r => r.json()).catch(() => null)
            );
            const signals = (await Promise.all(signalPromises)).filter(Boolean);

            if (signals.length === 0) {
                if (tbody) tbody.innerHTML = `<tr><td colspan="11" class="empty-state">Chưa có tín hiệu thời gian thực từ Spark Streaming.</td></tr>`;
                return;
            }

            // 1. Update the 3 individual scalping cards
            signals.forEach(s => {
                const symPrefix = s.symbol.split("-")[0].toLowerCase(); // btc, eth, sol
                const priceEl = document.getElementById(`${symPrefix}-scalp-price`);
                const changeEl = document.getElementById(`${symPrefix}-scalp-change`);
                const urgencyEl = document.getElementById(`${symPrefix}-urgency-badge`);
                const actionBox = document.getElementById(`${symPrefix}-action-box`);
                const actionIcon = document.getElementById(`${symPrefix}-action-icon`);
                const actionLabel = document.getElementById(`${symPrefix}-action-label`);
                const actionSub = document.getElementById(`${symPrefix}-action-sub`);
                const indImbalance = document.getElementById(`${symPrefix}-ind-imbalance`);
                const indVol = document.getElementById(`${symPrefix}-ind-vol`);
                const indAbsorption = document.getElementById(`${symPrefix}-ind-absorption`);
                const planEntry = document.getElementById(`${symPrefix}-plan-entry`);
                const planSl = document.getElementById(`${symPrefix}-plan-sl`);
                const planTp = document.getElementById(`${symPrefix}-plan-tp`);

                if (priceEl) priceEl.textContent = formatCurrency(s.current_price);
                if (changeEl) {
                    const isPos = s.change_24h >= 0;
                    changeEl.textContent = `${isPos ? "+" : ""}${s.change_24h.toFixed(2)}%`;
                    changeEl.style.color = isPos ? "var(--accent-green)" : "var(--accent-red)";
                }

                if (urgencyEl) {
                    urgencyEl.textContent = s.urgency;
                    urgencyEl.className = `scalp-urgency-badge ${s.urgency.toLowerCase()}`;
                }

                if (actionLabel) actionLabel.textContent = s.action_label;
                if (actionSub) actionSub.textContent = s.reason;

                const isBull = s.action.includes("LONG") || s.action.includes("BUY") || s.action.includes("ACCUMULATION");
                const isBear = s.action.includes("SHORT") || s.action.includes("SELL") || s.action.includes("DISTRIBUTION");

                if (actionBox) {
                    actionBox.className = `scalp-action-box ${isBull ? "bullish" : (isBear ? "bearish" : "")}`;
                }

                if (actionIcon) {
                    actionIcon.textContent = isBull ? "🟢" : (isBear ? "🔴" : "⚖️");
                }

                if (indImbalance) {
                    const sign = s.order_imbalance >= 0 ? "+" : "";
                    indImbalance.textContent = `${sign}${(s.order_imbalance * 100).toFixed(1)}%`;
                    indImbalance.style.color = isBull ? "var(--accent-green)" : (isBear ? "var(--accent-red)" : "var(--accent-cyan)");
                }

                if (indWhales) {
                    indWhales.textContent = `${s.whale_buys_count || 0} Mua / ${s.whale_sells_count || 0} Xả`;
                }

                if (indVol) {
                    const rawVol = s.parkinson_volatility_pct !== undefined ? s.parkinson_volatility_pct : (s.parkinson_volatility ? s.parkinson_volatility * 100 : 0);
                    const volPct = Number(rawVol).toFixed(2);
                    indVol.textContent = `${volPct}% (${s.volatility_regime || 'MED'})`;
                }

                if (indAbsorption) {
                    const abs = s.absorption_status || s.absorption_detected || "NORMAL";
                    indAbsorption.textContent = abs.replace('_', ' ');
                    indAbsorption.style.color = (abs === "NORMAL" || abs === "BÌNH THƯỜNG") ? "var(--text-muted)" : (abs.includes("BULL") ? "var(--accent-green)" : "var(--accent-red)");
                }

                // Adaptive Dynamic Trade Setup (Risk:Reward 1:2.5 based on Parkinson Volatility)
                const p = s.current_price;
                const slPct = s.stop_loss_pct !== undefined ? s.stop_loss_pct : (s.sl_pct !== undefined ? s.sl_pct : 0.6);
                const tpPct = s.take_profit_pct !== undefined ? s.take_profit_pct : (s.tp_pct !== undefined ? s.tp_pct : 1.5);
                const dynSl = s.stop_loss_price || s.dynamic_sl;
                const dynTp = s.take_profit_price || s.dynamic_tp;

                if (isBull) {
                    const sl = dynSl || (p * (1 - slPct / 100));
                    const tp = dynTp || (p * (1 + tpPct / 100));
                    if (planEntry) planEntry.textContent = `~${formatCurrency(p)}`;
                    if (planSl) planSl.textContent = `~${formatCurrency(sl)} (-${slPct}%)`;
                    if (planTp) planTp.textContent = `~${formatCurrency(tp)} (+${tpPct}%)`;
                } else if (isBear) {
                    const sl = dynSl || (p * (1 + slPct / 100));
                    const tp = dynTp || (p * (1 - tpPct / 100));
                    if (planEntry) planEntry.textContent = `~${formatCurrency(p)}`;
                    if (planSl) planSl.textContent = `~${formatCurrency(sl)} (+${slPct}%)`;
                    if (planTp) planTp.textContent = `~${formatCurrency(tp)} (-${tpPct}%)`;
                } else {
                    if (planEntry) planEntry.textContent = "Chờ Breakout";
                    if (planSl) planSl.textContent = "---";
                    if (planTp) planTp.textContent = "---";
                }
            });

            // 2. Update the full summary table
            if (tbody) {
                let html = "";
                signals.forEach(s => {
                    const changeColor = s.change_24h >= 0 ? "var(--accent-green)" : "var(--accent-red)";
                    const changeSign = s.change_24h >= 0 ? "+" : "";
                    const imbColor = s.order_imbalance >= 0 ? "var(--accent-green)" : "var(--accent-red)";

                    let sigClass = "sentiment-badge";
                    if (s.action.includes("LONG") || s.action.includes("ACCUMULATION")) sigClass += " bullish";
                    else if (s.action.includes("SHORT") || s.action.includes("DISTRIBUTION")) sigClass += " bearish";

                    let urgencyColor = "var(--text-muted)";
                    if (s.urgency === "HIGH") urgencyColor = "var(--accent-red)";
                    else if (s.urgency === "MEDIUM") urgencyColor = "var(--accent-yellow)";

                    html += `
                        <tr>
                            <td><strong>${s.symbol}</strong></td>
                            <td style="color:var(--accent-cyan); font-weight:700;">${formatCurrency(s.current_price)}</td>
                            <td style="color:${changeColor}; font-weight:600;">${changeSign}${s.change_24h.toFixed(2)}%</td>
                            <td style="color:${imbColor}; font-weight:700;">${(s.order_imbalance * 100).toFixed(1)}%</td>
                            <td>${Number(s.buy_volume || 0).toFixed(2)}</td>
                            <td>${Number(s.sell_volume || 0).toFixed(2)}</td>
                            <td><strong style="color:var(--accent-green);">${s.whale_buys_count || 0}</strong></td>
                            <td><strong style="color:var(--accent-red);">${s.whale_sells_count || 0}</strong></td>
                            <td><span class="${sigClass}">${s.action_label}</span></td>
                            <td><strong style="color:${urgencyColor};">${s.urgency}</strong></td>
                            <td style="font-size:0.85rem; color:var(--text-muted);">${s.reason}</td>
                        </tr>
                    `;
                });
                tbody.innerHTML = html;
            }
        } catch (e) {
            console.error("Error in fetchAllScalpingSignals:", e);
        }
    }

    // -------------------------------------------------------------------------
    // 7.5. MACHINE LEARNING: BTC 5M DIRECTION (XGBOOST) CONTROLLER
    // -------------------------------------------------------------------------
    async function fetchMlPrediction() {
        try {
            const res = await fetch("/api/ml/prediction");
            const data = await res.json();
            if (!data) return;

            const priceEl = document.getElementById("pred-current-price");
            const dirLabel = document.getElementById("pred-direction-label");
            const dirBox = document.getElementById("pred-direction-box");
            const probUpEl = document.getElementById("pred-prob-up");
            const probNotUpEl = document.getElementById("pred-prob-not-up");
            const probBar = document.getElementById("pred-prob-bar");
            const featTimeEl = document.getElementById("pred-feature-time");
            const targetTimeEl = document.getElementById("pred-target-time");
            const modelBadge = document.getElementById("ml-model-badge");

            if (priceEl && data.current_price) priceEl.textContent = formatCurrency(data.current_price);
            if (modelBadge && data.model_version) modelBadge.textContent = `XGBClassifier v${data.model_version}`;

            const isUp = data.predicted_direction === 1;
            if (dirLabel) {
                dirLabel.textContent = isUp ? "🟢 TĂNG (UP)" : "🔴 KHÔNG TĂNG (NOT_UP)";
                dirLabel.style.color = isUp ? "var(--accent-green)" : "var(--accent-red)";
            }
            if (dirBox) {
                dirBox.style.background = isUp ? "rgba(0, 255, 136, 0.15)" : "rgba(255, 59, 105, 0.15)";
                dirBox.style.borderColor = isUp ? "var(--accent-green)" : "var(--accent-red)";
            }

            const pUp = (data.probability_up * 100).toFixed(1);
            const pNotUp = (data.probability_not_up * 100).toFixed(1);
            if (probUpEl) probUpEl.textContent = `${pUp}%`;
            if (probNotUpEl) probNotUpEl.textContent = `${pNotUp}%`;
            if (probBar) {
                probBar.style.width = `${pUp}%`;
                probBar.style.background = isUp ? "linear-gradient(90deg, #00f0ff, #00f090)" : "linear-gradient(90deg, #ff9900, #ff3b69)";
            }

            if (featTimeEl && data.feature_timestamp) {
                featTimeEl.textContent = data.feature_timestamp.slice(11, 19) + " UTC";
            }
            if (targetTimeEl && data.target_timestamp) {
                targetTimeEl.textContent = data.target_timestamp.slice(11, 19) + " UTC (+5m)";
            }
        } catch (e) {
            console.error("Error in fetchMlPrediction:", e);
        }
    }

    async function fetchMlEvaluation() {
        try {
            const res = await fetch("/api/ml/evaluation");
            const data = await res.json();
            if (!data || !data.metrics) return;

            const m = data.metrics;
            const b = data.baseline_comparison || {};
            const base = b.baseline || {};

            const accEl = document.getElementById("eval-accuracy");
            const baseEl = document.getElementById("eval-baseline");
            const balAccEl = document.getElementById("eval-bal-acc");
            const rocAucEl = document.getElementById("eval-roc-auc");
            const verdictEl = document.getElementById("eval-baseline-verdict");
            const featListEl = document.getElementById("eval-feature-list");

            if (accEl) accEl.textContent = `${(m.accuracy * 100).toFixed(2)}%`;
            if (baseEl) baseEl.textContent = `${(base.accuracy * 100).toFixed(2)}%`;
            if (balAccEl) balAccEl.textContent = `${(m.balanced_accuracy * 100).toFixed(2)}%`;
            if (rocAucEl) rocAucEl.textContent = m.roc_auc !== null ? m.roc_auc.toFixed(2) : "N/A";

            if (verdictEl) {
                const lift = b.accuracy_lift_pct || 0;
                const outperforms = b.outperforms_baseline;
                verdictEl.innerHTML = `Baseline Lift: <strong>${lift >= 0 ? "+" : ""}${lift.toFixed(2)}%</strong> (${outperforms ? "<span style='color:var(--accent-green);'>VƯỢT TRỘI</span>" : "<span style='color:var(--text-secondary);'>TƯƠNG ĐƯƠNG BASELINE</span>"})`;
            }

            if (featListEl && data.feature_importance && data.feature_importance.length > 0) {
                let html = "";
                data.feature_importance.slice(0, 5).forEach((f, idx) => {
                    const pct = Math.round(f.importance * 100);
                    html += `
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:2px;">
                            <span style="font-family:var(--font-mono); color:var(--text-secondary); font-size:11px;">#${idx+1} ${f.feature}</span>
                            <span style="font-family:var(--font-mono); color:var(--accent-cyan); font-weight:700;">${pct}%</span>
                        </div>
                        <div style="width:100%; height:4px; background:rgba(255,255,255,0.06); border-radius:2px; margin-bottom:6px;">
                            <div style="width:${Math.min(pct * 4, 100)}%; height:100%; background:var(--accent-cyan); border-radius:2px;"></div>
                        </div>
                    `;
                });
                featListEl.innerHTML = html;
            }
        } catch (e) {
            console.error("Error in fetchMlEvaluation:", e);
        }
    }

    async function fetchMlHistory() {
        try {
            const res = await fetch("/api/ml/history");
            const data = await res.json();
            const tbody = document.getElementById("ml-prediction-history-tbody");
            if (!tbody) return;

            if (!data.history || data.history.length === 0) {
                tbody.innerHTML = `<tr><td colspan="8" class="empty-state">Chưa có bản ghi dự báo nào trong PostgreSQL.</td></tr>`;
                return;
            }

            let html = "";
            data.history.forEach(r => {
                const isPredUp = r.predicted_direction === 1;
                const predBadge = isPredUp 
                    ? `<span class="badge-tag side-buy">TĂNG (UP)</span>` 
                    : `<span class="badge-tag side-sell">KHÔNG TĂNG</span>`;

                let outcomeBadge = `<span style="color:var(--text-muted);">⏳ Đang chờ (5m)</span>`;
                let actualPxText = "---";
                if (r.actual_future_price) {
                    actualPxText = formatCurrency(r.actual_future_price);
                    if (r.is_correct === true) {
                        outcomeBadge = `<span style="color:var(--accent-green); font-weight:700;">✅ CHÍNH XÁC</span>`;
                    } else if (r.is_correct === false) {
                        outcomeBadge = `<span style="color:var(--accent-red); font-weight:700;">❌ CHƯA ĐÚNG</span>`;
                    }
                }

                html += `
                    <tr>
                        <td style="font-family:var(--font-mono); font-size:11px;">${r.feature_time.slice(0, 19).replace('T', ' ')}</td>
                        <td style="font-family:var(--font-mono); font-weight:600;">${formatCurrency(r.current_price)}</td>
                        <td>${predBadge}</td>
                        <td style="font-family:var(--font-mono);">${(r.probability_up * 100).toFixed(1)}%</td>
                        <td style="font-family:var(--font-mono); font-size:11px; color:var(--text-muted);">${r.target_time.slice(11, 19)}</td>
                        <td style="font-family:var(--font-mono);">${actualPxText}</td>
                        <td>${r.actual_label ? `<span class="badge-tag ${r.actual_label === 'UP' ? 'side-buy' : 'side-sell'}">${r.actual_label}</span>` : '---'}</td>
                        <td>${outcomeBadge}</td>
                    </tr>
                `;
            });
            tbody.innerHTML = html;
        } catch (e) {
            console.error("Error in fetchMlHistory:", e);
        }
    }

    // -------------------------------------------------------------------------
    // 8. STREAMING CONTROLS (BATCH RUNNER REMOVED)
    // -------------------------------------------------------------------------
    function initBatchControls() {
        // Pure Streaming Mode: Batch triggers deactivated
    }

    // -------------------------------------------------------------------------
    // 9. TAB 6: PIPELINE TELEMETRY LOGIC
    // -------------------------------------------------------------------------
    async function fetchPipelineTelemetry() {
        try {
            const res = await fetch("/api/pipeline/telemetry");
            const data = await res.json();
            if (data.components && data.components.warehouse) {
                if (el.dwTelemetrySize) {
                    el.dwTelemetrySize.textContent = data.components.warehouse.database_size;
                }
            }
        } catch (e) {
            console.error("Error in fetchPipelineTelemetry:", e);
        }
    }

    // -------------------------------------------------------------------------
    // 10. HELPER FUNCTIONS & ZERO-SNAP REFRESH CONTROLLER
    // -------------------------------------------------------------------------
    function formatCurrency(val) {
        if (!val || isNaN(val)) return "$0.00";
        if (val >= 1000) {
            return "$" + val.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
        return "$" + val.toFixed(4);
    }

    function updateQuickStats(stats) {
        if (!stats) return;
        if (stats.open !== undefined) el.statOpen.textContent = formatCurrency(stats.open);
        if (stats.high !== undefined) el.statHigh.textContent = formatCurrency(stats.high);
        if (stats.low !== undefined) el.statLow.textContent = formatCurrency(stats.low);
        if (stats.close !== undefined) el.statClose.textContent = formatCurrency(stats.close);
        if (stats.vwap !== undefined && stats.vwap !== null) el.statVwap.textContent = formatCurrency(stats.vwap);
        if (stats.volume !== undefined && stats.volume !== null) el.statVol.textContent = Number(stats.volume).toFixed(2);
        if (stats.upper_band_2 !== undefined && el.statVwapUpper) el.statVwapUpper.textContent = formatCurrency(stats.upper_band_2);
        if (stats.lower_band_2 !== undefined && el.statVwapLower) el.statVwapLower.textContent = formatCurrency(stats.lower_band_2);
        if (stats.parkinson_volatility !== undefined && el.statParkinsonVol) {
            el.statParkinsonVol.textContent = stats.parkinson_volatility ? (stats.parkinson_volatility * 100).toFixed(2) + "%" : "---";
        }
        if (stats.cvd !== undefined && el.statCvd) {
            const cvdNum = Number(stats.cvd || 0);
            el.statCvd.textContent = `${cvdNum >= 0 ? "+" : ""}${cvdNum.toFixed(2)} Coin`;
            el.statCvd.style.color = cvdNum >= 0 ? "var(--accent-green)" : "var(--accent-red)";
        }
    }

    function updateClock() {
        const now = new Date();
        el.clock.textContent = now.toUTCString().slice(17, 25) + " UTC";
    }

    // Concurrency guard & scroll position anchor
    let isRefreshing = false;

    async function refreshActiveTab(showLoader = false) {
        if (isRefreshing) return;
        isRefreshing = true;

        // Save scroll position prior to data update
        const prevScrollY = window.scrollY;
        const prevScrollX = window.scrollX;

        try {
            // Always refresh top ticker cards (BTC, ETH, SOL)
            await fetchOverview();

            if (state.activeTab === "tab-candlestick") {
                await fetchCandles(showLoader);
            } else if (state.activeTab === "tab-orderflow") {
                await Promise.all([
                    fetchOrderFlow(),
                    fetchOrderFlowCharts()
                ]);
            } else if (state.activeTab === "tab-whales") {
                await Promise.all([
                    fetchWhales(),
                    fetchWhaleStats()
                ]);
            } else if (state.activeTab === "tab-signals") {
                await Promise.all([
                    fetchAllScalpingSignals(),
                    fetchDerivatives(),
                    fetchMlPrediction(),
                    fetchMlEvaluation(),
                    fetchMlHistory()
                ]);
            } else if (state.activeTab === "tab-warehouse") {
                await Promise.all([
                    fetchDwStats(),
                    fetchDwQuery(!showLoader)
                ]);
            } else if (state.activeTab === "tab-pipeline") {
                await fetchPipelineTelemetry();
            }
        } finally {
            isRefreshing = false;
            // Seamlessly anchor scroll position so the user never gets snapped back to top
            if (!showLoader && Math.abs(window.scrollY - prevScrollY) > 2) {
                window.scrollTo({
                    top: prevScrollY,
                    left: prevScrollX,
                    behavior: 'instant'
                });
            }
        }
    }

    function startAutoRefresh() {
        if (state.timerId) clearInterval(state.timerId);
        state.timerId = setInterval(() => {
            if (state.autoRefresh) {
                refreshActiveTab(false);
            }
        }, state.refreshIntervalMs);
    }

    // -------------------------------------------------------------------------
    // 11. EVENT LISTENERS
    // -------------------------------------------------------------------------

    // Ticker card selection (BTC, ETH, SOL)
    el.tickerCards.forEach(card => {
        card.addEventListener("click", () => {
            const sym = card.getAttribute("data-symbol");
            if (sym && sym !== state.symbol) {
                state.symbol = sym;
                el.tickerCards.forEach(c => c.classList.remove("active"));
                card.classList.add("active");
                el.activeSymName.textContent = sym;
                if (el.webhookSymBadge) el.webhookSymBadge.textContent = sym;
                refreshActiveTab(true);
            }
        });
    });

    // Timeframe selector (1m, 5m, 15m, 1h, 1d)
    el.timeframeBtns.forEach(btn => {
        btn.addEventListener("click", () => {
            const tf = btn.getAttribute("data-tf");
            if (tf && tf !== state.timeframe) {
                state.timeframe = tf;
                el.timeframeBtns.forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                fetchCandles(true);
            }
        });
    });

    // Data Source switcher (Live vs DW)
    if (el.srcBtnLive && el.srcBtnDw) {
        el.srcBtnLive.addEventListener("click", () => {
            if (state.candleSource !== "live") {
                state.candleSource = "live";
                el.srcBtnLive.classList.add("active");
                el.srcBtnDw.classList.remove("active");
                fetchCandles(true);
                fetchOrderFlow();
                fetchWhales();
            }
        });
        el.srcBtnDw.addEventListener("click", () => {
            if (state.candleSource !== "dw") {
                state.candleSource = "dw";
                el.srcBtnDw.classList.add("active");
                el.srcBtnLive.classList.remove("active");
                fetchCandles(true);
                fetchOrderFlow();
                fetchWhales();
            }
        });
    }

    // Whale Tier Filters
    el.tierFilterBtns.forEach(btn => {
        btn.addEventListener("click", () => {
            const tier = btn.getAttribute("data-tier");
            if (tier && tier !== state.whaleTier) {
                state.whaleTier = tier;
                el.tierFilterBtns.forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                fetchWhales();
            }
        });
    });

    // Whale Symbol Filter
    if (el.whaleSymbolFilter) {
        el.whaleSymbolFilter.addEventListener("change", (e) => {
            state.whaleSymbol = e.target.value;
            fetchWhales();
        });
    }

    // Data Warehouse view selector
    el.dwViewBtns.forEach(btn => {
        btn.addEventListener("click", () => {
            const tbl = btn.getAttribute("data-table");
            if (tbl && tbl !== state.dwTable) {
                state.dwTable = tbl;
                state.dwOffset = 0;
                el.dwViewBtns.forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                fetchDwQuery(false);
            }
        });
    });

    // DW Symbol filter change
    if (el.dwFilterSymbol) {
        el.dwFilterSymbol.addEventListener("change", (e) => {
            state.dwSymbol = e.target.value;
            state.dwOffset = 0;
            fetchDwQuery(false);
        });
    }

    // DW limit change
    if (el.dwLimitSelect) {
        el.dwLimitSelect.addEventListener("change", (e) => {
            state.dwLimit = parseInt(e.target.value, 10);
            state.dwOffset = 0;
            fetchDwQuery(false);
        });
    }

    // DW Pagination buttons
    if (el.btnDwPrev) {
        el.btnDwPrev.addEventListener("click", () => {
            if (state.dwOffset > 0) {
                state.dwOffset = Math.max(0, state.dwOffset - state.dwLimit);
                fetchDwQuery(false);
            }
        });
    }

    if (el.btnDwNext) {
        el.btnDwNext.addEventListener("click", () => {
            if (state.dwOffset + state.dwLimit < state.dwTotalCount) {
                state.dwOffset += state.dwLimit;
                fetchDwQuery(false);
            }
        });
    }

    // Export CSV button
    if (el.btnExportCsv) {
        el.btnExportCsv.addEventListener("click", () => {
            const url = `/api/warehouse/export?table=${state.dwTable}&symbol=${state.dwSymbol}`;
            window.open(url, "_blank");
        });
    }

    // Auto-refresh toggle
    if (el.autoRefreshToggle) {
        el.autoRefreshToggle.addEventListener("change", (e) => {
            state.autoRefresh = e.target.checked;
        });
    }

    // Manual refresh button
    if (el.btnManualRefresh) {
        el.btnManualRefresh.addEventListener("click", () => {
            refreshActiveTab(true);
            checkHealth();
        });
    }

    // Webhook API Test button (Module 5.2)
    if (el.btnTestWebhook) {
        el.btnTestWebhook.addEventListener("click", async () => {
            el.btnTestWebhook.textContent = "⏳ Đang Bắn Webhook...";
            el.btnTestWebhook.disabled = true;
            try {
                const res = await fetch(`/api/signals/webhook?symbol=${state.symbol}`);
                const data = await res.json();
                if (el.webhookJsonPreview) {
                    el.webhookJsonPreview.textContent = JSON.stringify(data, null, 2);
                }
            } catch (e) {
                if (el.webhookJsonPreview) el.webhookJsonPreview.textContent = "Lỗi kết nối Webhook: " + e.message;
            } finally {
                el.btnTestWebhook.textContent = "⚡ Bắn Thử Tín Hiệu Webhook";
                el.btnTestWebhook.disabled = false;
            }
        });
    }

    // Copy Telegram Alert button (Module 5.1)
    if (el.btnCopyTelegramAlert) {
        el.btnCopyTelegramAlert.addEventListener("click", () => {
            if (el.whaleTelegramPreview) {
                const text = el.whaleTelegramPreview.textContent;
                navigator.clipboard.writeText(text).then(() => {
                    const originalText = el.btnCopyTelegramAlert.textContent;
                    el.btnCopyTelegramAlert.textContent = "✅ Đã Sao Chép Tin Nhắn!";
                    setTimeout(() => {
                        el.btnCopyTelegramAlert.textContent = originalText;
                    }, 2000);
                }).catch(() => {
                    el.btnCopyTelegramAlert.textContent = "❌ Không thể sao chép";
                });
            }
        });
    }

    // Trigger ML Inference button (XGBoost BTC Prediction)
    const btnTriggerInference = document.getElementById("btn-trigger-inference");
    if (btnTriggerInference) {
        btnTriggerInference.addEventListener("click", async () => {
            btnTriggerInference.textContent = "⏳ Đang suy luận...";
            btnTriggerInference.disabled = true;
            try {
                await fetch("/api/ml/trigger_prediction", { method: "POST" });
                await Promise.all([
                    fetchMlPrediction(),
                    fetchMlHistory()
                ]);
            } catch (e) {
                console.error("Error triggering inference:", e);
            } finally {
                btnTriggerInference.textContent = "⚡ Dự Báo Ngay";
                btnTriggerInference.disabled = false;
            }
        });
    }

    // -------------------------------------------------------------------------
    // 12. STARTUP INITIALIZATION
    // -------------------------------------------------------------------------
    initTheme();
    initTabs();
    initBatchControls();
    updateClock();
    setInterval(updateClock, 1000);
    initChart();
    checkHealth();
    refreshActiveTab(true);
    startAutoRefresh();
});
