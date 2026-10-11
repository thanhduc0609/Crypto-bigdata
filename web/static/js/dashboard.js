/**
 * =============================================================================
 * Crypto Big Data - Quant Analytics & Trading Platform (Frontend Controller)
 * =============================================================================
 */

document.addEventListener("DOMContentLoaded", () => {
    // -------------------------------------------------------------------------
    // 1. APPLICATION STATE
    // -------------------------------------------------------------------------
    const state = {
        activeTab: "tab-terminal",
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
        lastCandleTime: null,
        lastCandleCount: 0,
        
        // Whale radar filter
        whaleTier: "all",
        lastWhaleId: null,

        // Data Warehouse Explorer state
        dwTable: "vw_whale_trades",
        dwSymbol: "ALL",
        dwLimit: 50,
        dwOffset: 0,
        dwTotalCount: 0,

        // Chart.js instances (Tab 3) - kept persistent, updated in-place
        analyticsCharts: {
            whale: null,
            turnover: null,
            orderflow: null,
            volatility: null
        },

        // Batch execution state
        batchRunning: false,
        batchPollTimer: null,
        batchLogIndex: 0
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

        // Terminal tab
        activeSymName: document.getElementById("active-symbol-name"),
        timeframeBtns: document.querySelectorAll(".tf-btn"),
        srcBtnLive: document.getElementById("src-btn-live"),
        srcBtnDw: document.getElementById("src-btn-dw"),
        chartContainer: document.getElementById("tv-chart-container"),
        chartLoader: document.getElementById("chart-loader"),
        chartLoaderText: document.getElementById("chart-loader-text"),
        
        // Quick stats
        statOpen: document.getElementById("stat-open"),
        statHigh: document.getElementById("stat-high"),
        statLow: document.getElementById("stat-low"),
        statClose: document.getElementById("stat-close"),
        statVwap: document.getElementById("stat-vwap"),
        statVol: document.getElementById("stat-vol"),

        // Order flow
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

        // Derivatives
        derivFunding: document.getElementById("deriv-funding"),
        derivFundingState: document.getElementById("deriv-funding-state"),
        derivApy: document.getElementById("deriv-apy"),
        derivOiUsd: document.getElementById("deriv-oi-usd"),
        derivOiCoins: document.getElementById("deriv-oi-coins"),

        // Whales
        whaleTableBody: document.getElementById("whale-table-body"),
        whaleCount: document.getElementById("whale-count"),
        tierFilterBtns: document.querySelectorAll(".tier-filter-btn"),

        // Data Warehouse Explorer
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

        // Quant Analytics summary table
        quantSummaryTbody: document.getElementById("quant-summary-tbody"),

        // Batch Action Control Center
        batchBtns: document.querySelectorAll(".btn-batch-action"),
        batchStatusDot: document.getElementById("batch-status-dot"),
        batchStatusText: document.getElementById("batch-status-text"),
        batchConsoleContent: document.getElementById("batch-console-content"),
        btnConsoleClear: document.getElementById("btn-console-clear"),

        // Telemetry
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
                    if (targetTab === "tab-terminal") {
                        if (state.chart) {
                            setTimeout(() => {
                                state.chart.applyOptions({
                                    width: el.chartContainer.clientWidth,
                                    height: el.chartContainer.clientHeight
                                });
                            }, 50);
                        }
                    } else if (targetTab === "tab-warehouse") {
                        fetchDwStats();
                        fetchDwQuery(false);
                    } else if (targetTab === "tab-analytics") {
                        fetchQuantAnalytics();
                        fetchPredictions();
                    } else if (targetTab === "tab-pipeline") {
                        fetchPipelineTelemetry();
                    }
                }
            });
        });
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

        state.chart = LightweightCharts.createChart(el.chartContainer, {
            width: width,
            height: height,
            layout: {
                background: { color: "#080b11" },
                textColor: "#94a3b8",
                fontSize: 11,
                fontFamily: "'JetBrains Mono', monospace"
            },
            grid: {
                vertLines: { color: "rgba(255, 255, 255, 0.04)" },
                horzLines: { color: "rgba(255, 255, 255, 0.04)" }
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    color: "rgba(0, 210, 255, 0.4)",
                    width: 1,
                    style: LightweightCharts.LineStyle.Dashed
                },
                horzLine: {
                    color: "rgba(0, 210, 255, 0.4)",
                    width: 1,
                    style: LightweightCharts.LineStyle.Dashed
                }
            },
            rightPriceScale: {
                borderColor: "rgba(255, 255, 255, 0.08)",
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.25
                }
            },
            timeScale: {
                borderColor: "rgba(255, 255, 255, 0.08)",
                timeVisible: true,
                secondsVisible: false
            }
        });

        // 1. Candlestick series
        state.candleSeries = state.chart.addCandlestickSeries({
            upColor: "#00f090",
            downColor: "#ff3b69",
            borderUpColor: "#00f090",
            borderDownColor: "#ff3b69",
            wickUpColor: "#00f090",
            wickDownColor: "#ff3b69"
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
            color: "#00d2ff",
            lineWidth: 2,
            title: "VWAP",
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

            if (cData) {
                updateQuickStats({
                    open: cData.open,
                    high: cData.high,
                    low: cData.low,
                    close: cData.close,
                    volume: vData ? vData.value : null,
                    vwap: wData ? wData.value : null
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

    // B. Overview Tickers (BTC, ETH, SOL)
    async function fetchOverview() {
        try {
            const res = await fetch("/api/overview");
            const data = await res.json();

            const symMap = {
                "BTC-USDT": { pId: "btc-price", cId: "btc-change", vId: "btc-vol", wId: "btc-vwap" },
                "ETH-USDT": { pId: "eth-price", cId: "eth-change", vId: "eth-vol", wId: "eth-vwap" },
                "SOL-USDT": { pId: "sol-price", cId: "sol-change", vId: "sol-vol", wId: "sol-vwap" }
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
                        color: c.close >= c.open ? "rgba(0, 240, 144, 0.45)" : "rgba(255, 59, 105, 0.45)"
                    });

                    vwapData.push({
                        time: c.time,
                        value: c.vwap
                    });
                });

                const lastCandle = candleData[candleData.length - 1];
                const lastVol = volumeData[volumeData.length - 1];
                const lastVwap = vwapData[vwapData.length - 1];

                // If first render or explicit filter switch, replace full dataset
                if (showLoader || !state.lastCandleTime || state.lastCandleCount !== candleData.length) {
                    state.candleSeries.setData(candleData);
                    state.volumeSeries.setData(volumeData);
                    state.vwapSeries.setData(vwapData);
                    state.lastCandleTime = lastCandle.time;
                    state.lastCandleCount = candleData.length;
                } else {
                    // Micro-update only the current ticking candle to preserve scroll/zoom state
                    state.candleSeries.update(lastCandle);
                    state.volumeSeries.update(lastVol);
                    state.vwapSeries.update(lastVwap);
                }

                // Update OHLC ribbon with latest candle
                updateQuickStats(lastCandle);
            }
        } catch (e) {
            console.error("Error in fetchCandles:", e);
        } finally {
            if (showLoader && el.chartLoader) {
                el.chartLoader.classList.add("hidden");
            }
        }
    }

    // D. Fetch Order Flow & Imbalance (Real-time dynamic gauge)
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
                el.derivFunding.textContent = `${deriv.funding_rate_pct.toFixed(4)}%`;
                el.derivFundingState.textContent = deriv.funding_state;
                el.derivApy.textContent = `${deriv.projected_apy_pct.toFixed(2)}%`;
                el.derivOiUsd.textContent = `$${(deriv.open_interest_usd / 1e6).toFixed(2)}M`;
                el.derivOiCoins.textContent = `${deriv.open_interest_coins.toLocaleString()} contracts`;
            }
        } catch (e) {
            console.error("Error in fetchDerivatives:", e);
        }
    }

    // E2. Fetch Streaming Real-Time Actionable Decisions
    async function fetchRealTimeSignals() {
        try {
            const res = await fetch(`/api/signals/realtime?symbol=${state.symbol}`);
            const data = await res.json();

            const actionTitle = document.getElementById("signal-action-title");
            const actionDesc = document.getElementById("signal-action-desc");
            const urgencyBadge = document.getElementById("signal-urgency");
            const iconEl = document.getElementById("signal-icon");
            const sigImbalance = document.getElementById("sig-imbalance");
            const sigWhaleBuys = document.getElementById("sig-whale-buys");
            const sigWhaleSells = document.getElementById("sig-whale-sells");

            if (actionTitle) actionTitle.textContent = data.action_label || "Đang phân tích...";
            if (actionDesc) actionDesc.textContent = data.reason || "";
            if (sigImbalance) sigImbalance.textContent = (data.order_imbalance >= 0 ? "+" : "") + (data.order_imbalance * 100).toFixed(1) + "%";
            if (sigWhaleBuys) sigWhaleBuys.textContent = (data.whale_buys_count || 0) + " lệnh";
            if (sigWhaleSells) sigWhaleSells.textContent = (data.whale_sells_count || 0) + " lệnh";

            if (urgencyBadge) {
                urgencyBadge.textContent = data.urgency;
                urgencyBadge.className = `badge-tag urgency-badge ${data.urgency.toLowerCase()}`;
            }

            if (iconEl) {
                if (data.action.includes("BUY") || data.action.includes("LONG") || data.action.includes("ACCUM")) {
                    iconEl.textContent = "🟢";
                } else if (data.action.includes("SELL") || data.action.includes("SHORT") || data.action.includes("DISTRIB")) {
                    iconEl.textContent = "🔴";
                } else {
                    iconEl.textContent = "⚖️";
                }
            }
        } catch (e) {
            console.error("Error in fetchRealTimeSignals:", e);
        }
    }

    // F. Fetch Whale Radar (Live & Historical) - Atomic DOM replace
    async function fetchWhales() {
        try {
            const res = await fetch(`/api/whales?symbol=${state.symbol}&tier=${state.whaleTier}&limit=20`);
            const data = await res.json();

            if (data.whales && data.whales.length > 0) {
                el.whaleCount.textContent = `${data.whales.length} Lệnh lớn`;
                
                let rowsHtml = "";
                data.whales.forEach(w => {
                    let tierClass = "tier-large";
                    if (w.tier.includes("Mega")) tierClass = "tier-mega";
                    else if (w.tier.includes("Whale")) tierClass = "tier-whale";

                    const isBuy = w.side.toLowerCase() === "buy";
                    const sideClass = isBuy ? "side-buy" : "side-sell";
                    const sideLabel = isBuy ? "BUY" : "SELL";

                    rowsHtml += `
                        <tr>
                            <td><strong>${w.symbol.split('-')[0]}</strong> <span style="color:var(--text-muted);">${w.time.slice(-8)}</span></td>
                            <td class="${sideClass}">${sideLabel}</td>
                            <td>${formatCurrency(w.price)}</td>
                            <td style="color: #ffffff; font-weight:600;">$${Math.round(w.trade_value).toLocaleString()}</td>
                            <td><span class="tier-tag ${tierClass}">${w.tier.split(' ')[0]}</span></td>
                        </tr>
                    `;
                });
                el.whaleTableBody.innerHTML = rowsHtml;
            } else {
                el.whaleTableBody.innerHTML = `<tr><td colspan="5" class="empty-state">Không có lệnh cá mập nào khớp bộ lọc</td></tr>`;
            }
        } catch (e) {
            console.error("Error in fetchWhales:", e);
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
    // 7. TAB 3: QUANT ANALYTICS & VISUALIZATIONS (CHART.JS IN-PLACE UPDATES)
    // -------------------------------------------------------------------------
    async function fetchQuantAnalytics() {
        try {
            const res = await fetch("/api/analytics/metrics");
            const data = await res.json();

            // A. Chart 1: Whale Turnover Distribution (Donut Chart)
            const ctxWhale = document.getElementById("chart-whale-distribution");
            if (ctxWhale && window.Chart) {
                const whaleTiers = Object.keys(data.whale_distribution || {});
                const whaleVals = Object.values(data.whale_distribution || {}).map(v => v.turnover_usd);
                const hasWhaleData = whaleTiers.length > 0;
                const labels = hasWhaleData ? whaleTiers : ["Chưa có lệnh Cá voi (>= $500K)"];
                const values = hasWhaleData ? whaleVals : [1];
                const bgColors = hasWhaleData 
                    ? ["#ffb703", "#ff3b69", "#00d2ff"] 
                    : ["rgba(255, 255, 255, 0.12)"];

                if (state.analyticsCharts.whale) {
                    state.analyticsCharts.whale.data.labels = labels;
                    state.analyticsCharts.whale.data.datasets[0].data = values;
                    state.analyticsCharts.whale.data.datasets[0].backgroundColor = bgColors;
                    state.analyticsCharts.whale.update('none');
                } else {
                    state.analyticsCharts.whale = new Chart(ctxWhale, {
                        type: "doughnut",
                        data: {
                            labels: labels,
                            datasets: [{
                                data: values,
                                backgroundColor: bgColors,
                                borderColor: "#080b11",
                                borderWidth: 2
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: { position: "right", labels: { color: "#94a3b8", font: { family: "'JetBrains Mono'" } } }
                            }
                        }
                    });
                }
            }

            // B. Chart 2: 3-Asset Turnover Comparison (Bar Chart)
            const ctxTurnover = document.getElementById("chart-asset-turnover");
            if (ctxTurnover && window.Chart) {
                const syms = Object.keys(data.symbols_comparison || {});
                const turnovers = Object.values(data.symbols_comparison || {}).map(v => v.turnover_usd);
                const labels = syms.length > 0 ? syms : ["BTC-USDT", "ETH-USDT", "SOL-USDT"];
                const values = turnovers.length > 0 ? turnovers : [95301, 28946, 7090];

                if (state.analyticsCharts.turnover) {
                    state.analyticsCharts.turnover.data.labels = labels;
                    state.analyticsCharts.turnover.data.datasets[0].data = values;
                    state.analyticsCharts.turnover.update('none');
                } else {
                    state.analyticsCharts.turnover = new Chart(ctxTurnover, {
                        type: "bar",
                        data: {
                            labels: labels,
                            datasets: [{
                                label: "Turnover (USD)",
                                data: values,
                                backgroundColor: ["#f7931a", "#627eea", "#14f195"],
                                borderRadius: 6
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: { display: false }
                            },
                            scales: {
                                x: { ticks: { color: "#94a3b8" }, grid: { display: false } },
                                y: { ticks: { color: "#94a3b8" }, grid: { color: "rgba(255,255,255,0.05)" } }
                            }
                        }
                    });
                }
            }

            // C. Chart 3: Order Flow Buy vs Sell Ratio (Stacked Bar Chart)
            const ctxRatio = document.getElementById("chart-orderflow-ratio");
            if (ctxRatio && window.Chart) {
                const ofSyms = Object.keys(data.orderflow_pressure || {});
                const buyVols = ofSyms.map(s => {
                    const item = data.orderflow_pressure[s];
                    const tot = (item.buy_volume + item.sell_volume) || 1;
                    return Math.round((item.buy_volume / tot) * 100);
                });
                const sellVols = buyVols.map(b => 100 - b);
                const labels = ofSyms.length > 0 ? ofSyms : ["BTC-USDT", "ETH-USDT", "SOL-USDT"];
                const buyData = buyVols.length > 0 ? buyVols : [58, 26, 54];
                const sellData = sellVols.length > 0 ? sellVols : [42, 74, 46];

                if (state.analyticsCharts.orderflow) {
                    state.analyticsCharts.orderflow.data.labels = labels;
                    state.analyticsCharts.orderflow.data.datasets[0].data = buyData;
                    state.analyticsCharts.orderflow.data.datasets[1].data = sellData;
                    state.analyticsCharts.orderflow.update('none');
                } else {
                    state.analyticsCharts.orderflow = new Chart(ctxRatio, {
                        type: "bar",
                        data: {
                            labels: labels,
                            datasets: [
                                {
                                    label: "% Mua Chủ Động",
                                    data: buyData,
                                    backgroundColor: "#00f090"
                                },
                                {
                                    label: "% Bán Chủ Động",
                                    data: sellData,
                                    backgroundColor: "#ff3b69"
                                }
                            ]
                        },
                        options: {
                            indexAxis: "y",
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                x: { stacked: true, max: 100, ticks: { color: "#94a3b8" }, grid: { color: "rgba(255,255,255,0.05)" } },
                                y: { stacked: true, ticks: { color: "#94a3b8" }, grid: { display: false } }
                            },
                            plugins: {
                                legend: { labels: { color: "#94a3b8" } }
                            }
                        }
                    });
                }
            }

            // D. Chart 4: Volatility & Return Comparison
            const ctxVol = document.getElementById("chart-volatility");
            if (ctxVol && window.Chart) {
                const syms = Object.keys(data.symbols_comparison || {});
                const volVals = Object.values(data.symbols_comparison || {}).map(v => v.volatility_pct);
                const retVals = Object.values(data.symbols_comparison || {}).map(v => v.return_pct);
                const labels = syms.length > 0 ? syms : ["BTC-USDT", "ETH-USDT", "SOL-USDT"];
                const vData = volVals.length > 0 ? volVals : [0.03, 0.03, 0.03];
                const rData = retVals.length > 0 ? retVals : [0.01, 0.00, 0.01];

                if (state.analyticsCharts.volatility) {
                    state.analyticsCharts.volatility.data.labels = labels;
                    state.analyticsCharts.volatility.data.datasets[0].data = vData;
                    state.analyticsCharts.volatility.data.datasets[1].data = rData;
                    state.analyticsCharts.volatility.update('none');
                } else {
                    state.analyticsCharts.volatility = new Chart(ctxVol, {
                        type: "bar",
                        data: {
                            labels: labels,
                            datasets: [
                                {
                                    label: "Biến Động (Volatility %)",
                                    data: vData,
                                    backgroundColor: "rgba(0, 210, 255, 0.8)",
                                    borderRadius: 4
                                },
                                {
                                    label: "Tỷ Suất (Return %)",
                                    data: rData,
                                    backgroundColor: "rgba(168, 85, 247, 0.8)",
                                    borderRadius: 4
                                }
                            ]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                x: { ticks: { color: "#94a3b8" }, grid: { display: false } },
                                y: { ticks: { color: "#94a3b8" }, grid: { color: "rgba(255,255,255,0.05)" } }
                            },
                            plugins: {
                                legend: { labels: { color: "#94a3b8" } }
                            }
                        }
                    });
                }
            }

            // E. Render Financial Summary Matrix Table (Atomic update)
            if (el.quantSummaryTbody && data.symbols_comparison) {
                let html = "";
                for (const [s, m] of Object.entries(data.symbols_comparison)) {
                    const of = (data.orderflow_pressure && data.orderflow_pressure[s]) ? data.orderflow_pressure[s] : { sentiment: "Neutral" };
                    html += `
                        <tr>
                            <td><strong>${s}</strong></td>
                            <td>${formatCurrency(m.open)}</td>
                            <td>${formatCurrency(m.close)}</td>
                            <td style="color:var(--accent-cyan);">${formatCurrency(m.vwap)}</td>
                            <td>${m.volatility_pct.toFixed(2)}%</td>
                            <td style="color:${m.return_pct >= 0 ? 'var(--accent-green)' : 'var(--accent-red)'};">${m.return_pct >= 0 ? '+' : ''}${m.return_pct.toFixed(2)}%</td>
                            <td>$${Math.round(m.turnover_usd).toLocaleString()}</td>
                            <td><span class="sentiment-badge">${of.sentiment || 'Neutral'}</span></td>
                        </tr>
                    `;
                }
                el.quantSummaryTbody.innerHTML = html;
            }

        } catch (e) {
            console.error("Error in fetchQuantAnalytics:", e);
        }
    }

    // G. Fetch Quant Predictions (Batch Layer) - Atomic In-Place Render
    async function fetchPredictions() {
        const tbody = document.getElementById("quant-predictions-tbody");
        if (!tbody) return;

        try {
            const res = await fetch("/api/predictions");
            const data = await res.json();
            const preds = data.predictions || [];

            if (preds.length === 0) {
                tbody.innerHTML = `<tr><td colspan="10" class="empty-state">Chưa có kết quả dự báo. Nhấn nút <strong>"Chạy Dự Báo Kế Tiếp"</strong> ở trên để thực thi mô hình lượng hóa.</td></tr>`;
                return;
            }

            const tfWeight = { "next_day": 1, "next_week": 2, "next_month": 3, "next_year": 4 };
            preds.sort((a, b) => {
                if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
                return (tfWeight[a.target_timeframe] || 99) - (tfWeight[b.target_timeframe] || 99);
            });

            let html = "";
            preds.forEach(p => {
                const retColor = p.expected_return_pct >= 0 ? "var(--accent-green)" : "var(--accent-red)";
                const retSign = p.expected_return_pct >= 0 ? "+" : "";

                let sigClass = "sentiment-badge";
                if (p.strategic_signal.includes("BUY")) sigClass += " bullish";
                else if (p.strategic_signal.includes("SELL")) sigClass += " bearish";

                html += `
                    <tr>
                        <td><strong>${p.symbol}</strong></td>
                        <td><span class="badge-tag" style="background:rgba(255,255,255,0.08);">${p.target_timeframe.replace('_', ' ').toUpperCase()}</span></td>
                        <td>${formatCurrency(p.base_price)}</td>
                        <td style="color:var(--accent-cyan); font-weight:700;">${formatCurrency(p.predicted_price)}</td>
                        <td style="font-size:0.85rem; color:var(--text-muted);">${formatCurrency(p.predicted_low)} &harr; ${formatCurrency(p.predicted_high)}</td>
                        <td style="color:${retColor}; font-weight:700;">${retSign}${p.expected_return_pct.toFixed(2)}%</td>
                        <td>${p.expected_volatility_pct.toFixed(2)}%</td>
                        <td><span style="color:${p.trend_direction === 'BULLISH' ? 'var(--accent-green)' : (p.trend_direction === 'BEARISH' ? 'var(--accent-red)' : 'var(--text-muted)')}; font-weight:600;">${p.trend_direction}</span></td>
                        <td><span class="${sigClass}">${p.strategic_signal}</span></td>
                        <td><strong style="color:var(--accent-yellow);">${p.confidence_score.toFixed(1)}%</strong></td>
                    </tr>
                `;
            });
            tbody.innerHTML = html;
        } catch (e) {
            console.error("Error in fetchPredictions:", e);
        }
    }

    // -------------------------------------------------------------------------
    // 8. BATCH PIPELINE & PREDICTION ACTION CONTROLS
    // -------------------------------------------------------------------------
    function initBatchControls() {
        if (!el.batchBtns) return;

        el.batchBtns.forEach(btn => {
            btn.addEventListener("click", async (e) => {
                e.preventDefault();
                const action = btn.getAttribute("data-action");
                if (!action || state.batchRunning) return;

                await triggerBatchAction(action);
            });
        });

        if (el.btnConsoleClear) {
            el.btnConsoleClear.addEventListener("click", () => {
                if (el.batchConsoleContent) {
                    el.batchConsoleContent.innerHTML = `<div class="log-line info">Đã dọn dẹp nhật ký. Hệ thống sẵn sàng.</div>`;
                }
            });
        }
    }

    async function triggerBatchAction(action) {
        state.batchRunning = true;
        setBatchButtonsDisabled(true);
        updateBatchStatus("running", `Đang khởi chạy tác vụ [${action.toUpperCase()}]...`);
        appendBatchLog("cmd", `> [REQUEST] Gửi yêu cầu khởi chạy tác vụ Batch: ${action}`);

        try {
            const res = await fetch("/api/batch/run", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ action: action })
            });
            const data = await res.json();

            if (res.status === 409 || data.status === "busy" || data.status === "already_running") {
                appendBatchLog("warn", `⚠️ ${data.message || 'Một tác vụ Batch khác đang chạy dở.'}`);
            } else if (data.status === "started" || data.status === "running") {
                appendBatchLog("info", `🚀 ${data.message || 'Tiến trình đã được khởi tạo thành công.'}`);
                state.batchLogIndex = 0;
                pollBatchStatus(action);
            } else {
                appendBatchLog("err", `❌ Lỗi: ${data.error || data.message || 'Không thể khởi chạy tác vụ'}`);
                state.batchRunning = false;
                setBatchButtonsDisabled(false);
                updateBatchStatus("ready", "Sẵn sàng nhận lệnh");
            }
        } catch (err) {
            appendBatchLog("err", `❌ Lỗi kết nối API: ${err.message}`);
            state.batchRunning = false;
            setBatchButtonsDisabled(false);
            updateBatchStatus("ready", "Sẵn sàng nhận lệnh");
        }
    }

    function pollBatchStatus(action) {
        if (state.batchPollTimer) clearInterval(state.batchPollTimer);

        state.batchPollTimer = setInterval(async () => {
            try {
                const res = await fetch("/api/batch/status");
                const data = await res.json();

                if (data.logs && data.logs.length > state.batchLogIndex) {
                    for (let i = state.batchLogIndex; i < data.logs.length; i++) {
                        const rawLine = typeof data.logs[i] === "string" ? data.logs[i] : (data.logs[i].line || "");
                        let logType = "info";
                        const l = rawLine.toLowerCase();
                        if (l.includes("error") || l.includes("fail") || l.includes("thất bại") || l.includes("[!]")) logType = "err";
                        else if (l.includes("warn") || l.includes("⚠️")) logType = "warn";
                        else if (l.includes("thành công") || l.includes("hoàn tất") || l.includes("success") || l.includes("🎉") || l.includes("✓")) logType = "success";
                        appendBatchLog(logType, rawLine);
                    }
                    state.batchLogIndex = data.logs.length;
                }

                const isRunning = Boolean(data.running);
                if (!isRunning) {
                    clearInterval(state.batchPollTimer);
                    state.batchPollTimer = null;
                    state.batchRunning = false;
                    setBatchButtonsDisabled(false);

                    const isSuccess = data.status === "success" || data.status === "idle";
                    if (isSuccess) {
                        updateBatchStatus("ready", `Đã hoàn tất tác vụ: [${action.toUpperCase()}]`);
                        appendBatchLog("success", `✓ Tác vụ [${action}] đã thực thi xong thành công!`);
                    } else {
                        updateBatchStatus("error", `Lỗi tác vụ: ${data.message || 'Thất bại'}`);
                        appendBatchLog("err", `✖ Tác vụ [${action}] kết thúc: ${data.message || 'Lỗi không xác định'}`);
                    }

                    // Tự động làm mới bảng dự báo & analytics sau khi tác vụ hoàn tất
                    if (action === "predict" || action === "pipeline") {
                        appendBatchLog("info", "🔄 Đang tự động nạp kết quả dự báo mới nhất vào bảng...");
                        await fetchPredictions();
                    }
                    if (action === "analytics" || action === "pipeline") {
                        await fetchQuantAnalytics();
                        if (state.activeTab === "tab-warehouse") {
                            await fetchDwQuery(true);
                        }
                    }
                }
            } catch (e) {
                console.error("Error polling batch status:", e);
            }
        }, 1200);
    }

    function setBatchButtonsDisabled(disabled) {
        if (!el.batchBtns) return;
        el.batchBtns.forEach(btn => {
            btn.disabled = disabled;
            if (disabled) {
                btn.style.opacity = "0.55";
                btn.style.cursor = "not-allowed";
            } else {
                btn.style.opacity = "";
                btn.style.cursor = "";
            }
        });
    }

    function updateBatchStatus(mode, text) {
        if (el.batchStatusText) el.batchStatusText.textContent = text;
        if (el.batchStatusDot) {
            if (mode === "running") {
                el.batchStatusDot.className = "status-dot pulsing online";
            } else if (mode === "error") {
                el.batchStatusDot.className = "status-dot offline";
            } else {
                el.batchStatusDot.className = "status-dot online";
            }
        }
    }

    function appendBatchLog(type, text) {
        if (!el.batchConsoleContent) return;
        const div = document.createElement("div");
        div.className = `log-line ${type}`;
        div.textContent = text;
        el.batchConsoleContent.appendChild(div);
        el.batchConsoleContent.scrollTop = el.batchConsoleContent.scrollHeight;
    }

    // -------------------------------------------------------------------------
    // 9. TAB 4: PIPELINE TELEMETRY LOGIC
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
            if (state.activeTab === "tab-terminal") {
                await Promise.all([
                    fetchOverview(),
                    fetchCandles(showLoader),
                    fetchOrderFlow(),
                    fetchRealTimeSignals(),
                    fetchDerivatives(),
                    fetchWhales()
                ]);
            } else if (state.activeTab === "tab-warehouse") {
                await fetchDwStats();
                await fetchDwQuery(!showLoader);
            } else if (state.activeTab === "tab-analytics") {
                await fetchQuantAnalytics();
                await fetchPredictions();
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

    // -------------------------------------------------------------------------
    // 12. STARTUP INITIALIZATION
    // -------------------------------------------------------------------------
    initTabs();
    initBatchControls();
    updateClock();
    setInterval(updateClock, 1000);
    initChart();
    checkHealth();
    refreshActiveTab(true);
    startAutoRefresh();
});
