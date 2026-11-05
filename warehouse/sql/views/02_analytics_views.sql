-- =============================================================================
-- Crypto Big Data - Real-Time Streaming Analytics & Data Mart Views
-- Database: PostgreSQL 16
-- Description: Hệ thống Views phục vụ Web Trading Dashboard, Power BI,
--              Quant Trading và Giám sát vận hành Pipeline Streaming
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

-- Drop views if exist to allow clean column type/schema updates
DROP VIEW IF EXISTS warehouse.vw_whale_trades CASCADE;
DROP VIEW IF EXISTS warehouse.vw_rolling_5m_metrics CASCADE;
DROP VIEW IF EXISTS warehouse.vw_order_flow_pressure CASCADE;
DROP VIEW IF EXISTS warehouse.vw_streaming_volatility CASCADE;
DROP VIEW IF EXISTS warehouse.vw_streaming_pipeline_telemetry CASCADE;
DROP VIEW IF EXISTS warehouse.vw_hourly_market_summary CASCADE;
DROP VIEW IF EXISTS warehouse.vw_daily_market_summary CASCADE;
DROP VIEW IF EXISTS warehouse.vw_weekly_market_summary CASCADE;
DROP VIEW IF EXISTS warehouse.vw_monthly_market_summary CASCADE;
DROP VIEW IF EXISTS warehouse.vw_yearly_market_summary CASCADE;

-- -----------------------------------------------------------------------------
-- 1. VIEW: Large Orders / Whale Transactions (Radar Dòng tiền Cá mập)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_whale_trades AS
SELECT 
    t.trade_id,
    t.symbol,
    t.side,
    t.price,
    t.size,
    t.trade_value AS trade_value_usd,
    t.trade_time,
    t.trade_date,
    COALESCE(
        t.whale_tier::text,
        CASE 
            WHEN t.trade_value >= 1000000 THEN 'Mega Whale (>= $1M)'
            WHEN t.trade_value >= 500000  THEN 'Big Whale ($500K - $1M)'
            WHEN t.trade_value >= 250000  THEN 'Whale ($250K - $500K)'
            ELSE 'Large Order ($100K - $250K)'
        END
    ) AS whale_tier,
    ROUND(EXTRACT(EPOCH FROM (t.created_at - t.trade_time))::numeric, 2) AS streaming_latency_sec,
    t.kafka_partition,
    t.kafka_offset
FROM fact_crypto_trades t
WHERE t.trade_value >= 100000
ORDER BY t.trade_time DESC;


-- -----------------------------------------------------------------------------
-- 2. VIEW: Rolling 5-Minute Streaming Momentum (Tín hiệu Lướt sóng Scalping 5M)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_rolling_5m_metrics AS
WITH ranked_candles AS (
    SELECT 
        symbol,
        window_start,
        window_end,
        open,
        high,
        low,
        close,
        volume,
        buy_volume,
        sell_volume,
        vwap,
        trade_imbalance,
        total_trade_value,
        trade_count,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC) AS rn
    FROM fact_crypto_1m_candles
),
recent_5m AS (
    SELECT * FROM ranked_candles WHERE rn <= 5
)
SELECT 
    symbol,
    MIN(window_start) AS window_5m_start,
    MAX(window_end) AS window_5m_end,
    (array_agg(open ORDER BY window_start ASC))[1] AS open_5m,
    MAX(high) AS high_5m,
    MIN(low) AS low_5m,
    (array_agg(close ORDER BY window_start DESC))[1] AS close_5m,
    SUM(volume) AS volume_5m,
    SUM(total_trade_value) AS turnover_5m_usd,
    SUM(trade_count) AS trades_5m,
    ROUND(((array_agg(close ORDER BY window_start DESC))[1] - (array_agg(open ORDER BY window_start ASC))[1]) 
          / NULLIF((array_agg(open ORDER BY window_start ASC))[1], 0) * 100, 3) AS return_5m_pct,
    ROUND(SUM(total_trade_value) / NULLIF(SUM(volume), 0), 2) AS vwap_5m,
    ROUND(AVG(trade_imbalance), 4) AS avg_imbalance_5m,
    ROUND(SUM(buy_volume) / NULLIF(SUM(buy_volume) + SUM(sell_volume), 0) * 100, 1) AS buy_ratio_5m_pct,
    CASE 
        WHEN AVG(trade_imbalance) > 0.25 AND ((array_agg(close ORDER BY window_start DESC))[1] > (array_agg(open ORDER BY window_start ASC))[1])
            THEN 'STRONG BUY MOMENTUM'
        WHEN AVG(trade_imbalance) < -0.25 AND ((array_agg(close ORDER BY window_start DESC))[1] < (array_agg(open ORDER BY window_start ASC))[1])
            THEN 'STRONG SELL MOMENTUM'
        ELSE 'NEUTRAL CONSOLIDATION'
    END AS scalping_signal
FROM recent_5m
GROUP BY symbol;


-- -----------------------------------------------------------------------------
-- 3. VIEW: Real-time Order Flow Pressure & CVD (Áp lực Mua/Bán & Sentiment)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_order_flow_pressure AS
SELECT 
    symbol,
    SUM(buy_volume) AS total_buy_volume,
    SUM(sell_volume) AS total_sell_volume,
    ROUND(SUM(buy_volume) - SUM(sell_volume), 4) AS cumulative_volume_delta,
    ROUND(SUM(buy_volume) / NULLIF(SUM(sell_volume), 0), 3) AS buy_to_sell_ratio,
    ROUND(AVG(trade_imbalance), 4) AS net_order_imbalance,
    ROUND(SUM(buy_volume) / NULLIF(SUM(buy_volume) + SUM(sell_volume), 0) * 100, 1) AS buy_volume_pct,
    ROUND(SUM(sell_volume) / NULLIF(SUM(buy_volume) + SUM(sell_volume), 0) * 100, 1) AS sell_volume_pct,
    CASE 
        WHEN AVG(trade_imbalance) > 0.2 THEN 'Bullish Pressure (Mua manh)'
        WHEN AVG(trade_imbalance) < -0.2 THEN 'Bearish Pressure (Ban manh)'
        ELSE 'Neutral (Can bang)'
    END AS market_sentiment
FROM fact_crypto_1m_candles
GROUP BY symbol;


-- -----------------------------------------------------------------------------
-- 4. VIEW: Streaming Volatility & Risk Metrics (Đo lường biến động Parkinson)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_streaming_volatility AS
WITH candle_spreads AS (
    SELECT 
        symbol,
        window_start,
        open,
        high,
        low,
        close,
        (high - low) AS candle_spread,
        ROUND((high - low) / NULLIF(low, 0) * 100, 3) AS spread_pct,
        CASE 
            WHEN low > 0 AND high >= low THEN
                POWER(LN(high / low), 2) / (4.0 * LN(2.0))
            ELSE 0.0
        END AS parkinson_term
    FROM fact_crypto_1m_candles
)
SELECT 
    symbol,
    COUNT(*) AS evaluated_candles,
    ROUND(AVG(candle_spread), 4) AS avg_candle_spread_usd,
    ROUND(MAX(spread_pct), 3) AS max_1m_spread_pct,
    ROUND(AVG(spread_pct), 3) AS avg_1m_spread_pct,
    ROUND(SQRT(AVG(parkinson_term)) * 100, 4) AS parkinson_volatility_pct,
    CASE 
        WHEN SQRT(AVG(parkinson_term)) * 100 >= 1.5 THEN 'EXTREME VOLATILITY'
        WHEN SQRT(AVG(parkinson_term)) * 100 >= 0.8 THEN 'HIGH VOLATILITY'
        WHEN SQRT(AVG(parkinson_term)) * 100 >= 0.3 THEN 'MODERATE VOLATILITY'
        ELSE 'LOW VOLATILITY'
    END AS volatility_regime
FROM candle_spreads
GROUP BY symbol;


-- -----------------------------------------------------------------------------
-- 5. VIEW: Streaming Pipeline Telemetry & Health (Giám sát độ trễ & Trạng thái)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_streaming_pipeline_telemetry AS
SELECT 
    s.symbol,
    s.exchange,
    s.kafka_topic,
    s.kafka_partition,
    s.whale_threshold_usd,
    COALESCE(c.total_candles, 0) AS total_1m_candles,
    COALESCE(t.total_whales, 0) AS total_whale_alerts,
    c.latest_candle_window,
    c.last_candle_received_at,
    t.latest_whale_trade_time,
    ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - c.latest_candle_window))::numeric, 0) AS stream_lag_seconds,
    CASE 
        WHEN c.latest_candle_window IS NULL THEN 'NO DATA YET'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - c.latest_candle_window)) <= 180 THEN 'ONLINE & STREAMING (OK)'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - c.latest_candle_window)) <= 600 THEN 'WARN (MINOR LAG)'
        ELSE 'OFFLINE / STREAM PAUSED'
    END AS pipeline_status
FROM dim_symbol s
LEFT JOIN (
    SELECT 
        symbol,
        COUNT(*) AS total_candles,
        MAX(window_end) AS latest_candle_window,
        MAX(created_at) AS last_candle_received_at
    FROM fact_crypto_1m_candles
    GROUP BY symbol
) c ON s.symbol = c.symbol
LEFT JOIN (
    SELECT 
        symbol,
        COUNT(*) AS total_whales,
        MAX(trade_time) AS latest_whale_trade_time
    FROM fact_crypto_trades
    GROUP BY symbol
) t ON s.symbol = t.symbol;


-- -----------------------------------------------------------------------------
-- 6. VIEW: Hourly Market Summary (Tổng hợp thị trường theo Giờ)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_hourly_market_summary AS
SELECT 
    symbol,
    date_trunc('hour', window_start) AS hour_bucket,
    (array_agg(open ORDER BY window_start ASC))[1] AS open_price,
    MAX(high) AS high_price,
    MIN(low) AS low_price,
    (array_agg(close ORDER BY window_start DESC))[1] AS close_price,
    SUM(volume) AS total_volume,
    SUM(total_trade_value) AS total_turnover_usd,
    SUM(trade_count) AS total_trades,
    ROUND(SUM(total_trade_value) / NULLIF(SUM(volume), 0), 2) AS hourly_vwap,
    ROUND(AVG(trade_imbalance), 4) AS avg_trade_imbalance
FROM fact_crypto_1m_candles
GROUP BY symbol, date_trunc('hour', window_start)
ORDER BY hour_bucket DESC, symbol;


-- -----------------------------------------------------------------------------
-- 7. VIEW: Daily Market Summary (Tổng hợp thị trường theo Ngày)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_daily_market_summary AS
SELECT 
    symbol,
    date_trunc('day', window_start)::date AS trade_date,
    (array_agg(open ORDER BY window_start ASC))[1] AS open_price,
    MAX(high) AS high_price,
    MIN(low) AS low_price,
    (array_agg(close ORDER BY window_start DESC))[1] AS close_price,
    SUM(volume) AS total_volume,
    SUM(total_trade_value) AS total_turnover_usd,
    SUM(trade_count) AS total_trades,
    ROUND(SUM(total_trade_value) / NULLIF(SUM(volume), 0), 2) AS daily_vwap,
    ROUND(((array_agg(close ORDER BY window_start DESC))[1] - (array_agg(open ORDER BY window_start ASC))[1]) 
          / NULLIF((array_agg(open ORDER BY window_start ASC))[1], 0) * 100, 2) AS return_pct,
    ROUND((MAX(high) - MIN(low)) / NULLIF(MIN(low), 0) * 100, 2) AS volatility_pct
FROM fact_crypto_1m_candles
GROUP BY symbol, date_trunc('day', window_start)::date
ORDER BY trade_date DESC, symbol;


-- -----------------------------------------------------------------------------
-- 8. VIEW: Weekly Market Summary (Tổng hợp thị trường theo Tuần)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_weekly_market_summary AS
SELECT 
    symbol,
    date_trunc('week', window_start)::date AS week_start_date,
    (array_agg(open ORDER BY window_start ASC))[1] AS open_price,
    MAX(high) AS high_price,
    MIN(low) AS low_price,
    (array_agg(close ORDER BY window_start DESC))[1] AS close_price,
    SUM(volume) AS total_volume,
    SUM(total_trade_value) AS total_turnover_usd,
    SUM(trade_count) AS total_trades,
    ROUND(SUM(total_trade_value) / NULLIF(SUM(volume), 0), 2) AS weekly_vwap,
    ROUND(((array_agg(close ORDER BY window_start DESC))[1] - (array_agg(open ORDER BY window_start ASC))[1]) 
          / NULLIF((array_agg(open ORDER BY window_start ASC))[1], 0) * 100, 2) AS weekly_return_pct
FROM fact_crypto_1m_candles
GROUP BY symbol, date_trunc('week', window_start)::date
ORDER BY week_start_date DESC, symbol;


-- -----------------------------------------------------------------------------
-- 9. VIEW: Monthly Market Summary (Tổng hợp thị trường theo Tháng)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_monthly_market_summary AS
SELECT 
    symbol,
    date_trunc('month', window_start)::date AS month_start_date,
    (array_agg(open ORDER BY window_start ASC))[1] AS open_price,
    MAX(high) AS high_price,
    MIN(low) AS low_price,
    (array_agg(close ORDER BY window_start DESC))[1] AS close_price,
    SUM(volume) AS total_volume,
    SUM(total_trade_value) AS total_turnover_usd,
    SUM(trade_count) AS total_trades,
    ROUND(SUM(total_trade_value) / NULLIF(SUM(volume), 0), 2) AS monthly_vwap,
    ROUND(((array_agg(close ORDER BY window_start DESC))[1] - (array_agg(open ORDER BY window_start ASC))[1]) 
          / NULLIF((array_agg(open ORDER BY window_start ASC))[1], 0) * 100, 2) AS monthly_return_pct,
    ROUND((MAX(high) - MIN(low)) / NULLIF(MIN(low), 0) * 100, 2) AS monthly_volatility_pct
FROM fact_crypto_1m_candles
GROUP BY symbol, date_trunc('month', window_start)::date
ORDER BY month_start_date DESC, symbol;


-- -----------------------------------------------------------------------------
-- 10. VIEW: Yearly Market Summary (Tổng hợp thị trường theo Năm)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_yearly_market_summary AS
SELECT 
    symbol,
    date_trunc('year', window_start)::date AS year_start_date,
    (array_agg(open ORDER BY window_start ASC))[1] AS open_price,
    MAX(high) AS high_price,
    MIN(low) AS low_price,
    (array_agg(close ORDER BY window_start DESC))[1] AS close_price,
    SUM(volume) AS total_volume,
    SUM(total_trade_value) AS total_turnover_usd,
    SUM(trade_count) AS total_trades,
    ROUND(SUM(total_trade_value) / NULLIF(SUM(volume), 0), 2) AS yearly_vwap,
    ROUND(((array_agg(close ORDER BY window_start DESC))[1] - (array_agg(open ORDER BY window_start ASC))[1]) 
          / NULLIF((array_agg(open ORDER BY window_start ASC))[1], 0) * 100, 2) AS yearly_return_pct,
    ROUND((MAX(high) - MIN(low)) / NULLIF(MIN(low), 0) * 100, 2) AS yearly_volatility_pct
FROM fact_crypto_1m_candles
GROUP BY symbol, date_trunc('year', window_start)::date
ORDER BY year_start_date DESC, symbol;
