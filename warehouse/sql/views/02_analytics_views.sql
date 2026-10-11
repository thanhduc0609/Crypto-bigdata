-- =============================================================================
-- Crypto Big Data - Business & Power BI Analytics Views
-- Database: PostgreSQL 16
-- =============================================================================

SET search_path TO warehouse, public;

-- -----------------------------------------------------------------------------
-- 1. VIEW: Large Orders / Whale Transactions (Dòng tiền cá mập)
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
    CASE 
        WHEN t.trade_value >= 2000000 THEN 'Mega Whale (>= $2M)'
        WHEN t.trade_value >= 1000000 THEN 'Big Whale ($1M - $2M)'
        ELSE 'Whale ($500K - $1M)'
    END AS whale_tier
FROM fact_crypto_trades t
WHERE t.trade_value >= 500000
ORDER BY t.trade_time DESC;


-- -----------------------------------------------------------------------------
-- 2. VIEW: Hourly Market Summary (Tổng hợp thị trường theo Giờ)
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
-- 3. VIEW: Daily Market Summary (Tổng hợp thị trường theo Ngày)
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
-- 4. VIEW: Weekly Market Summary (Tổng hợp thị trường theo Tuần)
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
-- 5. VIEW: Monthly Market Summary (Tổng hợp thị trường theo Tháng)
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
-- 6. VIEW: Real-time Order Flow Pressure (Áp lực Mua/Bán & Sentiment)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_order_flow_pressure AS
SELECT 
    symbol,
    SUM(buy_volume) AS total_buy_volume,
    SUM(sell_volume) AS total_sell_volume,
    ROUND(SUM(buy_volume) / NULLIF(SUM(sell_volume), 0), 3) AS buy_to_sell_ratio,
    ROUND(AVG(trade_imbalance), 4) AS net_order_imbalance,
    CASE 
        WHEN AVG(trade_imbalance) > 0.2 THEN 'Bullish Pressure (Mua manh)'
        WHEN AVG(trade_imbalance) < -0.2 THEN 'Bearish Pressure (Ban manh)'
        ELSE 'Neutral (Can bang)'
    END AS market_sentiment
FROM fact_crypto_1m_candles
GROUP BY symbol;


-- -----------------------------------------------------------------------------
-- 7. VIEW: Yearly Market Summary (Tổng hợp thị trường theo Năm)
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

