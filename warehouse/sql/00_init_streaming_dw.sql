-- =============================================================================
-- Crypto Big Data - Real-Time Streaming Data Warehouse (All-in-One Init)
-- Target Database: PostgreSQL 16
-- Schema: warehouse
-- Architecture: Star Schema (dim_symbol, dim_alert_tier, fact_crypto_trades, fact_crypto_1m_candles)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

-- -----------------------------------------------------------------------------
-- 0. INITIALIZATION & VIEW MANAGEMENT (NON-DESTRUCTIVE)
-- Note: Legacy table drops are decoupled into explicit administrative tasks.
-- -----------------------------------------------------------------------------

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
-- 1. DIMENSION TABLE: dim_symbol
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_symbol (
    symbol_id SERIAL PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL UNIQUE,
    base_asset VARCHAR(16) NOT NULL,
    quote_asset VARCHAR(16) NOT NULL,
    exchange VARCHAR(32) DEFAULT 'OKX',
    kafka_topic VARCHAR(64) DEFAULT 'crypto-trades',
    kafka_partition INT,
    whale_threshold_usd NUMERIC(18, 2) DEFAULT 500000.00,
    price_precision INT DEFAULT 2,
    volume_precision INT DEFAULT 4,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE warehouse.dim_symbol ADD COLUMN IF NOT EXISTS kafka_topic VARCHAR(64) DEFAULT 'crypto-trades';
ALTER TABLE warehouse.dim_symbol ADD COLUMN IF NOT EXISTS kafka_partition INT;
ALTER TABLE warehouse.dim_symbol ADD COLUMN IF NOT EXISTS whale_threshold_usd NUMERIC(18, 2) DEFAULT 500000.00;
ALTER TABLE warehouse.dim_symbol ADD COLUMN IF NOT EXISTS price_precision INT DEFAULT 2;
ALTER TABLE warehouse.dim_symbol ADD COLUMN IF NOT EXISTS volume_precision INT DEFAULT 4;
ALTER TABLE warehouse.dim_symbol ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

INSERT INTO warehouse.dim_symbol (symbol, base_asset, quote_asset, exchange, kafka_topic, kafka_partition, whale_threshold_usd, price_precision, volume_precision, is_active)
VALUES 
    ('BTC-USDT', 'BTC', 'USDT', 'OKX', 'crypto-trades', 0, 500000.00, 2, 4, TRUE)
ON CONFLICT (symbol) DO UPDATE SET
    kafka_topic = EXCLUDED.kafka_topic,
    kafka_partition = EXCLUDED.kafka_partition,
    whale_threshold_usd = EXCLUDED.whale_threshold_usd,
    price_precision = EXCLUDED.price_precision,
    volume_precision = EXCLUDED.volume_precision,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP;

-- -----------------------------------------------------------------------------
-- 2. DIMENSION TABLE: dim_alert_tier
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_alert_tier (
    tier_id SERIAL PRIMARY KEY,
    tier_code VARCHAR(32) NOT NULL UNIQUE,
    tier_name VARCHAR(64) NOT NULL,
    min_value_usd NUMERIC(18, 2) NOT NULL,
    max_value_usd NUMERIC(18, 2),
    badge_color VARCHAR(16) DEFAULT '#3b82f6',
    alert_channel VARCHAR(64) DEFAULT 'ALL',
    description TEXT
);

INSERT INTO warehouse.dim_alert_tier (tier_code, tier_name, min_value_usd, max_value_usd, badge_color, alert_channel, description)
VALUES
    ('MEGA_WHALE', 'Mega Whale (>= $500K)', 500000.00, NULL, '#ef4444', 'TELEGRAM,DASHBOARD,SOUND', 'Giao dịch cá mập cực lớn từ $500,000 trở lên'),
    ('BIG_WHALE', 'Big Whale ($100K - $500K)', 100000.00, 499999.99, '#f59e0b', 'TELEGRAM,DASHBOARD', 'Giao dịch cá mập lớn từ $100,000 đến $500,000'),
    ('WHALE', 'Whale ($50K - $100K)', 50000.00, 99999.99, '#3b82f6', 'DASHBOARD', 'Giao dịch cá mập từ $50,000 đến $100,000'),
    ('LARGE_ORDER', 'Large Order ($10K - $50K)', 10000.00, 49999.99, '#10b981', 'DASHBOARD', 'Lệnh khớp quy mô lớn từ $10,000 đến $50,000')
ON CONFLICT (tier_code) DO UPDATE SET
    tier_name = EXCLUDED.tier_name,
    min_value_usd = EXCLUDED.min_value_usd,
    max_value_usd = EXCLUDED.max_value_usd,
    badge_color = EXCLUDED.badge_color,
    alert_channel = EXCLUDED.alert_channel,
    description = EXCLUDED.description;

-- -----------------------------------------------------------------------------
-- 3. FACT TABLE: fact_crypto_trades (Whale & High-Value Trades Sink)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.fact_crypto_trades (
    trade_id VARCHAR(64) PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    size NUMERIC(18, 8) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('buy', 'sell')),
    trade_value NUMERIC(18, 4) NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trade_date DATE NOT NULL,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_arrival_time TIMESTAMP WITH TIME ZONE,
    whale_tier VARCHAR(32),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE warehouse.fact_crypto_trades ADD COLUMN IF NOT EXISTS kafka_arrival_time TIMESTAMP WITH TIME ZONE;
ALTER TABLE warehouse.fact_crypto_trades ADD COLUMN IF NOT EXISTS whale_tier VARCHAR(32);

UPDATE warehouse.fact_crypto_trades
SET whale_tier = CASE 
    WHEN trade_value >= 500000 THEN 'Mega Whale (>= $500K)'
    WHEN trade_value >= 100000 THEN 'Big Whale ($100K - $500K)'
    WHEN trade_value >= 50000  THEN 'Whale ($50K - $100K)'
    ELSE 'Large Order ($10K - $50K)'
END
WHERE whale_tier IS NULL;

CREATE INDEX IF NOT EXISTS idx_fact_trades_symbol_time ON warehouse.fact_crypto_trades (symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trades_time_desc ON warehouse.fact_crypto_trades (trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trades_date ON warehouse.fact_crypto_trades (trade_date);
CREATE INDEX IF NOT EXISTS idx_fact_trades_value ON warehouse.fact_crypto_trades (trade_value DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trades_side ON warehouse.fact_crypto_trades (symbol, side);
CREATE INDEX IF NOT EXISTS idx_fact_trades_tier ON warehouse.fact_crypto_trades (whale_tier);

-- -----------------------------------------------------------------------------
-- 4. FACT TABLE: fact_crypto_1m_candles (Streaming 1m Aggregations Sink)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.fact_crypto_1m_candles (
    symbol VARCHAR(32) NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(24, 8) NOT NULL,
    trade_count BIGINT,
    buy_volume NUMERIC(24, 8) DEFAULT 0,
    sell_volume NUMERIC(24, 8) DEFAULT 0,
    vwap NUMERIC(18, 8),
    trade_imbalance NUMERIC(10, 4) DEFAULT 0,
    total_trade_value NUMERIC(24, 4),
    source VARCHAR(32) DEFAULT 'realtime_stream',
    is_closed BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, window_start)
);

-- Safe migrations for existing table schema evolution
ALTER TABLE warehouse.fact_crypto_1m_candles ADD COLUMN IF NOT EXISTS source VARCHAR(32) DEFAULT 'realtime_stream';
ALTER TABLE warehouse.fact_crypto_1m_candles ADD COLUMN IF NOT EXISTS is_closed BOOLEAN DEFAULT TRUE;
ALTER TABLE warehouse.fact_crypto_1m_candles ALTER COLUMN trade_count DROP NOT NULL;
ALTER TABLE warehouse.fact_crypto_1m_candles ALTER COLUMN vwap DROP NOT NULL;
ALTER TABLE warehouse.fact_crypto_1m_candles ALTER COLUMN total_trade_value DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fact_1m_symbol_window ON warehouse.fact_crypto_1m_candles (symbol, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_1m_window_start ON warehouse.fact_crypto_1m_candles (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_1m_symbol_created ON warehouse.fact_crypto_1m_candles (symbol, created_at DESC);

-- -----------------------------------------------------------------------------
-- 5. ANALYTICAL STREAMING VIEWS
-- -----------------------------------------------------------------------------

-- View 1: Whale Radar
CREATE OR REPLACE VIEW warehouse.vw_whale_trades AS
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
            WHEN t.trade_value >= 500000 THEN 'Mega Whale (>= $500K)'
            WHEN t.trade_value >= 100000 THEN 'Big Whale ($100K - $500K)'
            WHEN t.trade_value >= 50000  THEN 'Whale ($50K - $100K)'
            ELSE 'Large Order ($10K - $50K)'
        END
    ) AS whale_tier,
    ROUND(EXTRACT(EPOCH FROM (t.created_at - t.trade_time))::numeric, 2) AS streaming_latency_sec,
    t.kafka_partition,
    t.kafka_offset
FROM warehouse.fact_crypto_trades t
WHERE t.trade_value >= 10000
ORDER BY t.trade_time DESC;

-- View 2: Rolling 5M Momentum
CREATE OR REPLACE VIEW warehouse.vw_rolling_5m_metrics AS
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
    FROM warehouse.fact_crypto_1m_candles
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

-- View 3: Order Flow Pressure & CVD
CREATE OR REPLACE VIEW warehouse.vw_order_flow_pressure AS
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
FROM warehouse.fact_crypto_1m_candles
GROUP BY symbol;

-- View 4: Streaming Volatility (Parkinson)
CREATE OR REPLACE VIEW warehouse.vw_streaming_volatility AS
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
    FROM warehouse.fact_crypto_1m_candles
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

-- View 5: Streaming Pipeline Telemetry & Health
CREATE OR REPLACE VIEW warehouse.vw_streaming_pipeline_telemetry AS
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
FROM warehouse.dim_symbol s
LEFT JOIN (
    SELECT 
        symbol,
        COUNT(*) AS total_candles,
        MAX(window_end) AS latest_candle_window,
        MAX(created_at) AS last_candle_received_at
    FROM warehouse.fact_crypto_1m_candles
    GROUP BY symbol
) c ON s.symbol = c.symbol
LEFT JOIN (
    SELECT 
        symbol,
        COUNT(*) AS total_whales,
        MAX(trade_time) AS latest_whale_trade_time
    FROM warehouse.fact_crypto_trades
    GROUP BY symbol
) t ON s.symbol = t.symbol;

-- View 6: Hourly Summary
CREATE OR REPLACE VIEW warehouse.vw_hourly_market_summary AS
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
FROM warehouse.fact_crypto_1m_candles
GROUP BY symbol, date_trunc('hour', window_start)
ORDER BY hour_bucket DESC, symbol;

-- View 7: Daily Summary
CREATE OR REPLACE VIEW warehouse.vw_daily_market_summary AS
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
FROM warehouse.fact_crypto_1m_candles
GROUP BY symbol, date_trunc('day', window_start)::date
ORDER BY trade_date DESC, symbol;

-- View 8: Weekly Summary
CREATE OR REPLACE VIEW warehouse.vw_weekly_market_summary AS
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
FROM warehouse.fact_crypto_1m_candles
GROUP BY symbol, date_trunc('week', window_start)::date
ORDER BY week_start_date DESC, symbol;

-- View 9: Monthly Summary
CREATE OR REPLACE VIEW warehouse.vw_monthly_market_summary AS
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
FROM warehouse.fact_crypto_1m_candles
GROUP BY symbol, date_trunc('month', window_start)::date
ORDER BY month_start_date DESC, symbol;

-- View 10: Yearly Summary
CREATE OR REPLACE VIEW warehouse.vw_yearly_market_summary AS
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
FROM warehouse.fact_crypto_1m_candles
GROUP BY symbol, date_trunc('year', window_start)::date
ORDER BY year_start_date DESC, symbol;

-- -----------------------------------------------------------------------------
-- 6. FACT TABLE: fact_btc_predictions (XGBoost Real-Time Inference Sink)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.fact_btc_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL DEFAULT 'BTC-USDT',
    feature_timestamp TIMESTAMPTZ NOT NULL,
    prediction_timestamp TIMESTAMPTZ NOT NULL,
    target_timestamp TIMESTAMPTZ NOT NULL,
    predicted_direction INTEGER NOT NULL, -- 1: UP, 0: NOT_UP
    probability_up DOUBLE PRECISION NOT NULL,
    probability_not_up DOUBLE PRECISION NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_name VARCHAR(100) NOT NULL DEFAULT 'btc_xgboost_direction',
    current_price DOUBLE PRECISION,
    horizon_minutes INTEGER NOT NULL DEFAULT 5,
    actual_direction INTEGER,
    actual_future_price DOUBLE PRECISION,
    is_correct BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_btc_prediction UNIQUE (symbol, feature_timestamp, model_version)
);

CREATE INDEX IF NOT EXISTS idx_btc_pred_feature_ts ON warehouse.fact_btc_predictions (feature_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_btc_pred_target_ts ON warehouse.fact_btc_predictions (target_timestamp DESC);

-- -----------------------------------------------------------------------------
-- 7. FACT TABLE: fact_btc_model_evaluation (Model Benchmark & Evaluation Archive)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.fact_btc_model_evaluation (
    eval_id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    evaluation_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    target_symbol VARCHAR(20) NOT NULL DEFAULT 'BTC-USDT',
    test_samples INTEGER NOT NULL,
    accuracy DOUBLE PRECISION NOT NULL,
    precision DOUBLE PRECISION NOT NULL,
    recall DOUBLE PRECISION NOT NULL,
    f1_score DOUBLE PRECISION NOT NULL,
    balanced_accuracy DOUBLE PRECISION NOT NULL,
    roc_auc DOUBLE PRECISION,
    baseline_accuracy DOUBLE PRECISION NOT NULL,
    accuracy_lift_pct DOUBLE PRECISION NOT NULL,
    confusion_matrix JSONB,
    feature_importance JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

