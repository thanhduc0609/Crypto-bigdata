-- =============================================================================
-- Fact Table: fact_crypto_1m_candles
-- Description: Bảng Fact lưu trữ nến 1 phút và các chỉ số định lượng streaming
--              (OHLCV, VWAP, Order Flow Imbalance, Buy/Sell Volumes)
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS fact_crypto_1m_candles (
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

-- Safe migrations for schema evolution without DROP/TRUNCATE
ALTER TABLE fact_crypto_1m_candles ADD COLUMN IF NOT EXISTS source VARCHAR(32) DEFAULT 'realtime_stream';
ALTER TABLE fact_crypto_1m_candles ADD COLUMN IF NOT EXISTS is_closed BOOLEAN DEFAULT TRUE;
ALTER TABLE fact_crypto_1m_candles ALTER COLUMN trade_count DROP NOT NULL;
ALTER TABLE fact_crypto_1m_candles ALTER COLUMN vwap DROP NOT NULL;
ALTER TABLE fact_crypto_1m_candles ALTER COLUMN total_trade_value DROP NOT NULL;

-- Indexes for Fast Streaming Lookups & Time-series Queries
CREATE INDEX IF NOT EXISTS idx_fact_1m_symbol_window ON fact_crypto_1m_candles (symbol, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_1m_window_start ON fact_crypto_1m_candles (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_1m_symbol_created ON fact_crypto_1m_candles (symbol, created_at DESC);

