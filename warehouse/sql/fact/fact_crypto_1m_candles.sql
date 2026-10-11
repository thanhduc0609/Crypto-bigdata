-- =============================================================================
-- Fact Table: fact_crypto_1m_candles
-- Description: Bảng Fact lưu trữ nến 1 phút và các chỉ số tài chính tổng hợp (OHLC, VWAP, Imbalance)
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
    trade_count BIGINT NOT NULL,
    buy_volume NUMERIC(24, 8) DEFAULT 0,
    sell_volume NUMERIC(24, 8) DEFAULT 0,
    vwap NUMERIC(18, 8) NOT NULL,
    trade_imbalance NUMERIC(10, 4) DEFAULT 0,
    total_trade_value NUMERIC(24, 4) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, window_start)
);

-- Indexes for Query Optimization
CREATE INDEX IF NOT EXISTS idx_fact_1m_symbol_window ON fact_crypto_1m_candles (symbol, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_1m_window_start ON fact_crypto_1m_candles (window_start DESC);
