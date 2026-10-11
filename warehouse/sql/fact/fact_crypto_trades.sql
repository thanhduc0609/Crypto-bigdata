-- =============================================================================
-- Fact Table: fact_crypto_trades
-- Description: Bảng Fact lưu trữ chi tiết từng giao dịch khớp lệnh thực tế (Granular Level)
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS fact_crypto_trades (
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Query Optimization
CREATE INDEX IF NOT EXISTS idx_fact_trades_symbol_time ON fact_crypto_trades (symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trades_date ON fact_crypto_trades (trade_date);
CREATE INDEX IF NOT EXISTS idx_fact_trades_value ON fact_crypto_trades (trade_value DESC);
