-- =============================================================================
-- Fact Table: fact_crypto_open_interest
-- Description: Bảng Fact lưu trữ khối lượng hợp đồng mở (Open Interest)
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS fact_crypto_open_interest (
    symbol VARCHAR(32) NOT NULL,
    snapshot_time TIMESTAMP WITH TIME ZONE NOT NULL,
    oi_contracts NUMERIC(24, 8) NOT NULL,
    oi_usd NUMERIC(24, 4) NOT NULL,
    exchange VARCHAR(32) DEFAULT 'OKX',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_oi_symbol_time ON fact_crypto_open_interest (symbol, snapshot_time DESC);
