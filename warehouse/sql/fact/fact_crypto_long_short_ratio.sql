-- =============================================================================
-- Fact Table: fact_crypto_long_short_ratio
-- Description: Bảng Fact lưu trữ tỷ lệ vị thế Long vs Short
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS fact_crypto_long_short_ratio (
    symbol VARCHAR(32) NOT NULL,
    snapshot_time TIMESTAMP WITH TIME ZONE NOT NULL,
    long_account_ratio NUMERIC(6, 4) NOT NULL,
    short_account_ratio NUMERIC(6, 4) NOT NULL,
    long_short_ratio NUMERIC(8, 4) NOT NULL,
    exchange VARCHAR(32) DEFAULT 'OKX',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_ls_symbol_time ON fact_crypto_long_short_ratio (symbol, snapshot_time DESC);
