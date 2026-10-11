-- =============================================================================
-- Fact Table: fact_crypto_funding_rates
-- Description: Bảng Fact lưu trữ tỷ lệ phí cấp vốn phái sinh (Funding Rates)
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS fact_crypto_funding_rates (
    symbol VARCHAR(32) NOT NULL,
    funding_time TIMESTAMP WITH TIME ZONE NOT NULL,
    next_funding_time TIMESTAMP WITH TIME ZONE NOT NULL,
    funding_rate NUMERIC(12, 8) NOT NULL,
    predicted_funding_rate NUMERIC(12, 8) NOT NULL,
    exchange VARCHAR(32) DEFAULT 'OKX',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, funding_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_funding_symbol_time ON fact_crypto_funding_rates (symbol, funding_time DESC);
