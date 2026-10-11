-- =============================================================================
-- Fact Table: fact_crypto_liquidations
-- Description: Bảng Fact lưu trữ các sự kiện thanh lý vị thế đòn bẩy lớn
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS fact_crypto_liquidations (
    liquidation_id VARCHAR(64) PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('buy', 'sell')),
    position_type VARCHAR(10) NOT NULL CHECK (position_type IN ('long', 'short')),
    price NUMERIC(18, 8) NOT NULL,
    size NUMERIC(18, 8) NOT NULL,
    liquidation_value NUMERIC(18, 4) NOT NULL,
    liquidation_time TIMESTAMP WITH TIME ZONE NOT NULL,
    exchange VARCHAR(32) DEFAULT 'OKX',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_liq_symbol_time ON fact_crypto_liquidations (symbol, liquidation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_liq_value ON fact_crypto_liquidations (liquidation_value DESC);
