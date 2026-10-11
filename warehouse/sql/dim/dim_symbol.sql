-- =============================================================================
-- Dimension Table: dim_symbol
-- Description: Danh mục các cặp tiền mã hóa giao dịch (Trading Pairs)
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id SERIAL PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL UNIQUE,
    base_asset VARCHAR(16) NOT NULL,
    quote_asset VARCHAR(16) NOT NULL,
    exchange VARCHAR(32) DEFAULT 'OKX',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed basic trading symbols
INSERT INTO dim_symbol (symbol, base_asset, quote_asset, exchange)
VALUES 
    ('BTC-USDT', 'BTC', 'USDT', 'OKX'),
    ('ETH-USDT', 'ETH', 'USDT', 'OKX'),
    ('SOL-USDT', 'SOL', 'USDT', 'OKX')
ON CONFLICT (symbol) DO NOTHING;
