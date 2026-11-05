-- =============================================================================
-- Fact Table: fact_crypto_trades
-- Description: Bảng Fact lưu trữ chi tiết các giao dịch lớn/cá mập streaming
--              được nạp liên tục từ Spark Structured Streaming Speed Layer
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
    kafka_arrival_time TIMESTAMP WITH TIME ZONE,
    whale_tier VARCHAR(32),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Bổ sung các cột telemetry streaming nếu bảng đã tồn tại từ trước
ALTER TABLE fact_crypto_trades ADD COLUMN IF NOT EXISTS kafka_arrival_time TIMESTAMP WITH TIME ZONE;
ALTER TABLE fact_crypto_trades ADD COLUMN IF NOT EXISTS whale_tier VARCHAR(32);

-- Cập nhật cấp bậc cá mập cho toàn bộ bản ghi theo phân cấp định lượng
UPDATE fact_crypto_trades
SET whale_tier = CASE 
    WHEN trade_value >= 1000000 THEN 'Mega Whale (>= $1M)'
    WHEN trade_value >= 500000  THEN 'Big Whale ($500K - $1M)'
    WHEN trade_value >= 250000  THEN 'Whale ($250K - $500K)'
    ELSE 'Large Order ($100K - $250K)'
END;

-- Indexes for Streaming Query Optimization & Radar Lookups
CREATE INDEX IF NOT EXISTS idx_fact_trades_symbol_time ON fact_crypto_trades (symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trades_time_desc ON fact_crypto_trades (trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trades_date ON fact_crypto_trades (trade_date);
CREATE INDEX IF NOT EXISTS idx_fact_trades_value ON fact_crypto_trades (trade_value DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trades_side ON fact_crypto_trades (symbol, side);
CREATE INDEX IF NOT EXISTS idx_fact_trades_tier ON fact_crypto_trades (whale_tier);
