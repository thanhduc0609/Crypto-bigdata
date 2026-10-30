-- =============================================================================
-- Dimension Table: dim_symbol
-- Description: Danh mục các cặp tiền mã hóa giao dịch cho Real-Time Streaming
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
    kafka_topic VARCHAR(64) DEFAULT 'crypto-trades',
    kafka_partition INT,
    whale_threshold_usd NUMERIC(18, 2) DEFAULT 500000.00,
    price_precision INT DEFAULT 2,
    volume_precision INT DEFAULT 4,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Bổ sung các cột metadata streaming nếu bảng đã tồn tại từ trước
ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS kafka_topic VARCHAR(64) DEFAULT 'crypto-trades';
ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS kafka_partition INT;
ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS whale_threshold_usd NUMERIC(18, 2) DEFAULT 500000.00;
ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS price_precision INT DEFAULT 2;
ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS volume_precision INT DEFAULT 4;
ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

-- Seed / Cập nhật cấu hình phân luồng Kafka và ngưỡng cảnh báo cho BTC-USDT
INSERT INTO dim_symbol (symbol, base_asset, quote_asset, exchange, kafka_topic, kafka_partition, whale_threshold_usd, price_precision, volume_precision, is_active)
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

