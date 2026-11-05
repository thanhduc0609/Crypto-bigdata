-- =============================================================================
-- Dimension Table: dim_alert_tier
-- Description: Bảng phân loại cấp độ cảnh báo cá mập và biến động Streaming
-- Database: PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;

CREATE TABLE IF NOT EXISTS dim_alert_tier (
    tier_id SERIAL PRIMARY KEY,
    tier_code VARCHAR(32) NOT NULL UNIQUE,
    tier_name VARCHAR(64) NOT NULL,
    min_value_usd NUMERIC(18, 2) NOT NULL,
    max_value_usd NUMERIC(18, 2),
    badge_color VARCHAR(16) DEFAULT '#3b82f6',
    alert_channel VARCHAR(64) DEFAULT 'ALL',
    description TEXT
);

-- Seed predefined streaming alert tiers
INSERT INTO dim_alert_tier (tier_code, tier_name, min_value_usd, max_value_usd, badge_color, alert_channel, description)
VALUES
    ('MEGA_WHALE', 'Mega Whale (>= $1M)', 1000000.00, NULL, '#ef4444', 'TELEGRAM,DASHBOARD,SOUND', 'Giao dịch cá mập cực lớn từ $1,000,000 trở lên'),
    ('BIG_WHALE', 'Big Whale ($500K - $1M)', 500000.00, 999999.99, '#f59e0b', 'TELEGRAM,DASHBOARD', 'Giao dịch cá mập lớn từ $500,000 đến $1,000,000'),
    ('WHALE', 'Whale ($250K - $500K)', 250000.00, 499999.99, '#3b82f6', 'DASHBOARD', 'Giao dịch cá mập từ $250,000 đến $500,000'),
    ('LARGE_ORDER', 'Large Order ($100K - $250K)', 100000.00, 249999.99, '#10b981', 'DASHBOARD', 'Lệnh khớp quy mô lớn từ $100,000 đến $250,000')
ON CONFLICT (tier_code) DO UPDATE SET
    tier_name = EXCLUDED.tier_name,
    min_value_usd = EXCLUDED.min_value_usd,
    max_value_usd = EXCLUDED.max_value_usd,
    badge_color = EXCLUDED.badge_color,
    alert_channel = EXCLUDED.alert_channel,
    description = EXCLUDED.description;
