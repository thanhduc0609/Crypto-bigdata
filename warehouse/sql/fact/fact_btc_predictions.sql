-- =============================================================================
-- Fact Table: fact_btc_predictions
-- Stores near-real-time 5-minute price direction predictions from XGBoost
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_btc_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL DEFAULT 'BTC-USDT',
    feature_timestamp TIMESTAMPTZ NOT NULL,
    prediction_timestamp TIMESTAMPTZ NOT NULL,
    target_timestamp TIMESTAMPTZ NOT NULL,
    predicted_direction INTEGER NOT NULL, -- 1: UP, 0: NOT_UP
    probability_up DOUBLE PRECISION NOT NULL,
    probability_not_up DOUBLE PRECISION NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_name VARCHAR(100) NOT NULL DEFAULT 'btc_xgboost_direction',
    current_price DOUBLE PRECISION,
    horizon_minutes INTEGER NOT NULL DEFAULT 5,
    actual_direction INTEGER,
    actual_future_price DOUBLE PRECISION,
    is_correct BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_btc_prediction UNIQUE (symbol, feature_timestamp, model_version)
);

CREATE INDEX IF NOT EXISTS idx_btc_pred_feature_ts ON warehouse.fact_btc_predictions (feature_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_btc_pred_target_ts ON warehouse.fact_btc_predictions (target_timestamp DESC);
