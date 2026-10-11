-- =============================================================================
-- Fact Table: Market Predictions & Quantitative Forecast (Batch Layer)
-- Target: warehouse.fact_market_predictions
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_market_predictions (
    prediction_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL REFERENCES warehouse.dim_symbol(symbol),
    target_timeframe VARCHAR(20) NOT NULL, -- 'next_day', 'next_week', 'next_month', 'next_year'
    target_period_start TIMESTAMP NOT NULL,
    base_price NUMERIC(18, 4) NOT NULL,
    predicted_price NUMERIC(18, 4) NOT NULL,
    predicted_high NUMERIC(18, 4) NOT NULL,
    predicted_low NUMERIC(18, 4) NOT NULL,
    expected_return_pct NUMERIC(8, 2) NOT NULL,
    expected_volatility_pct NUMERIC(8, 2) NOT NULL,
    trend_direction VARCHAR(20) NOT NULL, -- 'BULLISH', 'BEARISH', 'SIDEWAYS'
    strategic_signal VARCHAR(20) NOT NULL, -- 'STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL'
    confidence_score NUMERIC(5, 2) NOT NULL,
    model_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_pred_symbol ON warehouse.fact_market_predictions(symbol);
CREATE INDEX IF NOT EXISTS idx_fact_pred_tf ON warehouse.fact_market_predictions(target_timeframe);
CREATE INDEX IF NOT EXISTS idx_fact_pred_created ON warehouse.fact_market_predictions(created_at DESC);
