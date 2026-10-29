-- =============================================================================
-- Fact Table: fact_btc_model_evaluation
-- Stores benchmark evaluation results comparing XGBoost vs Majority Baseline
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_btc_model_evaluation (
    eval_id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    evaluation_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    target_symbol VARCHAR(20) NOT NULL DEFAULT 'BTC-USDT',
    test_samples INTEGER NOT NULL,
    accuracy DOUBLE PRECISION NOT NULL,
    precision DOUBLE PRECISION NOT NULL,
    recall DOUBLE PRECISION NOT NULL,
    f1_score DOUBLE PRECISION NOT NULL,
    balanced_accuracy DOUBLE PRECISION NOT NULL,
    roc_auc DOUBLE PRECISION,
    baseline_accuracy DOUBLE PRECISION NOT NULL,
    accuracy_lift_pct DOUBLE PRECISION NOT NULL,
    confusion_matrix JSONB,
    feature_importance JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
