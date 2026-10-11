"""
=============================================================================
Crypto Big Data - Core Data Schemas (Shared across Batch & Streaming)
=============================================================================
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    DateType
)

# Schema khớp với payload gửi từ WebSocket Producer
RAW_TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("size", DoubleType(), False),
    StructField("side", StringType(), True),
    StructField("timestamp", LongType(), False)
])

# Schema dữ liệu giao dịch sau khi làm sạch
CLEANED_TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("size", DoubleType(), False),
    StructField("side", StringType(), True),
    StructField("trade_time", TimestampType(), False),
    StructField("trade_value", DoubleType(), False),
    StructField("trade_date", DateType(), False),
    StructField("kafka_partition", LongType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_arrival_time", TimestampType(), True)
])

# Schema nến 1 phút và các chỉ báo kỹ thuật tổng hợp
CANDLE_1M_SCHEMA = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("trade_count", LongType(), False),
    StructField("buy_volume", DoubleType(), False),
    StructField("sell_volume", DoubleType(), False),
    StructField("vwap", DoubleType(), False),
    StructField("trade_imbalance", DoubleType(), False),
    StructField("total_trade_value", DoubleType(), False)
])

# =============================================================================
# MEDALLION BATCH ARCHITECTURE SCHEMAS (BRONZE - SILVER - GOLD)
# =============================================================================

# 1. Bronze Schemas (Raw preservation with source metadata)
BRONZE_CANDLE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("ts", LongType(), False),
    StructField("raw_json", StringType(), False),
    StructField("source", StringType(), False),
    StructField("ingestion_time", TimestampType(), False)
])

# 2. Silver Schemas (Validated, standardized, deduplicated)
SILVER_CANDLE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("turnover_usd", DoubleType(), False),
    StructField("vwap", DoubleType(), False),
    StructField("trade_count", LongType(), False),
    StructField("source", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])

SILVER_FUNDING_RATE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("funding_time", TimestampType(), False),
    StructField("next_funding_time", TimestampType(), True),
    StructField("funding_rate", DoubleType(), False),
    StructField("predicted_funding_rate", DoubleType(), True),
    StructField("projected_apy_pct", DoubleType(), True),
    StructField("market_regime", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("ingestion_time", TimestampType(), False)
])

SILVER_OPEN_INTEREST_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("snapshot_time", TimestampType(), False),
    StructField("oi_contracts", DoubleType(), False),
    StructField("oi_usd", DoubleType(), False),
    StructField("avg_contract_val_usd", DoubleType(), True),
    StructField("exchange", StringType(), True),
    StructField("ingestion_time", TimestampType(), False)
])

# 3. Gold Schemas (Analytics-ready aggregated datasets)
GOLD_CANDLE_AGG_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("bucket_time", TimestampType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("total_volume", DoubleType(), False),
    StructField("total_trades", LongType(), False),
    StructField("turnover_usd", DoubleType(), False),
    StructField("buy_volume", DoubleType(), True),
    StructField("sell_volume", DoubleType(), True),
    StructField("return_pct", DoubleType(), False),
    StructField("volatility_pct", DoubleType(), False),
    StructField("vwap", DoubleType(), False),
    StructField("trade_imbalance", DoubleType(), True)
])

GOLD_PREDICTION_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("target_timeframe", StringType(), False),
    StructField("target_period_start", TimestampType(), False),
    StructField("base_price", DoubleType(), False),
    StructField("predicted_price", DoubleType(), False),
    StructField("predicted_high", DoubleType(), False),
    StructField("predicted_low", DoubleType(), False),
    StructField("expected_return_pct", DoubleType(), False),
    StructField("expected_volatility_pct", DoubleType(), False),
    StructField("trend_direction", StringType(), False),
    StructField("strategic_signal", StringType(), False),
    StructField("confidence_score", DoubleType(), False),
    StructField("model_name", StringType(), False),
    StructField("created_at", TimestampType(), False)
])

