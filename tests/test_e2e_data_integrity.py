"""
=============================================================================
End-to-End Data Integrity & Correctness Verification Suite
=============================================================================
Scope: BTC-USDT spot on OKX only
Pipeline: Kafka -> Bronze -> Silver -> Gold -> PostgreSQL -> Analytics/ML
Verifies Sections B, C, D, E, F, G against independently calculated mathematical answers.
=============================================================================
"""

import os
import sys
import json
import math
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np

# Setup project root
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import ensure_java_and_hadoop, create_spark_session
ensure_java_and_hadoop()

from ingestion.historical.candle_parser import parse_okx_rest_candle
from processing.batch.bronze_to_silver import process_bronze_to_silver
from processing.batch.silver_to_gold import process_silver_to_gold
from warehouse.etl.streaming_sink import write_candles_batch, write_historical_candles_batch
from warehouse.etl.db_config import get_db_connection
from ml.predict import run_single_prediction, MAX_STALENESS_MINUTES
from processing.features.feature_engineering import (
    COMMON_FEATURE_COLUMNS,
    compute_features,
    add_future_direction_target
)


class DataIntegrityAuditor:
    def __init__(self):
        self.results = []

    def log(self, category: str, criteria: str, expected: str, actual: str, status: str):
        self.results.append({
            "category": category,
            "criteria": criteria,
            "expected": expected,
            "actual": actual,
            "status": status
        })
        print(f"[{status}] [{category}] {criteria}")
        if status != "PASS":
            print(f"       Expected: {expected}")
            print(f"       Actual  : {actual}")

    def print_report(self):
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        skipped = sum(1 for r in self.results if r["status"] == "SKIP")
        print("\n" + "=" * 90)
        print("END-TO-END DATA INTEGRITY & CORRECTNESS VERIFICATION REPORT")
        print("=" * 90)
        print(f"{'Category':<22} | {'Criteria':<32} | {'Status':<6} | Details")
        print("-" * 90)
        for r in self.results:
            details = f"Exp: {r['expected']} | Act: {r['actual']}" if r["status"] != "PASS" else "Verified against exact expectation"
            print(f"{r['category']:<22} | {r['criteria']:<32} | {r['status']:<6} | {details}")
        print("-" * 90)
        print(f"TOTAL CRITERIA: {len(self.results)} | PASSED: {passed} | FAILED: {failed} | SKIPPED: {skipped}")
        print("=" * 90)
        return failed == 0


auditor = DataIntegrityAuditor()


# =============================================================================
# SECTION C: SYNTHETIC FIXTURE GENERATION (KNOWN MATHEMATICAL TRUTH)
# =============================================================================

def build_synthetic_fixture():
    """
    Creates a multi-minute fixture with known ground truth:
      - Minute 0 (12:00:00 - 12:00:59.999): out-of-order, same-ts, boundary start/end, duplicate.
      - Minute 1 (12:01:00 - 12:01:59.999): boundary check, 100% buy (imbalance = +1.0).
      - Minute 2 (12:02:00 - 12:02:59.999): 100% sell (imbalance = -1.0).
      - Minute 3: GAP (0 trades) -> must not create synthetic candle.
      - Minute 4 (12:04:00 - 12:04:59.999): single trade.
      - 9 Invalid records testing symbol, NaN, Inf, zero/negative, nulls.
    """
    base_t = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone.utc)
    base_ms = int(base_t.timestamp() * 1000)

    # Valid trades for Minute 0
    t01 = {"trade_id": "T01", "symbol": "BTC-USDT", "price": 90000.0, "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 0, "kafka_offset": 1} # 12:00:00.000
    t02 = {"trade_id": "T02", "symbol": "BTC-USDT", "price": 90200.0, "size": 2.0, "side": "buy", "raw_timestamp": base_ms + 20000, "kafka_offset": 2} # arrives out-of-order
    t03 = {"trade_id": "T03", "symbol": "BTC-USDT", "price": 90100.0, "size": 1.5, "side": "sell", "raw_timestamp": base_ms + 10000, "kafka_offset": 3} # arrives out-of-order
    t04 = {"trade_id": "T04", "symbol": "BTC-USDT", "price": 90150.0, "size": 0.5, "side": "buy", "raw_timestamp": base_ms + 30000, "kafka_offset": 4} # identical ts trade 1
    t05 = {"trade_id": "T05", "symbol": "BTC-USDT", "price": 90160.0, "size": 1.0, "side": "sell", "raw_timestamp": base_ms + 30000, "kafka_offset": 5} # identical ts trade 2
    t06 = {"trade_id": "T06", "symbol": "BTC-USDT", "price": 90300.0, "size": 2.0, "side": "buy", "raw_timestamp": base_ms + 59999, "kafka_offset": 6} # 12:00:59.999 boundary end

    # Duplicate trade within batch
    t03_dup = {"trade_id": "T03", "symbol": "BTC-USDT", "price": 90100.0, "size": 1.5, "side": "sell", "raw_timestamp": base_ms + 10000, "kafka_offset": 7}

    # Conflicting trade_id with older timestamp/offset (must be discarded by deterministic dedup)
    t04_conflict = {"trade_id": "T04", "symbol": "BTC-USDT", "price": 99999.0, "size": 10.0, "side": "buy", "raw_timestamp": base_ms + 29000, "kafka_offset": 0}

    # Valid trades for Minute 1 (100% Buy)
    t07 = {"trade_id": "T07", "symbol": "BTC-USDT", "price": 90400.0, "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 60000, "kafka_offset": 8} # 12:01:00.000 boundary start
    t08 = {"trade_id": "T08", "symbol": "BTC-USDT", "price": 90500.0, "size": 3.0, "side": "buy", "raw_timestamp": base_ms + 100000, "kafka_offset": 9}

    # Valid trades for Minute 2 (100% Sell)
    t09 = {"trade_id": "T09", "symbol": "BTC-USDT", "price": 90350.0, "size": 2.0, "side": "sell", "raw_timestamp": base_ms + 135000, "kafka_offset": 10}
    t10 = {"trade_id": "T10", "symbol": "BTC-USDT", "price": 90250.0, "size": 2.0, "side": "sell", "raw_timestamp": base_ms + 165000, "kafka_offset": 11}

    # Minute 3 is GAP (no trades between base_ms + 180000 and base_ms + 239999)

    # Valid trade for Minute 4
    t11 = {"trade_id": "T11", "symbol": "BTC-USDT", "price": 90300.0, "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 250000, "kafka_offset": 12}

    # 9 Invalid Records
    inv_records = [
        {"trade_id": "INV01", "symbol": "ETH-USDT", "price": 2800.0, "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 5000}, # Wrong symbol
        {"trade_id": "     ", "symbol": "BTC-USDT", "price": 90000.0, "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 6000}, # Empty trade_id
        {"trade_id": "INV02", "symbol": "BTC-USDT", "price": -90000.0, "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 7000}, # Negative price
        {"trade_id": "INV03", "symbol": "BTC-USDT", "price": 0.0, "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 8000}, # Zero price
        {"trade_id": "INV04", "symbol": "BTC-USDT", "price": float("nan"), "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 9000}, # NaN price
        {"trade_id": "INV05", "symbol": "BTC-USDT", "price": float("inf"), "size": 1.0, "side": "buy", "raw_timestamp": base_ms + 10000}, # Inf price
        {"trade_id": "INV06", "symbol": "BTC-USDT", "price": 90000.0, "size": -0.5, "side": "buy", "raw_timestamp": base_ms + 11000}, # Negative size
        {"trade_id": "INV07", "symbol": "BTC-USDT", "price": 90000.0, "size": 1.0, "side": "hold", "raw_timestamp": base_ms + 12000}, # Invalid side
        {"trade_id": "INV08", "symbol": "BTC-USDT", "price": 90000.0, "size": 1.0, "side": "buy", "raw_timestamp": 0}, # Zero timestamp
    ]

    # Combine all into batch 1
    batch_1_records = [t01, t02, t03, t04, t05, t06, t03_dup, t04_conflict, t07, t08, t09, t10, t11] + inv_records
    return batch_1_records, base_t, base_ms


# =============================================================================
# TEST SUITE IMPLEMENTATION
# =============================================================================

def test_data_integrity_end_to_end():
    spark = None
    temp_dir = tempfile.mkdtemp(prefix="crypto_integrity_audit_")

    try:
        spark = create_spark_session("DataIntegrityAudit", include_kafka=False, include_s3=False, include_postgres=False)

        temp_path = Path(temp_dir)
        bronze_dir = temp_path / "bronze"
        silver_dir = temp_path / "silver"
        gold_dir = temp_path / "gold"
        bronze_dir.mkdir(parents=True)
        silver_dir.mkdir(parents=True)
        gold_dir.mkdir(parents=True)

        batch_1_records, base_t, base_ms = build_synthetic_fixture()
        df_batch1 = pd.DataFrame(batch_1_records)
        df_batch1["raw_timestamp"] = df_batch1["raw_timestamp"].fillna(0).astype("int64")
        df_batch1["kafka_offset"] = df_batch1["kafka_offset"].fillna(-1).astype("int64")
        df_batch1["price"] = df_batch1["price"].astype("float64")
        df_batch1["size"] = df_batch1["size"].astype("float64")
        df_batch1.to_parquet(bronze_dir / "bronze_batch_1.parquet", index=False)

        b_path = str(bronze_dir).replace("\\", "/")
        s_path = str(silver_dir).replace("\\", "/")
        g_path = str(gold_dir).replace("\\", "/")

        with patch("processing.batch.bronze_to_silver.get_bronze_trades_path", return_value=b_path), \
             patch("processing.batch.bronze_to_silver.get_silver_trades_path", return_value=s_path), \
             patch("processing.batch.silver_to_gold.get_silver_trades_path", return_value=s_path), \
             patch("processing.batch.silver_to_gold.get_gold_candles_path", return_value=g_path), \
             patch("processing.batch.silver_to_gold.write_candles_batch"):

            # -----------------------------------------------------------------
            # SECTION D: INGESTION -> BRONZE -> SILVER AUDIT
            # -----------------------------------------------------------------
            res_silver = process_bronze_to_silver(spark, symbol="BTC-USDT")

            total_in = len(batch_1_records) # 22
            expected_rejected = 9 # 9 invalid records
            expected_valid_before_dedup = 13 # 6 (min0) + 2 (dups) + 2 (min1) + 2 (min2) + 1 (min4) = 13
            expected_duplicates = 2 # t03_dup and t04_conflict
            expected_unique_valid = 11 # 6 (min0) + 2 (min1) + 2 (min2) + 1 (min4) = 11

            # Check counting identity: total = valid_before_dedup + rejected
            act_total = res_silver["total"]
            act_rejected = res_silver["rejected"]
            act_valid_before = res_silver.get("valid_before_dedup", act_total - act_rejected)
            act_valid_unique = res_silver["valid"]
            act_dups = res_silver.get("duplicates", act_valid_before - act_valid_unique)

            c1 = (act_total == total_in) and (act_total == act_valid_before + act_rejected)
            auditor.log("Ingestion->Silver", "Counting Formula (Total=Valid+Rej)", f"{total_in}={expected_valid_before_dedup}+{expected_rejected}", f"{act_total}={act_valid_before}+{act_rejected}", "PASS" if c1 else "FAIL")

            c2 = (act_valid_before == act_valid_unique + act_dups) and (act_valid_unique == expected_unique_valid) and (act_dups == expected_duplicates)
            auditor.log("Ingestion->Silver", "Dedup Accounting (Valid=Unique+Dup)", f"{expected_valid_before_dedup}={expected_unique_valid}+{expected_duplicates}", f"{act_valid_before}={act_valid_unique}+{act_dups}", "PASS" if c2 else "FAIL")

            # Check Millisecond precision & UTC timezone
            silver_df = spark.read.parquet(s_path)
            t06_row = silver_df.filter(silver_df.trade_id == "T06").collect()[0]
            # base_ms + 59999 -> microsecond should be 999000
            micro_ok = (t06_row.trade_time.microsecond == 999000)
            auditor.log("Ingestion->Silver", "Timestamp Precision (Millisecond)", "999000 us", f"{t06_row.trade_time.microsecond} us", "PASS" if micro_ok else "FAIL")

            # Check Conflicting Trade Resolution: T04 should have price=90150.0 (offset 4), NOT 99999.0 (offset 0)
            t04_row = silver_df.filter(silver_df.trade_id == "T04").collect()[0]
            t04_ok = (t04_row.price == 90150.0)
            auditor.log("Ingestion->Silver", "Conflicting Trade Deduplication", "price=90150.0", f"price={t04_row.price}", "PASS" if t04_ok else "FAIL")

            # -----------------------------------------------------------------
            # SECTION E: SILVER -> GOLD AUDIT (MATHEMATICAL TRUTH COMPARISON)
            # -----------------------------------------------------------------
            res_gold = process_silver_to_gold(spark, symbol="BTC-USDT", sync_to_postgres=False)

            gold_df = spark.read.parquet(g_path).orderBy("window_start").collect()

            # Check Gap handling: 4 candles should be produced (Minute 0, 1, 2, 4). Minute 3 MUST be absent!
            act_candle_count = len(gold_df)
            auditor.log("Silver->Gold", "Time Gap Handling (No Fake Candles)", "4 candles (Min 0, 1, 2, 4)", f"{act_candle_count} candles", "PASS" if act_candle_count == 4 else "FAIL")

            m0 = gold_df[0] # Minute 0
            # Expected Minute 0:
            # Open: 90000.0, High: 90300.0, Low: 90000.0, Close: 90300.0, Volume: 8.0, Trade Count: 6
            # Total Trade Value: 721385.0, Buy Vol: 5.5, Sell Vol: 2.5, VWAP: 90173.125, Imbalance: 0.375
            auditor.log("Silver->Gold", "Minute 0: Event-time Open", "90000.0", str(m0.open), "PASS" if math.isclose(m0.open, 90000.0) else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: Event-time Close", "90300.0", str(m0.close), "PASS" if math.isclose(m0.close, 90300.0) else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: High & Low Extremes", "H=90300.0, L=90000.0", f"H={m0.high}, L={m0.low}", "PASS" if (m0.high == 90300.0 and m0.low == 90000.0) else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: Total Volume", "8.0 BTC", f"{m0.volume} BTC", "PASS" if math.isclose(m0.volume, 8.0) else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: Unique Trade Count", "6", str(m0.trade_count), "PASS" if m0.trade_count == 6 else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: Total Trade Value (Turnover)", "721385.0 USDT", f"{m0.total_trade_value} USDT", "PASS" if math.isclose(m0.total_trade_value, 721385.0) else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: Buy + Sell = Volume", "5.5 + 2.5 = 8.0", f"{m0.buy_volume} + {m0.sell_volume} = {m0.volume}", "PASS" if math.isclose(m0.buy_volume + m0.sell_volume, m0.volume) else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: Calculated VWAP", "90173.125 USDT", f"{m0.vwap:.3f} USDT", "PASS" if math.isclose(m0.vwap, 90173.125, rel_tol=1e-5) else "FAIL")
            auditor.log("Silver->Gold", "Minute 0: Order Flow Imbalance", "0.3750", f"{m0.trade_imbalance:.4f}", "PASS" if math.isclose(m0.trade_imbalance, 0.375, rel_tol=1e-5) else "FAIL")

            # Mathematical Boundaries check
            bounds_ok = (m0.low <= m0.open <= m0.high) and (m0.low <= m0.close <= m0.high) and (m0.low <= m0.vwap <= m0.high) and (-1.0 <= m0.trade_imbalance <= 1.0)
            auditor.log("Silver->Gold", "Minute 0: Mathematical Geometry Bounds", "Low <= (O, C, VWAP) <= High & Imb in [-1, 1]", f"L={m0.low} <= O={m0.open}, C={m0.close}, V={m0.vwap:.1f} <= H={m0.high}", "PASS" if bounds_ok else "FAIL")

            # Minute 1 (100% Buy) -> Imbalance must be strictly +1.0
            m1 = gold_df[1]
            imb1_ok = math.isclose(m1.trade_imbalance, 1.0) and (m1.sell_volume == 0.0)
            auditor.log("Silver->Gold", "Minute 1: 100% Buy Imbalance", "Imbalance = +1.0, Sell = 0.0", f"Imbalance = {m1.trade_imbalance}, Sell = {m1.sell_volume}", "PASS" if imb1_ok else "FAIL")

            # Minute 2 (100% Sell) -> Imbalance must be strictly -1.0
            m2 = gold_df[2]
            imb2_ok = math.isclose(m2.trade_imbalance, -1.0) and (m2.buy_volume == 0.0)
            auditor.log("Silver->Gold", "Minute 2: 100% Sell Imbalance", "Imbalance = -1.0, Buy = 0.0", f"Imbalance = {m2.trade_imbalance}, Buy = {m2.buy_volume}", "PASS" if imb2_ok else "FAIL")

            # Check Idempotency: Re-running Silver -> Gold yields identical numbers
            process_silver_to_gold(spark, symbol="BTC-USDT", sync_to_postgres=False)
            gold_df_replay = spark.read.parquet(g_path).orderBy("window_start").collect()
            idempotent_ok = (len(gold_df_replay) == 4) and (gold_df_replay[0].volume == m0.volume) and (gold_df_replay[0].vwap == m0.vwap)
            auditor.log("Silver->Gold", "Idempotent Batch Re-run", "Identical keys and metric values", f"4 candles, Vol={gold_df_replay[0].volume}", "PASS" if idempotent_ok else "FAIL")

            # Check Late-arriving trade: T_LATE arrives for Minute 0
            late_trade = [{"trade_id": "T_LATE", "symbol": "BTC-USDT", "price": 90250.0, "size": 1.0, "side": "sell", "raw_timestamp": base_ms + 45000, "kafka_offset": 20}]
            df_late = pd.DataFrame(late_trade)
            df_late["raw_timestamp"] = df_late["raw_timestamp"].astype("int64")
            df_late["kafka_offset"] = df_late["kafka_offset"].astype("int64")
            df_late["price"] = df_late["price"].astype("float64")
            df_late["size"] = df_late["size"].astype("float64")
            df_late.to_parquet(bronze_dir / "bronze_late.parquet", index=False)
            process_bronze_to_silver(spark, symbol="BTC-USDT")
            process_silver_to_gold(spark, symbol="BTC-USDT", sync_to_postgres=False)

            gold_df_late = spark.read.parquet(g_path).orderBy("window_start").collect()
            m0_late = gold_df_late[0]
            m1_late = gold_df_late[1]

            # Minute 0 volume updated from 8.0 to 9.0; Minute 1 volume remains strictly 4.0
            late_updated_ok = (m0_late.volume == 9.0) and (m0_late.trade_count == 7) and (m1_late.volume == 4.0)
            auditor.log("Silver->Gold", "Late Data Deterministic Update", "Min 0 Vol: 8.0->9.0; Min 1 Vol unchanged (4.0)", f"Min 0 Vol: {m0_late.volume}; Min 1 Vol: {m1_late.volume}", "PASS" if late_updated_ok else "FAIL")

        # ---------------------------------------------------------------------
        # SECTION F: REST, GOLD & POSTGRESQL DW AUDIT
        # ---------------------------------------------------------------------
        # 1. REST Parser validation
        raw_rest_ok = ["1730000000000", "91000.0", "91500.0", "90800.0", "91200.0", "2.5", "2.5", "228000.0", "1"]
        c_rest = parse_okx_rest_candle(raw_rest_ok, symbol="BTC-USDT", require_closed=True)
        parser_ok = (c_rest is not None) and (c_rest["volume"] == 2.5) and (c_rest["total_trade_value"] == 228000.0) and (c_rest["is_closed"] is True) and (c_rest["buy_volume"] is None)
        auditor.log("REST->Gold->DW", "REST Parser (Vol=row[5], Value=row[7])", "Vol=2.5 BTC, Value=228000.0 USDT, Microstructure=NULL", f"Vol={c_rest['volume']}, Value={c_rest['total_trade_value']}, BuyVol={c_rest['buy_volume']}", "PASS" if parser_ok else "FAIL")

        # Unconfirmed REST candle rejected when require_closed=True
        raw_rest_unclosed = list(raw_rest_ok)
        raw_rest_unclosed[8] = "0"
        unclosed_rej = (parse_okx_rest_candle(raw_rest_unclosed, require_closed=True) is None)
        auditor.log("REST->Gold->DW", "REST Unconfirmed Rejection", "None (rejected)", "None", "PASS" if unclosed_rej else "FAIL")

        # 2. Live PostgreSQL Temp Table Reconciliation Audit
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            temp_audit_tbl = "temp_data_integrity_audit"
            cur.execute(f"CREATE TEMP TABLE {temp_audit_tbl} (LIKE warehouse.fact_crypto_1m_candles INCLUDING ALL);")
            conn.commit()

            t_test = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)
            t_test_end = t_test + timedelta(minutes=1)

            rest_rec = {
                "symbol": "BTC-USDT", "window_start": t_test, "window_end": t_test_end,
                "open": 91000.0, "high": 91500.0, "low": 90800.0, "close": 91200.0, "volume": 50.0,
                "total_trade_value": 4560000.0, "source": "historical_rest", "is_closed": True
            }
            stream_rec = {
                "symbol": "BTC-USDT", "window_start": t_test, "window_end": t_test_end,
                "open": 91010.0, "high": 91400.0, "low": 90900.0, "close": 91180.0, "volume": 20.0,
                "trade_count": 150, "buy_volume": 12.0, "sell_volume": 8.0, "vwap": 91100.0,
                "trade_imbalance": 0.20, "total_trade_value": 1820000.0, "source": "realtime_stream", "is_closed": True
            }

            # Order 1: REST first -> Stream second
            write_historical_candles_batch([rest_rec], conn=conn, table_name=temp_audit_tbl, close_conn=False)
            write_candles_batch([stream_rec], conn=conn, table_name=temp_audit_tbl, close_conn=False)
            cur.execute(f"SELECT open, volume, source, trade_count FROM {temp_audit_tbl} WHERE window_start=%s", (t_test,))
            row_o1 = cur.fetchone()

            # Order 2: Stream first -> REST second
            cur.execute(f"TRUNCATE {temp_audit_tbl};")
            conn.commit()
            write_candles_batch([stream_rec], conn=conn, table_name=temp_audit_tbl, close_conn=False)
            write_historical_candles_batch([rest_rec], conn=conn, table_name=temp_audit_tbl, close_conn=False)
            cur.execute(f"SELECT open, volume, source, trade_count FROM {temp_audit_tbl} WHERE window_start=%s", (t_test,))
            row_o2 = cur.fetchone()

            reconcile_ok = (row_o1 == row_o2) and (row_o1[0] == 91000.0) and (row_o1[1] == 50.0) and (row_o1[2] == "historical_rest") and (row_o1[3] is None)
            auditor.log("REST->Gold->DW", "Arrival Order Invariance & REST Priority", "Both orders yield canonical REST (open=91000, tc=NULL)", f"O1={row_o1[0]}, O2={row_o2[0]}, tc={row_o1[3]}", "PASS" if reconcile_ok else "FAIL")

            cur.close()
            conn.close()
        except Exception as db_err:
            auditor.log("REST->Gold->DW", "Live PostgreSQL Reconciliation", "PASS", f"SKIP ({db_err})", "SKIP")

        # ---------------------------------------------------------------------
        # SECTION G: ANALYTICS & ML AUDIT
        # ---------------------------------------------------------------------
        # Feature vector alignment: 15 common features
        auditor.log("Analytics & ML", "Feature Vector Size (15 Common Features)", "15 features", f"{len(COMMON_FEATURE_COLUMNS)} features", "PASS" if len(COMMON_FEATURE_COLUMNS) == 15 else "FAIL")

        # Exact +5m target matching with gap
        base_target_t = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)
        times_target = [base_target_t + timedelta(minutes=i) for i in [0, 1, 2, 4, 5, 6, 7]]
        closes_target = [100.0, 102.0, 105.0, 110.0, 120.0, 125.0, 130.0]
        df_target_test = pd.DataFrame({"window_start": times_target, "close": closes_target})
        labeled_df = add_future_direction_target(df_target_test, horizon_minutes=5)

        # t=0 + 5m -> t=5m price is 120.0 -> direction 1.0
        # t=4 + 5m -> t=9m is missing (gap) -> direction must be NaN!
        target_exact_ok = (labeled_df.loc[0, "target_direction"] == 1.0) and np.isnan(labeled_df.loc[3, "target_direction"])
        auditor.log("Analytics & ML", "Target Formulation (+5m Exact & Gap Handling)", "t0=1.0, t3(gap)=NaN", f"t0={labeled_df.loc[0, 'target_direction']}, t3={labeled_df.loc[3, 'target_direction']}", "PASS" if target_exact_ok else "FAIL")

        # ML Quality Gate: Reject Past Target Timestamp
        now_mock = datetime.now(timezone.utc)
        past_times = [now_mock - timedelta(minutes=45 - i) for i in range(40)]
        closes_mock = [90000.0 + (i % 5) * 20.0 for i in range(40)]
        stale_df = pd.DataFrame({
            "window_start": past_times,
            "window_end": [t + timedelta(minutes=1) for t in past_times],
            "open": [c - 5.0 for c in closes_mock],
            "high": [c + 15.0 for c in closes_mock],
            "low": [c - 15.0 for c in closes_mock],
            "close": closes_mock,
            "volume": [10.0] * 40,
            "source": ["historical_rest"] * 40,
            "is_closed": [True] * 40
        })
        # If target (past_times[-1] + 5m) <= now_mock, must reject
        past_target_rejected = (run_single_prediction(stale_df) is None)
        auditor.log("Analytics & ML", "ML Realtime Guard: Past Target Rejection", "None (Rejected)", "None" if past_target_rejected else "Emitted", "PASS" if past_target_rejected else "FAIL")

        # ML Quality Gate: Reject Unreconciled Streaming
        fresh_times = [now_mock - timedelta(minutes=42 - i) for i in range(40)]
        stream_ml_df = pd.DataFrame({
            "window_start": fresh_times,
            "window_end": [t + timedelta(minutes=1) for t in fresh_times],
            "open": [c - 5.0 for c in closes_mock],
            "high": [c + 15.0 for c in closes_mock],
            "low": [c - 15.0 for c in closes_mock],
            "close": closes_mock,
            "volume": [10.0] * 40,
            "source": ["realtime_stream"] * 40, # REJECTED
            "is_closed": [True] * 40
        })
        stream_rejected = (run_single_prediction(stream_ml_df) is None)
        auditor.log("Analytics & ML", "ML Quality Gate: Reject Unreconciled Stream", "None (Rejected)", "None" if stream_rejected else "Emitted", "PASS" if stream_rejected else "FAIL")

    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass
        shutil.rmtree(temp_dir, ignore_errors=True)

    return auditor.print_report()


if __name__ == "__main__":
    success = test_data_integrity_end_to_end()
    sys.exit(0 if success else 1)
