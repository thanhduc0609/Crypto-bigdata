"""
=============================================================================
Test Suite: Live Inference Reliability, Finalized Candles & Outcome Backfill
=============================================================================
Tests:
  1. Finalized Candle Enforcement: in-flight unclosed candles are strictly rejected.
  2. Feature Warmup Protection: ensures at least 26 candles exist before inference.
  3. Delayed Event Handling: handles out-of-order candles gracefully by chronological sort.
  4. Restart Recovery & Idempotency: multiple predictions for same candle update, not duplicate.
  5. Outcome Resolution & Zero Lookahead: ensures future price is strictly evaluated after target time.
=============================================================================
"""

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.predict import run_single_prediction, backfill_actual_outcomes
from warehouse.etl.db_config import get_db_connection


def create_test_candles(n: int = 40, base_time: datetime = None) -> pd.DataFrame:
    """Creates a deterministic sequence of n 1m candles for testing."""
    if base_time is None:
        base_time = datetime.now(timezone.utc) - timedelta(minutes=n)
    
    records = []
    price = 85000.0
    for i in range(n):
        t_start = base_time + timedelta(minutes=i)
        t_end = t_start + timedelta(minutes=1)
        price += (i % 5 - 2) * 10.0
        records.append({
            "symbol": "BTC-USDT",
            "window_start": t_start,
            "window_end": t_end,
            "open": price,
            "high": price + 15.0,
            "low": price - 15.0,
            "close": price + 5.0,
            "volume": 10.0 + (i % 3),
            "total_trade_value": price * 10.0,
            "source": "realtime_stream"
        })
    return pd.DataFrame(records)


def run_inference_reliability_tests():
    print("=" * 80)
    print("TEST SUITE: INFERENCE RELIABILITY & FINALIZED CANDLE VALIDATION")
    print("=" * 80)

    passed = 0
    total = 5

    # -------------------------------------------------------------------------
    # TEST 1: In-Flight Candle Rejection (Finalized Candle Enforcement)
    # -------------------------------------------------------------------------
    print("\n--- TEST 1: FINALIZED CANDLE ENFORCEMENT ---")
    now_utc = datetime.now(timezone.utc)
    # Create 35 finalized candles and 1 in-flight candle whose window_end is in the future
    base_t = now_utc - timedelta(minutes=34)
    candles_35 = create_test_candles(n=35, base_time=base_t)
    
    # Add an unclosed in-flight candle (window_end 30 seconds into the future)
    in_flight_start = now_utc.replace(second=0, microsecond=0)
    in_flight_end = in_flight_start + timedelta(minutes=1)
    in_flight_row = pd.DataFrame([{
        "symbol": "BTC-USDT",
        "window_start": in_flight_start,
        "window_end": in_flight_end,
        "open": 89999.0,
        "high": 90000.0,
        "low": 89990.0,
        "close": 89999.0,
        "volume": 0.5,
        "total_trade_value": 45000.0,
        "source": "realtime_stream"
    }])
    candles_with_inflight = pd.concat([candles_35, in_flight_row], ignore_index=True)

    pred1 = run_single_prediction(candles_with_inflight)
    assert pred1 is not None, "Prediction returned None."
    # The feature timestamp MUST be the finalized candle, NOT the in-flight candle
    pred_feat_ts = pd.to_datetime(pred1["feature_timestamp"]).astimezone(timezone.utc)
    in_flight_ts = in_flight_start.astimezone(timezone.utc)
    print(f"[*] In-flight candle time : {in_flight_ts}")
    print(f"[*] Used prediction time  : {pred_feat_ts}")
    assert pred_feat_ts < in_flight_ts, f"In-flight candle was improperly used for prediction! ({pred_feat_ts} >= {in_flight_ts})"
    print("[PASS] Test 1: In-flight candle safely discarded. Only finalized candle used for inference.")
    passed += 1

    # -------------------------------------------------------------------------
    # TEST 2: Warmup Buffer Protection (< 26 candles)
    # -------------------------------------------------------------------------
    print("\n--- TEST 2: WARMUP BUFFER PROTECTION ---")
    short_candles = create_test_candles(n=15)
    pred2 = run_single_prediction(short_candles)
    assert pred2 is None, f"Expected None for insufficient warmup buffer, got {pred2}"
    print("[PASS] Test 2: Inference correctly refused when warmup buffer is < 26 candles.")
    passed += 1

    # -------------------------------------------------------------------------
    # TEST 3: Delayed & Out-of-Order Event Handling
    # -------------------------------------------------------------------------
    print("\n--- TEST 3: DELAYED & OUT-OF-ORDER CANDLE HANDLING ---")
    valid_candles = create_test_candles(n=35)
    # Shuffle order to simulate delayed arrival
    shuffled_candles = valid_candles.sample(frac=1.0, random_state=42).reset_index(drop=True)
    pred3 = run_single_prediction(shuffled_candles)
    assert pred3 is not None, "Failed prediction on shuffled dataframe."
    # Compare with prediction on in-order dataframe
    pred3_orig = run_single_prediction(valid_candles)
    assert pred3["feature_timestamp"] == pred3_orig["feature_timestamp"], "Timestamp mismatch on shuffled data!"
    assert pred3["predicted_direction"] == pred3_orig["predicted_direction"], "Direction mismatch on shuffled data!"
    print(f"[PASS] Test 3: Out-of-order candles sorted chronologically. Direction={pred3['direction_label']}, P(UP)={pred3['probability_up']}")
    passed += 1

    # -------------------------------------------------------------------------
    # TEST 4: Restart Recovery & Prediction Idempotency
    # -------------------------------------------------------------------------
    print("\n--- TEST 4: RESTART RECOVERY & IDEMPOTENCY ---")
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        # Count records before second call
        feat_time = pred3["feature_timestamp"]
        cur.execute(
            "SELECT count(*) FROM warehouse.fact_btc_predictions WHERE symbol = 'BTC-USDT' AND feature_timestamp = %s;",
            (feat_time,)
        )
        count_before = cur.fetchone()[0]
        assert count_before == 1, f"Expected exactly 1 prediction row, found {count_before}"

        # Call prediction again for the exact same candles
        run_single_prediction(valid_candles)

        cur.execute(
            "SELECT count(*) FROM warehouse.fact_btc_predictions WHERE symbol = 'BTC-USDT' AND feature_timestamp = %s;",
            (feat_time,)
        )
        count_after = cur.fetchone()[0]
        assert count_after == 1, f"Duplicate prediction inserted! Expected 1, found {count_after}"
        print(f"[PASS] Test 4: Idempotent upsert verified. Repeated inference on same timestamp did not duplicate.")
        passed += 1

        # ---------------------------------------------------------------------
        # TEST 5: Outcome Resolution & Zero Lookahead Verification
        # ---------------------------------------------------------------------
        print("\n--- TEST 5: OUTCOME RESOLUTION & ZERO LOOKAHEAD ---")
        # Check that the prediction has no actual outcome initially
        cur.execute(
            "SELECT actual_direction, is_correct, target_timestamp, prediction_timestamp FROM warehouse.fact_btc_predictions WHERE symbol = 'BTC-USDT' AND feature_timestamp = %s;",
            (feat_time,)
        )
        row = cur.fetchone()
        actual_dir, is_correct, target_ts, pred_ts = row[0], row[1], row[2], row[3]
        print(f"[*] Initial state: Actual Direction = {actual_dir}, Target Time = {target_ts}")
        assert actual_dir is None, "Lookahead bias detected! Actual direction was prematurely set at prediction time."
        assert is_correct is None, "Lookahead bias detected! is_correct was set at prediction time."
        
        # Now simulate 5 minutes passing by inserting the target candle
        # Now simulate 5 minutes passing by updating target_timestamp to the past
        # and inserting the target candle with known future price $86,200.00
        past_target_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        cur.execute(
            "UPDATE warehouse.fact_btc_predictions SET target_timestamp = %s WHERE symbol = 'BTC-USDT' AND feature_timestamp = %s;",
            (past_target_time, feat_time)
        )

        resolved_candles = pd.concat([valid_candles, pd.DataFrame([{
            "symbol": "BTC-USDT",
            "window_start": past_target_time,
            "window_end": past_target_time + timedelta(minutes=1),
            "open": 86000.0,
            "high": 86500.0,
            "low": 85900.0,
            "close": 86200.0,  # higher than entry price -> UP (1)
            "volume": 25.0,
            "total_trade_value": 2155000.0
        }])], ignore_index=True)

        backfill_actual_outcomes(resolved_candles)

        cur.execute(
            "SELECT actual_direction, is_correct, actual_future_price FROM warehouse.fact_btc_predictions WHERE symbol = 'BTC-USDT' AND feature_timestamp = %s;",
            (feat_time,)
        )
        res_row = cur.fetchone()
        print(f"[*] Resolved state: Actual Direction = {res_row[0]}, is_correct = {res_row[1]}, Future Price = ${res_row[2]}")
        assert res_row[0] is not None, "Outcome resolution failed: actual_direction is still None."
        assert res_row[1] is not None, "Outcome resolution failed: is_correct is still None."
        assert float(res_row[2]) == 86200.0, f"Future price mismatch: expected 86200.0, got {res_row[2]}"
        print("[PASS] Test 5: Future outcome resolution verified strictly after horizon elapsed.")
        passed += 1

        print("\n" + "=" * 80)
        print(f"INFERENCE RELIABILITY SUMMARY: {passed}/{total} TESTS PASSED (100.0%)")
        print("=" * 80)
        return True

    finally:
        # Cleanup test prediction row
        if "feat_time" in locals():
            cur.execute(
                "DELETE FROM warehouse.fact_btc_predictions WHERE symbol = 'BTC-USDT' AND feature_timestamp = %s;",
                (feat_time,)
            )
            conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    success = run_inference_reliability_tests()
    sys.exit(0 if success else 1)
