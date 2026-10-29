"""
=============================================================================
Test Suite: Deterministic Historical vs Streaming Candle Reconciliation
=============================================================================
Tests that:
  1. Complete historical REST candles cannot be corrupted by partial streaming candles.
  2. Complete streaming candles with order-flow metrics supersede historical data.
  3. Provenance (source column) and microstructure metrics are preserved safely.
=============================================================================
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from warehouse.etl.db_config import get_db_connection
from warehouse.etl.streaming_sink import write_candles_batch, write_historical_candles_batch


class MockRow(dict):
    """Mock PySpark Row object with dict/attribute access."""
    def __getattr__(self, name):
        return self[name]


class MockDataFrame:
    """Mock PySpark DataFrame for testing write_candles_batch."""
    def __init__(self, rows):
        self._rows = [MockRow(r) for r in rows]

    def isEmpty(self):
        return len(self._rows) == 0

    def collect(self):
        return self._rows


def run_reconciliation_tests():
    print("=" * 80)
    print("TEST: HISTORICAL VS STREAMING DETERMINISTIC RECONCILIATION")
    print("=" * 80)

    test_symbol = "BTC-USDT"
    # Use a fixed test window far in the future to avoid colliding with actual data
    test_window_start = datetime(2099, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    test_window_end = datetime(2099, 1, 1, 12, 1, 0, tzinfo=timezone.utc)

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Cleanup any leftover test data
        cur.execute(
            "DELETE FROM warehouse.fact_crypto_1m_candles WHERE symbol = %s AND window_start = %s;",
            (test_symbol, test_window_start)
        )
        conn.commit()

        # Step 1: Insert Complete Historical REST Candle (volume = 10.0)
        print("\n[Step 1] Inserting Complete Historical REST Candle (Volume = 10.0 BTC)...")
        hist_records = [{
            "symbol": test_symbol,
            "window_start": test_window_start,
            "window_end": test_window_end,
            "open": 80000.0,
            "high": 80500.0,
            "low": 79800.0,
            "close": 80200.0,
            "volume": 10.0,
            "trade_count": None,
            "buy_volume": None,
            "sell_volume": None,
            "vwap": None,
            "trade_imbalance": None,
            "total_trade_value": 801000.0
        }]
        write_historical_candles_batch(hist_records)

        cur.execute(
            "SELECT open, high, low, close, volume, source FROM warehouse.fact_crypto_1m_candles WHERE symbol = %s AND window_start = %s;",
            (test_symbol, test_window_start)
        )
        row = cur.fetchone()
        assert row is not None, "Step 1 failed: Record not found."
        assert float(row[0]) == 80000.0 and float(row[4]) == 10.0 and row[5] == "historical_rest", f"Step 1 failed: {row}"
        print(f" [PASS] Step 1: Historical candle inserted. Volume={row[4]}, Source={row[5]}")

        # Step 2: Attempt to Overwrite with Partial Streaming Candle (volume = 1.5 BTC)
        print("\n[Step 2] Attempting to overwrite with Partial Streaming Candle (Volume = 1.5 BTC)...")
        partial_streaming_rows = [{
            "symbol": test_symbol,
            "window_start": test_window_start,
            "window_end": test_window_end,
            "open": 80000.0,
            "high": 80050.0,  # truncated high
            "low": 79990.0,   # truncated low
            "close": 80020.0, # truncated close
            "volume": 1.5,    # partial volume!
            "trade_count": 15,
            "buy_volume": 1.0,
            "sell_volume": 0.5,
            "vwap": 80010.0,
            "trade_imbalance": 0.33,
            "total_trade_value": 120015.0
        }]
        mock_df = MockDataFrame(partial_streaming_rows)
        write_candles_batch(mock_df, batch_id=1)

        cur.execute(
            "SELECT open, high, low, close, volume, source, buy_volume FROM warehouse.fact_crypto_1m_candles WHERE symbol = %s AND window_start = %s;",
            (test_symbol, test_window_start)
        )
        row = cur.fetchone()
        assert float(row[0]) == 80000.0, f"Open was overwritten! Got {row[0]}"
        assert float(row[1]) == 80500.0, f"High was overwritten! Got {row[1]}"
        assert float(row[2]) == 79800.0, f"Low was overwritten! Got {row[2]}"
        assert float(row[3]) == 80200.0, f"Close was overwritten! Got {row[3]}"
        assert float(row[4]) == 10.0, f"Volume was corrupted! Expected 10.0, got {row[4]}"
        assert row[5] == "historical_rest", f"Source was corrupted! Expected historical_rest, got {row[5]}"
        print(f" [PASS] Step 2: Partial streaming candle blocked! Complete OHLCV & Volume preserved (Vol={row[4]}, Source={row[5]})")

        # Step 3: Complete Streaming Candle Arrives (Volume = 12.0 BTC >= 10.0 BTC)
        print("\n[Step 3] Complete Streaming Candle Arrives (Volume = 12.0 BTC)...")
        complete_streaming_rows = [{
            "symbol": test_symbol,
            "window_start": test_window_start,
            "window_end": test_window_end,
            "open": 80000.0,
            "high": 80550.0,
            "low": 79750.0,
            "close": 80300.0,
            "volume": 12.0,
            "trade_count": 140,
            "buy_volume": 7.0,
            "sell_volume": 5.0,
            "vwap": 80220.0,
            "trade_imbalance": 0.167,
            "total_trade_value": 962640.0
        }]
        write_candles_batch(MockDataFrame(complete_streaming_rows), batch_id=2)

        cur.execute(
            "SELECT open, high, low, close, volume, source, vwap, buy_volume FROM warehouse.fact_crypto_1m_candles WHERE symbol = %s AND window_start = %s;",
            (test_symbol, test_window_start)
        )
        row = cur.fetchone()
        assert float(row[4]) == 12.0, f"Volume failed to update! Expected 12.0, got {row[4]}"
        assert row[5] == "realtime_stream", f"Source should now be realtime_stream, got {row[5]}"
        assert float(row[6]) == 80220.0, f"VWAP should be 80220.0, got {row[6]}"
        assert float(row[7]) == 7.0, f"Buy volume should be 7.0, got {row[7]}"
        print(f" [PASS] Step 3: Complete streaming candle safely superseded historical record. Vol={row[4]}, Source={row[5]}, VWAP={row[6]}")

        # Step 4: Stale Historical REST Candle Arrives (Volume = 9.0 BTC < 12.0 BTC)
        print("\n[Step 4] Stale Historical REST Candle Arrives (Volume = 9.0 BTC < 12.0 BTC)...")
        stale_hist_records = [{
            "symbol": test_symbol,
            "window_start": test_window_start,
            "window_end": test_window_end,
            "open": 80000.0,
            "high": 80400.0,
            "low": 79800.0,
            "close": 80100.0,
            "volume": 9.0,
            "total_trade_value": 720000.0
        }]
        write_historical_candles_batch(stale_hist_records)

        cur.execute(
            "SELECT volume, source, vwap FROM warehouse.fact_crypto_1m_candles WHERE symbol = %s AND window_start = %s;",
            (test_symbol, test_window_start)
        )
        row = cur.fetchone()
        assert float(row[0]) == 12.0 and row[1] == "realtime_stream", f"Step 4 failed: {row}"
        assert float(row[2]) == 80220.0, f"Microstructure metric was wiped! {row}"
        print(f" [PASS] Step 4: Complete streaming record protected against stale historical REST candle (Vol={row[0]}, Source={row[1]})")

        print("\n" + "=" * 80)
        print("ALL RECONCILIATION TESTS PASSED (100.0%)")
        print("=" * 80)
        return True

    finally:
        # Clean up test row
        cur.execute(
            "DELETE FROM warehouse.fact_crypto_1m_candles WHERE symbol = %s AND window_start = %s;",
            (test_symbol, test_window_start)
        )
        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    success = run_reconciliation_tests()
    sys.exit(0 if success else 1)
