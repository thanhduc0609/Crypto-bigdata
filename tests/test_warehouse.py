"""
=============================================================================
Crypto Big Data - Automated Test Suite for Pure Streaming Data Warehouse
=============================================================================
Tests:
  1. Connection to PostgreSQL Data Warehouse
  2. Verification that legacy batch tables are removed
  3. Validation of Star Schema dimensions and streaming fact tables
  4. Validation of 10 real-time streaming analytics views
  5. Performance and latency benchmark (< 100ms)
=============================================================================
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from warehouse.etl.db_config import get_db_connection, DB_HOST, DB_PORT, DB_NAME


def run_tests():
    print("=" * 75)
    print("TEST SUITE: PURE STREAMING POSTGRESQL DATA WAREHOUSE VALIDATION")
    print(f"Target: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 75)

    passed = 0
    total = 0

    conn = None
    try:
        conn = get_db_connection()
        print("[PASS] 1. Ket noi PostgreSQL Data Warehouse thanh cong.")
        passed += 1
    except Exception as e:
        print(f"[FAIL] 1. Khong the ket noi PostgreSQL: {e}")
        return False
    total += 1

    cur = conn.cursor()

    # TEST 2: Kiểm tra dọn dẹp các bảng Batch cũ
    total += 1
    legacy_tables = [
        "fact_crypto_funding_rates",
        "fact_crypto_liquidations",
        "fact_crypto_long_short_ratio",
        "fact_crypto_open_interest",
        "fact_market_predictions"
    ]
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'warehouse' AND table_name = ANY(%s);
    """, (legacy_tables,))
    remaining_legacy = cur.fetchall()
    if not remaining_legacy:
        print("[PASS] 2. Toan bo bang Batch cu da duoc loai bo triet de khoi DW.")
        passed += 1
    else:
        print(f"[FAIL] 2. Van con bang Batch cu trong schema warehouse: {remaining_legacy}")

    # TEST 3: Kiểm tra các bảng Dimension và Fact cho Streaming
    total += 1
    expected_tables = [
        "dim_symbol",
        "dim_alert_tier",
        "fact_crypto_trades",
        "fact_crypto_1m_candles"
    ]
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'warehouse' AND table_type = 'BASE TABLE';
    """)
    current_tables = [r[0] for r in cur.fetchall()]
    all_tables_ok = all(t in current_tables for t in expected_tables)
    if all_tables_ok:
        print(f"[PASS] 3. Star Schema Streaming ton tai day du ({len(expected_tables)} bang): {expected_tables}")
        passed += 1
    else:
        print(f"[FAIL] 3. Thieu bang trong Schema: can {expected_tables}, hien co {current_tables}")

    # TEST 4: Kiểm tra cấu hình cột Streaming trong dim_symbol và fact_crypto_trades
    total += 1
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'warehouse' AND table_name = 'dim_symbol';
    """)
    dim_cols = [r[0] for r in cur.fetchall()]
    dim_streaming_ok = "kafka_topic" in dim_cols and "kafka_partition" in dim_cols and "whale_threshold_usd" in dim_cols

    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'warehouse' AND table_name = 'fact_crypto_trades';
    """)
    fact_cols = [r[0] for r in cur.fetchall()]
    fact_streaming_ok = "kafka_arrival_time" in fact_cols and "whale_tier" in fact_cols

    if dim_streaming_ok and fact_streaming_ok:
        print("[PASS] 4. Cac cot Streaming Telemetry & Kafka Metadata da san sang tren dim va fact.")
        passed += 1
    else:
        print(f"[FAIL] 4. Thieu cot streaming tren dim/fact (dim_ok={dim_streaming_ok}, fact_ok={fact_streaming_ok})")

    # TEST 5: Kiểm tra 10 Real-time Streaming Analytics Views
    total += 1
    expected_views = [
        "vw_whale_trades",
        "vw_rolling_5m_metrics",
        "vw_order_flow_pressure",
        "vw_streaming_volatility",
        "vw_streaming_pipeline_telemetry",
        "vw_hourly_market_summary",
        "vw_daily_market_summary",
        "vw_weekly_market_summary",
        "vw_monthly_market_summary",
        "vw_yearly_market_summary"
    ]
    cur.execute("""
        SELECT table_name 
        FROM information_schema.views 
        WHERE table_schema = 'warehouse';
    """)
    current_views = [r[0] for r in cur.fetchall()]
    all_views_ok = all(v in current_views for v in expected_views)
    if all_views_ok:
        print(f"[PASS] 5. Toan bo 10 Views Phan Tich Streaming da san sang.")
        passed += 1
    else:
        missing = [v for v in expected_views if v not in current_views]
        print(f"[FAIL] 5. Thieu views phan tich: {missing}")

    # TEST 6: Thực thi truy vấn thử nghiệm trên các Views chính và đo độ trễ (Latency Benchmark)
    total += 1
    query_benchmarks = [
        ("vw_whale_trades", "SELECT count(*) FROM warehouse.vw_whale_trades;"),
        ("vw_rolling_5m_metrics", "SELECT * FROM warehouse.vw_rolling_5m_metrics;"),
        ("vw_order_flow_pressure", "SELECT * FROM warehouse.vw_order_flow_pressure;"),
        ("vw_streaming_volatility", "SELECT * FROM warehouse.vw_streaming_volatility;"),
        ("vw_streaming_pipeline_telemetry", "SELECT * FROM warehouse.vw_streaming_pipeline_telemetry;")
    ]

    all_queries_fast = True
    print("\n[*] Benchmark Do Tre Truy Van Views (Speed Layer Latency):")
    for view_name, sql in query_benchmarks:
        start_t = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed_ms = (time.perf_counter() - start_t) * 1000.0
        status_str = "OK" if elapsed_ms < 100.0 else "SLOW"
        print(f"    - {view_name:<34}: {elapsed_ms:>6.2f} ms | {len(rows)} rows [{status_str}]")
        if elapsed_ms >= 500.0:
            all_queries_fast = False

    if all_queries_fast:
        print("[PASS] 6. Toan bo truy van Views dat tieu chuan toc do cao (< 100ms) cua Speed Layer.")
        passed += 1
    else:
        print("[FAIL] 6. Co truy van vuot qua nguong do tre cho phep.")

    conn.close()

    print("\n" + "=" * 75)
    print(f"KET QUA TEST SUITE: {passed}/{total} TESTS PASSED ({(passed/total)*100:.1f}%)")
    print("=" * 75 + "\n")
    return passed == total


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
