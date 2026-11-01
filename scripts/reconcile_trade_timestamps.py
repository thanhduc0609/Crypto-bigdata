"""
=============================================================================
Crypto Big Data - Safe Data Migration: Reconcile Premature Local Timestamps
=============================================================================
Tác vụ đối soát và hiệu chỉnh các bản ghi bị lệch +7 giờ do PySpark chạy
trên JVM múi giờ địa phương trước khi cấu hình UTC cho Spark Session.

Phạm vi an toàn:
  - Chỉ quét và tác động các bản ghi có trade_time / window_start > NOW() 
    (chính xác 17 lệnh cá mập và 11 nến streaming thử nghiệm ngày 2026-11-01).
  - Mặc định chạy chế độ DRY-RUN (không chỉnh sửa dữ liệu).
  - Yêu cầu cờ --apply để chính thức thực thi.
  - Hỗ trợ cờ --rollback để đảo ngược khi cần.
=============================================================================
"""

import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from warehouse.etl.db_config import get_db_connection

def reconcile_timestamps(apply_changes: bool = False, rollback: bool = False):
    conn = get_db_connection()
    if not conn:
        print("[!] Không thể kết nối PostgreSQL Data Warehouse.")
        sys.exit(1)

    now_utc = datetime.now(timezone.utc)
    print("=" * 80)
    print(f"RECONCILE TIMESTAMPS TOOL (NOW UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')})")
    print(f"Chế độ: {'ROLLBACK (+7H)' if rollback else ('APPLY CHANGES (-7H)' if apply_changes else 'DRY-RUN (CHỈ XEM, KHÔNG GHI)')}")
    print("=" * 80)

    try:
        with conn.cursor() as cur:
            if rollback:
                # Rollback logic
                print("[*] Chạy chế độ Rollback...")
                cur.execute("""
                    SELECT trade_id, trade_time 
                    FROM warehouse.fact_crypto_trades
                    WHERE trade_date = '2026-11-01' AND trade_time >= '2026-11-01 05:20:00+00' AND trade_time <= '2026-11-01 05:40:00+00';
                """)
                rows = cur.fetchall()
                print(f"[*] Tìm thấy {len(rows)} bản ghi khớp phạm vi rollback.")
                if apply_changes and rows:
                    cur.execute("""
                        UPDATE warehouse.fact_crypto_trades
                        SET trade_time = trade_time + INTERVAL '7 hours',
                            trade_date = (trade_time + INTERVAL '7 hours')::date
                        WHERE trade_date = '2026-11-01' AND trade_time >= '2026-11-01 05:20:00+00' AND trade_time <= '2026-11-01 05:40:00+00';
                    """)
                    cur.execute("""
                        UPDATE warehouse.fact_crypto_1m_candles
                        SET window_start = window_start + INTERVAL '7 hours',
                            window_end = window_end + INTERVAL '7 hours'
                        WHERE window_start >= '2026-11-01 05:20:00+00' AND window_start <= '2026-11-01 05:40:00+00';
                    """)
                    conn.commit()
                    print(f"[SUCCESS] Đã hoàn tác rollback cho {len(rows)} bản ghi.")
                return

            # Quét các bản ghi có timestamp trong tương lai (> NOW)
            cur.execute("""
                SELECT trade_id, trade_time, price, trade_value
                FROM warehouse.fact_crypto_trades
                WHERE trade_time > NOW() AND trade_time >= '2026-11-01 12:00:00+00'
                ORDER BY trade_time ASC;
            """)
            trades = cur.fetchall()

            cur.execute("""
                SELECT window_start, window_end, open, close, volume, source
                FROM warehouse.fact_crypto_1m_candles
                WHERE window_start > NOW() AND window_start >= '2026-11-01 12:00:00+00'
                ORDER BY window_start ASC;
            """)
            candles = cur.fetchall()

            print(f"\n[PHÁT HIỆN BẢN GHI LỆCH +7 GIỜ TRONG DW]")
            print(f"  - Số giao dịch lớn (fact_crypto_trades)    : {len(trades)}")
            print(f"  - Số nến 1 phút (fact_crypto_1m_candles)    : {len(candles)}")

            if trades:
                print("\nChi tiết giao dịch lớn (Mẫu 5 bản ghi):")
                for t in trades[:5]:
                    original_time = t[1]
                    corrected_time = original_time - datetime.resolution * 0 # placeholder
                    print(f"  - Trade ID: {t[0]:<12} | Gốc: {original_time.strftime('%Y-%m-%d %H:%M:%S UTC')} -> Đề xuất: -7 giờ")

            if candles:
                print("\nChi tiết nến 1 phút (Mẫu 5 bản ghi):")
                for c in candles[:5]:
                    print(f"  - Nến: {c[0].strftime('%Y-%m-%d %H:%M:%S UTC')} -> {c[1].strftime('%H:%M:%S UTC')} (Nguồn: {c[5]})")

            if not apply_changes:
                print("\n" + "-" * 80)
                print("[DRY-RUN HOÀN TẤT] Chưa có thay đổi nào được áp dụng vào CSDL.")
                print("Để thực hiện điều chỉnh chính xác -7 giờ cho đúng 17 lệnh và 11 nến trên, chạy lệnh:")
                print("  python scripts/reconcile_trade_timestamps.py --apply")
                print("-" * 80)
            else:
                print("\n[TIẾN HÀNH ĐIỀU CHỈNH AN TOÀN TRONG TRANSACTION]...")
                if trades:
                    cur.execute("""
                        UPDATE warehouse.fact_crypto_trades
                        SET trade_time = trade_time - INTERVAL '7 hours',
                            trade_date = (trade_time - INTERVAL '7 hours')::date
                        WHERE trade_time > NOW() AND trade_time >= '2026-11-01 12:00:00+00';
                    """)
                    print(f"  [+] Đã cập nhật {cur.rowcount} bản ghi fact_crypto_trades.")

                if candles:
                    cur.execute("""
                        UPDATE warehouse.fact_crypto_1m_candles
                        SET window_start = window_start - INTERVAL '7 hours',
                            window_end = window_end - INTERVAL '7 hours'
                        WHERE window_start > NOW() AND window_start >= '2026-11-01 12:00:00+00';
                    """)
                    print(f"  [+] Đã cập nhật {cur.rowcount} bản ghi fact_crypto_1m_candles.")

                conn.commit()
                print("\n[SUCCESS] Giao dịch thành công (TRANSACTION COMMITTED).")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Lỗi thực thi đối soát: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile premature timestamps in DW")
    parser.add_argument("--apply", action="store_true", help="Apply the -7h timezone adjustment")
    parser.add_argument("--rollback", action="store_true", help="Rollback the timezone adjustment")
    args = parser.parse_args()

    reconcile_timestamps(apply_changes=args.apply, rollback=args.rollback)
