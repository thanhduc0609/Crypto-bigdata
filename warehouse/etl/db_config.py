"""
=============================================================================
Crypto Big Data - Real-Time Streaming Data Warehouse Configuration & Migration
=============================================================================
Architecture: Pure Streaming Serving Layer (PostgreSQL 16)
  - Dimensions : dim_symbol, dim_alert_tier
  - Facts      : fact_crypto_trades (Whale & Alert Trades), fact_crypto_1m_candles (1m Aggregations)
  - Views      : vw_whale_trades, vw_rolling_5m_metrics, vw_order_flow_pressure,
                 vw_streaming_volatility, vw_streaming_pipeline_telemetry,
                 vw_hourly_market_summary, vw_daily_market_summary, ...
=============================================================================
"""

import os
import sys
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine

project_root = Path(__file__).resolve().parent.parent.parent

def load_env_file(env_path: Path):
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() not in os.environ:
                os.environ[k.strip()] = v.strip().strip("'\"")

load_env_file(project_root / ".env")

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_NAME = os.getenv("POSTGRES_DB", "crypto_dw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")


def get_db_connection():
    """Tạo kết nối trực tiếp psycopg2 tới PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def get_db_engine():
    """Tạo SQLAlchemy engine cho batch insert hiệu năng cao"""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True)


def cleanup_legacy_batch_tables(conn):
    """
    Dọn dẹp các bảng Batch cũ không còn sử dụng trong dự án Pure Streaming
    nhằm giữ PostgreSQL luôn nhẹ, tinh gọn và đúng chuẩn kiến trúc Speed Layer.
    """
    legacy_tables = [
        "fact_crypto_funding_rates",
        "fact_crypto_liquidations",
        "fact_crypto_long_short_ratio",
        "fact_crypto_open_interest",
        "fact_market_predictions"
    ]
    with conn.cursor() as cur:
        for tbl in legacy_tables:
            cur.execute(f"DROP TABLE IF EXISTS warehouse.{tbl} CASCADE;")
    conn.commit()
    print("[*] Da don dep cac bang Batch cu khoi schema 'warehouse' thanh cong.")


def init_warehouse_schema():
    """Thực thi DDL và Views để khởi tạo Schema Data Warehouse (dim -> fact -> views)"""
    sql_dir = project_root / "warehouse" / "sql"
    dim_dir = sql_dir / "dim"
    fact_dir = sql_dir / "fact"
    views_dir = sql_dir / "views"

    sql_files = []
    
    # 1. Dimension tables
    if dim_dir.exists():
        sql_files.extend(sorted(dim_dir.glob("*.sql")))
    
    # 2. Fact tables
    if fact_dir.exists():
        sql_files.extend(sorted(fact_dir.glob("*.sql")))
    
    # 3. Views
    if views_dir.exists():
        sql_files.extend(sorted(views_dir.glob("*.sql")))

    print(f"[*] Dang ket noi toi PostgreSQL Data Warehouse: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    with get_db_connection() as conn:
        # Bước 1: Dọn dẹp bảng batch cũ
        cleanup_legacy_batch_tables(conn)

        # Bước 2: Nạp các tệp DDL và Views theo thứ tự
        with conn.cursor() as cur:
            for sql_file in sql_files:
                print(f"[+] Thuc thi SQL: {sql_file.relative_to(project_root)}")
                cur.execute(sql_file.read_text(encoding="utf-8"))
        conn.commit()

        # Bước 3: Kiểm tra và in báo cáo tình trạng Data Warehouse
        print("\n" + "=" * 65)
        print("BAO CAO TINH TRANG POSTGRESQL STREAMING DATA WAREHOUSE")
        print("=" * 65)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'warehouse' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            tables = [r[0] for r in cur.fetchall()]
            print(f"[*] Bang Du Lieu (Tables): {len(tables)}")
            for t in tables:
                cur.execute(f"SELECT count(*) FROM warehouse.{t};")
                count = cur.fetchone()[0]
                print(f"    - warehouse.{t:<26}: {count:>6} rows")

            cur.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'warehouse'
                ORDER BY table_name;
            """)
            views = [r[0] for r in cur.fetchall()]
            print(f"\n[*] Views Phan Tich (Streaming Views): {len(views)}")
            for v in views:
                print(f"    - warehouse.{v}")

        print("=" * 65)
    print("[OK] Khoi tao toan bo Schema Pure Streaming Data Warehouse hoan tat!\n")


if __name__ == "__main__":
    try:
        init_warehouse_schema()
    except Exception as e:
        print(f"[ERROR] Khong the khoi tao Schema: {e}")
        sys.exit(1)
