"""
=============================================================================
Crypto Big Data - Data Warehouse Database Configuration
=============================================================================
"""

import os
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

    # Fallback nếu dùng file cũ
    if not sql_files:
        ddl_file = sql_dir / "01_schema_ddl.sql"
        views_file = sql_dir / "02_analytics_views.sql"
        if ddl_file.exists():
            sql_files.append(ddl_file)
        if views_file.exists():
            sql_files.append(views_file)

    print(f"[*] Dang ket noi toi PostgreSQL Data Warehouse: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for sql_file in sql_files:
                print(f"[+] Thuc thi SQL: {sql_file.relative_to(project_root)}")
                cur.execute(sql_file.read_text(encoding="utf-8"))
        conn.commit()
    print("[OK] Khoi tao toan bo Schema (dim, fact, views) thanh cong trong schema 'warehouse'!")


if __name__ == "__main__":
    try:
        init_warehouse_schema()
    except Exception as e:
        print(f"[ERROR] Khong the khoi tao Schema: {e}")
