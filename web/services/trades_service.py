"""
=============================================================================
Crypto Big Data - Web Large Trades Service (Tab 2)
=============================================================================
Service xử lý nghiệp vụ truy vấn giao dịch lớn từ PostgreSQL Data Warehouse:
  - Lọc theo ngưỡng tối thiểu (min_value) và khoảng thời gian (1h, 24h, 7d, all).
  - Thống kê tổng số và tổng giá trị trên toàn bộ tập kết quả thỏa điều kiện.
  - Phân trang chuẩn xác có thứ tự: ORDER BY trade_time DESC, trade_id DESC.
  - Xuất danh sách giao dịch lớn ra file CSV chuẩn.
=============================================================================
"""

import io
import csv
import math
from datetime import datetime, timezone
from typing import Dict, Any, Callable, Tuple, Optional


def format_iso_utc(dt):
    """Chuyển đổi datetime sang chuỗi ISO UTC chuẩn ending with Z"""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(dt)


def get_large_trades(
    symbol: str,
    time_range: str,
    min_value: float,
    page: int,
    page_size: int,
    get_db_conn: Callable[[], Any]
) -> Tuple[Dict[str, Any], int]:
    """
    Truy vấn danh sách giao dịch lớn từ PostgreSQL Data Warehouse có phân trang.
    Thống kê tổng số và tổng giá trị được tính toán trên toàn bộ tập kết quả.
    """
    offset = (page - 1) * page_size
    conn = get_db_conn()
    if not conn:
        return {
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }, 500

    try:
        with conn.cursor() as cur:
            where_conditions = ["symbol = %s", "trade_value >= %s"]
            params = [symbol, min_value]

            if time_range == "1h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '1 hour'")
            elif time_range == "24h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '24 hours'")
            elif time_range == "7d":
                where_conditions.append("trade_time >= NOW() - INTERVAL '7 days'")

            where_sql = "WHERE " + " AND ".join(where_conditions)

            # 1. Thống kê toàn bộ tập kết quả thỏa điều kiện
            count_sql = f"""
                SELECT COUNT(*), COALESCE(SUM(trade_value), 0.0), MAX(trade_time)
                FROM warehouse.fact_crypto_trades
                {where_sql};
            """
            cur.execute(count_sql, params)
            agg = cur.fetchone()
            total_count = agg[0]
            total_value = float(agg[1])
            data_ts = format_iso_utc(agg[2])

            # 2. Truy vấn dữ liệu có phân trang, sắp xếp ổn định deterministic
            query_sql = f"""
                SELECT trade_id, trade_time, side, price, size, trade_value
                FROM warehouse.fact_crypto_trades
                {where_sql}
                ORDER BY trade_time DESC, trade_id DESC
                LIMIT %s OFFSET %s;
            """
            cur.execute(query_sql, params + [page_size, offset])
            rows = cur.fetchall()

            trades = []
            for r in rows:
                trades.append({
                    "trade_id": str(r[0]),
                    "trade_time": format_iso_utc(r[1]),
                    "side": r[2].upper(),
                    "price": float(r[3]),
                    "size": float(r[4]),
                    "trade_value": float(r[5])
                })

            total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1

            return {
                "status": "success",
                "source": "postgres_dw",
                "total_count": total_count,
                "total_value": round(total_value, 2),
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "trades": trades,
                "data_timestamp": data_ts
            }, 200
    except Exception as e:
        return {
            "status": "error",
            "message": f"Lỗi truy vấn giao dịch lớn từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }, 500
    finally:
        conn.close()


def generate_large_trades_csv(
    symbol: str,
    time_range: str,
    min_value: float,
    get_db_conn: Callable[[], Any]
) -> Tuple[Optional[str], Optional[str]]:
    """
    Xuất nội dung CSV danh sách giao dịch lớn từ PostgreSQL Data Warehouse.
    Trả về (csv_string, error_message).
    """
    conn = get_db_conn()
    if not conn:
        return None, "Không thể kết nối PostgreSQL Data Warehouse"

    try:
        with conn.cursor() as cur:
            where_conditions = ["symbol = %s", "trade_value >= %s"]
            params = [symbol, min_value]

            if time_range == "1h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '1 hour'")
            elif time_range == "24h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '24 hours'")
            elif time_range == "7d":
                where_conditions.append("trade_time >= NOW() - INTERVAL '7 days'")

            where_sql = "WHERE " + " AND ".join(where_conditions)
            cur.execute(f"""
                SELECT trade_id, trade_time, side, price, size, trade_value
                FROM warehouse.fact_crypto_trades
                {where_sql}
                ORDER BY trade_time DESC, trade_id DESC
                LIMIT 5000;
            """, params)
            rows = cur.fetchall()

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["trade_id", "trade_time_utc", "side", "price_usdt", "size_btc", "value_usdt"])
            for r in rows:
                writer.writerow([
                    str(r[0]),
                    format_iso_utc(r[1]),
                    r[2].upper(),
                    f"{float(r[3]):.2f}",
                    f"{float(r[4]):.6f}",
                    f"{float(r[5]):.2f}"
                ])

            return output.getvalue(), None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()

