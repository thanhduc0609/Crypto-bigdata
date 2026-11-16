"""
=============================================================================
Crypto Big Data - Web Prediction Service (Tab 3)
=============================================================================
Service xử lý nghiệp vụ dự báo hướng giá XGBoost và lịch sử đối chiếu:
  - Dự báo gần nhất từ warehouse.fact_btc_predictions.
  - Lịch sử dự báo phân trang và trạng thái đối chiếu thực tế.
  - Metadata mô hình và kết quả đánh giá đọc trực tiếp từ artifact chuẩn.
=============================================================================
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path
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


def get_latest_prediction(
    symbol: str,
    get_db_conn: Callable[[], Any]
) -> Tuple[Dict[str, Any], int]:
    """
    Trả về kết quả dự báo gần nhất từ warehouse.fact_btc_predictions.
    Chỉ đọc dữ liệu thật, không tự ý chạy suy luận khi chỉ xem trang.
    """
    conn = get_db_conn()
    if not conn:
        return {
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }, 500

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    prediction_id, symbol, feature_timestamp, prediction_timestamp, target_timestamp,
                    predicted_direction, probability_up, probability_not_up,
                    model_version, model_name, current_price, horizon_minutes,
                    actual_direction, actual_future_price, is_correct
                FROM warehouse.fact_btc_predictions
                WHERE symbol = %s
                ORDER BY feature_timestamp DESC
                LIMIT 1;
            """, (symbol,))
            row = cur.fetchone()

            if not row:
                return {
                    "status": "empty",
                    "prediction": None,
                    "message": "Chưa có dự báo nào trong cơ sở dữ liệu.",
                    "source": "postgres_dw"
                }, 200

            now_utc = datetime.now(timezone.utc)
            target_ts = row[4].astimezone(timezone.utc) if row[4] else None

            # Xác định trạng thái đối chiếu
            if target_ts and target_ts > now_utc:
                eval_status = "PENDING_TARGET"
            elif row[12] is None:
                eval_status = "MISSING_ACTUAL"
            else:
                eval_status = "EVALUATED"

            pred = {
                "prediction_id": row[0],
                "symbol": row[1],
                "feature_timestamp": format_iso_utc(row[2]),
                "prediction_timestamp": format_iso_utc(row[3]),
                "target_timestamp": format_iso_utc(row[4]),
                "predicted_direction": row[5],
                "direction_label": "UP" if row[5] == 1 else "NOT_UP",
                "probability_up": round(float(row[6]), 4) if row[6] is not None else None,
                "probability_not_up": round(float(row[7]), 4) if row[7] is not None else None,
                "model_version": row[8],
                "model_name": row[9],
                "current_price": float(row[10]) if row[10] is not None else None,
                "horizon_minutes": row[11],
                "actual_direction": row[12],
                "actual_direction_label": "UP" if row[12] == 1 else ("NOT_UP" if row[12] == 0 else None),
                "actual_future_price": float(row[13]) if row[13] is not None else None,
                "is_correct": row[14],
                "eval_status": eval_status,
                "source": "postgres_dw"
            }

            return {
                "status": "success",
                "prediction": pred,
                "data_timestamp": pred["feature_timestamp"]
            }, 200
    except Exception as e:
        return {
            "status": "error",
            "message": f"Lỗi truy vấn dự báo từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }, 500
    finally:
        conn.close()


def get_prediction_history(
    symbol: str,
    page: int,
    page_size: int,
    get_db_conn: Callable[[], Any]
) -> Tuple[Dict[str, Any], int]:
    """
    Trả về lịch sử dự báo dạng bảng có phân trang từ warehouse.fact_btc_predictions.
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
            cur.execute("""
                SELECT count(DISTINCT to_timestamp(floor(extract(epoch from feature_timestamp) / 300.0) * 300))
                FROM warehouse.fact_btc_predictions 
                WHERE symbol = %s;
            """, (symbol,))
            total_count = cur.fetchone()[0]

            cur.execute("""
                WITH binned_predictions AS (
                    SELECT DISTINCT ON (to_timestamp(floor(extract(epoch from feature_timestamp) / 300.0) * 300))
                        prediction_id, feature_timestamp, current_price,
                        predicted_direction, probability_up, probability_not_up,
                        target_timestamp, actual_future_price, actual_direction, is_correct
                    FROM warehouse.fact_btc_predictions
                    WHERE symbol = %s
                    ORDER BY to_timestamp(floor(extract(epoch from feature_timestamp) / 300.0) * 300) DESC, feature_timestamp ASC
                )
                SELECT 
                    prediction_id, feature_timestamp, current_price,
                    predicted_direction, probability_up, probability_not_up,
                    target_timestamp, actual_future_price, actual_direction, is_correct
                FROM binned_predictions
                ORDER BY feature_timestamp DESC
                LIMIT %s OFFSET %s;
            """, (symbol, page_size, offset))
            rows = cur.fetchall()

            now_utc = datetime.now(timezone.utc)
            history = []
            for r in rows:
                target_ts = r[6].astimezone(timezone.utc) if r[6] else None
                if target_ts and target_ts > now_utc:
                    eval_status = "PENDING_TARGET"
                elif r[8] is None:
                    eval_status = "MISSING_ACTUAL"
                else:
                    eval_status = "EVALUATED"

                history.append({
                    "prediction_id": r[0],
                    "feature_timestamp": format_iso_utc(r[1]),
                    "current_price": float(r[2]) if r[2] is not None else None,
                    "predicted_direction": r[3],
                    "direction_label": "UP" if r[3] == 1 else "NOT_UP",
                    "probability_up": round(float(r[4]), 4) if r[4] is not None else None,
                    "probability_not_up": round(float(r[5]), 4) if r[5] is not None else None,
                    "target_timestamp": format_iso_utc(r[6]),
                    "actual_future_price": float(r[7]) if r[7] is not None else None,
                    "actual_direction": r[8],
                    "actual_direction_label": "UP" if r[8] == 1 else ("NOT_UP" if r[8] == 0 else None),
                    "is_correct": r[9],
                    "eval_status": eval_status
                })

            total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1

            return {
                "status": "success",
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "history": history
            }, 200
    except Exception as e:
        return {
            "status": "error",
            "message": f"Lỗi truy vấn lịch sử dự báo: {e}",
            "source": "postgres_dw"
        }, 500
    finally:
        conn.close()


def get_model_metadata(project_root: Path) -> Tuple[Dict[str, Any], int]:
    """
    Trả về thông tin metadata và kết quả đánh giá mô hình đọc trực tiếp từ artifact thật.
    Không hardcode 18 đặc trưng hay phiên bản.
    """
    meta_path = project_root / "ml" / "artifacts" / "model_metadata.json"
    eval_path = project_root / "ml" / "artifacts" / "evaluation_results.json"

    meta_data = {}
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta_data = json.load(f)
        except Exception as e:
            print(f"[!] Warning reading model metadata: {e}")

    eval_data = {}
    if eval_path.exists():
        try:
            with open(eval_path, "r", encoding="utf-8") as f:
                eval_data = json.load(f)
        except Exception as e:
            print(f"[!] Warning reading evaluation results: {e}")

    ds_meta = meta_data.get("dataset_metadata", {})
    return {
        "status": "success",
        "model_name": meta_data.get("model_name", "btc_xgboost_direction"),
        "model_version": meta_data.get("model_version", "2.0.0"),
        "model_type": meta_data.get("model_type", "XGBClassifier"),
        "prediction_horizon_minutes": meta_data.get("prediction_horizon_minutes", 5),
        "created_at": meta_data.get("created_at"),
        "n_features": ds_meta.get("n_features", len(ds_meta.get("feature_names", []))),
        "feature_names": ds_meta.get("feature_names", []),
        "metrics": eval_data.get("metrics", {}),
        "feature_importance": eval_data.get("feature_importance", [])
    }, 200

