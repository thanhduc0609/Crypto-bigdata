"""Evidence from production cleaning, a read-only DW snapshot and daily replay."""
from collections import Counter
import json
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from processing.quality.inventory import candle_profile, continuity, sha256


def trade_replay(spark, frame, directory):
    from processing.batch.bronze_to_silver import transform_bronze_trades
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    input_path = directory / "snapshot_input.parquet"
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), input_path,
                   coerce_timestamps="us", allow_truncated_timestamps=True)
    parsed, invalid, valid, cleaned = transform_bronze_trades(spark.read.parquet(str(input_path)))
    parsed_pd, invalid_pd, valid_pd, after = [df.toPandas() for df in (parsed, invalid, valid, cleaned)]
    after.to_parquet(directory / "cleaned_replay.parquet", index=False)
    invalid_pd.to_parquet(directory / "rejected_replay.parquet", index=False)
    prices = pd.to_numeric(parsed_pd.price, errors="coerce")
    sizes = pd.to_numeric(parsed_pd["size"], errors="coerce")
    stamps = pd.to_numeric(parsed_pd.raw_timestamp, errors="coerce")
    flags = {
        "missing_trade_id": parsed_pd.trade_id.isna() | parsed_pd.trade_id.fillna("").str.strip().eq(""),
        "wrong_or_missing_symbol": parsed_pd.symbol.ne("BTC-USDT"),
        "invalid_price": ~np.isfinite(prices) | (prices <= 0),
        "invalid_size": ~np.isfinite(sizes) | (sizes <= 0),
        "invalid_timestamp": stamps.isna() | (stamps <= 0),
        "invalid_side": ~parsed_pd.side.isin(["buy", "sell"]),
    }
    reasons = pd.Series("valid", index=parsed_pd.index)
    for name, bad in reversed(list(flags.items())):
        reasons.loc[bad] = name
    assert (reasons != "valid").sum() == len(invalid_pd), "Audit reasons disagree with production validation"
    duplicates = len(valid_pd) - len(after)
    assert len(frame) == len(invalid_pd) + duplicates + len(after)
    group_fields = ["trade_id", "raw_timestamp"] + (["kafka_offset"] if "kafka_offset" in valid_pd else [])
    conflicting_ids = 0
    ambiguous_ties = 0
    if len(valid_pd):
        values = valid_pd[["trade_id", "raw_timestamp", "price", "size", "side"]].drop_duplicates()
        conflicting_ids = int((values.groupby("trade_id").size() > 1).sum())
        distinct = valid_pd.drop_duplicates(group_fields + ["price", "size", "side"])
        ambiguous_ties = int((distinct.groupby(group_fields, dropna=False).size() > 1).sum())
    lineage_cols = [c for c in ("trade_id", "symbol", "raw_timestamp", "price", "size", "side", "kafka_partition", "kafka_offset", "_source_object", "_source_sha256") if c in after]
    after[lineage_cols].head(50).to_csv(directory / "lineage_examples.csv", index=False, encoding="utf-8-sig")
    invalid_pd.head(50).to_csv(directory / "rejected_examples.csv", index=False, encoding="utf-8-sig")
    total_values = pd.to_numeric(after.trade_value, errors="coerce")
    result = dict(mode="read_only_replay_of_production_transform", raw_rows=len(frame),
                  rejected_rows=len(invalid_pd), rejection_reasons_primary=dict(Counter(reasons[reasons != "valid"])),
                  rejection_reasons_overlapping={k: int(v.sum()) for k, v in flags.items()},
                  valid_before_dedup=len(valid_pd), duplicate_rows_removed=duplicates, cleaned_rows=len(after),
                  conflicting_trade_ids=conflicting_ids, ambiguous_dedup_ties=ambiguous_ties,
                  count_balance_valid=True, duplicate_keys_after=int(after.trade_id.duplicated().sum()),
                  derived_trade_value_non_finite=int((~np.isfinite(total_values)).sum()),
                  first_event=str(after.trade_time.min()) if len(after) else None,
                  last_event=str(after.trade_time.max()) if len(after) else None,
                  event_minutes=continuity(pd.to_datetime(after.raw_timestamp, unit="ms", utc=True).dt.floor("min")),
                  missing_minute_interpretation="No observed trades in the captured event-time range; not proof of missing exchange trades",
                  input_sha256=sha256(input_path), output_sha256=sha256(directory / "cleaned_replay.parquet"))
    return result, after


def daily_replay(repo, directory):
    from ingestion.historical.okx_daily_backfill import clean_daily, aggregate_complete_months
    pointer = json.loads((repo / "data/long_term/current.json").read_text(encoding="utf-8"))
    run = repo / "data/long_term/runs" / pointer["run_id"]
    assert sha256(run / "forecast.json") == pointer["forecast_sha256"]
    source = json.loads((run / "source.json").read_text(encoding="utf-8"))
    published = json.loads((run / "forecast.json").read_text(encoding="utf-8"))
    assert sha256(run / "raw_rows.json") == published["raw_sha256"]
    raw = json.loads((run / "raw_rows.json").read_text(encoding="utf-8"))
    daily, quality = clean_daily(raw, start=source["requested_start"], as_of=source["as_of"])
    monthly, excluded = aggregate_complete_months(daily, as_of=source["as_of"])
    pd.testing.assert_frame_equal(daily, pd.read_parquet(run / "daily.parquet"))
    pd.testing.assert_frame_equal(monthly, pd.read_parquet(run / "monthly.parquet"))
    daily.to_parquet(directory / "daily_replay.parquet", index=False)
    monthly.to_parquet(directory / "monthly_replay.parquet", index=False)
    pd.DataFrame([dict(raw_row_index=i, timestamp_ms=row[0], confirm=row[8], reason="unclosed")
                  for i, row in enumerate(raw) if isinstance(row, list) and len(row) >= 9 and str(row[8]) != "1"]
                 ).to_csv(directory / "daily_rejection_examples.csv", index=False, encoding="utf-8-sig")
    assert quality["raw_rows"] == quality["valid_daily_rows"] + sum(quality["rejected_rows"].values()) + quality["duplicate_rows_removed"]
    return dict(run_id=pointer["run_id"], source=source, quality=quality, complete_months=len(monthly),
                excluded_months=excluded, published_snapshot_matches_replay=True,
                source_as_of_used_not_current_clock=True,
                files={name: {"sha256": sha256(run / name), "bytes": (run / name).stat().st_size}
                       for name in ("raw_rows.json", "source.json", "daily.parquet", "monthly.parquet", "quality.json", "forecast.json")})


def postgres_profile(connect):
    """One repeatable-read, read-only transaction; event timestamps stay in UTC."""
    conn = connect()
    try:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute("SELECT NOW(), current_database(), current_setting('transaction_read_only')")
            stamp, database, readonly = cur.fetchone()
            assert readonly == "on"
            result = dict(snapshot_utc=stamp.isoformat(), database=database, read_only=True, tables={})
            cur.execute("SELECT column_name, numeric_scale FROM information_schema.columns WHERE table_schema='warehouse' AND table_name='fact_crypto_1m_candles' AND data_type='numeric'")
            result["candle_numeric_scales"] = dict(cur.fetchall())
            tables = {"fact_crypto_1m_candles": "window_start", "fact_crypto_trades": "trade_time",
                      "fact_btc_predictions": "feature_timestamp", "fact_btc_model_evaluation": None}
            for table, time_col in tables.items():
                cur.execute("SELECT to_regclass(%s)", ("warehouse." + table,))
                if cur.fetchone()[0] is None:
                    result["tables"][table] = {"status": "absent"}; continue
                cur.execute(f"SELECT COUNT(*), pg_total_relation_size('warehouse.{table}') FROM warehouse.{table}")
                count, size = cur.fetchone()
                item = dict(rows=count, relation_bytes=size)
                if time_col:
                    cur.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM warehouse.{table}")
                    item["first_event"], item["last_event"] = [v.isoformat() if v else None for v in cur.fetchone()]
                result["tables"][table] = item
            cur.execute("SELECT * FROM warehouse.fact_crypto_1m_candles ORDER BY symbol, window_start")
            frame = pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
            for name in frame.select_dtypes(include=["object"]).columns:
                if name in ("open", "high", "low", "close", "volume", "vwap", "total_trade_value", "buy_volume", "sell_volume", "trade_imbalance"):
                    frame[name] = pd.to_numeric(frame[name], errors="coerce")
            result["candles_by_symbol"] = {}
            for symbol, group in frame.groupby("symbol"):
                profile = candle_profile(group, symbol, as_of=stamp)
                profile["source_counts"] = group.source.value_counts(dropna=False).to_dict()
                profile["closed_counts"] = {str(k): int(v) for k, v in group.is_closed.value_counts(dropna=False).items()}
                end = pd.Timestamp(stamp).floor("min") - pd.Timedelta(minutes=1)
                start = end - pd.Timedelta(minutes=1439)
                observed = group[group.window_start.between(start, end)].window_start.nunique()
                profile["last_24h_closed_minute_grid"] = dict(start=start.isoformat(), end=end.isoformat(), expected=1440,
                    observed=int(observed), missing=1440-int(observed), note="Timestamp presence, not necessarily finalized candles")
                profile["unclosed_older_than_5m"] = int((group.is_closed.eq(False) & (group.window_start < pd.Timestamp(stamp)-pd.Timedelta(minutes=5))).sum())
                profile["duplicate_symbol_timestamp_keys"] = int(group.duplicated(["symbol", "window_start"]).sum())
                result["candles_by_symbol"][symbol] = profile
            cur.execute("SELECT source, COUNT(*), COUNT(*) FILTER(WHERE trade_count IS NULL), COUNT(*) FILTER(WHERE buy_volume IS NULL), COUNT(*) FILTER(WHERE sell_volume IS NULL) FROM warehouse.fact_crypto_1m_candles GROUP BY source")
            result["microstructure_nulls_by_source"] = [dict(zip(("source", "rows", "trade_count_null", "buy_volume_null", "sell_volume_null"), r)) for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE price <= 0 OR size <= 0 OR side NOT IN ('buy','sell')), COUNT(*) FILTER (WHERE trade_value < 100000) FROM warehouse.fact_crypto_trades")
            result["large_trades_scope"] = dict(zip(("rows", "invalid_basic_fields", "below_100k_rows"), cur.fetchone()))
            result["large_trades_scope"]["note"] = "DW streaming sink stores large trades, not all Bronze trades; do not compare counts as a cleaning loss."
        return result, frame
    finally:
        conn.rollback()
        conn.close()


def reconcile_candles(lake, warehouse, numeric_scales=None):
    """Compare only common keys and numeric fields; no authority/repair inference."""
    left, right = lake.copy(), warehouse.copy()
    for frame in (left, right):
        if "symbol" not in frame:
            frame["symbol"] = "BTC-USDT"
        frame["window_start"] = pd.to_datetime(frame.window_start, utc=True)
    keys = ["symbol", "window_start"]
    left, right = left.drop_duplicates(keys), right.drop_duplicates(keys)
    merged = left.merge(right, on=keys, suffixes=("_lake", "_dw"), how="outer", indicator=True)
    common = merged[merged._merge == "both"].copy()
    mismatch = pd.Series(False, index=common.index)
    raw_mismatch = pd.Series(False, index=common.index)
    per_field = {}
    raw_per_field = {}
    numeric_scales = numeric_scales or {**{c: 8 for c in ("open", "high", "low", "close", "volume")}, "total_trade_value": 4}
    for field in ("open", "high", "low", "close", "volume", "total_trade_value"):
        a, b = field + "_lake", field + "_dw"
        if a in common and b in common:
            values_a = pd.to_numeric(common[a], errors="coerce").to_numpy(dtype=float)
            values_b = pd.to_numeric(common[b], errors="coerce").to_numpy(dtype=float)
            raw_unequal = ~np.isclose(values_a, values_b, rtol=1e-12, atol=1e-8, equal_nan=True)
            # PostgreSQL NUMERIC rounds on storage. Half a storage unit plus a
            # few floating-point ULPs distinguishes rounding from real drift.
            tolerance = .5 * 10. ** (-numeric_scales.get(field, 8)) + 4 * np.spacing(np.maximum(abs(values_a), abs(values_b)))
            unequal = ~np.isclose(values_a, values_b, rtol=0, atol=tolerance, equal_nan=True)
            raw_mismatch |= raw_unequal
            raw_per_field[field] = int(raw_unequal.sum())
            per_field[field] = int(unequal.sum())
            mismatch |= unequal
    result = dict(lake_unique_keys=len(left), warehouse_unique_keys=len(right), common_keys=len(common),
                  lake_only_keys=int((merged._merge == "left_only").sum()), warehouse_only_keys=int((merged._merge == "right_only").sum()),
                  mismatching_common_keys=int(mismatch.sum()), mismatches_by_field=per_field,
                  raw_precision_mismatching_common_keys=int(raw_mismatch.sum()), raw_precision_mismatches_by_field=raw_per_field,
                  rounding_only_common_keys=int((raw_mismatch & ~mismatch).sum()), numeric_scales=numeric_scales,
                  tolerance_rule="Half a PostgreSQL NUMERIC storage unit + 4 floating-point ULPs; no relative tolerance",
                  note="Different capture times and REST authority may explain differences; no automatic repair performed")
    return result, common.loc[mismatch]
