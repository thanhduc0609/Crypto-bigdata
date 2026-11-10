"""Atomic canonical upsert with persistent before-images and a version ledger."""
import re
import pandas as pd

from processing.quality.canonical_repair import BASE_COLUMNS, SYMBOL, validate_candidate, db_connect
from warehouse.etl.streaming_sink import build_upsert_candles_sql, _normalize_candle_records


def apply_confirmed_snapshot(frame, plan, manifest, connect=db_connect, before_validate=None):
    """No deletes; only frozen minute keys. Any check failure rolls back all DDL/data.

    A retry of an already-applied version verifies its contents, without
    rewriting newer REST corrections. before_validate is an isolated-test hook.
    """
    validate_candidate(frame,plan)
    if (manifest["run_id"]!=plan["run_id"] or manifest["rows"]!=len(frame)
        or not re.fullmatch(r"[a-f0-9]{64}",manifest.get("manifest_sha256",""))):
        raise ValueError("Publication descriptor does not match the candidate")
    conn=connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='120s'")
            cur.execute("SET LOCAL lock_timeout='10s'")
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",("canonical_repair_"+SYMBOL,))
            cur.execute("""CREATE TABLE IF NOT EXISTS warehouse.canonical_dataset_versions(
                run_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ NOT NULL, row_count BIGINT NOT NULL, manifest_uri TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL, before_rows BIGINT NOT NULL, inserted_rows BIGINT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS warehouse.canonical_repair_backups(
                run_id TEXT NOT NULL,symbol TEXT NOT NULL,window_start TIMESTAMPTZ NOT NULL,
                row_before JSONB NOT NULL,PRIMARY KEY(run_id,symbol,window_start));
                CREATE TEMP TABLE repair_stage (LIKE warehouse.fact_crypto_1m_candles INCLUDING DEFAULTS) ON COMMIT DROP;""")
            from psycopg2.extras import execute_values
            columns=", ".join(BASE_COLUMNS)
            values=frame[list(BASE_COLUMNS)].astype(object)
            values=values.where(pd.notna(values),None)
            records=_normalize_candle_records(values.to_dict("records"))
            execute_values(cur,"INSERT INTO pg_temp.repair_stage ("+columns+") VALUES %s",records,page_size=2000)
            cur.execute("SELECT manifest_sha256,row_count,before_rows,inserted_rows FROM warehouse.canonical_dataset_versions WHERE run_id=%s",(plan["run_id"],))
            previous=cur.fetchone()
            if previous and (previous[0]!=manifest["manifest_sha256"] or previous[1]!=len(frame)):
                raise ValueError("Run ID is already bound to a different publication")
            if not previous:
                cur.execute("SELECT MAX(end_time) FROM warehouse.canonical_dataset_versions WHERE symbol=%s",(SYMBOL,))
                newest=cur.fetchone()[0]
                if newest is not None and newest>pd.Timestamp(plan["requested_end"]).to_pydatetime():
                    raise ValueError("A newer canonical cutoff is already published; refusing an older repair")
            before_rows=previous[2] if previous else 0
            inserted_rows=previous[3] if previous else 0
            if not previous:
                cur.execute("""INSERT INTO warehouse.canonical_repair_backups
                    SELECT %s,t.symbol,t.window_start,to_jsonb(t)
                    FROM warehouse.fact_crypto_1m_candles t JOIN pg_temp.repair_stage s USING(symbol,window_start)""",(plan["run_id"],))
                before_rows=cur.rowcount
                inserted_rows=len(frame)-before_rows
                sql=build_upsert_candles_sql("warehouse.fact_crypto_1m_candles")
                sql=re.sub(r"VALUES\s*\([^)]*\)","SELECT "+columns+" FROM pg_temp.repair_stage WHERE TRUE",sql,count=1,flags=re.I)
                cur.execute(sql)
            if before_validate:
                before_validate(cur)
            # Both sides use the same NUMERIC storage types. Exact comparisons
            # here avoid tolerance hiding a wrong write or unknown closure.
            differences=" OR ".join("t."+name+" IS DISTINCT FROM s."+name for name in BASE_COLUMNS if name not in ("symbol","window_start"))
            cur.execute("SELECT COUNT(*) FROM pg_temp.repair_stage s LEFT JOIN warehouse.fact_crypto_1m_candles t USING(symbol,window_start) WHERE t.symbol IS NULL OR "+differences)
            mismatches=cur.fetchone()[0]
            if mismatches:
                raise ValueError(f"Canonical DW verification failed for {mismatches} keys; transaction rolled back")
            cur.execute("SELECT COUNT(*) FROM warehouse.fact_crypto_1m_candles WHERE symbol=%s AND window_start BETWEEN %s AND %s",(SYMBOL,plan["requested_start"],plan["requested_end"]))
            if cur.fetchone()[0]!=len(frame):
                raise ValueError("Canonical DW range has missing/extra keys")
            if not previous:
                cur.execute("""INSERT INTO warehouse.canonical_dataset_versions
                    (run_id,symbol,start_time,end_time,row_count,manifest_uri,manifest_sha256,before_rows,inserted_rows)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (plan["run_id"],SYMBOL,plan["requested_start"],plan["requested_end"],len(frame),manifest["manifest_uri"],manifest["manifest_sha256"],before_rows,inserted_rows))
        conn.commit()
        return dict(run_id=plan["run_id"],committed=True,already_applied=bool(previous),rows=len(frame),
                    before_rows=before_rows,inserted_rows=inserted_rows,mismatching_keys=0,backup_table="warehouse.canonical_repair_backups")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
