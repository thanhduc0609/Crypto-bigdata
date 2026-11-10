"""Confirmed REST-only canonical repair with frozen bounds and raw evidence.

Preparing never writes S3/DW. Publication uses unique immutable keys; existing
Gold is never overwritten. Database publication is transactional and audited.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
from uuid import uuid4

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ingestion.historical.candle_parser import parse_okx_rest_candle
from processing.quality.inventory import continuity, read_frame, sha256, write_json, now

ROOT = Path(__file__).resolve().parents[2]
ENDPOINT = "/api/v5/market/history-candles"
DOCS = "https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks-history"
SYMBOL = "BTC-USDT"
BASE_COLUMNS = ("symbol", "window_start", "window_end", "open", "high", "low", "close", "volume",
                "trade_count", "buy_volume", "sell_volume", "vwap", "trade_imbalance", "total_trade_value", "source", "is_closed")
PRICE_FIELDS = ("open", "high", "low", "close", "volume", "total_trade_value")


def new_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "Z-" + uuid4().hex[:10]


def db_connect(readonly=False):
    import psycopg2
    return psycopg2.connect(host=os.getenv("POSTGRES_HOST", "127.0.0.1"), port=os.getenv("POSTGRES_PORT", "5433"),
        dbname=os.getenv("POSTGRES_DB", "crypto_dw"), user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""), connect_timeout=5,
        options="-c timezone=UTC -c statement_timeout=60000" + (" -c default_transaction_read_only=on" if readonly else ""))


def current_dw(connect=db_connect):
    conn = connect(readonly=True)
    try:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute("SELECT NOW()")
            as_of = cur.fetchone()[0]
            cur.execute("SELECT * FROM warehouse.fact_crypto_1m_candles WHERE symbol=%s ORDER BY window_start", (SYMBOL,))
            frame = pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
        return frame, as_of
    finally:
        conn.rollback(); conn.close()


def prepare(audit_directory, as_of=None):
    audit_directory = Path(audit_directory).resolve()
    audit = json.loads((audit_directory / "run.json").read_text(encoding="utf-8"))
    if audit["state"] != "completed":
        raise ValueError("A completed inventory is required")
    objects = json.loads((audit_directory / "downloaded_objects.json").read_text(encoding="utf-8"))
    selected = [o for o in objects if o["dataset"] == "crypto/gold/candles_1m" and o["key"].endswith(".parquet")]
    if not selected:
        raise ValueError("Historical Gold snapshot absent")
    import boto3
    client = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
    for item in selected:
        if sha256(item["local"]) != item["sha256"]:
            raise ValueError("Audit source checksum changed")
        client.head_object(Bucket=audit["s3"]["bucket"], Key=item["key"], IfMatch=item["etag"])
    legacy = pd.concat([read_frame(o) for o in selected], ignore_index=True)
    warehouse, db_as_of = current_dw()
    as_of = pd.Timestamp(as_of or db_as_of)
    end = as_of.floor("min") - pd.Timedelta(minutes=5)
    start = min(pd.to_datetime(legacy.window_start, utc=True).min(), pd.to_datetime(warehouse.window_start, utc=True).min())
    if start >= end:
        raise ValueError("Invalid recovery bounds")
    run_id = new_run_id()
    directory = ROOT / "data/canonical_repairs" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    legacy.to_parquet(directory / "legacy_gold_before.parquet", index=False)
    warehouse.to_parquet(directory / "warehouse_before.parquet", index=False)
    root_prefix = os.environ["S3_RUN_PREFIX"].strip("/")
    if not root_prefix.startswith("crypto/local-runs/") or any(p in ("", ".", "..") for p in root_prefix.split("/")):
        raise ValueError("Repair requires the configured project run namespace")
    plan = dict(run_id=run_id, state="prepared", symbol=SYMBOL, bar="1m", started_utc=now(),
                requested_start=start.isoformat(), requested_end=end.isoformat(), source_as_of=as_of.isoformat(),
                expected_rows=int((end-start).total_seconds()//60)+1,
                endpoint=os.getenv("OKX_REST_URL", "https://www.okx.com").rstrip("/")+ENDPOINT,
                docs=DOCS, source_audit=str(audit_directory), source_audit_sha256=sha256(audit_directory/"run.json"),
                bucket=audit["s3"]["bucket"], s3_prefix=root_prefix+"/canonical-repairs/"+run_id,
                ownership_token=uuid4().hex,
                legacy_rows=len(legacy), legacy_unknown_closed=int(legacy.is_closed.isna().sum()),
                legacy_continuity=continuity(legacy.window_start), dw_rows=len(warehouse),
                baseline_files={name:sha256(directory/name) for name in ("legacy_gold_before.parquet", "warehouse_before.parquet")})
    write_json(directory / "plan.json", plan)
    return directory


class RateLimiter:
    def __init__(self, interval=.25):
        self.lock, self.next, self.interval = threading.Lock(), 0., interval

    def wait(self):
        with self.lock:
            current = time.monotonic()
            delay = max(0, self.next-current)
            self.next = max(current, self.next)+self.interval
        if delay:
            time.sleep(delay)


def validated_row(row, start_ms, end_ms, as_of):
    """Never infer closure from age, source name or a null flag."""
    if not isinstance(row, list) or len(row) < 9 or str(row[8]) != "1":
        raise ValueError("REST row lacks explicit confirm=1")
    if not str(row[0]).isdigit():
        raise ValueError("Timestamp must be integer milliseconds")
    stamp = int(row[0])
    if stamp % 60000 or not start_ms <= stamp <= end_ms:
        raise ValueError("REST row is outside the exact minute grid")
    parsed = parse_okx_rest_candle(row, SYMBOL, require_closed=True)
    if parsed is None or parsed["total_trade_value"] is None:
        raise ValueError("Invalid REST OHLCV/turnover")
    if parsed["window_end"] > pd.Timestamp(as_of).to_pydatetime():
        raise ValueError("Candle ends after source cutoff")
    return parsed


def download(directory, workers=4):
    directory = Path(directory)
    plan = json.loads((directory/"plan.json").read_text(encoding="utf-8"))
    start_ms = int(pd.Timestamp(plan["requested_start"]).timestamp()*1000)
    end_ms = int(pd.Timestamp(plan["requested_end"]).timestamp()*1000)
    limiter = RateLimiter()
    chunks = [(start, min(end_ms, start+1440*60000-60000)) for start in range(start_ms, end_ms+1, 1440*60000)]
    def chunk(bounds):
        low, high = bounds
        path = directory/"raw"/str(low)
        path.mkdir(parents=True, exist_ok=True)
        session = requests.Session()
        session.headers.update({"User-Agent":"Mozilla/5.0 CryptoBigdata-CanonicalRecovery/1.0"})
        cursor, records, pages, outside, duplicates = high+60000, {}, [], 0, 0
        try:
            for _ in range(30):
                file = path/(str(cursor)+".json")
                params = dict(instId=SYMBOL, bar="1m", limit=300, after=cursor)
                if file.exists():
                    expected = file.with_suffix(".sha256").read_text(encoding="utf-8").strip()
                    if sha256(file) != expected:
                        raise ValueError("Cached REST page checksum mismatch")
                    saved = json.loads(file.read_text(encoding="utf-8"))
                    if saved["request"] != params:
                        raise ValueError("Cached REST request differs")
                else:
                    for attempt in range(4):
                        limiter.wait()
                        try:
                            response = session.get(plan["endpoint"], params=params, timeout=(5,15))
                            response.raise_for_status()
                            payload = response.json()
                            if payload.get("code") != "0" or not isinstance(payload.get("data"), list):
                                raise ValueError("Unsuccessful OKX response")
                            saved = dict(request=params, fetched_at=now(), response=payload)
                            break
                        except (requests.RequestException, ValueError):
                            if attempt == 3:
                                raise
                            time.sleep(min(2**attempt,4))
                    write_json(file,saved)
                    file.with_suffix(".sha256").write_text(sha256(file),encoding="utf-8")
                rows = saved["response"]["data"]
                if not rows:
                    raise ValueError("History exhausted before the requested range was complete")
                oldest = min(int(row[0]) for row in rows)
                if oldest >= cursor:
                    raise ValueError("Pagination stalled")
                digest = sha256(file)
                pages.append(dict(path=file.relative_to(directory).as_posix(),sha256=digest,rows=len(rows)))
                for raw in rows:
                    stamp = int(raw[0])
                    if not low <= stamp <= high:
                        outside += 1; continue
                    parsed = validated_row(raw,low,high,plan["source_as_of"])
                    if stamp in records:
                        if any(records[stamp][k] != parsed[k] for k in PRICE_FIELDS):
                            raise ValueError("Conflicting confirmed REST duplicate")
                        duplicates += 1; continue
                    parsed.update(provenance_run_id=plan["run_id"], source_page=file.relative_to(directory).as_posix(),
                                  source_page_sha256=digest, source_fetched_at=saved["fetched_at"])
                    records[stamp] = parsed
                if oldest <= low:
                    break
                cursor = oldest
            else:
                raise ValueError("Chunk pagination bound exceeded")
            if set(records) != set(range(low,high+60000,60000)):
                raise ValueError("Confirmed REST minute grid has missing points; no interpolation allowed")
            frame = pd.DataFrame([records[t] for t in sorted(records)])
            result = dict(first_ms=low,last_ms=high,rows=len(frame),pages=pages,outside_range_rows=outside,duplicate_rows=duplicates)
            frame.to_parquet(path/"confirmed.parquet",index=False)
            write_json(path/"chunk.json",result)
            return result
        finally:
            session.close()
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for future in as_completed([pool.submit(chunk,bounds) for bounds in chunks]):
            item=future.result(); results.append(item)
            print(f"[REST RECOVERY] {len(results)}/{len(chunks)} chunks; {sum(x['rows'] for x in results):,} confirmed minutes",flush=True)
    results.sort(key=lambda x:x["first_ms"])
    write_json(directory/"download.json",dict(chunks=results,rows=sum(x["rows"] for x in results),finished_utc=now()))
    return results


def validate_candidate(frame, plan):
    """Fail closed on any missing minute, uncertain closure or invented flow."""
    if frame.empty or not all(c in frame for c in BASE_COLUMNS):
        raise ValueError("Canonical schema missing")
    start, end = pd.Timestamp(plan["requested_start"]), pd.Timestamp(plan["requested_end"])
    stamps = pd.to_datetime(frame.window_start, utc=True)
    expected = pd.date_range(start,end,freq="min")
    if frame.symbol.ne(SYMBOL).any() or stamps.duplicated().any() or not pd.DatetimeIndex(stamps).astype("datetime64[ns, UTC]").equals(expected):
        raise ValueError("Canonical symbol/minute grid is not exact and continuous")
    if not frame.is_closed.map(lambda value: isinstance(value,(bool,np.bool_)) and bool(value)).all():
        raise ValueError("Closure must be explicitly confirmed")
    if frame.source.ne("historical_rest").any():
        raise ValueError("Recovery must be confirmed REST only")
    if not (pd.to_datetime(frame.window_end,utc=True)-stamps).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("Invalid candle duration")
    if pd.to_datetime(frame.window_end,utc=True).max() > pd.Timestamp(plan["source_as_of"]):
        raise ValueError("Canonical range includes a future/unclosed minute")
    numeric=frame[list(PRICE_FIELDS)].apply(pd.to_numeric,errors="coerce")
    if not np.isfinite(numeric.to_numpy()).all():
        raise ValueError("Non-finite canonical prices/volume")
    if ((numeric[["open","high","low","close"]]<=0).any(axis=1) | (numeric.volume<0) |
        (numeric.total_trade_value<0) | (numeric.low>numeric[["open","close"]].min(axis=1)) |
        (numeric.high<numeric[["open","close"]].max(axis=1))).any():
        raise ValueError("Invalid canonical OHLCV")
    if frame[["trade_count","buy_volume","sell_volume","vwap","trade_imbalance"]].notna().any().any():
        raise ValueError("REST recovery must not invent microstructure")
    return dict(rows=len(frame),missing_minutes=0,duplicate_keys=0,unknown_closed=0,unclosed=0,all_rest_confirmed=True)


def build_candidate(directory):
    directory=Path(directory)
    plan=json.loads((directory/"plan.json").read_text(encoding="utf-8"))
    capture=json.loads((directory/"download.json").read_text(encoding="utf-8"))
    frames=[]
    raw_pages=[]
    for chunk in capture["chunks"]:
        frame=pd.read_parquet(directory/"raw"/str(chunk["first_ms"])/"confirmed.parquet")
        by_time={}
        for page in chunk["pages"]:
            file=directory/page["path"]
            if sha256(file)!=page["sha256"]:
                raise ValueError("REST evidence checksum changed")
            payload=json.loads(file.read_text(encoding="utf-8"))
            for raw in payload["response"]["data"]:
                if chunk["first_ms"] <= int(raw[0]) <= chunk["last_ms"]:
                    record=validated_row(raw,chunk["first_ms"],chunk["last_ms"],plan["source_as_of"])
                    stamp=int(raw[0])
                    if stamp in by_time and any(by_time[stamp][0][k]!=record[k] for k in PRICE_FIELDS):
                        raise ValueError("Conflicting confirmed REST evidence")
                    by_time.setdefault(stamp,(record,page["sha256"],page["path"],payload["fetched_at"]))
            raw_pages.append(page)
        for row in frame.to_dict("records"):
            stamp=int(pd.Timestamp(row["window_start"]).timestamp()*1000)
            original,digest,page,fetched=by_time[stamp]
            if (any(row[k]!=original[k] for k in PRICE_FIELDS) or row["source_page_sha256"]!=digest or
                row["source_page"]!=page or row["source_fetched_at"]!=fetched):
                raise ValueError("Candidate differs from its REST evidence")
        frames.append(frame)
    frame=pd.concat(frames,ignore_index=True).sort_values("window_start").reset_index(drop=True)
    checks=validate_candidate(frame,plan)
    if len(frame)!=plan["expected_rows"]:
        raise ValueError("Candidate count differs from frozen plan")
    legacy=pd.read_parquet(directory/"legacy_gold_before.parquet")
    old_times=pd.DatetimeIndex(pd.to_datetime(legacy.window_start,utc=True))
    new_times=pd.DatetimeIndex(pd.to_datetime(frame.window_start,utc=True))
    if len(old_times.difference(new_times)):
        raise ValueError("Candidate would discard historical keys")
    lineage=["provenance_run_id","source_page","source_page_sha256","source_fetched_at"]
    # Physical symbol is encoded in the Hive partition, not duplicated in-file.
    fields=[pa.field("window_start",pa.timestamp("us",tz="UTC")),pa.field("window_end",pa.timestamp("us",tz="UTC"))]
    for name in BASE_COLUMNS[3:]:
        kind=pa.int64() if name=="trade_count" else pa.bool_() if name=="is_closed" else pa.string() if name=="source" else pa.float64()
        fields.append(pa.field(name,kind))
    fields.extend(pa.field(name,pa.string()) for name in lineage)
    target=directory/"gold/candles_1m/symbol=BTC-USDT/canonical.parquet"
    target.parent.mkdir(parents=True,exist_ok=True)
    physical=frame[[c for c in BASE_COLUMNS if c!="symbol"]+lineage]
    pq.write_table(pa.Table.from_pandas(physical,schema=pa.schema(fields),preserve_index=False),target,compression="snappy")
    summary=dict(run_id=plan["run_id"],state="candidate_verified",checks=checks,
        requested_start=plan["requested_start"],requested_end=plan["requested_end"],
        legacy_rows=len(legacy),legacy_missing_before=plan["legacy_continuity"]["missing"],
        legacy_unknown_closed_before=plan["legacy_unknown_closed"],legacy_unknown_closed_confirmed=int(legacy.is_closed.isna().sum()),
        added_minutes_in_legacy_range=int(((new_times>=old_times.min())&(new_times<=old_times.max())).sum())-len(old_times),
        added_minutes_total=len(frame)-len(legacy),gold_file=target.relative_to(directory).as_posix(),gold_sha256=sha256(target),
        raw_pages=raw_pages,raw_page_count=len(raw_pages),source_as_of=plan["source_as_of"],endpoint=plan["endpoint"],docs=DOCS,
        implementation_sha256=sha256(Path(__file__)),verified_utc=now())
    candidate_file=directory/"candidate.json"
    if candidate_file.exists():
        previous=json.loads(candidate_file.read_text(encoding="utf-8"))
        if (previous["gold_sha256"]!=summary["gold_sha256"] or previous["raw_pages"]!=summary["raw_pages"]
            or previous["requested_start"]!=summary["requested_start"] or previous["requested_end"]!=summary["requested_end"]):
            raise ValueError("A repair run cannot be rebound to different evidence")
        # Preserve the initial build identity. Revalidation does not pretend
        # the immutable snapshot was freshly collected or rebuilt by new code.
        summary["verified_utc"]=previous["verified_utc"]
        summary["implementation_sha256"]=previous["implementation_sha256"]
    write_json(directory/"candidate.json",summary)
    frame[["window_start","source_page","source_page_sha256","source_fetched_at"]].head(50).to_csv(directory/"closure_evidence_examples.csv",index=False,encoding="utf-8-sig")
    return summary


def load_candidate(directory):
    directory=Path(directory)
    plan=json.loads((directory/"plan.json").read_text(encoding="utf-8"))
    candidate=json.loads((directory/"candidate.json").read_text(encoding="utf-8"))
    target=directory/candidate["gold_file"]
    if sha256(target)!=candidate["gold_sha256"]:
        raise ValueError("Canonical candidate checksum changed")
    frame=pq.ParquetFile(target).read().to_pandas()
    frame.insert(0,"symbol",SYMBOL)
    validate_candidate(frame,plan)
    return plan,candidate,frame
