"""Read-only candle failover. Cache and worker limits are per web process."""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from copy import deepcopy
from datetime import datetime, timezone
import json
import http.client
import logging
import math
import socket
import threading
import time
import urllib.error
import urllib.request


class CandleFetchError(Exception):
    def __init__(self, reason, attempts=0):
        super().__init__(reason)
        self.reason, self.attempts = reason, attempts


class BoundedOKXTransport:
    """2 attempts, 1.25s socket timeout, 3.2s caller wait, <=4 workers.

    DNS and slow socket readers cannot be forcibly cancelled by urllib. The
    bounded executor prevents those calls from blocking callers indefinitely or
    growing an unbounded queue. No retries for bad JSON / permanent HTTP errors.
    """
    def __init__(self, opener=urllib.request.urlopen, clock=time.monotonic,
                 sleep=time.sleep, wait_seconds=3.2, workers=4):
        self.opener, self.clock, self.sleep = opener, clock, sleep
        self.wait_seconds = wait_seconds
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="candles-okx")
        self.slots = threading.BoundedSemaphore(workers)

    def _request(self, url):
        deadline = self.clock() + 3.0
        for attempt in (1, 2):
            remaining = deadline - self.clock()
            if remaining <= 0:
                raise CandleFetchError("upstream_timeout", attempt - 1)
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Crypto-bigdata/1.0", "Accept": "application/json"})
                with self.opener(req, timeout=min(1.25, remaining)) as response:
                    raw = response.read(1024 * 1024 + 1)
                    if len(raw) > 1024 * 1024:
                        raise CandleFetchError("upstream_payload_too_large", attempt)
                    data = json.loads(raw.decode("utf-8"))
                    if not isinstance(data, dict):
                        raise CandleFetchError("upstream_invalid_payload", attempt)
                    return data, attempt
            except urllib.error.HTTPError as exc:
                if exc.code not in (408, 429, 500, 502, 503, 504):
                    raise CandleFetchError("upstream_http_rejected", attempt) from None
                reason = "upstream_http_transient"
            except (urllib.error.URLError, TimeoutError, socket.timeout, ConnectionError, OSError, http.client.HTTPException):
                reason = "upstream_network_error"
            except (ValueError, UnicodeError):
                raise CandleFetchError("upstream_invalid_json", attempt) from None
            if attempt == 2:
                raise CandleFetchError(reason, attempt)
            self.sleep(min(0.15, max(0, deadline - self.clock())))

    def fetch(self, url):
        if not self.slots.acquire(blocking=False):
            raise CandleFetchError("upstream_busy")
        try:
            future = self.pool.submit(self._request, url)
        except Exception:
            self.slots.release()
            raise
        future.add_done_callback(lambda _: self.slots.release())
        try:
            return future.result(timeout=self.wait_seconds)
        except FutureTimeout:
            raise CandleFetchError("upstream_deadline", attempts=None) from None


def iso_utc(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class CandleService:
    TTL = 3.0
    FAILURE_TTL = 5.0
    STALE_CACHE_LIMIT = 120.0
    MAX_KEYS = 64
    DURATIONS = {"1m": 60, "1h": 3600, "1d": 86400}

    def __init__(self, transport=None, clock=time.monotonic, wall=time.time):
        self.transport = transport or BoundedOKXTransport()
        self.clock, self.wall = clock, wall
        self.lock = threading.Lock()
        self.cache = OrderedDict()
        self.live_cache = OrderedDict()
        self.inflight = {}

    def _validate(self, payload, timeframe):
        rows = payload.get("candles")
        if not rows:
            raise ValueError("empty")
        previous = None
        duration = self.DURATIONS[timeframe]
        for row in rows:
            stamp = row["time"]
            if (not isinstance(stamp, int) or stamp % duration or stamp > self.wall() + 5
                    or (previous is not None and stamp <= previous)):
                raise ValueError("timestamp")
            prices = [row[k] for k in ("open", "high", "low", "close")]
            if not all(math.isfinite(p) and p > 0 for p in prices):
                raise ValueError("price")
            o, h, low, c = prices
            if low > min(o, c) or h < max(o, c) or low > h:
                raise ValueError("ohlc")
            if not math.isfinite(row["volume"]) or row["volume"] < 0:
                raise ValueError("volume")
            for field in ("vwap", "buy_volume", "sell_volume", "trade_imbalance", "total_trade_value"):
                val = row.get(field)
                if val is not None and (not math.isfinite(val) or (field != "trade_imbalance" and val < 0)):
                    raise ValueError(field)
            if row.get("is_closed") and stamp + duration > self.wall() + 5:
                raise ValueError("future_close")
            previous = stamp

    def _decorate(self, payload, served_from, reason=None, attempts=0):
        result = deepcopy(payload)
        now = self.wall()
        stamp = result["candles"][-1]["time"]
        # Bucket start is not the last trade timestamp. DW exposes its latest
        # contributing minute separately; OKX does not expose that in candles.
        observed = result.get("latest_observation_time")
        age = max(0, now - (observed + 60 if observed is not None
                           else stamp + self.DURATIONS[result["timeframe"]]))
        fetched = result.get("fetched_at_epoch", now)
        cache_age = max(0, now - fetched)
        stale = age > 120 or (served_from == "stale_cache" and cache_age > self.TTL)
        rows = result["candles"]
        missing = sum(max(0, (b["time"] - a["time"]) // self.DURATIONS[result["timeframe"]] - 1)
                      for a, b in zip(rows, rows[1:]))
        partial = any(r.get("coverage_complete") is False for r in rows)
        result.update(served_from=served_from, degraded=bool(reason) or stale or partial or bool(missing),
                      fallback_reason=reason, upstream_attempts=attempts,
                      is_stale=stale, freshness_lag_seconds=round(age, 1),
                      freshness_reference="latest_minute_end" if observed is not None else "latest_bucket_end",
                      cache_age_seconds=round(cache_age, 1), fetched_at=iso_utc(fetched),
                      response_timestamp=iso_utc(now), missing_intervals=missing,
                      coverage_complete=False if partial or missing else (None if observed is None else True))
        result.pop("fetched_at_epoch", None)
        return result

    def get(self, *, source_pref, symbol, timeframe, limit, okx_rest_url,
            get_db_conn, loader):
        if source_pref not in ("live", "dw") or timeframe not in self.DURATIONS or not 1 <= limit <= 300:
            return {"status": "error", "message": "source=live|dw, timeframe=1m|1h|1d, limit=1..300"}, 400
        key = (source_pref, symbol, timeframe, limit, okx_rest_url)
        with self.lock:
            entry = self.cache.get(key)
            if entry and self.clock() < entry[0]:
                body, code = deepcopy(entry[1]), entry[2]
                if code == 200:
                    origin = body.get("cache_origin", body["served_from"])
                    body = self._decorate(body, "stale_cache" if origin == "stale_cache" else "cache",
                                          body.get("fallback_reason"), 0)
                    body["cache_origin"] = origin
                return body, code
            event = self.inflight.get(key)
            if event is None:
                event = self.inflight[key] = threading.Event()
                owner = True
            else:
                owner = False
        if not owner:
            if event.wait(7.5):
                return self.get(source_pref=source_pref, symbol=symbol, timeframe=timeframe, limit=limit,
                                okx_rest_url=okx_rest_url, get_db_conn=get_db_conn, loader=loader)
            return {"status": "error", "message": "Nguồn nến đang bận; vui lòng thử lại", "reason": "request_busy"}, 503
        try:
            result, code = self._load(key, source_pref, symbol, timeframe, limit, okx_rest_url, get_db_conn, loader)
            if code != 200 or result.get("degraded"):
                logging.getLogger(__name__).warning(
                    "candles symbol=%s tf=%s status=%s source=%s via=%s reason=%s attempts=%s stale=%s",
                    symbol, timeframe, code, result.get("source"), result.get("served_from"),
                    result.get("fallback_reason", result.get("reason")), result.get("upstream_attempts"), result.get("is_stale"))
            # Keep an internal observation time for cache age; never advance it
            # when serving a failed refresh or a cache hit.
            cached = deepcopy(result)
            if code == 200:
                cached["fetched_at_epoch"] = datetime.fromisoformat(result["fetched_at"].replace("Z", "+00:00")).timestamp()
            with self.lock:
                self.cache[key] = (self.clock() + (self.FAILURE_TTL if code != 200 or result.get("degraded") else self.TTL), cached, code)
                self.cache.move_to_end(key)
                while len(self.cache) > self.MAX_KEYS:
                    self.cache.popitem(last=False)
            return result, code
        finally:
            with self.lock:
                self.inflight.pop(key).set()

    def _load(self, key, source_pref, symbol, timeframe, limit, base, db, loader):
        attempts, reason = 0, None
        kwargs = dict(symbol=symbol, timeframe=timeframe, limit=limit, okx_rest_url=base, get_db_conn=db)
        if source_pref == "live":
            def fetch(url):
                nonlocal attempts
                data, attempts = self.transport.fetch(url)
                return data
            try:
                live, code = loader(source_pref="live", fetch_okx_json=fetch, **kwargs)
                if code != 200:
                    raise ValueError("provider_error")
                self._validate(live, timeframe)
                live["fetched_at_epoch"] = self.wall()
                with self.lock:
                    self.live_cache[key] = deepcopy(live)
                    self.live_cache.move_to_end(key)
                    while len(self.live_cache) > self.MAX_KEYS:
                        self.live_cache.popitem(last=False)
                current = self._decorate(live, "upstream", attempts=attempts)
                if current["is_stale"]:
                    raise CandleFetchError("upstream_stale_candles", attempts)
                return current, 200
            except CandleFetchError as exc:
                reason, attempts = exc.reason, exc.attempts
            except (ValueError, TypeError, KeyError, IndexError, OverflowError):
                reason = "upstream_invalid_candles"
        try:
            dw, code = loader(source_pref="dw", fetch_okx_json=lambda _: None, **kwargs)
            if code != 200:
                raise ValueError("db_unavailable")
            self._validate(dw, timeframe)
            dw["fetched_at_epoch"] = self.wall()
            candidate = self._decorate(dw, "database", reason, attempts)
        except Exception:
            candidate = None
        # Prefer the newest useful observation, not whichever fallback was
        # queried first. Old DW data remains available with an explicit warning.
        with self.lock:
            cached = deepcopy(self.live_cache.get(key))
        if cached and self.wall() - cached["fetched_at_epoch"] <= self.STALE_CACHE_LIMIT:
            if (candidate is None or cached["candles"][-1]["time"] > candidate["candles"][-1]["time"]
                    or (cached["candles"][-1]["time"] == candidate["candles"][-1]["time"]
                        and (candidate.get("latest_observation_time") or 0) <= cached["fetched_at_epoch"])):
                return self._decorate(cached, "stale_cache", reason, attempts), 200
        if candidate is not None:
            return candidate, 200
        return {"status": "error", "source": "unavailable", "degraded": True,
                "reason": reason or "database_unavailable", "upstream_attempts": attempts,
                "message": "Không có nến hợp lệ từ OKX, cache hoặc PostgreSQL; thử lại sau."}, 503
