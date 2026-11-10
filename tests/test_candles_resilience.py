"""Offline failure injection: no network, database writes, or background workers."""
import io
import json
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from web.services.candles_resilience import BoundedOKXTransport, CandleFetchError, CandleService
from web.services.overview_service import get_candles_data


NOW = 1794144030  # Fixed UTC, not wall clock dependent
MINUTE = NOW // 60 * 60


def payload(tf="1m", source="live_okx", stamp=None):
    duration = CandleService.DURATIONS[tf]
    stamp = stamp if stamp is not None else NOW // duration * duration
    return dict(status="success", symbol="BTC-USDT", timeframe=tf, source=source,
                data_timestamp="2026-11-08T00:00:00Z", candles=[dict(
                    time=stamp, open=100., high=110., low=90., close=105., volume=2.,
                    total_trade_value=210., vwap=105., is_closed=False)])


class Response(io.BytesIO):
    status = 200


class TransportTests(unittest.TestCase):
    def make(self, effects, **kwargs):
        opener = Mock(side_effect=effects)
        transport = BoundedOKXTransport(opener=opener, sleep=lambda _: None, **kwargs)
        self.addCleanup(transport.pool.shutdown, wait=True)
        return transport, opener

    def test_success_one_request_with_socket_timeout(self):
        transport, opener = self.make([Response(b'{"code":"0"}')])
        self.assertEqual(transport.fetch("https://example.test"), ({"code": "0"}, 1))
        self.assertLessEqual(opener.call_args.kwargs["timeout"], 1.25)

    def test_network_error_retries_once(self):
        transport, opener = self.make([URLError("offline"), Response(b'{}')])
        self.assertEqual(transport.fetch("https://example.test")[1], 2)
        self.assertEqual(opener.call_count, 2)

    def test_transient_http_retries_once(self):
        transport, opener = self.make([HTTPError("x", 503, "bad", {}, None), Response(b'{}')])
        self.assertEqual(transport.fetch("https://example.test")[1], 2)

    def test_permanent_http_not_retried(self):
        transport, opener = self.make([HTTPError("x", 400, "bad", {}, None)])
        with self.assertRaises(CandleFetchError) as ctx:
            transport.fetch("https://example.test")
        self.assertEqual(ctx.exception.reason, "upstream_http_rejected")
        self.assertEqual(opener.call_count, 1)

    def test_invalid_json_not_retried(self):
        transport, opener = self.make([Response(b'<html>')])
        with self.assertRaises(CandleFetchError) as ctx:
            transport.fetch("https://example.test")
        self.assertEqual(ctx.exception.reason, "upstream_invalid_json")
        self.assertEqual(opener.call_count, 1)

    def test_payload_limit_and_type(self):
        for raw in (b'[]', b'x' * (1024 * 1024 + 1)):
            with self.subTest(raw_length=len(raw)):
                transport, opener = self.make([Response(raw)])
                with self.assertRaises(CandleFetchError):
                    transport.fetch("https://example.test")
                self.assertEqual(opener.call_count, 1)

    def test_only_two_timeout_attempts(self):
        transport, opener = self.make([TimeoutError(), TimeoutError(), Response(b'{}')])
        with self.assertRaises(CandleFetchError) as ctx:
            transport.fetch("https://example.test")
        self.assertEqual(ctx.exception.attempts, 2)
        self.assertEqual(opener.call_count, 2)

    def test_wait_deadline_and_no_unbounded_queue(self):
        release, started = threading.Event(), threading.Event()
        def slow(*args, **kwargs):
            started.set()
            release.wait(2)
            return Response(b'{}')
        transport, opener = self.make(slow, workers=1, wait_seconds=0.03)
        self.addCleanup(release.set)
        begin = time.monotonic()
        try:
            with self.assertRaises(CandleFetchError) as ctx:
                transport.fetch("https://example.test")
            self.assertEqual(ctx.exception.reason, "upstream_deadline")
            self.assertLess(time.monotonic() - begin, 0.5)
            self.assertTrue(started.is_set())
            with self.assertRaises(CandleFetchError) as ctx:
                transport.fetch("https://example.test")
            self.assertEqual(ctx.exception.reason, "upstream_busy")
            self.assertEqual(opener.call_count, 1)
        finally:
            release.set()


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.now, self.tick = NOW, 100.
        self.transport = Mock()
        self.transport.fetch.return_value = ({"code": "0"}, 1)
        self.svc = CandleService(self.transport, clock=lambda: self.tick, wall=lambda: self.now)
        self.live = payload()
        self.dw = payload(source="postgres_dw")
        self.dw["latest_observation_time"] = MINUTE
        self.calls = []
        def loader(source_pref, fetch_okx_json, **kwargs):
            self.calls.append(source_pref)
            if source_pref == "live":
                fetch_okx_json("https://example.test")
                return deepcopy(self.live), 200
            return deepcopy(self.dw), 200
        self.loader = loader

    def get(self, **kwargs):
        return self.svc.get(**(dict(source_pref="live", symbol="BTC-USDT", timeframe="1m", limit=150,
                                   okx_rest_url="https://example.test", get_db_conn=lambda: None,
                                   loader=self.loader) | kwargs))

    def advance(self, seconds):
        self.now += seconds
        self.tick += seconds

    def test_live_cache_no_db_and_no_mutation(self):
        first, code = self.get()
        self.assertEqual(code, 200)
        self.assertEqual(first["served_from"], "upstream")
        first["candles"][0]["close"] = -1
        self.advance(1)
        second, _ = self.get()
        self.assertEqual(second["served_from"], "cache")
        self.assertEqual(second["candles"][0]["close"], 105)
        self.assertEqual(second["cache_age_seconds"], 1)
        self.assertEqual(second["upstream_attempts"], 0)
        self.assertEqual(self.calls, ["live"])

    def test_cache_expiry_fetches_again(self):
        self.get()
        self.advance(3.1)
        self.get()
        self.assertEqual(self.calls, ["live", "live"])

    def test_stale_upstream_uses_newer_dw(self):
        self.live["candles"][0]["time"] -= 600
        body, code = self.get()
        self.assertEqual(code, 200)
        self.assertEqual(body["source"], "postgres_dw")
        self.assertEqual(body["fallback_reason"], "upstream_stale_candles")
        self.assertFalse(body["is_stale"])

    def test_stale_upstream_still_visible_if_dw_empty_with_warning(self):
        self.live["candles"][0]["time"] -= 600
        self.dw["candles"] = []
        body, code = self.get()
        self.assertEqual(code, 200)
        self.assertEqual(body["served_from"], "stale_cache")
        self.assertTrue(body["is_stale"])

    def test_cache_keys_include_tf_limit_symbol_source(self):
        self.get()
        self.get(limit=10)
        self.get(symbol="ETH-USDT")
        self.get(source_pref="dw")
        self.assertEqual(self.calls, ["live", "live", "live", "dw"])

    def test_bad_params_rejected_before_io(self):
        for kwargs in ({"source_pref": "x"}, {"timeframe": "5m"}, {"limit": 0}, {"limit": -1}, {"limit": 301}):
            self.assertEqual(self.get(**kwargs)[1], 400)
        self.assertFalse(self.calls)

    def test_db_fallback_and_cooldown(self):
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        body, code = self.get()
        self.assertEqual(code, 200)
        self.assertEqual(body["source"], "postgres_dw")
        self.assertEqual(body["served_from"], "database")
        self.assertTrue(body["degraded"])
        self.assertEqual(body["upstream_attempts"], 2)
        self.advance(1)
        cached, _ = self.get()
        self.assertEqual(cached["fallback_reason"], "upstream_network_error")
        self.assertEqual(cached["served_from"], "cache")
        self.assertEqual(cached["cache_origin"], "database")
        self.assertEqual(self.transport.fetch.call_count, 1)

    def test_explicit_dw_does_not_contact_upstream(self):
        body, code = self.get(source_pref="dw")
        self.assertEqual(code, 200)
        self.assertFalse(body["degraded"])
        self.assertFalse(self.transport.fetch.called)

    def test_old_dw_is_reported_stale(self):
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        self.dw["candles"][0]["time"] -= 3600
        self.dw["latest_observation_time"] -= 3600
        body, _ = self.get()
        self.assertTrue(body["is_stale"])
        self.assertGreater(body["freshness_lag_seconds"], 3500)

    def test_live_cache_fallback_beats_older_db(self):
        first, _ = self.get()
        self.advance(4)
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        self.dw["candles"][0]["time"] -= 3600
        body, _ = self.get()
        self.assertEqual(body["source"], "live_okx")
        self.assertEqual(body["served_from"], "stale_cache")
        self.assertEqual(body["fetched_at"], first["fetched_at"])
        self.assertTrue(body["is_stale"])

    def test_newer_dw_beats_live_cache(self):
        self.get()
        self.advance(64)
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        self.dw["candles"][0]["time"] += 60
        self.dw["latest_observation_time"] += 60
        self.assertEqual(self.get()[0]["source"], "postgres_dw")

    def test_expired_live_cache_not_reused(self):
        self.get()
        self.advance(121)
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        self.dw["candles"] = []
        self.assertEqual(self.get()[1], 503)

    def test_no_useful_data_503_and_negative_cache(self):
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        self.dw["candles"] = []
        body, code = self.get()
        self.assertEqual(code, 503)
        self.assertEqual(body["source"], "unavailable")
        self.get()
        self.assertEqual(self.transport.fetch.call_count, 1)

    def test_recovery_after_cooldown(self):
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        self.get()
        self.advance(5.1)
        self.transport.fetch.side_effect = None
        body, _ = self.get()
        self.assertEqual(body["source"], "live_okx")
        self.assertFalse(body["degraded"])

    def test_invalid_prices_nan_negative_ohlc_future_duplicate_fallback(self):
        for field, value in (("close", float("nan")), ("open", -1), ("high", 99),
                             ("volume", -1), ("time", MINUTE + 120), ("is_closed", True)):
            with self.subTest(field=field):
                self.svc.cache.clear()
                self.svc.live_cache.clear()
                self.live = payload()
                self.live["candles"][0][field] = value
                body, code = self.get()
                self.assertEqual(code, 200)
                self.assertEqual(body["source"], "postgres_dw")
        self.svc.cache.clear()
        self.live = payload()
        self.live["candles"].append(deepcopy(self.live["candles"][0]))
        self.assertEqual(self.get()[0]["source"], "postgres_dw")

    def test_gaps_and_partial_aggregation_honest(self):
        self.live["candles"].insert(0, dict(self.live["candles"][0], time=MINUTE - 120, is_closed=True))
        body, _ = self.get()
        self.assertEqual(body["missing_intervals"], 1)
        self.assertFalse(body["coverage_complete"])
        self.assertTrue(body["degraded"])
        self.dw["candles"][0]["coverage_complete"] = False
        body, _ = self.get(source_pref="dw")
        self.assertFalse(body["coverage_complete"])
        self.assertTrue(body["degraded"])

    def test_db_exception_does_not_expose_error(self):
        self.transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        def loader(source_pref, **kwargs):
            if source_pref == "dw":
                raise RuntimeError("PRIVATE connection string")
            return self.loader(source_pref=source_pref, **kwargs)
        body, code = self.get(loader=loader)
        self.assertEqual(code, 503)
        self.assertNotIn("PRIVATE", json.dumps(body))

    def test_singleflight_concurrent_requests(self):
        entered, release = threading.Event(), threading.Event()
        def loader(**kwargs):
            entered.set()
            release.wait(2)
            return self.loader(**kwargs)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.get, loader=loader) for _ in range(8)]
            self.assertTrue(entered.wait(1))
            release.set()
            answers = [future.result(timeout=2) for future in futures]
        self.assertEqual(self.transport.fetch.call_count, 1)
        self.assertTrue(all(code == 200 for _, code in answers))

    def test_cache_entries_bounded(self):
        for limit in range(1, 70):
            self.get(limit=limit)
        self.assertEqual(len(self.svc.cache), 64)
        self.assertEqual(len(self.svc.live_cache), 64)


class LoaderAndRouteTests(unittest.TestCase):
    def raw(self, raw):
        return get_candles_data("live", "BTC-USDT", "1m", 10, "https://example.test", lambda: None,
                                lambda _: {"code": "0", "data": raw})

    def test_missing_turnover_is_null_not_invented(self):
        body, _ = self.raw([[str(MINUTE * 1000), "100", "110", "90", "105", "2", "2", "", "0"]])
        self.assertIsNone(body["candles"][0]["total_trade_value"])
        self.assertIsNone(body["candles"][0]["vwap"])

    def test_confirmation_required(self):
        with self.assertRaises(ValueError):
            self.raw([[str(MINUTE * 1000), "100", "110", "90", "105", "2"]])

    def test_utc_daily_bar(self):
        fetch = Mock(return_value={"code": "1"})
        get_candles_data("live", "BTC-USDT", "1d", 10, "https://example.test", lambda: None, fetch)
        self.assertIn("bar=1Dutc", fetch.call_args.args[0])

    def test_routes_bad_params_and_retry_header(self):
        import web.app as webapp
        client = webapp.app.test_client()
        with patch.object(webapp, "candle_service", CandleService(Mock(), wall=lambda: NOW)):
            for query in ("limit=abc", "limit=-1", "limit=0", "limit=301", "timeframe=5m", "source=banana"):
                self.assertEqual(client.get("/api/candles?" + query).status_code, 400)
        with patch.object(webapp.candle_service, "get", return_value=({"status": "error"}, 503)):
            response = client.get("/api/candles")
            self.assertEqual(response.headers["Retry-After"], "5")
            self.assertEqual(response.headers["Cache-Control"], "no-store")


if __name__ == "__main__":
    unittest.main(verbosity=2)
