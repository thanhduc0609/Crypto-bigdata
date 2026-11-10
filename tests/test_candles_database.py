"""Real SQL/Flask tests on an owned disposable DB. Never alter live DW."""
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from test_support.postgres_sandbox import PostgresRegressionCase
from test_support.candles import canonical_candles
from warehouse.etl.streaming_sink import write_historical_candles_batch, write_candles_batch
from web.services.candles_resilience import CandleService, CandleFetchError
from web.services.overview_service import get_candles_data


class CandleDatabaseTests(PostgresRegressionCase):
    suite_name = "candles_api"

    def seed(self, n=60, end=None, remove=None, unclosed=False):
        end = end or datetime(2020, 1, 2, 13, 0, tzinfo=timezone.utc)
        frame = canonical_candles(n, as_of=end)
        if remove is not None:
            frame = frame.drop(frame.index[remove])
        if unclosed:
            frame["source"] = "speed_layer"
            frame.loc[frame.index[-1], "is_closed"] = False
            write_candles_batch(frame.to_dict("records"), batch_id=1)
        else:
            write_historical_candles_batch(frame.to_dict("records"))
        return frame

    def read(self, tf="1m"):
        return get_candles_data("dw", "BTC-USDT", tf, 150, "https://example.test", self.db.connect, lambda _: None)

    def test_minute_values_and_connection_closed(self):
        frame = self.seed()
        body, code = self.read()
        self.assertEqual(code, 200)
        self.assertEqual(len(body["candles"]), 60)
        self.assertEqual(body["candles"][0]["time"], int(frame.iloc[0].window_start.timestamp()))
        self.assertAlmostEqual(body["candles"][-1]["close"], frame.iloc[-1].close, places=7)
        self.assertTrue(self.db.connections[-1].closed)

    def test_hour_complete_and_closed(self):
        self.seed()
        body, code = self.read("1h")
        self.assertEqual(code, 200, body)
        row = body["candles"][0]
        self.assertEqual(row["observed_minutes"], 60)
        self.assertTrue(row["coverage_complete"])
        self.assertTrue(row["is_closed"])
        self.assertIsNone(row["buy_volume"])
        self.assertIsNone(row["sell_volume"])

    def test_hour_missing_minute_not_closed(self):
        self.seed(remove=30)
        body, code = self.read("1h")
        self.assertEqual(code, 200, body)
        row = body["candles"][0]
        self.assertEqual(row["observed_minutes"], 59)
        self.assertFalse(row["coverage_complete"])
        self.assertFalse(row["is_closed"])

    def test_hour_unconfirmed_minute_not_closed(self):
        self.seed(unclosed=True)
        body, code = self.read("1h")
        self.assertEqual(code, 200, body)
        self.assertFalse(body["candles"][0]["is_closed"])

    def test_partial_day_and_actual_latest_observation(self):
        frame = self.seed()
        body, code = self.read("1d")
        self.assertEqual(code, 200, body)
        row = body["candles"][0]
        self.assertEqual(row["expected_minutes"], 1440)
        self.assertEqual(row["observed_minutes"], 60)
        self.assertFalse(row["coverage_complete"])
        self.assertFalse(row["is_closed"])
        self.assertEqual(body["latest_observation_time"], int(frame.iloc[-1].window_start.timestamp()))
        self.assertEqual(row["time"] % 86400, 0)

    def test_complete_day_closed_and_utc_aligned(self):
        frame = self.seed(1440, end=datetime(2020, 1, 3, tzinfo=timezone.utc))
        body, code = self.read("1d")
        self.assertEqual(code, 200, body)
        row = body["candles"][0]
        self.assertEqual(row["observed_minutes"], 1440)
        self.assertTrue(row["coverage_complete"])
        self.assertTrue(row["is_closed"])
        self.assertAlmostEqual(row["volume"], float(frame.volume.sum()), places=6)

    def test_flask_outage_fallback_all_timeframes(self):
        import web.app as webapp
        self.seed()
        transport = Mock()
        transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        with patch.object(webapp, "candle_service", CandleService(transport)), patch.object(webapp, "get_candles_db_connection", self.db.connect):
            for tf in ("1m", "1h", "1d"):
                with self.subTest(timeframe=tf):
                    response = webapp.app.test_client().get("/api/candles?timeframe=" + tf)
                    self.assertEqual(response.status_code, 200, response.json)
                    self.assertEqual(response.json["source"], "postgres_dw")
                    self.assertTrue(response.json["degraded"])
                    self.assertTrue(response.json["is_stale"])
                    self.assertEqual(response.json["upstream_attempts"], 2)

    def test_flask_empty_db_plus_upstream_outage_503(self):
        import web.app as webapp
        transport = Mock()
        transport.fetch.side_effect = CandleFetchError("upstream_network_error", 2)
        with patch.object(webapp, "candle_service", CandleService(transport)), patch.object(webapp, "get_candles_db_connection", self.db.connect):
            response = webapp.app.test_client().get("/api/candles")
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("candles", response.json)


if __name__ == "__main__":
    unittest.main(verbosity=2)
