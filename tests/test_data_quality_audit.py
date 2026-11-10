"""Offline guards for inventory scope, snapshot integrity and quality arithmetic."""
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd

from processing.quality.inventory import ReadOnlyS3, candle_profile, classify, committed_files, continuity, snapshot_objects
from processing.quality.profiles import reconcile_candles, daily_replay


class InventoryTests(unittest.TestCase):
    def test_namespace_separation(self):
        cases = {"crypto/gold/candles_1m/symbol=BTC-USDT/a.parquet": "shared",
                 "crypto/gold/candles_1m_staging_123/a.parquet": "staging",
                 "crypto/tests/s3/run/a.parquet": "test_or_audit",
                 "crypto/local-runs/r/audit/x/a.parquet": "test_or_audit",
                 "checkpoints/raw/state/1": "checkpoint",
                 "crypto/models/v2/a.json": "model",
                 "crypto/local-runs/r/long_term/run/daily.parquet": "forecast_snapshot",
                 "crypto/local-runs/r/bronze/trades/a.parquet": "run_scoped"}
        for key, expected in cases.items():
            with self.subTest(key=key):
                self.assertEqual(classify(key)[0], expected)

    def test_readonly_inventory_records_pagination(self):
        client = Mock()
        client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "crypto/bronze/trades/a.parquet", "Size": 1, "ETag": '"a"', "LastModified": datetime(2020, 1, 1, tzinfo=timezone.utc)}]},
            {"Contents": []}]
        result = ReadOnlyS3(client, "bucket").inventory()
        self.assertEqual(len(result["objects"]), 1)
        self.assertFalse(result["objects"][0]["after_cutoff"])
        self.assertEqual(client.mock_calls[0].args, ("list_objects_v2",))
        self.assertFalse(client.put_object.called)
        self.assertFalse(client.delete_object.called)

    def test_download_conditional_read_and_hash(self):
        client = Mock()
        client.get_object.return_value = {"Body": io.BytesIO(b"hello")}
        with tempfile.TemporaryDirectory() as directory:
            item = ReadOnlyS3(client, "b").download({"key": "d/a", "bytes": 5, "etag": '"e"'}, directory)
            self.assertEqual(Path(item["local"]).read_bytes(), b"hello")
            self.assertEqual(len(item["sha256"]), 64)
        self.assertEqual(client.get_object.call_args.kwargs, {"Bucket": "b", "Key": "d/a", "IfMatch": '"e"'})

    def test_download_rejects_traversal_before_get(self):
        client = Mock()
        with tempfile.TemporaryDirectory() as directory:
            for key in ("../../escape", "d\\escape"):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    ReadOnlyS3(client, "b").download({"key": key}, directory)
        self.assertFalse(client.get_object.called)

    def test_size_change_aborts(self):
        client = Mock()
        client.get_object.return_value = {"Body": io.BytesIO(b"hello")}
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
            ReadOnlyS3(client, "b").download({"key": "a", "bytes": 6, "etag": '"e"'}, directory)

    def test_snapshot_ignores_after_cutoff_and_checks_size_budget(self):
        reader = Mock()
        manifest = {"objects": [{"key": "a.parquet", "after_cutoff": True, "bytes": 10},
                                 {"key": "metadata.json", "after_cutoff": False, "bytes": 10}]}
        self.assertEqual(snapshot_objects(reader, manifest, "unused"), [])
        self.assertFalse(reader.download.called)
        manifest["objects"] = [{"key": "a.parquet", "after_cutoff": False, "bytes": 3 * 1024**3}]
        with self.assertRaises(ValueError):
            snapshot_objects(reader, manifest, "unused")

    def logs(self, directory, entries):
        items = []
        for filename, records in entries.items():
            path = Path(directory) / filename
            path.write_text("v1\n" + "\n".join(json.dumps(r) for r in records), encoding="utf-8")
            items.append({"key": "d/_spark_metadata/" + filename, "local": str(path)})
        return items

    def test_committed_files_replay_compact_add_delete_and_orphan(self):
        with tempfile.TemporaryDirectory() as directory:
            items = self.logs(directory, {"3.compact": [{"path": "s3a://b/d/a.parquet", "action": "add"}],
                "4": [{"path": "s3a://b/d/a.parquet", "action": "delete"}, {"path": "s3a://b/d/symbol%3DBTC-USDT/b.parquet", "action": "add"}]})
            items += [{"key": "d/symbol=BTC-USDT/b.parquet"}, {"key": "d/orphan.parquet"}]
            active, result = committed_files(items)
            self.assertEqual(active, {"d/symbol=BTC-USDT/b.parquet"})
            self.assertEqual(result["unreferenced_files"], 1)

    def test_committed_object_missing_aborts(self):
        with tempfile.TemporaryDirectory() as directory:
            items = self.logs(directory, {"0": [{"path": "s3a://b/d/missing.parquet"}]})
            with self.assertRaisesRegex(ValueError, "absent"):
                committed_files(items)

    def test_metadata_gap_aborts(self):
        with tempfile.TemporaryDirectory() as directory:
            items = self.logs(directory, {"0": [], "2": []})
            with self.assertRaisesRegex(ValueError, "batch 1"):
                committed_files(items)

    def test_directory_without_stream_log_explicit_mode(self):
        files, info = committed_files([{"key": "d/a.parquet"}])
        self.assertEqual(files, {"d/a.parquet"})
        self.assertEqual(info["mode"], "directory_listing")

    def test_continuity_unique_and_gap_not_interpolated(self):
        result = continuity(["2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:03:00Z"])
        self.assertEqual(result["observed"], 2)
        self.assertEqual(result["missing"], 2)
        self.assertEqual(result["expected_between_bounds"], 4)
        self.assertEqual(result["coverage_pct"], 50)

    def test_empty_continuity(self):
        self.assertEqual(continuity([])["missing"], 0)
        self.assertIsNone(continuity([])["first"])

    def test_candle_profile_balances_invalid_duplicates(self):
        base = dict(symbol="BTC-USDT", window_start=pd.Timestamp("2020-01-01", tz="UTC"),
                    open=100., high=110., low=90., close=105., volume=1., total_trade_value=105.)
        frame = pd.DataFrame([base, base, dict(base, high=99.), dict(base, volume=float("inf"))])
        profile = candle_profile(frame)
        self.assertEqual(profile["rows"], 4)
        self.assertEqual(profile["invalid_rows"], 2)
        self.assertEqual(profile["duplicate_rows"], 1)
        self.assertEqual(profile["unique_valid_rows"], 1)
        self.assertTrue(profile["count_balance_valid"])

    def test_missing_rest_microstructure_not_a_price_error(self):
        frame = pd.DataFrame([dict(window_start="2020-01-01T00:00:00Z", open=100., high=110., low=90., close=105., volume=1.,
                                   buy_volume=None, sell_volume=None, trade_count=None, total_trade_value=None)])
        self.assertEqual(candle_profile(frame)["invalid_rows"], 0)

    def test_microsecond_timestamps_align_without_nanosecond_assumption(self):
        frame = pd.DataFrame([dict(open=100., high=110., low=90., close=105., volume=1.)])
        frame["window_start"] = pd.Series([pd.Timestamp("2020-01-01T00:00:00Z")], dtype="datetime64[us, UTC]")
        self.assertEqual(candle_profile(frame)["invalid_rows"], 0)

    def test_alignment_future_and_duration_errors_are_counted_once(self):
        frame = pd.DataFrame([dict(window_start="2020-01-01T00:01:01Z", window_end="2020-01-01T00:03:01Z",
                                   open=100., high=110., low=90., close=105., volume=1.)])
        result = candle_profile(frame, as_of="2020-01-01T00:00:00Z")
        self.assertEqual(result["invalid_rows"], 1)
        self.assertEqual(result["invalid_reasons_overlapping"]["future_bucket"], 1)

    def test_reconciliation_common_keys_numeric_tolerance_and_mismatch(self):
        frame = pd.DataFrame([dict(symbol="BTC-USDT", window_start="2020-01-01T00:00:00Z", close=100., volume=1.)])
        near = frame.copy(); near["close"] += 1e-9
        result, _ = reconcile_candles(frame, near)
        self.assertEqual(result["mismatching_common_keys"], 0)
        near["close"] = 101.
        result, examples = reconcile_candles(frame, near)
        self.assertEqual(result["common_keys"], 1)
        self.assertEqual(result["mismatching_common_keys"], 1)
        self.assertEqual(len(examples), 1)

    def test_daily_replay_verifies_published_files_with_original_source_clock(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            result = daily_replay(repo, Path(directory))
            self.assertTrue(result["published_snapshot_matches_replay"])
            self.assertTrue(result["source_as_of_used_not_current_clock"])
            quality = result["quality"]
            self.assertEqual(quality["raw_rows"], quality["valid_daily_rows"] + sum(quality["rejected_rows"].values()) + quality["duplicate_rows_removed"])

    def test_dw_numeric_rounding_separate_from_true_reconciliation_drift(self):
        frame = pd.DataFrame([dict(window_start="2020-01-01T00:00:00Z", close=100., total_trade_value=123.123449)])
        stored = frame.copy(); stored["total_trade_value"] = 123.1234
        result, _ = reconcile_candles(frame, stored, {"close": 8, "total_trade_value": 4})
        self.assertEqual(result["mismatching_common_keys"], 0)
        self.assertEqual(result["rounding_only_common_keys"], 1)
        stored["total_trade_value"] = 123.12
        result, _ = reconcile_candles(frame, stored, {"close": 8, "total_trade_value": 4})
        self.assertEqual(result["mismatching_common_keys"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
