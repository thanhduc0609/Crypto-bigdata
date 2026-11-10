"""Real Spark cleaning on labelled synthetic fixtures, never production paths."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import pandas as pd

from scripts.run_data_quality_audit import build_spark
from processing.quality.profiles import trade_replay
from processing.batch.bronze_to_silver import transform_bronze_trades, process_bronze_to_silver


class ProductionCleaningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        # Spark's JVM removes its scratch subfolders at JVM shutdown, after
        # spark.stop(); keep them outside the earlier Python fixture cleanup.
        cls.spark = build_spark(Path(cls.temp.name).parent / ("spark-" + Path(cls.temp.name).name))
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def frame(self, trades):
        return pd.DataFrame([dict(raw_payload=json.dumps(trade), kafka_partition=0,
            kafka_offset=i, kafka_timestamp=pd.Timestamp("2020-01-01T00:02:00Z"),
            _source_object="synthetic/fixture.parquet", _source_sha256="fixture-not-market-data") for i, trade in enumerate(trades)])

    def test_real_transform_rejects_bad_rows_dedups_latest_and_preserves_lineage(self):
        base = dict(trade_id="a", symbol="BTC-USDT", price=100., size=2., side="buy", timestamp=1577836800000)
        trades = [base, dict(base, price=101., timestamp=1577836801000), dict(base, price=102., timestamp=1577836801000),
                  dict(base, trade_id="b"), dict(base, trade_id="bad", price=-1.), dict(base, symbol="ETH-USDT"),
                  dict(base, side="unknown"), dict(base, timestamp="not-a-time"), dict(base, size=0.)]
        with tempfile.TemporaryDirectory() as directory:
            result, after = trade_replay(self.spark, self.frame(trades), directory)
            self.assertEqual(result["raw_rows"], 9)
            self.assertEqual(result["rejected_rows"], 5)
            self.assertEqual(result["duplicate_rows_removed"], 2)
            self.assertEqual(result["cleaned_rows"], 2)
            self.assertEqual(sum(result["rejection_reasons_primary"].values()), 5)
            winner = after[after.trade_id == "a"].iloc[0]
            self.assertEqual(winner.price, 102.)
            self.assertEqual(winner.trade_value, 204.)
            self.assertEqual(winner._source_object, "synthetic/fixture.parquet")
            self.assertEqual(winner.kafka_offset, 2)

    def test_overlapping_reasons_not_double_counted(self):
        bad = dict(trade_id="bad", symbol="ETH-USDT", price=-1., size=0., side="bad", timestamp=-1)
        good = dict(trade_id="ok", symbol="BTC-USDT", price=100., size=2., side="sell", timestamp=1577836800000)
        with tempfile.TemporaryDirectory() as directory:
            result, _ = trade_replay(self.spark, self.frame([bad, good]), directory)
            self.assertEqual(result["rejected_rows"], 1)
            self.assertEqual(sum(result["rejection_reasons_primary"].values()), 1)
            self.assertGreater(sum(result["rejection_reasons_overlapping"].values()), 1)

    def test_no_lineage_columns_added_to_normal_production_input(self):
        trade = dict(trade_id="a", symbol="BTC-USDT", price=100., size=2., side="buy", timestamp=1577836800000)
        input_frame = self.frame([trade]).drop(columns=["_source_object", "_source_sha256"])
        raw = self.spark.createDataFrame(input_frame)
        _, _, _, cleaned = transform_bronze_trades(raw)
        self.assertNotIn("_source_object", cleaned.columns)
        self.assertEqual(cleaned.count(), 1)

    def test_batch_entrypoint_writes_only_owned_local_silver_with_same_counts(self):
        base = dict(trade_id="a", symbol="BTC-USDT", price=100., size=2., side="buy", timestamp=1577836800000)
        frame = self.frame([base, dict(base, timestamp=1577836801000), dict(base, trade_id="bad", price=-1.)])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            bronze, silver = root / "bronze", root / "silver"
            self.assertTrue(bronze.is_relative_to(root) and silver.is_relative_to(root))
            self.spark.createDataFrame(frame.drop(columns=["_source_object", "_source_sha256"])).write.parquet(str(bronze))
            with patch("processing.batch.bronze_to_silver.get_bronze_trades_path", return_value=str(bronze)), patch("processing.batch.bronze_to_silver.get_silver_trades_path", return_value=str(silver)):
                result = process_bronze_to_silver(self.spark)
            self.assertEqual(result["total"], 3)
            self.assertEqual(result["rejected"], 1)
            self.assertEqual(result["duplicates"], 1)
            self.assertEqual(result["valid"], 1)
            self.assertEqual(self.spark.read.parquet(str(silver)).count(), 1)

    def test_batch_accepts_captured_single_parquet_file(self):
        base=dict(trade_id="file-a",symbol="BTC-USDT",price=100.,size=2.,side="buy",timestamp=1577836800000)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();source=root/"bronze-input.parquet";silver=root/"silver"
            self.frame([base]).drop(columns=["_source_object","_source_sha256"]).to_parquet(source,index=False,coerce_timestamps="us")
            result=process_bronze_to_silver(self.spark,bronze_path=source.as_posix(),silver_path=silver.as_posix())
            self.assertEqual(result["status"],"success");self.assertEqual(result["valid"],1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
