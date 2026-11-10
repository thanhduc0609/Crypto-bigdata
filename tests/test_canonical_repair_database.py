"""Atomic publication regressions on an owned disposable PostgreSQL database."""
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.quality.canonical_repair import ROOT,build_candidate,load_candidate
from storage.canonical_publish import publish,activate
from test_support.canonical_repair import fixture,MemoryS3
from test_support.postgres_sandbox import PostgresRegressionCase
from warehouse.etl.canonical_repair import apply_confirmed_snapshot
from warehouse.etl.streaming_sink import write_historical_candles_batch


class CanonicalRepairDatabaseTests(PostgresRegressionCase):
    suite_name="canonical_repair"

    def setUp(self):
        super().setUp()
        base=ROOT/"data/canonical_repairs"
        base.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(prefix="synthetic-",dir=base)
        self.addCleanup(self.temp.cleanup)
        self.directory=Path(self.temp.name)
        fixture(self.directory)
        build_candidate(self.directory)
        self.plan,self.candidate,self.frame=load_candidate(self.directory)
        self.publication=publish(self.directory,MemoryS3())

    def apply(self,**kwargs):
        return apply_confirmed_snapshot(self.frame,self.plan,self.publication,connect=self.db.connect,**kwargs)

    def sql_write(self,sql,params=None):
        conn=self.db.connect()
        try:
            with conn.cursor() as cur:cur.execute(sql,params)
            conn.commit()
        finally:conn.close()

    def seed(self):
        record=self.frame.iloc[0].astype(object).where(self.frame.iloc[0].notna(),None).to_dict()
        # Existing unknown closure must only be replaced by confirmed evidence.
        write_historical_candles_batch([record])
        self.sql_write("UPDATE warehouse.fact_crypto_1m_candles SET is_closed=NULL,close=104 WHERE symbol='BTC-USDT'")

    def test_full_grid_and_unknown_flag_fixed_with_persistent_before_image(self):
        self.seed()
        result=self.apply()
        self.assertEqual(result["rows"],3)
        self.assertEqual(result["before_rows"],1)
        self.assertEqual(result["inserted_rows"],2)
        rows=self.query("SELECT is_closed,source,buy_volume,total_trade_value FROM warehouse.fact_crypto_1m_candles ORDER BY window_start")
        self.assertTrue(all(row[0] is True and row[1]=="historical_rest" and row[2] is None for row in rows))
        backup=self.query("SELECT row_before->'is_closed',row_before->>'close' FROM warehouse.canonical_repair_backups WHERE run_id=%s",(self.plan["run_id"],))
        self.assertIsNone(backup[0][0])
        self.assertEqual(float(backup[0][1]),104.)

    def test_rerun_is_idempotent_and_keeps_one_ledger_record(self):
        self.apply()
        result=self.apply()
        self.assertTrue(result["already_applied"])
        self.assertEqual(self.query("SELECT COUNT(*) FROM warehouse.fact_crypto_1m_candles")[0][0],3)
        self.assertEqual(self.query("SELECT COUNT(*) FROM warehouse.canonical_dataset_versions WHERE run_id=%s",(self.plan["run_id"],))[0][0],1)

    def test_post_write_validation_failure_rolls_back_all_changed_rows(self):
        self.seed()
        def corrupt(cur):cur.execute("UPDATE warehouse.fact_crypto_1m_candles SET close=99")
        with self.assertRaisesRegex(ValueError,"verification failed"):
            self.apply(before_validate=corrupt)
        self.assertEqual(self.query("SELECT COUNT(*),MIN(close) FROM warehouse.fact_crypto_1m_candles")[0],(1,Decimal('104.00000000')))

    def test_outside_frozen_range_is_preserved(self):
        record=self.frame.iloc[0].astype(object).where(self.frame.iloc[0].notna(),None).to_dict()
        record["window_start"]-=timedelta(minutes=10)
        record["window_end"]-=timedelta(minutes=10)
        write_historical_candles_batch([record])
        self.apply()
        self.assertEqual(self.query("SELECT COUNT(*) FROM warehouse.fact_crypto_1m_candles")[0][0],4)
        self.assertEqual(float(self.query("SELECT close FROM warehouse.fact_crypto_1m_candles ORDER BY window_start LIMIT 1")[0][0]),105.)

    def test_incomplete_candidate_rejected_without_partial_insert(self):
        self.frame=self.frame.iloc[:-1]
        with self.assertRaises(ValueError):self.apply()
        self.assertEqual(self.query("SELECT COUNT(*) FROM warehouse.fact_crypto_1m_candles")[0][0],0)

    def test_wrong_version_hash_cannot_rebind_run(self):
        self.apply()
        self.publication["manifest_sha256"]="f"*64
        with self.assertRaisesRegex(ValueError,"different publication"):self.apply()

    def test_rerun_does_not_overwrite_newer_rest_correction(self):
        self.apply()
        self.sql_write("UPDATE warehouse.fact_crypto_1m_candles SET close=106")
        with self.assertRaisesRegex(ValueError,"verification failed"):self.apply()
        self.assertEqual(float(self.query("SELECT MIN(close) FROM warehouse.fact_crypto_1m_candles")[0][0]),106.)

    def test_pointer_failure_after_commit_can_resume_without_duplicate_rows(self):
        pointer=self.directory/"configs/pointer.json"
        with patch("storage.canonical_publish.os.replace",side_effect=PermissionError("Injected pointer error")):
            with self.assertRaises(PermissionError):
                activate(self.directory,self.publication,pointer=pointer,connect=self.db.connect)
        self.assertFalse(pointer.exists())
        self.assertEqual(self.query("SELECT COUNT(*) FROM warehouse.fact_crypto_1m_candles")[0][0],3)
        active=activate(self.directory,self.publication,pointer=pointer,connect=self.db.connect)
        self.assertEqual(active["rows"],3)
        self.assertEqual(self.query("SELECT COUNT(*) FROM warehouse.fact_crypto_1m_candles")[0][0],3)
        self.assertFalse(json.loads((self.directory/"previous_pointer_state.json").read_text(encoding="utf-8"))["existed"])


if __name__=="__main__":unittest.main(verbosity=2)
