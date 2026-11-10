import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from unittest.mock import Mock

import pandas as pd

from processing.quality.canonical_repair import validated_row,validate_candidate,build_candidate,load_candidate,download
from processing.quality.inventory import sha256
from storage.canonical_publish import ImmutableRepairStore,publish
from storage.canonical_snapshot import load_descriptor,resolve_gold,assert_mutable_gold_path
from test_support.canonical_repair import fixture,MemoryS3


class RepairSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory=Path(self.temp.name).resolve()
        self.plan,self.frame,self.rows=fixture(self.directory)

    def test_rejects_unconfirmed_instead_of_inferring_from_age(self):
        low=int(pd.Timestamp(self.plan["requested_start"]).timestamp()*1000)
        for confirm in ("0",None,"",True,"true"):
            row=self.rows[0].copy();row[8]=confirm
            with self.subTest(confirm=confirm),self.assertRaises(ValueError):
                validated_row(row,low,low,self.plan["source_as_of"])

    def test_rejects_timestamp_alignment_bad_geometry_and_missing_turnover(self):
        low=int(self.rows[0][0])
        for position,value in ((0,str(low+1)),(0,float(low)),(1,"-1"),(2,"99"),(5,"NaN"),(7,"")):
            row=self.rows[0].copy();row[position]=value
            with self.subTest(position=position,value=value),self.assertRaises(ValueError):
                validated_row(row,low,low+60000,self.plan["source_as_of"])

    def test_candidate_continuity_and_explicit_boolean(self):
        for frame in (self.frame.iloc[:-1],pd.concat([self.frame,self.frame.iloc[:1]]),self.frame.assign(is_closed=None),self.frame.assign(is_closed="True")):
            with self.assertRaises(ValueError):
                validate_candidate(frame,self.plan)

    def test_no_synthetic_microstructure(self):
        with self.assertRaises(ValueError):
            validate_candidate(self.frame.assign(buy_volume=1.),self.plan)

    def test_raw_hash_and_tampered_candidate_fail_closed(self):
        page=next((self.directory/"raw").rglob("page.json"))
        page.write_text("{}",encoding="utf-8")
        with self.assertRaisesRegex(ValueError,"checksum"):
            build_candidate(self.directory)

    def test_build_confirms_unknown_flags_from_real_fixture_pages_and_parquet_schema(self):
        result=build_candidate(self.directory)
        self.assertEqual(result["legacy_unknown_closed_confirmed"],3)
        _,_,frame=load_candidate(self.directory)
        self.assertTrue(frame.is_closed.all())
        self.assertTrue(frame.buy_volume.isna().all())
        self.assertEqual(len(frame),3)

    def test_rebuild_is_byte_identical(self):
        first=build_candidate(self.directory)
        first_identity=sha256(self.directory/"candidate.json")
        second=build_candidate(self.directory)
        self.assertEqual(first["gold_sha256"],second["gold_sha256"])
        self.assertEqual(first_identity,sha256(self.directory/"candidate.json"))

    def test_tampered_gold_checksum_rejected(self):
        built=build_candidate(self.directory)
        (self.directory/built["gold_file"]).write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError,"checksum"):
            load_candidate(self.directory)

    def test_immutable_upload_and_resume_never_overwrites(self):
        memory=MemoryS3()
        store=ImmutableRepairStore(memory,self.plan)
        store.put_bytes("gold/data.parquet",b"original")
        store.put_bytes("gold/data.parquet",b"original")
        with self.assertRaisesRegex(ValueError,"different bytes"):
            store.put_bytes("gold/data.parquet",b"changed")
        self.assertTrue(all(key.startswith(self.plan["s3_prefix"]+"/") for _,_,key in memory.calls))

    def test_cannot_claim_shared_root_or_traverse_prefix(self):
        memory=MemoryS3()
        with self.assertRaises(ValueError):
            ImmutableRepairStore(memory,{**self.plan,"s3_prefix":"crypto/gold/candles_1m"})
        self.assertFalse(memory.calls)
        store=ImmutableRepairStore(memory,self.plan)
        for path in ("../shared","/root","a\\root","a//x"):
            with self.assertRaises(ValueError):
                store.put_bytes(path,b"x")

    def test_owned_marker_rejects_another_owner(self):
        memory=MemoryS3()
        ImmutableRepairStore(memory,self.plan)
        with self.assertRaisesRegex(ValueError,"different bytes"):
            ImmutableRepairStore(memory,{**self.plan,"ownership_token":"other-owner"})

    def test_publish_reads_back_full_gold_and_manifest(self):
        build_candidate(self.directory)
        memory=MemoryS3()
        result=publish(self.directory,memory)
        self.assertTrue(result["s3_readback_verified"])
        self.assertEqual(result["rows"],3)
        before=dict(memory.objects)
        again=publish(self.directory,memory)
        self.assertEqual(result,again)
        self.assertEqual(before,memory.objects)

    def descriptor(self):
        prefix="s3://unit-test-bucket/"+self.plan["s3_prefix"]
        return dict(schema_version=1,state="active",symbol="BTC-USDT",bar="1m",rows=3,run_id=self.plan["run_id"],
            gold_uri=prefix+"/gold/candles_1m",manifest_uri=prefix+"/manifest.json",gold_sha256="a"*64,manifest_sha256="b"*64)

    def test_descriptor_routes_s3_and_rejects_wrong_bucket(self):
        file=self.directory/"pointer.json";file.write_text(json.dumps(self.descriptor()),encoding="utf-8")
        self.assertIn("s3a://",resolve_gold("s3","unit-test-bucket",file))
        with self.assertRaises(ValueError):resolve_gold("s3","other-bucket",file)

    def test_explicit_local_research_storage_is_independent_of_operational_bucket(self):
        relative="data/canonical_repairs/unit/gold/candles_1m"
        (self.directory/relative).mkdir(parents=True)
        file=self.directory/"pointer.json"
        file.write_text(json.dumps({**self.descriptor(),"local_gold_path":relative}),encoding="utf-8")
        with patch("storage.canonical_snapshot.ROOT",self.directory),patch.dict(os.environ,{"CANONICAL_SNAPSHOT_STORAGE":"local"}):
            self.assertEqual(resolve_gold("s3","a-different-operational-bucket",file),(self.directory/relative).as_posix())

    def test_invalid_explicit_snapshot_storage_fails_closed(self):
        file=self.directory/"pointer.json";file.write_text(json.dumps(self.descriptor()),encoding="utf-8")
        with patch.dict(os.environ,{"CANONICAL_SNAPSHOT_STORAGE":"typo"}):
            with self.assertRaisesRegex(ValueError,"CANONICAL_SNAPSHOT_STORAGE"):
                resolve_gold("s3","unit-test-bucket",file)

    def test_immutable_snapshot_guard(self):
        with self.assertRaises(ValueError):assert_mutable_gold_path("s3://b/crypto/local-runs/a/canonical-repairs/r/gold/candles_1m")
        assert_mutable_gold_path("/owned-test/gold")

    def test_missing_descriptor_preserves_legacy_behavior(self):
        self.assertIsNone(resolve_gold("s3","b",self.directory/"absent.json"))

    def test_bad_descriptor_never_silently_falls_back(self):
        file=self.directory/"bad.json";file.write_text('{"state":"active"}',encoding="utf-8")
        with self.assertRaises(ValueError):resolve_gold("s3","b",file)

    def response(self,rows):
        response=Mock()
        response.json.return_value={"code":"0","data":rows}
        return response

    def test_capture_resume_uses_checksum_verified_raw_pages_without_refetch(self):
        session=Mock();session.get.return_value=self.response(list(reversed(self.rows)))
        with patch("processing.quality.canonical_repair.requests.Session",return_value=session),patch("processing.quality.canonical_repair.RateLimiter.wait"):
            download(self.directory,workers=1)
            self.assertEqual(session.get.call_count,1)
            download(self.directory,workers=1)
            self.assertEqual(session.get.call_count,1)

    def test_missing_source_minute_is_not_interpolated(self):
        session=Mock();session.get.return_value=self.response([self.rows[-1],self.rows[0]])
        with patch("processing.quality.canonical_repair.requests.Session",return_value=session),patch("processing.quality.canonical_repair.RateLimiter.wait"):
            with self.assertRaisesRegex(ValueError,"missing points"):
                download(self.directory,workers=1)

    def test_conflicting_rest_duplicate_aborts_capture(self):
        altered=self.rows[0].copy();altered[4]="104"
        session=Mock();session.get.return_value=self.response([*reversed(self.rows),altered])
        with patch("processing.quality.canonical_repair.requests.Session",return_value=session),patch("processing.quality.canonical_repair.RateLimiter.wait"):
            with self.assertRaisesRegex(ValueError,"Conflicting"):
                download(self.directory,workers=1)

    def test_stalled_pagination_aborts(self):
        row=self.rows[-1].copy();row[0]=str(int(row[0])+60000)
        session=Mock();session.get.return_value=self.response([row])
        with patch("processing.quality.canonical_repair.requests.Session",return_value=session),patch("processing.quality.canonical_repair.RateLimiter.wait"):
            with self.assertRaisesRegex(ValueError,"stalled"):
                download(self.directory,workers=1)


if __name__=="__main__":unittest.main(verbosity=2)
