"""Owned synthetic fixtures for operational capture, checkpoints and honest UI."""
from datetime import datetime,timezone,timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import pyarrow as pa
import pyarrow.parquet as pq
from runtime.environment import run_prefix
from runtime.medallion import version_paths,atomic_json
from runtime.telemetry import checkpoint_offsets,public_status,age
from runtime.bronze_capture import capture


class FakeS3:
    def __init__(self):
        self.prefix="crypto/local-runs/unit/bronze/trades/"
        sink=io.BytesIO();pq.write_table(pa.table({"raw_payload":["synthetic"],"kafka_offset":[10]}),sink)
        self.objects={self.prefix+"part.parquet":sink.getvalue(),self.prefix+"orphan.parquet":sink.getvalue(),
            self.prefix+"_spark_metadata/0":("v1\n"+json.dumps(dict(path="s3a://unit/"+self.prefix+"part.parquet",action="add"))+"\n").encode()}
        self.gets=0
    def get_paginator(self,name):return self
    def paginate(self,**kwargs):
        return [dict(Contents=[dict(Key=k,Size=len(v),ETag=hashlib.sha256(v).hexdigest()) for k,v in self.objects.items()])]
    def get_object(self,**kwargs):
        self.gets+=1;body=self.objects[kwargs["Key"]]
        assert kwargs["IfMatch"]==hashlib.sha256(body).hexdigest()
        return dict(Body=io.BytesIO(body))


class CaptureTests(unittest.TestCase):
    def test_obsolete_spark_footer_does_not_hide_appended_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            client=FakeS3();key=client.prefix+"part.parquet"
            table=pq.ParquetFile(io.BytesIO(client.objects[key])).read()
            table=table.replace_schema_metadata({b"org.apache.spark.sql.parquet.row.metadata":b'{"type":"struct","fields":[]}'})
            stream=io.BytesIO();pq.write_table(table,stream);client.objects[key]=stream.getvalue()
            result=capture(client,"unit","s3a://unit/"+client.prefix,Path(directory)/"run",Path(directory)/"cache")
            schema=pq.ParquetFile(result["path"]).schema_arrow
            self.assertIn("_source_object",schema.names)
            self.assertNotIn(b"org.apache.spark.sql.parquet.row.metadata",schema.metadata or {})
    def test_only_committed_files_and_source_checksums(self):
        with tempfile.TemporaryDirectory() as directory:
            client=FakeS3();root=Path(directory)
            result=capture(client,"unit","s3a://unit/"+client.prefix,root/"run",root/"cache")
            self.assertEqual(result["rows"],1);self.assertEqual(result["commit"]["unreferenced_files"],1)
            frame=pq.read_table(result["path"]).to_pandas()
            self.assertEqual(frame._source_object.iloc[0],client.prefix+"part.parquet")
            self.assertEqual(frame._source_sha256.iloc[0],hashlib.sha256(client.objects[client.prefix+"part.parquet"]).hexdigest())

    def test_unchanged_objects_use_verified_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            client=FakeS3();root=Path(directory)
            a=capture(client,"unit","s3a://unit/"+client.prefix,root/"a",root/"cache");calls=client.gets
            b=capture(client,"unit","s3a://unit/"+client.prefix,root/"b",root/"cache")
            self.assertEqual(client.gets,calls);self.assertEqual(a["packed_sha256"],b["packed_sha256"])

    def test_corrupt_cache_is_downloaded_again(self):
        with tempfile.TemporaryDirectory() as directory:
            client=FakeS3();root=Path(directory)
            first=capture(client,"unit","s3a://unit/"+client.prefix,root/"a",root/"cache");calls=client.gets
            Path(first["files"][0]["local"]).write_bytes(b"tampered")
            result=capture(client,"unit","s3a://unit/"+client.prefix,root/"b",root/"cache")
            self.assertGreater(client.gets,calls);self.assertEqual(result["rows"],1)

    def test_without_commit_log_not_claimed_as_bronze(self):
        with tempfile.TemporaryDirectory() as directory:
            client=FakeS3();del client.objects[client.prefix+"_spark_metadata/0"]
            with self.assertRaisesRegex(ValueError,"commit log"):capture(client,"unit","s3a://unit/"+client.prefix,Path(directory)/"a",Path(directory)/"cache")

    def test_missing_committed_object_is_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            client=FakeS3();del client.objects[client.prefix+"part.parquet"]
            with self.assertRaisesRegex(ValueError,"committed objects absent"):capture(client,"unit","s3a://unit/"+client.prefix,Path(directory)/"a",Path(directory)/"cache")

    def test_gap_in_metadata_is_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            client=FakeS3();client.objects[client.prefix+"_spark_metadata/2"]=b"v1\n"
            with self.assertRaisesRegex(ValueError,"gap"):capture(client,"unit","s3a://unit/"+client.prefix,Path(directory)/"a",Path(directory)/"cache")


class OwnershipTests(unittest.TestCase):
    def test_supervisor_backoff_bounded_and_stable_run_resets(self):
        from scripts.local_service import restart_delay
        self.assertEqual(restart_delay(10,0),5)
        self.assertEqual(restart_delay(10,100),60)
        self.assertEqual(restart_delay(180,100),5)
    def test_shared_or_escape_prefix_rejected(self):
        for value in ["crypto","crypto/gold","crypto/local-runs/../shared","crypto/local-runs/a/b",""]:
            with patch.dict(os.environ,{"S3_RUN_PREFIX":value}):
                with self.assertRaises(ValueError):run_prefix()

    def test_operational_gold_cannot_be_frozen_repair(self):
        with patch.dict(os.environ,{"S3_RUN_PREFIX":"crypto/local-runs/unit"}):
            paths=version_paths("20260101T010203123456Z-0123abcd","s3","unit")
            self.assertIn("/medallion-runs/",paths["gold"]);self.assertNotIn("canonical-repairs",paths["gold"])

    def test_bad_version_rejected(self):
        with self.assertRaises(ValueError):version_paths("../../shared","local")


class TelemetryTests(unittest.TestCase):
    def test_only_committed_checkpoint_offsets_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/"commits").mkdir();(root/"offsets").mkdir()
            (root/"commits/3").write_text("v1\n{}")
            (root/"offsets/3").write_text('v1\n{"batchTimestampMs":1}\n{"crypto-trades":{"0":11}}')
            (root/"offsets/4").write_text('v1\n{}\n{"crypto-trades":{"0":100}}')
            result=checkpoint_offsets(root)
            self.assertEqual(result["committed_batch"],3);self.assertEqual(result["offsets"]["crypto-trades"]["0"],11)

    def test_missing_checkpoint_is_unverified(self):
        with tempfile.TemporaryDirectory() as directory:self.assertEqual(checkpoint_offsets(directory)["status"],"unverified")

    def test_stale_and_missing_measurements_not_online(self):
        with tempfile.TemporaryDirectory() as directory:
            file=Path(directory)/"status.json";at=datetime(2020,1,1,tzinfo=timezone.utc)
            self.assertEqual(public_status(file,at)["status"],"unverified")
            atomic_json(file,dict(checked_at=(at-timedelta(minutes=3)).isoformat(),stages={}))
            self.assertEqual(public_status(file,at)["status"],"stale")

    def test_bad_json_does_not_turn_into_success(self):
        with tempfile.TemporaryDirectory() as directory:
            file=Path(directory)/"status.json";file.write_text("{broken")
            self.assertEqual(public_status(file)["status"],"unverified")

    def test_old_batch_marked_stale_independently(self):
        with tempfile.TemporaryDirectory() as directory:
            at=datetime(2020,1,1,tzinfo=timezone.utc);file=Path(directory)/"s.json"
            atomic_json(file,dict(checked_at=at.isoformat(),stages=dict(batch=dict(status="verified",completed_at=(at-timedelta(hours=1)).isoformat()))))
            status=public_status(file,at)
            self.assertEqual(status["status"],"current");self.assertEqual(status["stages"]["batch"]["status"],"stale")

    def test_future_or_invalid_timestamp_unverified(self):
        at=datetime(2020,1,1,tzinfo=timezone.utc)
        self.assertIsNone(age("bad",at));self.assertIsNone(age((at+timedelta(days=1)).isoformat(),at))


class WebEvidenceTests(unittest.TestCase):
    def test_health_does_not_claim_online_without_database(self):
        from web.app import app
        with patch("web.app.get_db_connection",return_value=None):
            response=app.test_client().get("/api/health")
            self.assertEqual(response.status_code,503);self.assertEqual(response.json["status"],"degraded")
            self.assertFalse(response.json["database_connected"])

    def test_futures_stays_collapsed_but_keeps_its_controls(self):
        from web.app import app
        html=app.test_client().get("/thi-truong").data.decode()
        self.assertIn('<details class="hero-subcard futures-subcard">',html)
        self.assertIn('id="plan-action-badge"',html)
        self.assertIn('<title>Phân tích thị trường · BTC-USDT</title>',html)
        self.assertNotIn('https://unpkg.com',html)

    def test_status_route_reads_only_existing_measurement(self):
        from web.app import app
        with patch("runtime.telemetry.public_status",return_value=dict(status="unverified",stages={})):
            response=app.test_client().get("/api/system/status")
            self.assertEqual(response.status_code,200);self.assertEqual(response.json["status"],"unverified")

    def test_model_review_must_match_frozen_checksum(self):
        from web.services.research_status import ml_review
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/"configs").mkdir()
            (root/"configs/ml_review.json").write_text(json.dumps(dict(run_id="20200101T010101123456Z",sha256="0"*64)))
            run=root/"data/ml_experiments/20200101T010101123456Z";run.mkdir(parents=True)
            (run/"evaluation.json").write_text("{}")
            with patch("web.services.research_status.ROOT",root):self.assertEqual(ml_review()[1],503)


if __name__=="__main__":unittest.main()
