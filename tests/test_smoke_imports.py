"""CI discovery must not open Kafka sockets or start a Spark JVM."""
from pathlib import Path
import subprocess
import sys
import unittest


class SmokeImportTests(unittest.TestCase):
    def test_stream_smoke_modules_import_without_network_or_hanging(self):
        code = "from unittest.mock import patch\nwith patch('socket.socket.connect', side_effect=AssertionError('network during import')):\n import tests.test_spark_streaming\n import tests.test_kafka_consumer\nprint('IMPORT_SAFE')"
        result=subprocess.run([sys.executable,'-c',code],cwd=Path(__file__).resolve().parents[1],capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(result.stdout.strip(),'IMPORT_SAFE')


if __name__=='__main__':unittest.main()
