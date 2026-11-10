"""Offline safety checks for the disposable regression database helper."""
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock,patch

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from test_support.postgres_sandbox import PostgresTestSandbox


class PostgresSandboxSafetyTests(unittest.TestCase):
    def sandbox(self):
        scope=PostgresTestSandbox('guard')
        scope.record['unit_fake']=True
        scope._save()
        return scope

    def test_invalid_suite_rejected_before_connection(self):
        with patch('psycopg2.connect') as connect:
            for suite in ('','../live','x'*19,'live;drop database'):
                with self.subTest(suite=suite),self.assertRaises(ValueError):PostgresTestSandbox(suite)
            connect.assert_not_called()

    def test_substituting_live_database_refused_before_connect(self):
        scope=self.sandbox()
        scope.database=scope.live_database
        with patch('psycopg2.connect') as connect:
            with self.assertRaisesRegex(RuntimeError,'outside'):scope.connect()
            with self.assertRaisesRegex(RuntimeError,'outside'):scope.__enter__()
            connect.assert_not_called()

    def test_current_database_mismatch_closes_connection(self):
        scope=self.sandbox();scope.created=True
        connection=MagicMock()
        cursor=connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value=(scope.live_database,)
        with patch('psycopg2.connect',return_value=connection):
            with self.assertRaisesRegex(RuntimeError,'wrong database'):scope.connect()
        connection.close.assert_called_once()
        self.assertEqual(scope.connections,[])

    def test_owner_mismatch_refuses_database_drop(self):
        scope=self.sandbox();scope.created=True
        connection=MagicMock()
        cursor=connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value=(scope.database,)
        cursor.fetchall.return_value=[('some-other-owner','guard')]
        with patch('psycopg2.connect',return_value=connection),patch.object(scope,'_admin') as admin:
            with self.assertRaisesRegex(RuntimeError,'Owner marker mismatch'):
                scope.__exit__(None,None,None)
            admin.assert_not_called()

    def test_unopened_scope_never_drops_any_database(self):
        scope=self.sandbox()
        with patch('psycopg2.connect') as connect:
            scope.__exit__(None,None,None)
            connect.assert_not_called()

    def test_reset_refused_when_inactive(self):
        scope=self.sandbox()
        with patch('psycopg2.connect') as connect:
            with self.assertRaisesRegex(RuntimeError,'not active'):scope.reset()
            connect.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
