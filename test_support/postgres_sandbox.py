"""Owned, disposable PostgreSQL databases for regression tests.

No configurable test database name, no shared-table cleanup, no force-drop.
Application connection aliases are patched only inside this test process.
"""
from contextlib import ExitStack
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import re
import unittest
from unittest.mock import patch
from uuid import uuid4

ROOT=Path(__file__).resolve().parent.parent


class PostgresTestSandbox:
    def __init__(self,suite):
        if not re.fullmatch(r'[a-z][a-z0-9_]{0,17}',suite):
            raise ValueError('Invalid regression suite name')
        from processing.core.env import load_env_vars
        load_env_vars()
        self.suite=suite
        stamp=datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        self.database=f'crypto_test_{suite}_{stamp}_{uuid4().hex[:10]}'
        self._identity=self.database
        self.live_database=os.getenv('POSTGRES_DB','crypto_dw')
        self._token=uuid4().hex
        self._settings={'host':os.getenv('POSTGRES_HOST','127.0.0.1'),
                        'port':int(os.getenv('POSTGRES_PORT','5433')),
                        'user':os.getenv('POSTGRES_USER','postgres'),
                        'password':os.getenv('POSTGRES_PASSWORD',''),
                        'connect_timeout':5,'options':'-c timezone=UTC'}
        self.directory=ROOT/'data/test_runs'/('postgres_'+suite)/self.database
        self.directory.mkdir(parents=True,exist_ok=False)
        self.connections=[]
        self.stack=ExitStack()
        self.created=False
        self.active=False
        self.record={'suite':suite,'database':self.database,'status':'created',
                     'verified_connections':0,'started_utc':datetime.now(timezone.utc).isoformat()}
        self._save()

    def _save(self):
        (self.directory/'result.json').write_text(json.dumps(self.record,indent=2),encoding='utf-8')

    def _assert_identity(self):
        if self.database!=self._identity or self.database==self.live_database or not re.fullmatch(r'crypto_test_[a-z0-9_]{1,51}',self.database) or len(self.database)>63:
            raise RuntimeError('Refusing operation outside this owned test database')

    def _admin(self):
        import psycopg2
        return psycopg2.connect(dbname='postgres',**self._settings)

    def connect(self):
        self._assert_identity()
        if not self.created:
            raise RuntimeError('Test database has not been created')
        import psycopg2
        conn=psycopg2.connect(dbname=self.database,**self._settings)
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT current_database()')
                if cur.fetchone()[0]!=self.database:
                    raise RuntimeError('Connection reached the wrong database')
            conn.commit()
        except Exception:
            conn.close(); raise
        self.connections.append(conn)
        self.record['verified_connections']+=1
        return conn

    def __enter__(self):
        self._assert_identity()
        if self.created:raise RuntimeError('A test sandbox cannot be reused')
        from psycopg2 import sql
        admin=self._admin()
        try:
            admin.autocommit=True
            with admin.cursor() as cur:
                cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.database)))
            self.created=True
        finally:admin.close()
        try:
            connection=self.connect()
            try:
                with connection.cursor() as cur:
                    cur.execute('CREATE TABLE public._codex_test_owner (token TEXT PRIMARY KEY, suite TEXT NOT NULL)')
                    cur.execute('INSERT INTO public._codex_test_owner VALUES (%s,%s)',(self._token,self.suite))
                connection.commit()
                with connection.cursor() as cur:
                    cur.execute((ROOT/'warehouse/sql/00_init_streaming_dw.sql').read_text(encoding='utf-8'))
                connection.commit()
            finally:connection.close()
            for name in ('warehouse.etl.db_config','warehouse.etl.streaming_sink','ml.predict','ml.dataset'):
                module=importlib.import_module(name)
                self.stack.enter_context(patch.object(module,'get_db_connection',self.connect))
            # The fixtures must never be augmented by real OKX data or remote downloads.
            self.stack.enter_context(patch('urllib.request.urlopen',side_effect=RuntimeError('Network disabled inside isolated regression tests')))
            self.active=True
            self.record['status']='running'; self._save()
            print('[TEST DB] '+self.database,flush=True)
            return self
        except Exception:
            self.__exit__(*__import__('sys').exc_info())
            raise

    def reset(self):
        if not self.active:raise RuntimeError('Test sandbox is not active')
        conn=self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE warehouse.fact_crypto_trades, warehouse.fact_crypto_1m_candles, warehouse.fact_btc_predictions, warehouse.fact_btc_model_evaluation RESTART IDENTITY')
            conn.commit()
        finally:conn.close()

    def __exit__(self,exc_type,exc,traceback):
        self.stack.close()
        error=None
        try:
            self._assert_identity()
            for conn in self.connections:
                if not conn.closed:conn.close()
            if self.created:
                owner=self.connect()
                try:
                    with owner.cursor() as cur:
                        cur.execute('SELECT token,suite FROM public._codex_test_owner')
                        if cur.fetchall()!=[(self._token,self.suite)]:
                            raise RuntimeError('Owner marker mismatch; refusing to drop database')
                finally:owner.close()
                admin=self._admin()
                try:
                    from psycopg2 import sql
                    admin.autocommit=True
                    with admin.cursor() as cur:
                        cur.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(self.database)))
                        cur.execute('SELECT 1 FROM pg_database WHERE datname=%s',(self.database,))
                        if cur.fetchone() is not None:raise RuntimeError('Test database was not removed')
                finally:admin.close()
                self.created=False
                self.record['database_dropped']=True
            self.record['status']='closed' if exc_type is None else 'failed'
        except Exception as problem:
            error=problem
            self.record['status']='cleanup_failed'
            self.record['cleanup_error_type']=type(problem).__name__
        finally:
            self.active=False
            self.record['finished_utc']=datetime.now(timezone.utc).isoformat()
            self._save()
            print('[TEST DB RESULT] '+str(self.directory/'result.json'),flush=True)
        if error is not None:
            if exc_type is None:raise error
            if exc is not None and hasattr(exc,'add_note'):
                exc.add_note('Disposable DB cleanup also failed; see the run result.json')
        return False


class PostgresRegressionCase(unittest.TestCase):
    suite_name='regression'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.db=PostgresTestSandbox(cls.suite_name)
        cls.db.__enter__()
        cls.addClassCleanup(cls.db.__exit__,None,None,None)

    def setUp(self):
        super().setUp()
        self.db.reset()
        self.reference_time=datetime.now(timezone.utc).replace(second=30,microsecond=0)

    def query(self,sql,params=None):
        conn=self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql,params)
                return cur.fetchall() if cur.description else []
        finally:conn.close()
