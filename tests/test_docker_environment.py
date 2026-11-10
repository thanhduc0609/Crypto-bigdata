"""Offline regression of Docker ownership, source selection and data preservation."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import yaml
from scripts import docker_environment as infra


def container(name, project='codex-crypto-local', service='postgres', running=False):
    return dict(Name='/' + name, Id=name + '-id', State=dict(Running=running),
                Config=dict(Env=[], Labels={'com.docker.compose.project': project,
                                             'com.docker.compose.service': service}))


class DockerSafetyTests(unittest.TestCase):
    def test_unrelated_projects_are_untouched(self):
        self.assertEqual(infra.legacy_groups([container('some-other-postgres', 'other')]), {})

    def test_expected_name_with_wrong_owner_refused(self):
        with self.assertRaisesRegex(RuntimeError, 'ownership'):
            infra.legacy_groups([container('crypto-postgres', 'unrelated')])

    def test_two_sources_require_explicit_selection(self):
        groups = {p: {'postgres': {}, 'kafka': {}} for p in ('codex-crypto-local', 'crypto-bigdata')}
        with self.assertRaisesRegex(ValueError, 'Multiple'):
            infra.choose_source(groups)
        self.assertEqual(infra.choose_source(groups, 'crypto-bigdata')[0], 'crypto-bigdata')

    def test_partial_source_cannot_be_migrated(self):
        with self.assertRaisesRegex(ValueError, 'both'):
            infra.choose_source({'codex-crypto-local': {'postgres': {}}})

    @patch.object(infra, 'kafka_properties', return_value={})
    def test_kafka_writable_layer_default_is_archived(self, _properties):
        fixture = container('codex-crypto-kafka', service='kafka')
        self.assertEqual(infra.kafka_directory(fixture), '/tmp/kafka-logs')
        fixture['Config']['Env'] = ['KAFKA_LOG_DIRS=/var/lib/kafka/data']
        self.assertEqual(infra.kafka_directory(fixture), '/var/lib/kafka/data')
        for value in ('/', '/tmp/../etc', '/a,/b', 'relative'):
            fixture['Config']['Env'] = ['KAFKA_LOG_DIRS=' + value]
            with self.assertRaises(ValueError): infra.kafka_directory(fixture)

    def test_generated_kafka_config_and_separate_metadata_are_respected(self):
        fixture = container('codex-crypto-kafka', service='kafka')
        with patch.object(infra, 'kafka_properties', return_value={'log.dirs': '/actual/data'}):
            self.assertEqual(infra.kafka_directory(fixture), '/actual/data')
        with patch.object(infra, 'kafka_properties', return_value={'log.dirs': '/actual/data', 'metadata.log.dir': '/other'}):
            with self.assertRaisesRegex(ValueError, 'metadata'): infra.kafka_directory(fixture)

    def test_env_update_preserves_private_values_and_other_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / '.env'
            env.write_text('# comment\nAWS_SECRET_ACCESS_KEY=synthetic-unit-secret\nPOSTGRES_USER=existing\nKAFKA_IMAGE=old\n', encoding='utf-8')
            infra.update_env(env, {'KAFKA_IMAGE': 'preserved:fixed', 'KAFKA_VOLUME_NAME': 'preserved-volume'})
            text = env.read_text()
            self.assertIn('AWS_SECRET_ACCESS_KEY=synthetic-unit-secret', text)
            self.assertIn('POSTGRES_USER=existing', text)
            self.assertEqual(text.count('KAFKA_IMAGE='), 1)
            self.assertIn('KAFKA_IMAGE=preserved:fixed', text)
            self.assertFalse(env.with_name('.env.docker-migration.tmp').exists())

    def test_counts_cover_warehouse_schema_and_quote_identifiers(self):
        with patch.object(infra, 'docker', side_effect=['["warehouse", "fact_crypto_trades"]\n["odd schema", "a\\\"b"]', '123', '4']) as call:
            result = infra.fingerprint('source')
        self.assertEqual(result, {'warehouse.fact_crypto_trades': 123, 'odd schema.a"b': 4})
        self.assertIn('"warehouse"."fact_crypto_trades"', call.call_args_list[1].args[-1])
        self.assertIn('"odd schema"."a""b"', call.call_args_list[2].args[-1])

    def test_launcher_blocks_legacy_without_verified_migration(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(infra, 'ROOT', Path(directory)), patch.object(infra, 'new_containers', return_value={}):
            with self.assertRaisesRegex(RuntimeError, 'Old Docker'):
                infra.guard_start([container('codex-crypto-postgres')])

    def test_verified_target_allows_stopped_rollback_containers_only(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(infra, 'ROOT', Path(directory)):
            folder = Path(directory) / 'data/runtime/docker-migration/run'
            folder.mkdir(parents=True)
            (folder / 'manifest.json').write_text(json.dumps(dict(status='verified', target_ids={'postgres': 'new-id'})))
            with patch.object(infra, 'new_containers', return_value={'postgres': {'Id': 'new-id'}}):
                infra.guard_start([container('codex-crypto-postgres')])
                with self.assertRaises(RuntimeError):
                    infra.guard_start([container('codex-crypto-postgres', running=True)])
            with patch.object(infra, 'new_containers', return_value={}):
                with self.assertRaises(RuntimeError): infra.guard_start([container('codex-crypto-postgres')])

    def test_retirement_cannot_read_manifest_outside_private_backup(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(infra, 'ROOT', Path(directory)), patch.object(infra, 'docker') as docker:
            with self.assertRaises(ValueError): infra.retire(Path(directory) / 'outside.json')
            docker.assert_not_called()

    def test_partial_migration_blocks_writers_even_after_old_containers_removed(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(infra, 'ROOT', Path(directory)):
            folder = Path(directory) / 'data/runtime/docker-migration/run'
            folder.mkdir(parents=True)
            (folder / 'manifest.json').write_text(json.dumps(dict(status='starting-new-stack', volumes={'postgres': 'copied-volume'})))
            current = {'postgres': {'Id': 'new-id', 'Mounts': [{'Type': 'volume', 'Name': 'copied-volume'}]}}
            with patch.object(infra, 'new_containers', return_value=current):
                with self.assertRaisesRegex(RuntimeError, 'incomplete migration'):
                    infra.guard_start([])

    def migration_fixture(self, root, source_project, mismatch=False):
        """Exercise ordering without Docker, credentials, network or real volumes."""
        events = []
        root.joinpath('.env').write_text('POSTGRES_PASSWORD=private-unit-value\n', encoding='utf-8')
        containers = []
        for project, prefix in [('codex-crypto-local', 'codex-crypto'), ('crypto-bigdata', 'crypto')]:
            for service in ('postgres', 'kafka'):
                item = container(prefix + '-' + service, project, service)
                item['Image'] = 'synthetic-image'
                if service == 'postgres':
                    item['Config']['Env'] = ['PG_MAJOR=16', 'POSTGRES_DB=unit', 'POSTGRES_USER=unit', 'POSTGRES_PASSWORD=private-unit-value']
                containers.append(item)
        cfg = {'services': {'postgres': {'environment': {'POSTGRES_DB': 'unit', 'POSTGRES_USER': 'unit', 'POSTGRES_PASSWORD': 'private-unit-value'}}}}
        def archive(cid, directory, path):
            events.append(('archive', cid))
            path.write_bytes(b'unit archive')
            return 'synthetic-checksum'
        def compose(*args):
            events.append(('compose', args[0]))
            return json.dumps(cfg) if args[0] == 'config' else ''
        def docker(*args, **kwargs):
            events.append((args[0], args[1] if len(args) > 1 else ''))
            return ''
        new = {s: {'Id': 'new-' + s} for s in ('postgres', 'kafka')}
        counts = [{'warehouse.fact': 10}, {'warehouse.fact': 9 if mismatch else 10}]
        with patch.object(infra, 'ROOT', root), patch.object(infra, 'inventory', return_value=containers), \
             patch.object(infra, 'new_containers', side_effect=[{}, new]), \
             patch.object(infra, 'compose', side_effect=compose), patch.object(infra, 'docker', side_effect=docker), \
             patch.object(infra, 'wait_postgres'), patch.object(infra, 'wait_kafka'), \
             patch.object(infra, 'kafka_directory', return_value='/actual/data'), \
             patch.object(infra, 'fingerprint', side_effect=counts), \
             patch.object(infra, 'kafka_offsets', return_value={'crypto-trades:0': 12}), \
             patch.object(infra, 'archive', side_effect=archive):
            if mismatch:
                with self.assertRaisesRegex(RuntimeError, 'differs'): infra.migrate(source_project)
            else:
                infra.migrate(source_project)
        record = json.loads(next(root.glob('data/runtime/docker-migration/*/manifest.json')).read_text())
        return events, record

    def test_both_legacy_project_choices_archive_before_releasing_target_labels(self):
        for project in ('crypto-bigdata', 'codex-crypto-local'):
            with self.subTest(project=project), tempfile.TemporaryDirectory() as directory:
                events, record = self.migration_fixture(Path(directory), project)
                archives = [i for i, e in enumerate(events) if e[0] == 'archive']
                removals = [i for i, e in enumerate(events) if e[0] == 'rm']
                self.assertLess(max(archives), min(removals))
                self.assertTrue(all(events[i][1].startswith('crypto-') for i in removals))
                self.assertLess(max(removals), events.index(('compose', 'up')))
                self.assertEqual(record['status'], 'verified')
                self.assertEqual(record['tables_before'], record['tables_after'])
                self.assertNotIn('private-unit-value', json.dumps(record))

    def test_verification_mismatch_never_publishes_success_or_retires_source(self):
        with tempfile.TemporaryDirectory() as directory:
            events, record = self.migration_fixture(Path(directory), 'codex-crypto-local', mismatch=True)
            self.assertNotEqual(record['status'], 'verified')
            self.assertFalse(any(e[0] == 'rm' and e[1].startswith('codex-') for e in events))

    def test_compose_uses_persistent_storage_and_optional_admin(self):
        cfg = yaml.safe_load((infra.ROOT / 'docker-compose.yml').read_text(encoding='utf-8-sig'))
        self.assertEqual(cfg['name'], 'crypto-bigdata')
        self.assertEqual(cfg['services']['pgadmin']['profiles'], ['admin'])
        self.assertTrue(any('/var/lib/kafka/data' in v for v in cfg['services']['kafka']['volumes']))
        for service in cfg['services'].values():
            self.assertNotIn('container_name', service)
            self.assertTrue(all(port.startswith('127.0.0.1:') for port in service['ports']))
        self.assertFalse((infra.ROOT / 'compose.local.yaml').exists())


if __name__ == '__main__': unittest.main()
