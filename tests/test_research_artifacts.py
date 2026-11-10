import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from runtime.research_artifacts import create, restore


class ResearchArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "clone"
        self.root.mkdir()
        self.archive = Path(self.temp.name) / "snapshot.zip"
        self.pointer = "data/long_term/current.json"
        self.data = "data/long_term/runs/20261109T000000Z/forecast.json"

    def bundle(self, payloads=None, required=None, entry_override=None):
        payloads = payloads or {self.pointer: b'{"run_id":"20261109T000000Z"}', self.data: b'{"frozen":true}'}
        with zipfile.ZipFile(self.archive, "w") as z:
            for name, content in payloads.items():
                if entry_override:
                    entry = zipfile.ZipInfo(name)
                    entry.external_attr = entry_override << 16
                    z.writestr(entry, content)
                else:
                    z.writestr(name, content)
        manifest = dict(schema_version=1, snapshot="unit", archive_sha256=hashlib.sha256(self.archive.read_bytes()).hexdigest(),
                        required_source_sha256=required or {}, files={name:dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()) for name,raw in payloads.items()})
        self.manifest = self.archive.with_suffix('.manifest.json')
        self.manifest.write_text(json.dumps(manifest))
        return manifest

    def test_restore_and_verify_are_idempotent_and_never_train(self):
        self.bundle()
        first=restore(self.root,self.archive)
        self.assertEqual(first['restored'],2)
        self.assertFalse(first['training_triggered'])
        self.assertEqual(restore(self.root,self.archive)['restored'],0)
        self.assertEqual(restore(self.root,self.archive,verify_only=True)['status'],'verified')

    def test_archive_tamper_rejected_before_write(self):
        self.bundle();self.archive.write_bytes(self.archive.read_bytes()+b'changed')
        with self.assertRaisesRegex(ValueError,'archive checksum'):
            restore(self.root,self.archive)
        self.assertEqual(list(self.root.iterdir()),[])

    def test_traversal_secrets_and_code_paths_rejected_even_with_matching_checksums(self):
        for name in ['../outside.json','/absolute.json','data/../outside.json','data\\long_term\\bad.json','C:/outside.json','.env','configs/local.env.example','web/app.py']:
            with self.subTest(name=name):
                self.bundle({name:b'not allowed'})
                with self.assertRaises(ValueError):restore(self.root,self.archive)
                self.assertEqual(list(self.root.iterdir()),[])

    def test_symlink_archive_entry_rejected(self):
        self.bundle(entry_override=0o120777)
        with self.assertRaisesRegex(ValueError,'Symlink'):
            restore(self.root,self.archive)

    def test_conflicting_local_data_prevents_all_new_writes(self):
        self.bundle();file=self.root/self.data;file.parent.mkdir(parents=True);file.write_bytes(b'own research')
        with self.assertRaisesRegex(ValueError,'Refusing to overwrite'):
            restore(self.root,self.archive)
        self.assertFalse((self.root/self.pointer).exists())
        self.assertEqual(file.read_bytes(),b'own research')

    def test_wrong_source_model_rejected_before_restore(self):
        file=self.root/'model.json';file.write_bytes(b'other weights')
        self.bundle(required={'model.json':hashlib.sha256(b'published weights').hexdigest()})
        with self.assertRaisesRegex(ValueError,'Source/model differs'):
            restore(self.root,self.archive)
        self.assertFalse((self.root/'data').exists())

    def test_verify_does_not_restore_missing_data(self):
        self.bundle()
        with self.assertRaisesRegex(ValueError,'missing files'):
            restore(self.root,self.archive,verify_only=True)
        self.assertEqual(list(self.root.iterdir()),[])

    def test_entry_checksum_mismatch_is_not_silently_accepted(self):
        manifest=self.bundle();manifest['files'][self.data]['sha256']='0'*64
        self.manifest.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError,'entry checksum'):
            restore(self.root,self.archive)

    def test_pointer_is_not_published_when_data_write_fails(self):
        self.bundle()
        with patch('runtime.research_artifacts.os.replace',side_effect=OSError('simulated copy failure')):
            with self.assertRaises(OSError):restore(self.root,self.archive)
        self.assertFalse((self.root/self.pointer).exists())

    def test_create_refuses_actual_environment_secrets_without_printing_them(self):
        file=self.root/self.data;file.parent.mkdir(parents=True)
        file.write_bytes(b'fixture-private-secret')
        (self.root/'.env').write_text('AWS_SECRET_ACCESS_KEY=fixture-private-secret')
        with patch('runtime.research_artifacts.collect',return_value=({file},{})):
            with self.assertRaisesRegex(ValueError,'Credential found') as caught:
                create(self.root,self.archive)
        self.assertNotIn('fixture-private-secret',str(caught.exception))
        self.assertFalse(self.archive.exists())


if __name__=='__main__':unittest.main()
