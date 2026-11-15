"""Export current sources, versioned research bundle and optional supplied evidence.

No hardcoded workstation paths, run IDs, test counts or automatic model training.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from runtime.research_artifacts import DEFAULT_ARCHIVE, inspect, private_values

SOURCE_SUFFIXES = {'.py', '.js', '.css', '.html', '.json', '.yaml', '.yml', '.ps1', '.md', '.txt', '.csv', '.sql', '.dll', '.exe', '.example'}
EVIDENCE_SUFFIXES = {'.md', '.json', '.csv', '.jpg', '.png', '.log'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=ROOT/'data/exports/handover')
    parser.add_argument('--evidence-dir', type=Path, help='Optional directory of measured reports/images; no guessed test claims')
    args = parser.parse_args()
    inspect(ROOT, ROOT/DEFAULT_ARCHIVE)
    names = subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode('utf-8').split('\0')
    members = {}
    for name in sorted(set(filter(None,names))):
        file = ROOT/name
        if (not file.is_file() or file.is_symlink() or not file.resolve().is_relative_to(ROOT)
                or file.parts[-1].endswith('.lock') or name.startswith(('data/','.venv/','.git/'))):
            continue
        if file.suffix in SOURCE_SUFFIXES or name in {'.gitignore','.gitattributes',DEFAULT_ARCHIVE}:
            members['repo/'+name] = file
    if args.evidence_dir:
        evidence=args.evidence_dir.resolve()
        if not evidence.is_dir():parser.error('Evidence directory is missing')
        for file in evidence.rglob('*'):
            if file.is_file() and not file.is_symlink() and file.suffix in EVIDENCE_SUFFIXES:
                members['evidence/'+file.relative_to(evidence).as_posix()] = file
    secrets = private_values(ROOT)
    for name,file in members.items():
        if any(value in file.read_bytes() for value in secrets):
            raise ValueError('Credential found in export: '+name)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    output = args.output_dir.resolve();output.mkdir(parents=True,exist_ok=True)
    archive = output/('crypto-bigdata-handover-'+stamp+'.zip')
    manifest = dict(created_at=datetime.now(timezone.utc).isoformat(),
        source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        uncommitted_changes=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT)),
        files={name:dict(bytes=file.stat().st_size,sha256=hashlib.sha256(file.read_bytes()).hexdigest()) for name,file in members.items()},
        note='Evidence is supplied as-is; this export does not certify new test runs.')
    with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
        for name,file in members.items():z.write(file,name)
        z.writestr('manifest.json',json.dumps(manifest,ensure_ascii=False,indent=2))
    with zipfile.ZipFile(archive) as z:
        if z.testzip():raise ValueError('Export archive failed integrity check')
    print(json.dumps(dict(archive=str(archive),files=len(members),sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),indent=2))


if __name__=='__main__':main()
