"""Versioned, checksum-bound research handover. Never downloads or trains models."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import zipfile

DEFAULT_ARCHIVE = "artifacts/research/academic-snapshot-20261109.zip"
SUFFIXES = {".json", ".csv", ".parquet", ".joblib"}
DATA_ROOTS = {"canonical_repairs", "long_term", "long_term_research", "ml_experiments"}
POINTERS = {"configs/canonical_snapshot.json", "configs/ml_review.json",
            "data/long_term/current.json", "data/long_term_research/current.json"}
MAX_TOTAL = 256 * 1024 * 1024


def sha(content):
    return hashlib.sha256(content).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def owned_path(root, name):
    """Only restore research data and its four explicit pointers, never code/secrets."""
    relative = PurePosixPath(name)
    if (not isinstance(name, str) or "\\" in name or ":" in name or relative.is_absolute()
            or name != relative.as_posix() or any(p in {".", ".."} for p in relative.parts)):
        raise ValueError("Unsafe artifact path")
    parts = relative.parts
    allowed = (name in POINTERS or len(parts) >= 3 and parts[0] == "data" and parts[1] in DATA_ROOTS)
    if not allowed or relative.suffix not in SUFFIXES:
        raise ValueError("Artifact is outside the research allowlist: " + name)
    target = root.joinpath(*parts)
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError("Artifact target escapes repository")
    return target


def run_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{10,80}", value):
        raise ValueError("Invalid research run ID")
    return value


def private_values(root):
    env = root / ".env"
    if not env.exists():
        return []
    values = []
    for line in env.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip("\"'")
        if key.strip() in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "POSTGRES_PASSWORD"} and len(value) >= 8:
            values.append(value.encode())
    return values


def collect(root):
    canonical = read_json(root / "configs/canonical_snapshot.json")
    review = read_json(root / "configs/ml_review.json")
    forecast = read_json(root / "data/long_term/current.json")
    research = read_json(root / "data/long_term_research/current.json")
    canonical_dir = root / "data/canonical_repairs" / run_id(canonical["run_id"])
    gold = root / canonical["local_gold_path"] / "symbol=BTC-USDT/canonical.parquet"
    owned_path(root, gold.relative_to(root).as_posix())
    ml_dir = root / "data/ml_experiments" / run_id(review["run_id"])
    forecast_dir = root / "data/long_term/runs" / run_id(forecast["run_id"])
    research_dir = root / "data/long_term_research" / run_id(research["run_id"])
    checks = {gold: canonical["gold_sha256"], ml_dir / "evaluation.json": review["sha256"],
              forecast_dir / "forecast.json": forecast["forecast_sha256"],
              research_dir / "research.json": research["sha256"],
              canonical_dir / "published_manifest.json": canonical["manifest_sha256"]}
    for file, expected in checks.items():
        if sha(file.read_bytes()) != expected:
            raise ValueError("Published artifact checksum mismatch: " + file.name)
    files = {root / name for name in POINTERS} | {gold, canonical_dir / "published_manifest.json"}
    for directory in (ml_dir, forecast_dir, research_dir):
        files.update(p for p in directory.iterdir() if p.is_file() and p.suffix in SUFFIXES)
    selection = read_json(ml_dir / "selection.json")
    provenance = read_json(ml_dir / "provenance.json")
    required = {**selection["code_sha256"], **{k: v for k, v in provenance["protected_sha256"].items()
                if k.startswith("ml/artifacts/")}}
    required["ml/long_term_research.py"] = read_json(research_dir / "research.json")["code_sha256"]
    return files, required


def create(root, archive):
    root, archive = Path(root).resolve(), Path(archive).resolve()
    files, required = collect(root)
    payloads = {}
    secrets = private_values(root)
    for file in sorted(files):
        if file.is_symlink():
            raise ValueError("Symlink artifacts are not supported")
        file = file.resolve()
        name = file.relative_to(root).as_posix()
        owned_path(root, name)
        content = file.read_bytes()
        if any(value in content for value in secrets):
            raise ValueError("Credential found in artifact: " + name)
        payloads[name] = content
    if sum(map(len, payloads.values())) > MAX_TOTAL:
        raise ValueError("Research bundle exceeds size limit; use a separate release")
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=archive.parent, suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            for name, content in sorted(payloads.items()):
                info = zipfile.ZipInfo(name, date_time=(2026, 11, 9, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100644 << 16
                bundle.writestr(info, content)
        manifest = {"schema_version": 1, "snapshot": archive.stem, "archive": archive.name,
                    "archive_sha256": sha(temporary.read_bytes()),
                    "files": {name: {"sha256": sha(content), "bytes": len(content)} for name, content in sorted(payloads.items())},
                    "required_source_sha256": required,
                    "note": "Frozen results; no retraining. Retrospective limitations remain unchanged."}
        validate_sources(root, required)
        sidecar = archive.with_suffix(".manifest.json")
        if archive.exists() and sha(archive.read_bytes()) != manifest["archive_sha256"]:
            raise ValueError("Published archive differs; use a new archive name for a new version")
        if sidecar.exists() and read_json(sidecar) != manifest:
            raise ValueError("Published manifest differs; use a new archive name for a new version")
        os.replace(temporary, archive)
        sidecar.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return manifest
    finally:
        temporary.unlink(missing_ok=True)


def validate_sources(root, required):
    for name, expected in required.items():
        path = (root / name).resolve()
        if not path.is_relative_to(root) or not path.is_file() or sha(path.read_bytes()) != expected:
            raise ValueError("Source/model differs from frozen research: " + name)


def inspect(root, archive, manifest_path=None):
    root, archive = Path(root).resolve(), Path(archive).resolve()
    manifest = read_json(manifest_path or archive.with_suffix(".manifest.json"))
    if manifest.get("schema_version") != 1 or sha(archive.read_bytes()) != manifest["archive_sha256"]:
        raise ValueError("Research archive checksum mismatch")
    records = manifest["files"]
    if not records or sum(r["bytes"] for r in records.values()) > MAX_TOTAL:
        raise ValueError("Invalid research bundle size")
    validate_sources(root, manifest["required_source_sha256"])
    content = {}
    with zipfile.ZipFile(archive) as bundle:
        names = bundle.namelist()
        if len(set(names)) != len(names) or set(names) != set(records):
            raise ValueError("Research archive membership mismatch")
        for name in names:
            owned_path(root, name)
            info = bundle.getinfo(name)
            if info.file_size != records[name]["bytes"] or info.file_size > MAX_TOTAL:
                raise ValueError("Research entry size mismatch")
            if (info.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError("Symlink archive entries are forbidden")
            raw = bundle.read(name)
            if sha(raw) != records[name]["sha256"]:
                raise ValueError("Research entry checksum mismatch: " + name)
            content[name] = raw
    return manifest, content


def restore(root, archive, manifest_path=None, verify_only=False):
    root = Path(root).resolve()
    manifest, content = inspect(root, archive, manifest_path)
    missing = []
    for name, raw in content.items():
        target = owned_path(root, name)
        if target.exists():
            if sha(target.read_bytes()) != sha(raw):
                raise ValueError("Refusing to overwrite a different local artifact: " + name)
        else:
            missing.append(name)
    if verify_only:
        if missing:
            raise ValueError("Research artifacts not restored: " + str(len(missing)) + " missing files")
    else:
        # Publish pointers last; a failed data copy must not activate an incomplete run.
        for name in sorted(missing, key=lambda n: (n in POINTERS, n)):
            target = owned_path(root, name)
            target.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=target.parent, suffix=".tmp", delete=False) as handle:
                temporary = Path(handle.name)
                handle.write(content[name])
            try:
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
    return {"status": "verified", "snapshot": manifest["snapshot"], "files": len(content),
            "restored": 0 if verify_only else len(missing), "training_triggered": False}
