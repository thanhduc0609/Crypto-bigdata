"""Resolve an explicitly activated, immutable canonical dataset descriptor."""
import json
import os
from pathlib import Path
import re
from urllib.parse import urlparse

ROOT=Path(__file__).resolve().parents[1]
DEFAULT_POINTER=ROOT/"configs/canonical_snapshot.json"


def load_descriptor(path=None):
    path=Path(path) if path is not None else DEFAULT_POINTER
    if not path.exists():
        return None
    body=json.loads(path.read_text(encoding="utf-8"))
    if (body.get("schema_version")!=1 or body.get("state")!="active" or body.get("symbol")!="BTC-USDT"
        or body.get("bar")!="1m" or not isinstance(body.get("rows"),int) or body["rows"]<=0):
        raise ValueError("Invalid canonical snapshot descriptor")
    for name in ("manifest_sha256","gold_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}",str(body.get(name,""))):
            raise ValueError("Canonical snapshot checksum missing")
    uri=urlparse(body.get("gold_uri",""))
    manifest=urlparse(body.get("manifest_uri",""))
    if (uri.scheme!="s3" or manifest.scheme!="s3" or not uri.netloc or uri.netloc!=manifest.netloc
        or not uri.path.endswith("/gold/candles_1m") or uri.query or uri.fragment or manifest.query or manifest.fragment
        or "/canonical-repairs/" not in uri.path or any(p in (".","..") for p in uri.path.split("/"))
        or manifest.path != uri.path.removesuffix("/gold/candles_1m")+"/manifest.json"):
        raise ValueError("Canonical snapshot URI is not an owned repair version")
    from storage.project_layout import resolve_uri
    body['gold_uri']=resolve_uri(body['gold_uri'])
    body['manifest_uri']=resolve_uri(body['manifest_uri'])
    return body


def resolve_gold(storage_mode,bucket,path=None):
    body=load_descriptor(path)
    if body is None:
        return None
    policy=os.getenv("CANONICAL_SNAPSHOT_STORAGE","inherit").lower()
    if policy not in {"inherit","local","s3"}:
        raise ValueError("CANONICAL_SNAPSHOT_STORAGE must be inherit, local or s3")
    if policy!="inherit":
        storage_mode=policy
    if storage_mode=="s3":
        uri=urlparse(body["gold_uri"])
        if bucket!=uri.netloc:
            raise ValueError("Canonical snapshot bucket differs from configured bucket")
        return body["gold_uri"].replace("s3://","s3a://",1)
    relative=Path(body["local_gold_path"])
    local=(ROOT/relative).resolve()
    if relative.is_absolute() or not local.is_relative_to(ROOT/"data/canonical_repairs") or not local.is_dir():
        raise ValueError("Activated local canonical snapshot is missing or outside its owned run")
    return local.as_posix()


def assert_mutable_gold_path(path):
    if "/canonical-repairs/" in str(path).replace("\\","/") or "/data/canonical_repairs/" in str(path).replace("\\","/"):
        raise ValueError("Activated repair snapshots are immutable; publish a new version instead of overwriting one")
