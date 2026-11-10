"""Single Compose entry point; archive and verify old Docker data before migration.

No volume pruning, offset reset, credentials in manifests, or silent data choice.
Run Merge-Docker.ps1 to stop owned application writers before migration.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "crypto-bigdata"
LEGACY_NAMES = {"codex-crypto-postgres", "codex-crypto-kafka", "crypto-postgres", "crypto-kafka", "crypto-pgadmin"}


def docker(*args, timeout=180):
    result = subprocess.run(["docker", *map(str, args)], cwd=ROOT, capture_output=True, timeout=timeout)
    if result.returncode:
        # Never echo config/environment or arguments containing credentials.
        raise RuntimeError("Docker command failed: " + " ".join(map(str, args[:2])) + "; inspect Docker Desktop/logs")
    return result.stdout.decode("utf-8", errors="replace").strip()


def compose(*args):
    return docker("compose", "--project-name", PROJECT, "--env-file", ROOT / ".env", "-f", ROOT / "docker-compose.yml", *args, timeout=240)


def inventory():
    ids = docker("ps", "-aq").split()
    return json.loads(docker("inspect", *ids)) if ids else []


def legacy_groups(containers):
    groups = {}
    for container in containers:
        if container["Name"].lstrip("/") not in LEGACY_NAMES:
            continue
        labels = container["Config"].get("Labels") or {}
        project = labels.get("com.docker.compose.project")
        service = labels.get("com.docker.compose.service")
        if project not in ("codex-crypto-local", PROJECT) or service not in ("postgres", "kafka", "pgadmin"):
            raise RuntimeError("Legacy container name has unexpected ownership; inspect manually")
        if service in groups.setdefault(project, {}):
            raise RuntimeError("Duplicate legacy service; review the source manually")
        groups[project][service] = container
    return groups


def choose_source(groups, requested=None):
    if requested:
        if requested not in groups:
            raise ValueError("Source project not found")
        chosen = requested
    elif len(groups) == 1:
        chosen = next(iter(groups))
    else:
        raise ValueError("Multiple old stacks exist; choose --source-project after reviewing their data")
    if not {"postgres", "kafka"} <= groups[chosen].keys():
        raise ValueError("Source must contain both PostgreSQL and Kafka")
    return chosen, groups[chosen]


def environment(container):
    return dict(item.split("=", 1) for item in container["Config"]["Env"] if "=" in item)


def kafka_properties(container):
    result = subprocess.run(['docker', 'cp', container['Id'] + ':/opt/kafka/config/server.properties', '-'],
                            capture_output=True, timeout=30)
    if result.returncode:
        raise RuntimeError('Cannot read generated Kafka config; manual data-directory review required')
    with tarfile.open(fileobj=io.BytesIO(result.stdout)) as archive_file:
        member = next(m for m in archive_file.getmembers() if m.isfile() and m.name.endswith('server.properties'))
        text = archive_file.extractfile(member).read().decode('utf-8')
    return dict(line.strip().split('=', 1) for line in text.splitlines()
                if line.strip() and not line.lstrip().startswith('#') and '=' in line)


def kafka_directory(container):
    props = kafka_properties(container)
    env = environment(container)
    value = env.get('KAFKA_LOG_DIRS') or props.get('log.dirs') or props.get('log.dir') or '/tmp/kafka-logs'
    if not value.startswith("/") or "," in value or ".." in value.split("/") or value == "/":
        raise ValueError("Unsupported Kafka data directories; manual migration required")
    metadata = env.get('KAFKA_METADATA_LOG_DIR') or props.get('metadata.log.dir')
    if metadata and metadata.rstrip('/') != value.rstrip('/'):
        raise ValueError('Separate Kafka metadata directory requires manual migration')
    if env.get('KAFKA_NODE_ID', props.get('node.id', '1')) != '1':
        raise ValueError('Only the local node.id=1 broker is supported')
    return value.rstrip("/")


def update_env(path, settings):
    """Only replace migration-owned keys, preserving passwords and other settings."""
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    result = [line for line in lines if line.split("=", 1)[0].strip() not in settings]
    result += ["", "# Preserved Docker storage/images after migration (do not reset these)."]
    result += [f"{key}={value}" for key, value in settings.items()]
    temporary = path.with_name(path.name + ".docker-migration.tmp")
    temporary.write_text("\n".join(result) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def fingerprint(container):
    """Include warehouse and other application schemas, not only public."""
    query = ("SELECT json_build_array(schemaname,tablename)::text FROM pg_tables "
             "WHERE schemaname NOT IN ('pg_catalog','information_schema') "
             "AND schemaname NOT LIKE 'pg_toast%' ORDER BY schemaname,tablename")
    command = 'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"'
    tables = docker("exec", container, "sh", "-c", command, "sh", query).splitlines()
    counts = {}
    for row in tables:
        schema, table = json.loads(row)
        quoted = '.'.join('"' + part.replace('"', '""') + '"' for part in (schema, table))
        counts[schema + '.' + table] = int(docker("exec", container, "sh", "-c", command, "sh", "SELECT count(*) FROM " + quoted))
    return counts


def wait_postgres(container):
    for _ in range(45):
        try:
            docker("exec", container, "sh", "-c", 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"', timeout=10)
            return
        except RuntimeError:
            time.sleep(2)
    raise RuntimeError("PostgreSQL did not become ready")


def kafka_offsets(container):
    text = docker("exec", container, "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--time", "-1", timeout=60)
    return dict(sorted((line.rsplit(":", 1)[0], int(line.rsplit(":", 1)[1])) for line in text.splitlines() if line.count(":") == 2))


def wait_kafka(container):
    for _ in range(45):
        try:
            docker("exec", container, "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092", timeout=10)
            return
        except (RuntimeError, subprocess.TimeoutExpired):
            time.sleep(2)
    raise RuntimeError("Kafka did not become ready")


def archive(container, directory, path):
    with path.open("xb") as stream:
        result = subprocess.run(["docker", "cp", f"{container}:{directory}/.", "-"], stdout=stream, stderr=subprocess.PIPE, timeout=600)
    if result.returncode or path.stat().st_size < 512:
        raise RuntimeError("Data archive failed for " + directory + "; original containers and volumes retained")
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def new_containers():
    return {c["Config"]["Labels"]["com.docker.compose.service"]: c for c in inventory()
            if c["Name"].lstrip("/").startswith(PROJECT + "-")
            and (c["Config"].get("Labels") or {}).get("com.docker.compose.project") == PROJECT}


def migrate(source_project=None):
    groups = legacy_groups(inventory())
    source_project, source = choose_source(groups, source_project)
    if new_containers():
        raise RuntimeError("New stack already exists; refusing to overwrite it")
    if any(c["State"]["Running"] for group, services in groups.items() if group != source_project for c in services.values()):
        raise RuntimeError("Another old stack is running. Stop it explicitly before selecting the source")
    config = json.loads(compose("config", "--format", "json"))
    pg_env = environment(source["postgres"])
    for key in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        if pg_env.get(key) != config["services"]["postgres"]["environment"].get(key):
            raise RuntimeError(f"Source {key} differs from .env; align it first without resetting data")
    if pg_env.get("PG_MAJOR") != "16" or pg_env.get("PGDATA", "/var/lib/postgresql/data") != "/var/lib/postgresql/data":
        raise RuntimeError("Only the PostgreSQL 16 default data directory is supported")
    directory = kafka_directory(source["kafka"])
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup = ROOT / "data/runtime/docker-migration" / run_id
    backup.mkdir(parents=True)
    ids = {key: value["Id"] for key, value in source.items()}
    # A private rollback copy; never included in the public manifest or Git.
    (backup / "environment.before").write_bytes((ROOT / ".env").read_bytes())
    print('Checking source PostgreSQL and Kafka...', flush=True)
    docker("start", ids["postgres"], ids["kafka"])
    wait_postgres(ids["postgres"]); wait_kafka(ids["kafka"])
    before = fingerprint(ids["postgres"])
    offsets = kafka_offsets(ids["kafka"])
    record = dict(source_project=source_project, source_ids=ids, tables_before=before, offsets_before=offsets,
                  status="backing-up", archives={}, volumes={})
    manifest = backup / "manifest.json"
    def save():
        manifest.write_text(json.dumps(record, indent=2), encoding="utf-8")
    save()
    print('Source measured; stopping containers for a consistent archive...', flush=True)
    docker("stop", "--time", "60", *ids.values())
    # Compose identifies services by project labels, even after docker rename.
    # Archive all old services in the target project before releasing its names.
    record['released_ids'] = []
    for service, container in groups.get(PROJECT, {}).items():
        if source_project == PROJECT and service in ('postgres', 'kafka'):
            continue  # These archives are made below, before any removal.
        directory_other = {'postgres': '/var/lib/postgresql/data', 'pgadmin': '/var/lib/pgadmin'}.get(service)
        if service == 'kafka': directory_other = kafka_directory(container)
        key = 'unselected-' + service
        record['archives'][key] = archive(container['Id'], directory_other, backup / (key + '.tar'))
        save()
    settings = {}
    for service, data_dir in [("postgres", "/var/lib/postgresql/data"), ("kafka", directory)]:
        print('Archiving and copying ' + service + ' (original data retained)...', flush=True)
        path = backup / (service + ".tar")
        record["archives"][service] = archive(ids[service], data_dir, path)
        volume = f"{PROJECT}_{service}_{run_id.lower()}"
        image = source[service]["Image"]  # Keep the exact image; no upgrade/downgrade.
        docker("volume", "create", "--label", f"com.docker.compose.project={PROJECT}",
               "--label", f"com.docker.compose.volume={service}_data", volume)
        docker("run", "--rm", "--user", "0", "--entrypoint", "sh",
               "--mount", f"type=volume,src={volume},dst=/target",
               "--mount", f"type=bind,src={backup},dst=/backup,readonly", image,
               "-c", f"tar -xpf /backup/{service}.tar -C /target", timeout=600)
        settings[service.upper() + "_VOLUME_NAME"] = volume
        record["volumes"][service] = volume
        tag = f"crypto-bigdata-preserved-{service}:{run_id.lower()}"
        docker("image", "tag", image, tag)
        settings[service.upper() + "_IMAGE"] = tag
        save()
    # Source pgAdmin is optional and has its own persistent settings.
    if source_project != PROJECT and 'pgadmin' in source:
        record['archives']['pgadmin'] = archive(ids['pgadmin'], '/var/lib/pgadmin', backup / 'pgadmin.tar')
        save()
    for container in groups.get(PROJECT, {}).values():
        docker('rm', container['Id'])  # Backups complete; retain every old volume.
        record['released_ids'].append(container['Id']); save()
    update_env(ROOT / ".env", settings)
    record['settings'] = settings
    record["status"] = "starting-new-stack"; save()
    print('Waiting for the unified Docker stack (maximum 180 seconds)...', flush=True)
    compose("up", "-d", "--no-recreate", "--wait", "--wait-timeout", "180", "postgres", "kafka")
    current = new_containers()
    record['target_ids'] = {s: c['Id'] for s, c in current.items()}
    save()
    after = fingerprint(current["postgres"]["Id"])
    after_offsets = kafka_offsets(current["kafka"]["Id"])
    if after != before or after_offsets != offsets:
        raise RuntimeError("Data verification differs; old containers, original volumes and backup retained")
    record.update(status="verified", tables_after=after, offsets_after=after_offsets,
                  target_ids={s: c["Id"] for s, c in current.items()})
    save()
    print("Migration verified: table counts and Kafka offsets unchanged. Backup: " + str(backup))
    print("Old containers stopped for rollback. Retire only after application checks.")


def guard_start(containers):
    groups = legacy_groups(containers)
    current = new_containers()
    verified = False
    for path in (ROOT / 'data/runtime/docker-migration').glob('*/manifest.json'):
        record = json.loads(path.read_text(encoding='utf-8'))
        matches = bool(record.get('target_ids')) and all(current.get(s, {}).get('Id') == cid for s, cid in record['target_ids'].items())
        mounts = {m.get('Name') for c in current.values() for m in c.get('Mounts', []) if m.get('Type') == 'volume'}
        selected = bool(mounts.intersection(record.get('volumes', {}).values()))
        if record.get('status') != 'verified' and (matches or selected):
            raise RuntimeError('Selected Docker data has an incomplete migration; inspect its manifest before starting writers')
        verified |= record.get('status') == 'verified' and matches
    if not groups or (verified and not any(c['State']['Running'] for services in groups.values() for c in services.values())):
        return
    raise RuntimeError('Old Docker stack detected. Run scripts/Merge-Docker.ps1 first; no empty database was created')


def retire(manifest_path):
    path = Path(manifest_path).resolve()
    path.relative_to((ROOT / "data/runtime/docker-migration").resolve())
    record = json.loads(path.read_text(encoding="utf-8"))
    if record["status"] != "verified":
        raise ValueError("Migration has not passed data verification")
    current = new_containers()
    if any(current.get(s, {}).get("Id") != cid or not current[s]["State"]["Running"] for s, cid in record["target_ids"].items()):
        raise ValueError("Verified target stack is not running")
    for service, expected in record["archives"].items():
        with (path.parent / (service + ".tar")).open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
                raise ValueError("Backup checksum mismatch")
    containers = {c["Id"]: c for c in inventory()}
    for cid in record["source_ids"].values():
        if cid in containers:
            if not legacy_groups([containers[cid]]):
                raise ValueError('Source no longer has expected legacy ownership')
            if containers[cid]["State"]["Running"]:
                raise ValueError("Old source restarted; refusing retirement")
    for cid in record["source_ids"].values():
        if cid in containers:
            docker("rm", cid)  # No -v: original volumes remain available.
    print("Retired verified old containers. Original volumes and backups retained.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["up", "status", "stop", "migrate", "retire"])
    parser.add_argument("--admin", action="store_true")
    parser.add_argument("--source-project", choices=["codex-crypto-local", PROJECT])
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    if args.command == "status":
        print(json.dumps({p: list(s) for p, s in legacy_groups(inventory()).items()}, indent=2))
        print(compose("ps"))
    elif args.command == "migrate":
        migrate(args.source_project)
    elif args.command == "retire":
        if args.manifest is None: parser.error("retire requires --manifest")
        retire(args.manifest)
    elif args.command == "stop":
        print(compose("--profile", "admin", "stop"))
    else:
        guard_start(inventory())
        if args.admin:
            cfg = json.loads(compose("--profile", "admin", "config", "--format", "json"))
            if not cfg["services"]["pgadmin"]["environment"].get("PGADMIN_DEFAULT_PASSWORD"):
                raise ValueError("Set PGADMIN_PASSWORD in .env before enabling pgAdmin")
        services = ["postgres", "kafka"] + (["pgadmin"] if args.admin else [])
        print('Waiting for Docker services (maximum 180 seconds)...', flush=True)
        print(compose("up", "-d", "--no-recreate", "--wait", "--wait-timeout", "180", *services))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
        print("Docker setup: " + str(exc), file=sys.stderr)
        sys.exit(1)
