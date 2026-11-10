"""Prepare, verify and publish a versioned confirmed-REST canonical recovery."""
import argparse
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from processing.core.env import load_env_vars
from processing.quality.canonical_repair import prepare, download, build_candidate


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-run",type=Path)
    parser.add_argument("--resume",type=Path)
    parser.add_argument("--action",choices=("capture","build","promote"),default="capture")
    args=parser.parse_args()
    load_env_vars()
    directory=args.resume.resolve() if args.resume else prepare(args.audit_run)
    print("[REPAIR RUN] "+str(directory),flush=True)
    if args.action=="capture":
        download(directory)
        print("[REST CAPTURE COMPLETE] "+str(directory),flush=True)
    elif args.action=="build":
        result=build_candidate(directory)
        print("[CANDIDATE VERIFIED] "+str(result["checks"]),flush=True)
    else:
        from storage.canonical_publish import publish,activate
        publication=publish(directory)
        print("[S3 SNAPSHOT VERIFIED] "+publication["manifest_uri"],flush=True)
        active=activate(directory,publication)
        print("[CANONICAL ACTIVE] "+str(active),flush=True)


if __name__=="__main__":
    main()
