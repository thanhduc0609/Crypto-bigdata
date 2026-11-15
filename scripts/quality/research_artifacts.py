"""Create, restore or verify the repository's trusted frozen research bundle."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from runtime.research_artifacts import DEFAULT_ARCHIVE, create, restore


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("create", "restore", "verify"))
    parser.add_argument("--archive", type=Path, default=ROOT / DEFAULT_ARCHIVE)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    if args.action == "create":
        manifest = create(args.root, args.archive)
        result = {"archive": str(args.archive), "sha256": manifest["archive_sha256"], "files": len(manifest["files"])}
    else:
        result = restore(args.root, args.archive, verify_only=args.action == "verify")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
