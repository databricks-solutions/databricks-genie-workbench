"""Read-only deploy guard: 0 clear, 1 dual_authority, 2 tooling/unresolvable input."""

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("bundles", nargs="+", type=Path)
    args = parser.parse_args(argv)
    try:
        from backend.services.version_control.platform.bundles import bundle_detection_evidence

        evidence = [bundle_detection_evidence(path) for path in args.bundles]
    except Exception as error:
        print(f"VC bundle guard tooling failure: {error}", file=sys.stderr)
        return 2
    print(json.dumps([asdict(item) for item in evidence], sort_keys=True))
    if any(item.status not in {"clear", "dual_authority"} for item in evidence):
        return 2
    return 1 if any(item.status == "dual_authority" for item in evidence) else 0


if __name__ == "__main__":
    raise SystemExit(main())
