"""Patch .build/app.yml env values with deploy-time config variables.

Called by the ``databricks.yml`` artifact build step after ``apx build``
to inject the correct catalog, schema, and warehouse ID into the
app.yml that ships inside the deployed snapshot.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ENV_MAP = {
    "GENIE_SPACE_OPTIMIZER_CATALOG": "catalog",
    "GENIE_SPACE_OPTIMIZER_SCHEMA": "schema",
    "GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID": "warehouse_id",
    "GENIE_SPACE_OPTIMIZER_JOB_ID": "job_id",
}


def _patch(app_yml: Path, overrides: dict[str, str]) -> None:
    lines = app_yml.read_text().splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip().startswith("- name:"):
            name = line.split("- name:", 1)[1].strip().strip('"').strip("'")
            out.append(line)
            i += 1
            if i < len(lines) and name in overrides:
                indent = len(lines[i]) - len(lines[i].lstrip())
                out.append(f"{' ' * indent}value: \"{overrides[name]}\"")
                i += 1
            continue
        out.append(line)
        i += 1
    app_yml.write_text("\n".join(out) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Patch .build/app.yml env values")
    parser.add_argument("app_yml", type=Path)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--job-id", default="", help="Bundle-managed runner job ID")
    args = parser.parse_args()

    if not args.app_yml.exists():
        print(f"[patch-app-yml] {args.app_yml} not found, skipping", file=sys.stderr)
        return 0

    overrides: dict[str, str] = {
        "GENIE_SPACE_OPTIMIZER_CATALOG": args.catalog,
        "GENIE_SPACE_OPTIMIZER_SCHEMA": args.schema,
        "GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID": args.warehouse_id,
    }
    if args.job_id:
        overrides["GENIE_SPACE_OPTIMIZER_JOB_ID"] = args.job_id
    _patch(args.app_yml, overrides)
    parts = [f"catalog={args.catalog}", f"schema={args.schema}", f"warehouse_id={args.warehouse_id}"]
    if args.job_id:
        parts.append(f"job_id={args.job_id}")
    print(f"[patch-app-yml] Patched {args.app_yml}: {', '.join(parts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
