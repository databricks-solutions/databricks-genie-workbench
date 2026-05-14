#!/usr/bin/env python
"""Export hi-fidelity Plan-4 raw-evidence fixtures from a real trial run.

Status
------
This is the **future hi-fidelity** Plan 4 fixture path. It preserves
shadow-comparison records emitted during a live optimization trial
that ran with ``GSO_RAW_EVIDENCE_SHADOW_V1=1`` (which doubles LLM
cost during Stage-2 dispatch — every dispatch runs both the
raw-evidence-on and raw-evidence-off path).

For the **default** Plan 4 path that does not require a live shadow
trial, see ``scripts/export_raw_evidence_fixtures_synthetic.py``.
That script invokes the real ``_emit_raw_evidence_shadow_comparison``
with synthetic ``(off, on)`` proposal pairs covering every valid
``structural_diff`` token (``identical``, ``count_differs``,
``keys_differ``, ``content_differs``, ``both_empty``) and writes
fixtures that satisfy the byte-stability test
(``tests/unit/optimization/test_raw_evidence_fixtures.py``). Use the
synthetic path unless you specifically need real Stage-2 proposal
content captured from a live LLM render.

Inputs
------
* ``--capture-path`` — NDJSON written by ``_RawEvidenceCaptureSink``
  during the shadow trial (one record per Stage-2 dispatch with
  ``GSO_RAW_EVIDENCE_CAPTURE_PATH`` set).

Outputs
-------
One fixture per record into
``tests/fixtures/raw_evidence_v1/<ag_id>__<skill_id>__<short_hash>.json``
preserving the comparison record verbatim. Idempotent: re-runs skip
files whose content hash already exists.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


def _short_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--capture-path", required=True, type=Path,
                   help="GSO_RAW_EVIDENCE_CAPTURE_PATH from the trial run")
    p.add_argument("--output-dir", required=True, type=Path,
                   help="e.g. tests/fixtures/raw_evidence_v1")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if not args.capture_path.is_file():
        print(f"capture file not found: {args.capture_path}", file=sys.stderr)
        return 1
    records = _read_ndjson(args.capture_path)
    if not records:
        print("capture file is empty — trial may not have triggered shadow mode")
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped = 0
    for record in records:
        ag_id = str(record.get("ag_id", "unknownAG"))
        skill_id = str(record.get("skill_id", "unknownSkill"))
        content_hash = _short_hash(json.dumps(record, sort_keys=True, default=str))
        out_path = args.output_dir / f"{ag_id}__{skill_id}__{content_hash}.json"
        if out_path.exists():
            skipped += 1
            continue
        if args.dry_run:
            print(f"[dry-run] would write {out_path}")
        else:
            out_path.write_text(json.dumps(record, indent=2, default=str),
                                encoding="utf-8")
        written += 1

    print(json.dumps({
        "captures_read": len(records),
        "fixtures_written": written,
        "fixtures_skipped_existing": skipped,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
