#!/usr/bin/env python
"""Snapshot Plan-1 narrowing fixtures from prompt-template constants.

This is the **default** Plan 1 fixture path. It does not require a live
trial run, MLflow access, or any of the three non-causal sites firing
(they are genuinely cold paths in most genie-space optimizations).

Why template-snapshot is sufficient
-----------------------------------
The byte-stability test
``tests/unit/optimization/test_narrowing_v1_fixtures.py`` only asserts
that the contract block ``<unified_rca_engine_contract>`` is **absent**
from each captured ``prompt_bytes``. With ``GSO_RCA_CONTRACT_NARROW_V1``
default-on (Plan 5 commit), Plan 1's narrowing logic strips the contract
block out of the three non-causal prompt templates **at module import
time** during constant assembly:

* ``EXPAND_INSTRUCTION_PROMPT``        (skill_id ``preflight-instruction-expand``)
* ``LEVER_4_JOIN_DISCOVERY_PROMPT``    (skill_id ``lever-4-join-discovery``)
* ``SQL_EXPRESSION_SEEDING_PROMPT``    (skill_id ``preflight-sql-expression-seeding``)

The constants on disk after import therefore *are* the byte-stability
ground truth. We snapshot them directly. Any future regression that
silently re-injects the contract into a non-causal template will flip
``contract_present`` to True the next time fixtures are regenerated, and
the pytest assertion will fail.

Usage
-----
    python scripts/export_narrowing_fixtures_from_templates.py \\
        --output-dir tests/fixtures/narrowing_v1

The script is idempotent: if a fixture file with identical
``prompt_bytes`` already exists at the target path, it is left
untouched. This keeps re-runs from churning git diff.

For the live render-time MLflow capture path (used when a future trial
actually exercises one of the non-causal sites at LLM-call time), see
``scripts/export_narrowing_fixtures.py``. That path is currently
deferred until at least one such trial is observed; this template
snapshot is the source of truth for "Plan 1 is live" until then.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path


def _short_hash(prompt_bytes: str) -> str:
    return hashlib.sha256(prompt_bytes.encode("utf-8")).hexdigest()[:12]


def _load_prompt_constants() -> dict[str, str]:
    """Import config.py with ``GSO_RCA_CONTRACT_NARROW_V1`` default-on
    (i.e. Plan 1 narrowing active) and return the three non-causal
    prompt template constants keyed by skill_id."""
    # Plan 5 set the flag default-on, so simply importing config.py is
    # enough — but make this script robust to running it in a shell
    # where someone has explicitly set the flag to "0" by clearing it.
    os.environ.pop("GSO_RCA_CONTRACT_NARROW_V1", None)

    from genie_space_optimizer.common import config as cfg

    # Sanity-check the registry mapping is what we expect, otherwise
    # fail loud rather than silently snapshotting the wrong constants.
    expected = {
        "preflight-instruction-expand",
        "lever-4-join-discovery",
        "preflight-sql-expression-seeding",
    }
    if set(cfg._NON_CAUSAL_PROMPT_NAMES) != expected:  # noqa: SLF001
        raise RuntimeError(
            f"_NON_CAUSAL_PROMPT_NAMES drifted from this script's "
            f"hard-coded mapping: registry={set(cfg._NON_CAUSAL_PROMPT_NAMES)}, "
            f"expected={expected}. Update both."
        )

    return {
        "preflight-instruction-expand": cfg.EXPAND_INSTRUCTION_PROMPT,
        "lever-4-join-discovery": cfg.LEVER_4_JOIN_DISCOVERY_PROMPT,
        "preflight-sql-expression-seeding": cfg.SQL_EXPRESSION_SEEDING_PROMPT,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="e.g. tests/fixtures/narrowing_v1",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be written without writing files.",
    )
    args = parser.parse_args(argv)

    templates = _load_prompt_constants()

    # Hard-fail if Plan 1 narrowing has regressed: the snapshot path
    # must produce contract-stripped fixtures or it's worse than useless.
    for skill_id, prompt_bytes in templates.items():
        if "<unified_rca_engine_contract>" in prompt_bytes:
            print(
                f"FATAL: skill {skill_id!r} template still contains the "
                f"contract block. Plan 1 narrowing has regressed; refusing "
                f"to write a stale fixture.",
                file=sys.stderr,
            )
            return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped_existing = 0
    captured_at = time.time()

    for skill_id, prompt_bytes in sorted(templates.items()):
        h = _short_hash(prompt_bytes)
        out_path = args.output_dir / f"{skill_id}__{h}.json"
        payload = {
            "skill_id": skill_id,
            "prompt_bytes": prompt_bytes,
            # No live LLM response when snapshotting the template;
            # leave empty so the byte-stability test (which ignores
            # this field) still passes.
            "llm_response_bytes": "",
            "captured_at": captured_at,
            # Marks provenance so a future reader knows this is a
            # template snapshot, not a live render captured via MLflow.
            "source_span_id": "",
            "source": "template_snapshot",
        }
        if out_path.exists():
            existing = json.loads(out_path.read_text(encoding="utf-8"))
            if existing.get("prompt_bytes") == prompt_bytes:
                skipped_existing += 1
                continue
        if args.dry_run:
            print(f"[dry-run] would write {out_path}")
        else:
            # Stable, deterministic JSON output: sorted keys + final newline.
            out_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        written += 1

    summary = {
        "skills_snapshotted": sorted(templates),
        "fixtures_written": written,
        "fixtures_skipped_existing": skipped_existing,
        "output_dir": str(args.output_dir),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
