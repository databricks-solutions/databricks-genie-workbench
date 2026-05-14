#!/usr/bin/env python
"""Snapshot Plan-4 raw-evidence fixtures from synthetic comparison records.

This is the **default** Plan 4 fixture path. It does not require a live
trial run with ``GSO_RAW_EVIDENCE_SHADOW_V1=1`` (which doubles LLM cost
during Stage-2 dispatch).

Why synthetic comparisons are sufficient
----------------------------------------
The byte-stability test ``tests/unit/optimization/test_raw_evidence_fixtures.py``
validates three properties of each fixture:

* ``skill_id`` is in ``raw_evidence._PROJECTOR_TABLE`` or
  ``raw_evidence._EXCLUDED_SKILLS``.
* ``structural_diff`` is one of
  ``{identical, count_differs, keys_differ, content_differs, both_empty}``.
* Counts (``off_proposal_count``, ``on_proposal_count``, ``n_evidence``)
  are non-negative.

All three properties are invariants of the **classification math**
inside ``_emit_raw_evidence_shadow_comparison``, not of any particular
LLM output. We construct ``(off_proposals, on_proposals)`` pairs that
exercise each of the five ``structural_diff`` cases, invoke the real
emit function (via dict capture, not the NDJSON sink), and serialize.

This guards against:
  * The structural-diff classifier ever returning an unrecognized token.
  * The proposal-counting math going negative (overflow / sentinel bugs).
  * The skill_id allowlist drifting from ``raw_evidence._PROJECTOR_TABLE``
    + ``_EXCLUDED_SKILLS``.

It does **not** guard against the upstream Stage-2 prompt rendering;
for that, see the future hi-fidelity path in
``scripts/export_raw_evidence_fixtures.py`` (which captures real
trial-run NDJSONs from a shadow run).

Usage
-----
    python scripts/export_raw_evidence_fixtures_synthetic.py \\
        --output-dir tests/fixtures/raw_evidence_v1

Idempotent: existing fixtures with identical content are left untouched.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any


# Synthetic ``(off_proposals, on_proposals)`` pairs — one per valid
# ``structural_diff`` token. Each scenario uses a real skill_id from
# ``raw_evidence._PROJECTOR_TABLE`` or ``_EXCLUDED_SKILLS`` so the
# allowlist assertion in test_fixture_skill_id_known passes.
_SCENARIOS: tuple[dict[str, Any], ...] = (
    # 1. identical: raw evidence didn't change Stage-2's output at all
    # for this skill / cluster combination.
    {
        "ag_id": "AG_SYNTH_RE_IDENTICAL_001",
        "skill_id": "lever-1-table-column-description",
        "n_evidence": 3,
        "off_proposals": [
            {
                "table_full_name": "main.sales.orders",
                "column_name": "order_id",
                "description": "Primary key for an individual order.",
            },
            {
                "table_full_name": "main.sales.orders",
                "column_name": "customer_id",
                "description": "FK to customers.customer_id.",
            },
        ],
        "on_proposals": [
            {
                "table_full_name": "main.sales.orders",
                "column_name": "order_id",
                "description": "Primary key for an individual order.",
            },
            {
                "table_full_name": "main.sales.orders",
                "column_name": "customer_id",
                "description": "FK to customers.customer_id.",
            },
        ],
        "expected_diff": "identical",
    },

    # 2. count_differs: raw evidence steered the LLM to propose more
    # column descriptions than the no-evidence baseline.
    {
        "ag_id": "AG_SYNTH_RE_COUNT_DIFFERS_002",
        "skill_id": "lever-2-mv-column-refinement",
        "n_evidence": 3,
        "off_proposals": [
            {
                "mv_full_name": "main.sales.daily_sales_mv",
                "column_name": "total_revenue",
                "definition_change": "SUM(amount)",
            },
        ],
        "on_proposals": [
            {
                "mv_full_name": "main.sales.daily_sales_mv",
                "column_name": "total_revenue",
                "definition_change": "SUM(amount)",
            },
            {
                "mv_full_name": "main.sales.daily_sales_mv",
                "column_name": "order_count",
                "definition_change": "COUNT(DISTINCT order_id)",
            },
            {
                "mv_full_name": "main.sales.daily_sales_mv",
                "column_name": "active_customer_count",
                "definition_change": "COUNT(DISTINCT customer_id)",
            },
        ],
        "expected_diff": "count_differs",
    },

    # 3. keys_differ: same number of proposals, but raw evidence pushed
    # the LLM to include extra fields per proposal.
    {
        "ag_id": "AG_SYNTH_RE_KEYS_DIFFER_003",
        "skill_id": "lever-3-tvf-routing",
        "n_evidence": 2,
        "off_proposals": [
            {
                "tvf_full_name": "main.sales.f_orders_by_region",
                "routing_hint": "Use when filtering by region.",
            },
        ],
        "on_proposals": [
            {
                "tvf_full_name": "main.sales.f_orders_by_region",
                "routing_hint": "Use when filtering by region.",
                "argument_template": "f_orders_by_region(<region_code>)",
                "supersedes": "main.sales.orders",
            },
        ],
        "expected_diff": "keys_differ",
    },

    # 4. content_differs: same length, same keys, but the LLM chose
    # different values when given raw evidence.
    {
        "ag_id": "AG_SYNTH_RE_CONTENT_DIFFERS_004",
        "skill_id": "lever-6-sql-expression",
        "n_evidence": 3,
        "off_proposals": [
            {
                "expression_name": "active_customer_count",
                "expression_sql": "COUNT(DISTINCT customer_id)",
                "applies_when": "Counting customers.",
            },
        ],
        "on_proposals": [
            {
                "expression_name": "active_customer_count",
                "expression_sql": (
                    "COUNT(DISTINCT CASE WHEN status = 'active' "
                    "THEN customer_id END)"
                ),
                "applies_when": "Counting active customers (status filter required).",
            },
        ],
        "expected_diff": "content_differs",
    },

    # 5. both_empty: no proposals from either path. Common when the
    # skill_id is in _EXCLUDED_SKILLS — Stage-2 dispatcher returns
    # empty and the comparison is trivially ``both_empty``.
    {
        "ag_id": "AG_SYNTH_RE_BOTH_EMPTY_005",
        "skill_id": "lever-5b-example-sql",
        "n_evidence": 0,
        "off_proposals": [],
        "on_proposals": [],
        "expected_diff": "both_empty",
    },
)


def _short_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


@contextlib.contextmanager
def _shadow_flag_on_and_capture():
    """Temporarily enable Plan 4 shadow flag and replace the
    NDJSON-writing sink call with a list-capturing stub."""
    from genie_space_optimizer.common import config as cfg

    saved_env = {
        k: os.environ.get(k)
        for k in ("GSO_RAW_EVIDENCE_SHADOW_V1", "GSO_RAW_EVIDENCE_V1")
    }
    os.environ["GSO_RAW_EVIDENCE_SHADOW_V1"] = "1"
    # Also keep V1 on for safety: the emit function early-returns if
    # neither flag is on.
    os.environ["GSO_RAW_EVIDENCE_V1"] = "1"

    captured: list[dict] = []
    saved_sink = cfg._record_raw_evidence_shadow_comparison  # noqa: SLF001
    cfg._record_raw_evidence_shadow_comparison = (  # noqa: SLF001
        lambda record: captured.append(dict(record))
    )

    try:
        yield captured
    finally:
        cfg._record_raw_evidence_shadow_comparison = saved_sink  # noqa: SLF001
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _build_fixture_for_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    """Run the real ``_emit_raw_evidence_shadow_comparison`` with the
    scenario's ``(off, on)`` pair, capture the comparison record."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _emit_raw_evidence_shadow_comparison,
    )

    with _shadow_flag_on_and_capture() as captured:
        _emit_raw_evidence_shadow_comparison(
            ag_id=scenario["ag_id"],
            skill_id=scenario["skill_id"],
            n_evidence=scenario["n_evidence"],
            off_proposals=scenario["off_proposals"],
            on_proposals=scenario["on_proposals"],
        )

    if not captured:
        raise RuntimeError(
            f"_emit_raw_evidence_shadow_comparison produced no record "
            f"for {scenario['ag_id']!r}; check shadow flag is set and "
            f"the function is not early-returning."
        )

    record = captured[0]
    record["source"] = "synthetic_comparison"
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="e.g. tests/fixtures/raw_evidence_v1",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be written without writing files.",
    )
    args = parser.parse_args(argv)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped_existing = 0

    # Sanity-check the skill_id allowlist matches our scenarios.
    from genie_space_optimizer.optimization.raw_evidence import (
        _EXCLUDED_SKILLS, _PROJECTOR_TABLE,
    )
    valid_skills = set(_PROJECTOR_TABLE) | set(_EXCLUDED_SKILLS)
    for s in _SCENARIOS:
        if s["skill_id"] not in valid_skills:
            print(
                f"FATAL: scenario {s['ag_id']!r} uses skill_id "
                f"{s['skill_id']!r} not in _PROJECTOR_TABLE or "
                f"_EXCLUDED_SKILLS; update the scenario or the "
                f"allowlist together.",
                file=sys.stderr,
            )
            return 1

    for scenario in _SCENARIOS:
        fixture = _build_fixture_for_scenario(scenario)

        # Hard-fail if the structural-diff classifier disagreed with
        # the scenario's hand-coded expectation. That would mean the
        # synthetic data doesn't actually exercise the code path we
        # think it does.
        actual_diff = fixture.get("structural_diff")
        if actual_diff != scenario["expected_diff"]:
            print(
                f"FATAL: scenario {scenario['ag_id']!r} expected "
                f"structural_diff={scenario['expected_diff']!r} but the "
                f"emit function classified it as {actual_diff!r}. "
                f"Adjust the scenario's off/on proposals to match.",
                file=sys.stderr,
            )
            return 1

        # Stable filename: ag_id + skill_id + content hash.
        content_hash = _short_hash(json.dumps(fixture, sort_keys=True, default=str))
        out_path = (
            args.output_dir
            / f"{fixture['ag_id']}__{fixture['skill_id']}__{content_hash}.json"
        )

        if out_path.exists():
            existing = json.loads(out_path.read_text(encoding="utf-8"))
            same = all(
                existing.get(k) == fixture.get(k)
                for k in (
                    "ag_id", "skill_id", "n_evidence",
                    "off_proposal_count", "on_proposal_count",
                    "off_proposal_keys", "on_proposal_keys",
                    "structural_diff",
                )
            )
            if same:
                skipped_existing += 1
                continue
        if args.dry_run:
            print(f"[dry-run] would write {out_path}")
        else:
            out_path.write_text(
                json.dumps(fixture, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        written += 1

    print(json.dumps({
        "scenarios": [s["ag_id"] for s in _SCENARIOS],
        "structural_diffs_covered": sorted({s["expected_diff"] for s in _SCENARIOS}),
        "fixtures_written": written,
        "fixtures_skipped_existing": skipped_existing,
        "output_dir": str(args.output_dir),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
