#!/usr/bin/env python
"""Snapshot Plan-2 lever5-split fixtures from synthetic comparison records.

This is the **default** Plan 2 fixture path. It does not require a live
trial run with ``GSO_LEVER5_SHADOW_V1=1`` (which doubles LLM cost), nor
does it depend on any non-causal site firing.

Why synthetic comparisons are sufficient
----------------------------------------
The byte-stability test
``tests/unit/optimization/test_lever5_split_fixtures.py`` validates two
properties of each fixture:

* The ``lever_5a`` field (the L5a output) must pass the no-SQL gate
  (``_validate_lever_5a_no_sql_output``) — i.e. instructions are
  prose-only with no fenced ```sql blocks or SELECT-FROM queries.
* The shadow-comparison fields (``ag_id``, ``instruction_text_jaccard``,
  ``example_sqls_set_overlap``, ``old_example_sqls_count``,
  ``new_example_sqls_count``) must be present and the two ratios must
  lie in ``[0.0, 1.0]``.

Both properties are invariants of the **comparison math**, not of any
particular live LLM output. We construct ``(old, new)`` pairs covering
distinct overlap regimes, invoke the real
``_emit_lever5_shadow_comparison`` to compute the comparison record
(via dict capture, not the NDJSON sink), and serialize.

This guards against:
  * The 5a no-SQL gate regressing.
  * The Jaccard / set-overlap math returning out-of-range values.
  * Any field ever being dropped from the comparison-record schema.

It does **not** guard against the live LLM call sites changing their
prompts; for that, see the future hi-fidelity path in
``scripts/export_lever5_split_fixtures.py`` (which captures real
trial-run output via MLflow).

Usage
-----
    python scripts/export_lever5_split_fixtures_synthetic.py \\
        --output-dir tests/fixtures/lever5_split_v1

The script is idempotent: existing fixtures with identical content are
left untouched.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


# Synthetic ``(old_holistic, new_split)`` pairs. Each scenario exercises
# a distinct region of the (instruction-overlap × sql-overlap) plane so
# the resulting fixtures together prove the comparison math handles
# every case.
_SCENARIOS: tuple[dict[str, Any], ...] = (
    # 1. High-overlap: holistic and 5a propose near-identical
    # instructions; 5b SQL set is the same as the holistic SQL set.
    # Expected: instruction_text_jaccard ≈ 0.7+, example_sqls_set_overlap = 1.0.
    {
        "ag_id": "AG_SYNTH_HIGH_OVERLAP_001",
        "cluster_ids": ["c-synth-001", "c-synth-002"],
        "old": {
            "instruction_text": (
                "Always join orders to customers using customer_id. "
                "When the question references both tables, use an INNER JOIN "
                "by default; switch to LEFT JOIN only if the question asks "
                "for customers without orders."
            ),
            "example_sql_proposals": [
                {
                    "example_question": "Total orders per customer",
                    "example_sql": (
                        "SELECT customer_id, COUNT(*) AS order_count "
                        "FROM orders GROUP BY customer_id"
                    ),
                    "usage_guidance": "Default aggregation by customer.",
                },
                {
                    "example_question": "Customers with no orders",
                    "example_sql": (
                        "SELECT c.customer_id FROM customers c "
                        "LEFT JOIN orders o ON c.customer_id = o.customer_id "
                        "WHERE o.order_id IS NULL"
                    ),
                    "usage_guidance": "Use LEFT JOIN for orphan-customer detection.",
                },
            ],
        },
        "new": {
            "instruction_text": (
                "Always join orders to customers using customer_id when both "
                "tables are referenced. Default to INNER JOIN; use LEFT JOIN "
                "only when the question explicitly asks for customers without "
                "matching orders."
            ),
            "example_sql_proposals": [
                {
                    "example_question": "Total orders per customer",
                    "example_sql": (
                        "SELECT customer_id, COUNT(*) AS order_count "
                        "FROM orders GROUP BY customer_id"
                    ),
                    "usage_guidance": "Default aggregation by customer.",
                },
                {
                    "example_question": "Customers with no orders",
                    "example_sql": (
                        "SELECT c.customer_id FROM customers c "
                        "LEFT JOIN orders o ON c.customer_id = o.customer_id "
                        "WHERE o.order_id IS NULL"
                    ),
                    "usage_guidance": "Use LEFT JOIN for orphan-customer detection.",
                },
            ],
        },
        "rationale": "Holistic and split paths converged on the same join policy.",
    },

    # 2. Low-overlap: holistic and split chose different topics; SQL
    # sets are entirely disjoint. Expected: jaccard ≈ 0, overlap = 0.
    {
        "ag_id": "AG_SYNTH_LOW_OVERLAP_002",
        "cluster_ids": ["c-synth-003"],
        "old": {
            "instruction_text": (
                "Use the orders_summary materialized view for any aggregation "
                "by customer because the underlying orders table contains "
                "draft rows that distort the totals."
            ),
            "example_sql_proposals": [
                {
                    "example_question": "Top 10 customers by lifetime value",
                    "example_sql": (
                        "SELECT customer_id, total_revenue "
                        "FROM orders_summary "
                        "ORDER BY total_revenue DESC LIMIT 10"
                    ),
                    "usage_guidance": "Always read from orders_summary, not orders.",
                },
            ],
        },
        "new": {
            "instruction_text": (
                "Filter customers by status='active' for any analysis of the "
                "customer base; inactive accounts skew counts and revenue."
            ),
            "example_sql_proposals": [
                {
                    "example_question": "Active customer count",
                    "example_sql": (
                        "SELECT COUNT(*) FROM customers WHERE status = 'active'"
                    ),
                    "usage_guidance": "Always filter by status when counting customers.",
                },
            ],
        },
        "rationale": (
            "Split path uncovered a different root cause than the holistic "
            "path; both proposals are valid but address disjoint failures."
        ),
    },

    # 3. Both-empty: neither path proposed anything (rare iteration where
    # no failures map to lever-5). Expected: jaccard = 1.0 (sentinel),
    # overlap = 1.0, counts = 0.
    {
        "ag_id": "AG_SYNTH_BOTH_EMPTY_003",
        "cluster_ids": ["c-synth-004"],
        "old": {
            "instruction_text": "",
            "example_sql_proposals": [],
        },
        "new": {
            "instruction_text": "",
            "example_sql_proposals": [],
        },
        "rationale": "No L5 changes warranted this iteration.",
    },
)


def _short_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


@contextlib.contextmanager
def _shadow_flag_on_and_capture():
    """Temporarily enable Plan 2 shadow flags and replace the
    NDJSON-writing sink call with a list-capturing stub. Yields the
    list that will be appended to once per ``_emit_lever5_shadow_comparison``
    call. Restores everything on exit."""
    from genie_space_optimizer.common import config as cfg

    saved_env = {
        k: os.environ.get(k)
        for k in ("GSO_LEVER5_SHADOW_V1", "GSO_LEVER5_SPLIT_V1")
    }
    os.environ["GSO_LEVER5_SHADOW_V1"] = "1"
    os.environ["GSO_LEVER5_SPLIT_V1"] = "1"

    captured: list[dict] = []
    saved_sink = cfg._record_lever5_shadow_comparison  # noqa: SLF001
    cfg._record_lever5_shadow_comparison = (  # noqa: SLF001
        lambda record: captured.append(dict(record))
    )

    try:
        yield captured
    finally:
        cfg._record_lever5_shadow_comparison = saved_sink  # noqa: SLF001
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _build_fixture_for_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    """Run the real ``_emit_lever5_shadow_comparison`` with the
    scenario's ``(old, new)`` pair, capture the comparison record, and
    augment with the L5-holistic / L5a / L5b fields the test schema
    expects (as well as the fields ``export_lever5_split_fixtures.py``
    populates from MLflow)."""
    from genie_space_optimizer.optimization.optimizer import (
        _emit_lever5_shadow_comparison,
    )

    with _shadow_flag_on_and_capture() as captured:
        _emit_lever5_shadow_comparison(
            ag_id=scenario["ag_id"],
            cluster_ids=scenario["cluster_ids"],
            old=scenario["old"],
            new=scenario["new"],
        )

    if not captured:
        raise RuntimeError(
            f"_emit_lever5_shadow_comparison produced no record for "
            f"{scenario['ag_id']!r}; check that the shadow flag is being "
            f"set and the function is not early-returning."
        )
    record = captured[0]

    # Augment with the test-required ``lever_5a`` field plus the
    # observability fields the live MLflow exporter populates.
    fixture: dict[str, Any] = {
        **record,
        "captured_at": time.time(),
        "lever_5_holistic": {
            "prompt_bytes": "",  # synthetic; not derivable without a live LLM render
            "response_bytes": json.dumps(scenario["old"], sort_keys=True),
        },
        "lever_5a": {
            "instruction_text": scenario["new"]["instruction_text"],
            "rationale": scenario["rationale"],
        },
        "lever_5b_proposals": [
            {
                "example_question": p.get("example_question", ""),
                "example_sql": p.get("example_sql", ""),
                "usage_guidance": p.get("usage_guidance", ""),
            }
            for p in scenario["new"].get("example_sql_proposals", [])
        ],
        "source": "synthetic_comparison",
    }
    return fixture


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="e.g. tests/fixtures/lever5_split_v1",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be written without writing files.",
    )
    args = parser.parse_args(argv)

    # Validator gate: each fixture's lever_5a slice must pass the
    # no-SQL gate or there's a bug in the synthetic data itself.
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped_existing = 0
    fixtures: list[tuple[Path, dict[str, Any]]] = []

    for scenario in _SCENARIOS:
        fixture = _build_fixture_for_scenario(scenario)
        ok, reason = _validate_lever_5a_no_sql_output(fixture["lever_5a"])
        if not ok:
            print(
                f"FATAL: synthetic scenario {scenario['ag_id']!r} fails "
                f"5a no-SQL gate: {reason}",
                file=sys.stderr,
            )
            return 1

        # Stable filename: ag_id + content hash of the comparison fields.
        comparison_hash_input = json.dumps(
            {k: fixture[k] for k in (
                "ag_id", "instruction_text_jaccard",
                "example_sqls_set_overlap",
                "old_example_sqls_count", "new_example_sqls_count",
                "old_example_sqls_hashes", "new_example_sqls_hashes",
            )},
            sort_keys=True,
        )
        h = _short_hash(comparison_hash_input)
        out_path = args.output_dir / f"{fixture['ag_id']}__{h}.json"
        fixtures.append((out_path, fixture))

    for out_path, fixture in fixtures:
        if out_path.exists():
            existing = json.loads(out_path.read_text(encoding="utf-8"))
            # Compare on the comparison-math fields; ignore captured_at
            # (which would otherwise force a rewrite every run).
            same = all(
                existing.get(k) == fixture.get(k)
                for k in (
                    "ag_id", "instruction_text_jaccard",
                    "example_sqls_set_overlap",
                    "old_example_sqls_count", "new_example_sqls_count",
                    "old_example_sqls_hashes", "new_example_sqls_hashes",
                    "lever_5a",
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
        "fixtures_written": written,
        "fixtures_skipped_existing": skipped_existing,
        "output_dir": str(args.output_dir),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
