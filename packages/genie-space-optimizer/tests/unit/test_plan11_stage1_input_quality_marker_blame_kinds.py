"""Trial 14 — ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1`` carries the
typed ``blame_kind_distribution`` alongside the Trial 13k
``seed_normalization_verdict``.

Locks the marker payload contract:

* ``blame_kind_distribution`` is always present (default ``{}``).
* Keys are restricted to the closed :data:`BLAME_KINDS` vocabulary.
* Zero / unknown counts are stripped.
* Coexists with ``seed_normalization_verdict`` from Trial 13k.
"""
from __future__ import annotations

import json
import re

from genie_space_optimizer.optimization.blame_entry import BLAME_KINDS
from genie_space_optimizer.optimization.run_analysis_contract import (
    plan11_stage1_input_quality_marker,
)

_MARKER_TAG = "GSO_PLAN11_STAGE1_INPUT_QUALITY_V1"


def _parse_marker(line: str) -> dict:
    """Pull the JSON payload out of a ``GSO_*`` marker line."""
    match = re.search(r"\{.*\}", line)
    assert match is not None, f"marker line missing JSON payload: {line!r}"
    return json.loads(match.group(0))


def test_marker_carries_blame_kind_distribution() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=2,
        qid="gs_001",
        schema_columns_source="typed_evidence_union",
        schema_columns_size=42,
        seeds_pre_normalize=3,
        seeds_post_normalize=2,
        seeds_normalized=1,
        seeds_dropped=1,
        contract_violation="",
        blame_kind_distribution={"column": 2, "filter": 1},
    )
    assert _MARKER_TAG in line
    payload = _parse_marker(line)
    assert payload["blame_kind_distribution"] == {"column": 2, "filter": 1}
    # Trial 13k verdict still emitted.
    assert payload["seed_normalization_verdict"] == "partial_drop"


def test_marker_defaults_blame_kind_distribution_to_empty_dict() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=0,
        qid="gs_002",
        schema_columns_source="empty",
        schema_columns_size=0,
    )
    payload = _parse_marker(line)
    assert payload["blame_kind_distribution"] == {}


def test_marker_strips_unknown_blame_kinds() -> None:
    """Drift safety: unknown ``kind`` keys from upstream callers MUST
    be silently dropped so the marker never publishes a key outside
    the closed vocabulary."""
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=0,
        qid="gs_003",
        schema_columns_source="empty",
        schema_columns_size=0,
        blame_kind_distribution={"column": 1, "bogus_kind": 99, "weird": 5},
    )
    payload = _parse_marker(line)
    assert payload["blame_kind_distribution"] == {"column": 1}


def test_marker_strips_zero_counts() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=0,
        qid="gs_004",
        schema_columns_source="empty",
        schema_columns_size=0,
        blame_kind_distribution={"column": 1, "filter": 0, "instruction": 0},
    )
    payload = _parse_marker(line)
    assert payload["blame_kind_distribution"] == {"column": 1}


def test_marker_accepts_every_allowed_kind() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=0,
        qid="gs_005",
        schema_columns_source="typed_evidence_union",
        schema_columns_size=10,
        blame_kind_distribution={k: 1 for k in BLAME_KINDS},
    )
    payload = _parse_marker(line)
    assert set(payload["blame_kind_distribution"].keys()) == BLAME_KINDS


def test_marker_keeps_trial_13k_verdict_field() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=0,
        qid="gs_006",
        schema_columns_source="empty",
        schema_columns_size=0,
        seeds_pre_normalize=3,
        seeds_post_normalize=0,
        blame_kind_distribution={"filter": 1},
    )
    payload = _parse_marker(line)
    assert payload["seed_normalization_verdict"] == "all_dropped"
    assert payload["blame_kind_distribution"] == {"filter": 1}
