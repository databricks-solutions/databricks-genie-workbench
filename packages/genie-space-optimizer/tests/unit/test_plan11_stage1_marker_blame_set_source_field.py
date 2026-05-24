"""Trial 13h — pin the three new blame_set provenance fields on
``GSO_PLAN11_STAGE1_DIAGNOSIS_V1``.

These fields are the canary surface for the Trial 13h fix: they let
postmortems distinguish "Stage 1 LLM is healthy" (``blame_set_source ==
"llm"``) from "Stage 1 LLM is drifting and we patched it up via the seed
backfill" (``blame_set_source == "seed_backfill"``) from "Stage 1 evidence
pipeline regressed and there is nothing to fall back on"
(``blame_set_source == "empty"``).

The frozenset ``STAGE1_BLAME_SET_SOURCES`` is the closed vocabulary for the
field — postmortems and the ``gso-postmortem`` SKILL canary checks consume
it directly.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    STAGE1_BLAME_SET_SOURCES,
    plan11_stage1_diagnosis_marker,
)


def _payload(line: str) -> dict:
    return json.loads(line.split(" ", 1)[1])


def test_stage1_blame_set_sources_closed_vocabulary() -> None:
    # Closed enum — postmortems join on these values, so we pin the set.
    assert STAGE1_BLAME_SET_SOURCES == frozenset(
        {"llm", "seed_backfill", "empty"}
    )


def test_marker_carries_blame_set_source_llm() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_001",
        outcome="diagnosed",
        rca_kind_label="top-N collapsed",
        blame_set_size=2,
        evidence_summary_chars=150,
        blame_set_source="llm",
        blame_set_llm_emitted=2,
        blame_set_post_schema_dropped=0,
    )
    payload = _payload(line)
    assert payload["blame_set_source"] == "llm"
    assert payload["blame_set_llm_emitted"] == 2
    assert payload["blame_set_post_schema_dropped"] == 0
    assert payload["blame_set_source"] in STAGE1_BLAME_SET_SOURCES


def test_marker_carries_blame_set_source_seed_backfill() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="missing top-N pattern",
        blame_set_size=1,
        evidence_summary_chars=200,
        blame_set_source="seed_backfill",
        blame_set_llm_emitted=0,
        blame_set_post_schema_dropped=0,
    )
    payload = _payload(line)
    assert payload["blame_set_source"] == "seed_backfill"
    assert payload["blame_set_llm_emitted"] == 0
    assert payload["blame_set_source"] in STAGE1_BLAME_SET_SOURCES


def test_marker_carries_blame_set_source_empty() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_999",
        outcome="diagnosed",
        rca_kind_label="cannot ground",
        blame_set_size=0,
        evidence_summary_chars=100,
        blame_set_source="empty",
        blame_set_llm_emitted=0,
        blame_set_post_schema_dropped=0,
    )
    payload = _payload(line)
    assert payload["blame_set_source"] == "empty"
    assert payload["blame_set_source"] in STAGE1_BLAME_SET_SOURCES
    # When source is empty, diagnosis_actionable must remain False —
    # backward-compatible invariant with the existing zero_blame_set gate.
    assert payload["diagnosis_actionable"] is False


def test_marker_carries_post_schema_dropped_count() -> None:
    """Schema-hallucination canary — the LLM emitted entries that all got
    dropped. Postmortems flag sustained ``blame_set_post_schema_dropped > 0``
    as a Stage 1 LLM drift indicator."""
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_007",
        outcome="diagnosed",
        rca_kind_label="hallucinated columns",
        blame_set_size=1,
        evidence_summary_chars=120,
        blame_set_source="seed_backfill",
        blame_set_llm_emitted=3,
        blame_set_post_schema_dropped=3,
    )
    payload = _payload(line)
    assert payload["blame_set_llm_emitted"] == 3
    assert payload["blame_set_post_schema_dropped"] == 3


def test_marker_defaults_preserve_backward_compat() -> None:
    """Pre-13h call sites (and outcomes other than ``diagnosed``) leave
    the new fields unset. The marker must still emit a well-formed
    payload with default values."""
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_001",
        outcome="declined",
    )
    payload = _payload(line)
    assert payload["blame_set_source"] == ""
    assert payload["blame_set_llm_emitted"] == 0
    assert payload["blame_set_post_schema_dropped"] == 0
