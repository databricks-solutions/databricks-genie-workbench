"""Trial 13 Phase 8 — single normalization boundary at the eval-row.

The architectural deal-breaker phase. Every production row shape
extension must land in ``canonical_eval_row.normalize_eval_row``
exclusively. Tests load the 7 captured real production rows and
assert the canonical projection is non-empty across all string fields.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.canonical_eval_row import (
    AsiMetadata,
    CanonicalEvalRow,
    JudgeRationales,
    normalize_eval_row,
)

_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "production_eval_rows_real.json"
)


def _load_all_captures() -> list[dict]:
    payload = json.loads(_FIXTURE.read_text())
    out: list[dict] = []
    for key, value in payload.items():
        if not isinstance(value, list):
            continue
        for entry in value:
            if isinstance(entry, dict) and "row" in entry:
                out.append(entry)
    return out


_CAPTURES = _load_all_captures()


def test_fixture_has_seven_real_rows() -> None:
    assert len(_CAPTURES) == 7, (
        f"Expected 7 captured production rows, got {len(_CAPTURES)}. "
        "Fixture drift breaks the Phase 1 corpus."
    )


@pytest.mark.parametrize(
    "capture", _CAPTURES, ids=lambda c: c.get("namespaced_qid", "?")
)
def test_normalize_produces_canonical_row(capture: dict) -> None:
    """Every captured production row normalizes into a canonical shape
    whose four string fields are non-empty.

    This is the architectural commitment of Phase 8: the four hard
    Stage-1 fields are guaranteed populated at admission. Any future
    row-shape regression must surface here, not at the Stage 1
    pre-flight contract.
    """
    raw = capture["row"]
    canonical = normalize_eval_row(raw)
    assert isinstance(canonical, CanonicalEvalRow), type(canonical)
    assert canonical.qid, f"qid empty for {capture['namespaced_qid']!r}"
    assert canonical.namespaced_qid == capture["namespaced_qid"]
    assert canonical.question_text, (
        f"question_text empty for {capture['namespaced_qid']!r} — "
        f"Phase 8 normalizer must absorb every production row shape."
    )
    assert canonical.ground_truth_sql, (
        f"ground_truth_sql empty for {capture['namespaced_qid']!r}"
    )
    assert canonical.generated_sql, (
        f"generated_sql empty for {capture['namespaced_qid']!r}"
    )


@pytest.mark.parametrize(
    "capture", _CAPTURES, ids=lambda c: c.get("namespaced_qid", "?")
)
def test_qid_namespace_split(capture: dict) -> None:
    canonical = normalize_eval_row(capture["row"])
    assert canonical.namespaced_qid == capture["namespaced_qid"]
    assert canonical.namespaced_qid.endswith(canonical.qid), (
        f"namespaced_qid {canonical.namespaced_qid!r} should end with "
        f"canonical qid {canonical.qid!r}"
    )
    assert canonical.qid.startswith("gs_"), canonical.qid


@pytest.mark.parametrize(
    "capture", _CAPTURES, ids=lambda c: c.get("namespaced_qid", "?")
)
def test_judge_rationales_populated_from_per_judge_keys(
    capture: dict,
) -> None:
    canonical = normalize_eval_row(capture["row"])
    assert isinstance(canonical.judge_rationales, JudgeRationales)
    assert canonical.judge_rationales.by_judge, (
        f"by_judge empty for {capture['namespaced_qid']!r}; production "
        f"rows publish <judge>/rationale keys."
    )
    primary = canonical.judge_rationales.primary()
    assert primary, "primary() rationale must be non-empty"


@pytest.mark.parametrize(
    "capture", _CAPTURES, ids=lambda c: c.get("namespaced_qid", "?")
)
def test_asi_metadata_populated_from_flat_keys(capture: dict) -> None:
    canonical = normalize_eval_row(capture["row"])
    assert isinstance(canonical.asi_metadata, AsiMetadata)
    assert canonical.asi_metadata.blame_set, (
        f"blame_set empty for {capture['namespaced_qid']!r} — "
        f"production rows carry metadata/<judge>/blame_set."
    )


def test_normalize_tolerates_missing_judges() -> None:
    """A row missing every judge rationale still normalizes without
    raising; ``by_judge`` is simply empty."""
    bare = {"client_request_id": "abc", "request": {"question": "Q?"}}
    canonical = normalize_eval_row(bare)
    assert canonical.question_text == "Q?"
    assert canonical.judge_rationales.by_judge == {}
    assert canonical.judge_rationales.primary() == ""
    assert canonical.asi_metadata.blame_set == []


def test_normalize_preserves_raw_passthrough() -> None:
    bare = {"sentinel": "value", "request": {"question": "Q?"}}
    canonical = normalize_eval_row(bare)
    assert canonical.raw["sentinel"] == "value"


def test_canonical_eval_row_is_frozen() -> None:
    bare = {"request": {"question": "Q?"}}
    canonical = normalize_eval_row(bare)
    with pytest.raises((AttributeError, Exception)):
        canonical.qid = "mutated"  # type: ignore[misc]
