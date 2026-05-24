"""Trial 13k — ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1`` carries a derived
``seed_normalization_verdict`` field with a closed vocabulary so
postmortems and CI canaries can grep a single field instead of
performing ``seeds_pre_normalize`` vs ``seeds_post_normalize``
arithmetic.

Vocabulary (mirrors ``STAGE1_SEED_NORMALIZATION_VERDICTS``):

  * ``"no_seeds"``     — ``pre == 0`` (judge silent).
  * ``"all_dropped"``  — ``pre > 0 and post == 0`` (Trial 13k canary).
  * ``"partial_drop"`` — ``pre > post > 0`` (drift warning).
  * ``"ok"``           — ``post == pre`` and ``pre > 0`` (healthy).
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    STAGE1_SEED_NORMALIZATION_VERDICTS,
    _seed_normalization_verdict,
    plan11_stage1_input_quality_marker,
)


def _payload(line: str) -> dict:
    return json.loads(line.split(" ", 1)[1])


def test_verdict_vocabulary_is_frozen() -> None:
    assert STAGE1_SEED_NORMALIZATION_VERDICTS == frozenset(
        {"no_seeds", "all_dropped", "partial_drop", "ok"}
    )


@pytest.mark.parametrize(
    "pre, post, expected",
    [
        (0, 0, "no_seeds"),
        (0, 5, "no_seeds"),       # nonsensical but defensive
        (3, 0, "all_dropped"),    # Trial 13k headline case
        (5, 2, "partial_drop"),
        (4, 4, "ok"),
        (1, 1, "ok"),
    ],
)
def test_verdict_helper_maps_counts(pre: int, post: int, expected: str) -> None:
    verdict = _seed_normalization_verdict(pre, post)
    assert verdict == expected
    assert verdict in STAGE1_SEED_NORMALIZATION_VERDICTS


def test_marker_carries_verdict_field() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_004",
        schema_columns_source="metadata_snapshot",
        schema_columns_size=98,
        seeds_pre_normalize=3,
        seeds_post_normalize=0,
        seeds_normalized=0,
        seeds_dropped=3,
    )
    payload = _payload(line)
    assert payload["seed_normalization_verdict"] == "all_dropped"
    assert (
        payload["seed_normalization_verdict"]
        in STAGE1_SEED_NORMALIZATION_VERDICTS
    )


def test_marker_verdict_round_trips_partial_drop() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=2,
        qid="gs_026",
        schema_columns_source="metadata_snapshot",
        schema_columns_size=98,
        seeds_pre_normalize=2,
        seeds_post_normalize=1,
        seeds_normalized=1,
        seeds_dropped=1,
    )
    payload = _payload(line)
    assert payload["seed_normalization_verdict"] == "partial_drop"
    assert payload["seeds_pre_normalize"] == 2
    assert payload["seeds_post_normalize"] == 1


def test_marker_verdict_defaults_to_no_seeds() -> None:
    """Minimal-field call (no seed stats) renders ``"no_seeds"`` — the
    Trial 13i pre-flight abstain canary case."""
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_minimal",
        schema_columns_source="empty",
        schema_columns_size=0,
    )
    payload = _payload(line)
    assert payload["seed_normalization_verdict"] == "no_seeds"
