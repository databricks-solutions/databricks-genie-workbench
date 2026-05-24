"""Trial 13i — end-to-end Stage 1 integration with the FQN seed normalizer.

This is the test that would have failed on capture-only bundles before
Trial 13i landed: a row with free-text ASI seed tokens
(``DEST_AIRPORT_CD`` etc.) and a non-empty ``schema_columns`` universe
containing the FQN equivalents. The post-13i flow:

  1. ``build_stage1_evidence_card`` reads the ASI free-text seed list.
  2. The seed normalizer swaps each bare token for its 4-part FQN by
     case-insensitive suffix match.
  3. The Stage 1 contract sees a schema-valid ``blame_set_seed``.
  4. ``diagnose_failing_qids`` receives a non-empty ``schema_columns``.
  5. Trial 13h's seed-backfill rescues if the LLM still emits empty
     ``blame_set``; otherwise the LLM's schema-valid entries survive.

The marker (``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1``) MUST report
``seeds_normalized > 0`` so postmortems can detect the seed-drift mode
in production.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.eval_row_access import (
    build_stage1_evidence_card,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)


_SCHEMA = (
    "main.airline.fact_flights.dest_airport_cd",
    "main.airline.fact_flights.orig_airport_cd",
    "main.airline.dim_carriers.carrier_cd",
)


def _capture_only_row() -> dict:
    """Build a row whose ASI surface carries the free-text seed tokens
    observed in the 98ec/dc89 capture bundles (gs_009/gs_026)."""
    return {
        "inputs/question_id": "gs_capture",
        "inputs/question": "What is the top destination airport?",
        "expected_response/value": (
            "SELECT dest_airport_cd FROM main.airline.fact_flights "
            "ORDER BY revenue DESC LIMIT 1"
        ),
        "response": "SELECT dest_airport_cd FROM fact_flights",
        # Production flat-key ASI surface.
        "metadata/arbiter/blame_set": (
            "[DEST_AIRPORT_CD, ORIG_AIRPORT_CD, LIMIT 10 vs RANK() <= 10]"
        ),
        "metadata/arbiter/failure_type": "top_n_cardinality_collapse",
        "metadata/arbiter/wrong_clause": "missing ORDER BY",
        "metadata/arbiter/counterfactual_fix": "Add ORDER BY revenue DESC LIMIT N",
        "metadata/arbiter/patch_family": "top_n",
    }


def test_seed_normalizer_swaps_bare_identifiers_in_built_card() -> None:
    """The card-build path must call the normalizer when
    ``schema_columns`` is supplied, swapping bare tokens for FQNs."""
    card = build_stage1_evidence_card(
        "gs_capture",
        _capture_only_row(),
        typed_evidence=None,
        schema_columns=_SCHEMA,
    )
    # The bare tokens resolved to FQNs; the compound token dropped.
    assert "main.airline.fact_flights.dest_airport_cd" in card["blame_set_seed"]
    assert "main.airline.fact_flights.orig_airport_cd" in card["blame_set_seed"]
    assert "LIMIT 10 vs RANK() <= 10" not in card["blame_set_seed"]

    stats = card["_seed_normalization"]
    assert stats["seeds_pre_normalize"] == 3
    assert stats["seeds_post_normalize"] == 2
    assert stats["seeds_normalized"] == 2
    assert stats["seeds_dropped"] == 1


def test_seed_normalizer_no_op_when_schema_columns_omitted() -> None:
    """Legacy call sites (no ``schema_columns``) get the pre-13i shape
    back: raw ASI tokens pass through unchanged with zero counts."""
    card = build_stage1_evidence_card(
        "gs_capture",
        _capture_only_row(),
        typed_evidence=None,
    )
    # All three tokens (including compound text) preserved as-is.
    assert "DEST_AIRPORT_CD" in card["blame_set_seed"]
    assert "ORIG_AIRPORT_CD" in card["blame_set_seed"]
    assert "LIMIT 10 vs RANK() <= 10" in card["blame_set_seed"]
    stats = card["_seed_normalization"]
    assert stats["seeds_pre_normalize"] == 3
    assert stats["seeds_post_normalize"] == 3
    assert stats["seeds_normalized"] == 0
    assert stats["seeds_dropped"] == 0


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
def test_normalized_seeds_let_stage1_diagnose_reach_actionable_outcome(
    MockLlmCall,
) -> None:
    """The full Trial 13i loop: capture-style seeds + non-empty
    schema_columns -> diagnose_failing_qids produces a per-QID diagnosis
    whose blame_set is non-empty and schema-grounded."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    # The LLM emits an empty blame_set, forcing Trial 13h's seed-backfill
    # to kick in. The seed was just normalized by Trial 13i, so the
    # backfill source is the FQN swap.
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=LlmReasoningResponse(
            call_id="plan11_stage1_diagnose.iter_1",
            skill_id="plan11_diagnose",
            succeeded=True,
            parsed_output={
                "diagnoses": [
                    {
                        "qid": "gs_capture",
                        "rca_kind_label": "top-N collapsed",
                        "observed_failure": "Wrong rows returned",
                        "generated_sql_issue": "Missing ORDER BY + LIMIT",
                        "expected_sql_shape": "ORDER BY revenue DESC LIMIT 10",
                        "blame_set": [],
                        "evidence_summary": "Top-N pattern not honored",
                        "confidence": "high",
                    }
                ]
            },
            declined=None,
            raw_text="{...}",
            tokens_input=100,
            tokens_output=50,
            duration_ms=1234,
            error=None,
        )
    )

    card = build_stage1_evidence_card(
        "gs_capture",
        _capture_only_row(),
        typed_evidence=None,
        schema_columns=_SCHEMA,
    )
    diagnoses = diagnose_failing_qids(
        failing_qids=[card],
        schema_columns=list(_SCHEMA),
        optimization_run_id="trial13i-int",
        iteration=1,
        w=MagicMock(),
    )

    assert len(diagnoses) == 1
    diag = diagnoses[0]
    assert diag.qid == "gs_capture"
    # The seed (now FQNs after normalization) rescued the LLM's empty
    # blame_set via Trial 13h's backfill.
    assert diag.blame_set
    for b in diag.blame_set:
        assert b in _SCHEMA, (
            f"Expected backfilled blame_set entries to be schema-valid; "
            f"got {b!r} not in {_SCHEMA}"
        )
