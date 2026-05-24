"""Trial 13k — :class:`Stage1InputEvidenceContract` distinguishes
"ASI judge emitted nothing" from "ASI judge emitted seeds but none
normalized to an FQN".

Before Trial 13k the contract reported a single generic
``blame_set_empty`` violation whenever ``card["blame_set_seed"]`` was
empty. Postmortems on the capture lane had to compute
``seeds_pre_normalize`` vs ``seeds_post_normalize`` arithmetic out
of the marker payload to decide whether the bottleneck was upstream
(judge silent) or downstream (judge emitted free text the FQN
normalizer rejected).

Trial 13k adds a more specific ``seeds_unnormalizable`` tag that
replaces ``blame_set_empty`` when the resolved blame set is empty
but ``_seed_normalization.seeds_pre_normalize > 0`` — i.e. seeds
existed but every one was dropped by the normalizer. The two tags
are mutually exclusive so the rendered
:meth:`Stage1InputCardEmptyError.as_declined_reason` stays a single
colon-joined string.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
    DEFAULT_STAGE1_CONTRACT,
    Stage1InputCardEmptyError,
)


def _base_card(**overrides):
    card = {
        "qid": "gs_004",
        "question_text": "What is the total revenue?",
        "ground_truth_sql": "SELECT 1",
        "generated_sql": "SELECT 2",
        "judge_rationale": "judge says it's wrong",
        "blame_set_seed": [],
        "rca_evidence": {
            "observed_failure": "wrong filter",
            "generated_sql_issue": "",
            "expected_sql_shape": "",
            "suggested_repair_family": "",
        },
    }
    card.update(overrides)
    return card


def test_emits_seeds_unnormalizable_when_pre_positive_and_post_zero() -> None:
    """The Trial 13k headline case: capture-lane card where the parser
    delivered ASI seeds but the FQN normalizer dropped every one."""
    card = _base_card(
        blame_set_seed=[],
        _seed_normalization={
            "seeds_pre_normalize": 3,
            "seeds_post_normalize": 0,
            "seeds_normalized": 0,
            "seeds_dropped": 3,
        },
    )

    violations = DEFAULT_STAGE1_CONTRACT.validate(card)
    tags = [v.field for v in violations]

    assert "seeds_unnormalizable" in tags
    assert "blame_set_empty" not in tags  # mutually exclusive

    err = Stage1InputCardEmptyError(
        [v for v in violations if v.field == "seeds_unnormalizable"]
    )
    assert err.as_declined_reason() == "evidence_card_empty:seeds_unnormalizable"

    seeds_violation = next(
        v for v in violations if v.field == "seeds_unnormalizable"
    )
    assert seeds_violation.value == {
        "seeds_pre_normalize": 3,
        "seeds_dropped": 3,
    }


def test_emits_blame_set_empty_when_no_seed_normalization_stats() -> None:
    """Back-compat: legacy cards without ``_seed_normalization`` keep the
    generic ``blame_set_empty`` declined reason. The contract must not
    crash when the stats dict is missing."""
    card = _base_card(blame_set_seed=[])
    # No ``_seed_normalization`` key at all.
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "blame_set_empty" in tags
    assert "seeds_unnormalizable" not in tags


def test_emits_blame_set_empty_when_pre_zero_and_post_zero() -> None:
    """ASI judge was silent — no seeds were ever emitted. The generic
    ``blame_set_empty`` tag is the correct verdict; ``seeds_unnormalizable``
    would be misleading (nothing was normalized away)."""
    card = _base_card(
        blame_set_seed=[],
        _seed_normalization={
            "seeds_pre_normalize": 0,
            "seeds_post_normalize": 0,
            "seeds_normalized": 0,
            "seeds_dropped": 0,
        },
    )
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "blame_set_empty" in tags
    assert "seeds_unnormalizable" not in tags


def test_no_violation_when_blame_set_populated_regardless_of_stats() -> None:
    """When the resolved blame set is non-empty neither tag fires, even
    if the ``_seed_normalization`` stats say seeds were dropped (e.g.
    partial-drop case where some seeds survived)."""
    card = _base_card(
        blame_set_seed=["catalog.schema.table.column"],
        _seed_normalization={
            "seeds_pre_normalize": 4,
            "seeds_post_normalize": 1,
            "seeds_normalized": 1,
            "seeds_dropped": 3,
        },
    )
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "blame_set_empty" not in tags
    assert "seeds_unnormalizable" not in tags


def test_handles_malformed_seed_normalization_stats() -> None:
    """Defensive: a corrupt ``_seed_normalization`` payload (e.g. non-int
    ``seeds_pre_normalize``) must not propagate as a ``ValueError``;
    the contract falls back to ``blame_set_empty``."""
    card = _base_card(
        blame_set_seed=[],
        _seed_normalization={"seeds_pre_normalize": "garbage"},
    )
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "blame_set_empty" in tags
    assert "seeds_unnormalizable" not in tags
