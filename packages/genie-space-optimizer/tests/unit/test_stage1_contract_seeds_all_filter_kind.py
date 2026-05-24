"""Trial 14 — :class:`Stage1InputEvidenceContract` distinguishes
``seeds_all_filter_kind`` from the legacy / Trial 13k tags.

Three-way classification matrix this test pins:

+-------------------------+--------------+----------------------------+
| structured payload      | pre_normalize| violation tag              |
+=========================+==============+============================+
| non-empty, all          | (any)        | ``seeds_all_filter_kind``  |
| filter/instruction      |              | (Trial 14)                 |
+-------------------------+--------------+----------------------------+
| empty/None              | > 0          | ``seeds_unnormalizable``   |
|                         |              | (Trial 13k)                |
+-------------------------+--------------+----------------------------+
| empty/None              | 0            | ``blame_set_empty``        |
|                         |              | (Trial 11 legacy)          |
+-------------------------+--------------+----------------------------+
| non-empty with a        | (any)        | NO violation               |
| column/table/join ref   |              | (happy path)               |
+-------------------------+--------------+----------------------------+

Tag order matters — ``seeds_all_filter_kind`` MUST take priority
over ``seeds_unnormalizable`` because it points postmortems at the
correct upstream signal (judge identified behaviour blame, not a
schema_columns coverage gap).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
    DEFAULT_STAGE1_CONTRACT,
)


_VALID_NARRATIVE = {
    "question_text": "What is total revenue?",
    "ground_truth_sql": "SELECT SUM(revenue) FROM sales",
    "generated_sql": "SELECT AVG(revenue) FROM sales",
    "judge_rationale": "Aggregation differs",
    "rca_evidence": {"observed_failure": "wrong_aggregation"},
}


def _card(**overrides: object) -> dict:
    base = dict(_VALID_NARRATIVE)
    base.update(overrides)
    return base


# ── seeds_all_filter_kind — Trial 14 canary ─────────────────────────


def test_all_filter_kind_emits_seeds_all_filter_kind() -> None:
    card = _card(
        blame_set_seed=[],
        _blame_structured=[
            {"kind": "filter", "ref": None, "description": "x = 1"},
        ],
        _seed_normalization={
            "seeds_pre_normalize": 0,
            "seeds_post_normalize": 0,
            "seeds_normalized": 0,
            "seeds_dropped": 0,
        },
    )
    violations = DEFAULT_STAGE1_CONTRACT.validate(card)
    tags = [v.field for v in violations]
    assert "seeds_all_filter_kind" in tags
    assert "seeds_unnormalizable" not in tags
    assert "blame_set_empty" not in tags
    fired = next(v for v in violations if v.field == "seeds_all_filter_kind")
    assert fired.value["blame_kind_distribution"] == {"filter": 1}


def test_all_instruction_kind_also_emits_seeds_all_filter_kind() -> None:
    """The tag name is shorthand; both ``filter`` and ``instruction``
    are non-schema-resolvable and trigger the same arm."""
    card = _card(
        blame_set_seed=[],
        _blame_structured=[
            {"kind": "instruction", "ref": None, "description": "prefer mv"},
            {"kind": "filter", "ref": None, "description": "y > 0"},
        ],
    )
    violations = DEFAULT_STAGE1_CONTRACT.validate(card)
    tags = [v.field for v in violations]
    assert "seeds_all_filter_kind" in tags
    fired = next(v for v in violations if v.field == "seeds_all_filter_kind")
    assert fired.value["blame_kind_distribution"] == {"instruction": 1, "filter": 1}


# ── seeds_unnormalizable — Trial 13k (legacy fallback) ──────────────


def test_legacy_dropped_seeds_still_emits_seeds_unnormalizable() -> None:
    """No structured payload → legacy Trial 13k arm fires."""
    card = _card(
        blame_set_seed=[],
        _seed_normalization={
            "seeds_pre_normalize": 3,
            "seeds_post_normalize": 0,
            "seeds_normalized": 0,
            "seeds_dropped": 3,
        },
    )
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "seeds_unnormalizable" in tags
    assert "seeds_all_filter_kind" not in tags


# ── blame_set_empty — Trial 11 legacy ───────────────────────────────


def test_no_seeds_anywhere_emits_blame_set_empty() -> None:
    card = _card(
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
    assert "seeds_all_filter_kind" not in tags


# ── happy path ──────────────────────────────────────────────────────


def test_schema_kind_present_does_not_fire_any_blame_violation() -> None:
    """Structured payload contains a column entry → seeds_seed
    is populated → no violation fires."""
    card = _card(
        blame_set_seed=["main.airline.fact.dest_col"],
        _blame_structured=[
            {"kind": "column", "ref": "main.airline.fact.dest_col"},
            {"kind": "filter", "ref": None, "description": "x = 1"},
        ],
        _seed_normalization={
            "seeds_pre_normalize": 1,
            "seeds_post_normalize": 1,
            "seeds_normalized": 0,
            "seeds_dropped": 0,
        },
    )
    tags = [v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card)]
    assert "seeds_all_filter_kind" not in tags
    assert "seeds_unnormalizable" not in tags
    assert "blame_set_empty" not in tags
