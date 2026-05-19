"""Plan 7 Task 6 — deterministic post-LLM validators for Plan 7."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)
from genie_space_optimizer.optimization.rollback_learning import (
    _validate_forbidden_signatures_subset_of_applied,
    _validate_revised_blame_set_subset_of_allowlist,
    _validate_revised_patch_type_in_closed_enum,
)


def _build(
    *,
    revised_blame_set=("sales.fact_sales.revenue",),
    forbidden_signatures=("fp_abc",),
    revised_patch_type=PatchType.ADD_EXAMPLE_SQL,
) -> NextAttemptHypothesis:
    return NextAttemptHypothesis(
        rolled_back_intent_id="i_001",
        cluster_id="H001",
        ag_id="AG3",
        iteration=2,
        why_failed="x",
        failure_mode="x",
        revised_repair_shape=RepairShape.TOP_N_BY_METRIC,
        revised_patch_type=revised_patch_type,
        revised_blame_set=revised_blame_set,
        additional_evidence_needed=(),
        forbidden_signatures=forbidden_signatures,
        confidence="high",
    )


def test_blame_set_passes_when_every_entry_in_allowlist() -> None:
    h = _build(revised_blame_set=("sales.fact_sales.revenue",))
    _validate_revised_blame_set_subset_of_allowlist(
        h,
        identifier_allowlist={
            "sales.fact_sales.revenue",
            "sales.fact_sales.region",
        },
    )


def test_blame_set_passes_when_none() -> None:
    h = _build(revised_blame_set=None)
    _validate_revised_blame_set_subset_of_allowlist(
        h, identifier_allowlist={"sales.fact_sales.revenue"},
    )


def test_blame_set_rejects_unknown_identifier() -> None:
    h = _build(revised_blame_set=("sales.fact_sales.evil_column",))
    with pytest.raises(ValueError, match="revised_blame_set entries outside"):
        _validate_revised_blame_set_subset_of_allowlist(
            h, identifier_allowlist={"sales.fact_sales.revenue"},
        )


def test_blame_set_is_case_sensitive() -> None:
    h = _build(revised_blame_set=("sales.fact_sales.Revenue",))
    with pytest.raises(ValueError):
        _validate_revised_blame_set_subset_of_allowlist(
            h, identifier_allowlist={"sales.fact_sales.revenue"},
        )


def test_forbidden_signatures_passes_when_every_entry_in_applied() -> None:
    h = _build(forbidden_signatures=("fp_abc",))
    _validate_forbidden_signatures_subset_of_applied(
        h, applied_patch_fingerprints={"fp_abc", "fp_def"},
    )


def test_forbidden_signatures_passes_when_empty() -> None:
    h = _build(forbidden_signatures=())
    _validate_forbidden_signatures_subset_of_applied(
        h, applied_patch_fingerprints=set(),
    )


def test_forbidden_signatures_rejects_unknown_fingerprint() -> None:
    h = _build(forbidden_signatures=("fp_hallucinated",))
    with pytest.raises(
        ValueError, match="forbidden_signatures entries outside",
    ):
        _validate_forbidden_signatures_subset_of_applied(
            h, applied_patch_fingerprints={"fp_abc"},
        )


def test_forbidden_signatures_rejects_when_only_subset_unknown() -> None:
    h = _build(forbidden_signatures=("fp_abc", "fp_hallucinated"))
    with pytest.raises(ValueError):
        _validate_forbidden_signatures_subset_of_applied(
            h, applied_patch_fingerprints={"fp_abc"},
        )


def test_revised_patch_type_passes_for_closed_enum_member() -> None:
    h = _build(revised_patch_type=PatchType.ADD_EXAMPLE_SQL)
    _validate_revised_patch_type_in_closed_enum(h)


def test_revised_patch_type_passes_when_none() -> None:
    h = _build(revised_patch_type=None)
    _validate_revised_patch_type_in_closed_enum(h)
