"""Plan 7 Task 11 — apply_forbidden_signatures_to_rollback_fingerprints."""
from __future__ import annotations

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)
from genie_space_optimizer.optimization.rollback_learning import (
    apply_forbidden_signatures_to_rollback_fingerprints,
)


def _hypothesis(
    cluster_id: str,
    forbidden_signatures: tuple[str, ...] = (),
) -> NextAttemptHypothesis:
    return NextAttemptHypothesis(
        rolled_back_intent_id=f"i_{cluster_id}",
        cluster_id=cluster_id, ag_id="AG3", iteration=2,
        why_failed="x", failure_mode="x",
        revised_repair_shape=RepairShape.TOP_N_BY_METRIC,
        revised_patch_type=PatchType.ADD_EXAMPLE_SQL,
        revised_blame_set=None,
        additional_evidence_needed=(),
        forbidden_signatures=forbidden_signatures,
        confidence="high",
    )


def test_unions_forbidden_signatures_into_existing_set() -> None:
    existing = {"sig_existing_1", "sig_existing_2"}
    hypotheses = {
        "H001": _hypothesis("H001", forbidden_signatures=("sig_llm_001",)),
    }
    result = apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=existing, hypotheses_by_cluster_id=hypotheses,
    )
    assert result == {"sig_existing_1", "sig_existing_2", "sig_llm_001"}


def test_returns_a_new_set_does_not_mutate_input() -> None:
    existing = {"sig_existing"}
    hypotheses = {
        "H001": _hypothesis("H001", forbidden_signatures=("sig_llm",)),
    }
    result = apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=existing, hypotheses_by_cluster_id=hypotheses,
    )
    assert existing == {"sig_existing"}
    assert result == {"sig_existing", "sig_llm"}


def test_handles_empty_hypotheses_dict() -> None:
    existing = {"sig_a", "sig_b"}
    result = apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=existing, hypotheses_by_cluster_id={},
    )
    assert result == {"sig_a", "sig_b"}


def test_handles_empty_prior_set() -> None:
    hypotheses = {
        "H001": _hypothesis("H001", forbidden_signatures=("sig_a", "sig_b")),
    }
    result = apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=set(), hypotheses_by_cluster_id=hypotheses,
    )
    assert result == {"sig_a", "sig_b"}


def test_deduplicates_across_hypotheses_and_prior() -> None:
    existing = {"sig_duplicate"}
    hypotheses = {
        "H001": _hypothesis("H001", forbidden_signatures=("sig_duplicate",)),
        "H002": _hypothesis("H002", forbidden_signatures=("sig_new",)),
    }
    result = apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=existing, hypotheses_by_cluster_id=hypotheses,
    )
    assert result == {"sig_duplicate", "sig_new"}


def test_handles_hypothesis_with_empty_forbidden_signatures() -> None:
    existing = {"sig_a"}
    hypotheses = {
        "H001": _hypothesis("H001", forbidden_signatures=()),
    }
    result = apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=existing, hypotheses_by_cluster_id=hypotheses,
    )
    assert result == {"sig_a"}
