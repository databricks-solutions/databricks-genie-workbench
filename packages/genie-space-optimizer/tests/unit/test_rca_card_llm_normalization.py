from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rca import RCACard, RcaKind
from genie_space_optimizer.optimization.rca_card_llm import (
    NormalizationOutcome,
    normalize_card_rationale,
)


def _card(rationale: str = "deterministic rationale") -> RCACard:
    return RCACard(
        card_id="card_x",
        cluster_id="c1",
        qids=("gs_026",),
        root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        grounding_terms=frozenset({"plural_top_n_collapse"}),
        intended_patch_shape="enforce_explicit_top_n_cardinality",
        allowed_patch_families=frozenset({"cardinality_preserving_top_n_guidance"}),
        forbidden_patch_families=frozenset({"avoid_unrequested_defensive_filters"}),
        rationale=rationale,
    )


def test_normalizer_returns_polished_card_on_success() -> None:
    def fake_caller(prompt: str) -> str:
        return "Top-N collapse on plural zone_vp_name; require ORDER BY+LIMIT."

    outcome = normalize_card_rationale(card=_card(), llm_caller=fake_caller)
    assert outcome.skipped is False
    assert outcome.skip_reason is None
    assert outcome.card.rationale == "Top-N collapse on plural zone_vp_name; require ORDER BY+LIMIT."
    # All other fields preserved.
    assert outcome.card.root_cause == RcaKind.TOP_N_CARDINALITY_COLLAPSE
    assert outcome.card.grounding_terms == frozenset({"plural_top_n_collapse"})


def test_normalizer_skips_on_caller_exception() -> None:
    def raising_caller(prompt: str) -> str:
        raise TimeoutError("LLM took too long")

    outcome = normalize_card_rationale(card=_card("det"), llm_caller=raising_caller)
    assert outcome.skipped is True
    assert outcome.skip_reason == "llm_call_failed"
    assert outcome.card.rationale == "det"  # deterministic rationale preserved


def test_normalizer_skips_on_empty_response() -> None:
    def empty_caller(prompt: str) -> str:
        return ""

    outcome = normalize_card_rationale(card=_card("det"), llm_caller=empty_caller)
    assert outcome.skipped is True
    assert outcome.skip_reason == "empty_response"
    assert outcome.card.rationale == "det"


def test_normalizer_skips_when_response_too_long() -> None:
    def long_caller(prompt: str) -> str:
        return "x" * 5000  # exceeds max_chars guard

    outcome = normalize_card_rationale(card=_card("det"), llm_caller=long_caller, max_chars=2000)
    assert outcome.skipped is True
    assert outcome.skip_reason == "response_too_long"
    assert outcome.card.rationale == "det"


def test_normalizer_uses_default_max_chars() -> None:
    long_response = "x" * 1500
    def caller(prompt: str) -> str:
        return long_response
    outcome = normalize_card_rationale(card=_card("det"), llm_caller=caller)
    # Default max_chars is 1024, so 1500 is too long.
    assert outcome.skipped is True
    assert outcome.skip_reason == "response_too_long"
