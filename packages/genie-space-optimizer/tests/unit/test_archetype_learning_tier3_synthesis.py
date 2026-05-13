"""Tests for Section E Tier 3 provisional-archetype synthesis."""

from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.archetype_learning import (
    PatternCandidate,
    ProvisionalArchetype,
)


def _candidate() -> PatternCandidate:
    return PatternCandidate(
        signature_hash="sigA",
        member_cluster_ids=("C1", "C2", "C3"),
        union_qids=("gs_1", "gs_2", "gs_3"),
        root_cause_label="SYNONYM_OR_ENTITY_MATCH_MISSING",
        grounding_terms=frozenset({"snack_brand", "beverage_brand"}),
        intended_patch_shape="entity_disambiguation",
        asi_question_intent="single",
        member_count=3,
    )


def test_synthesize_returns_provisional_archetype_when_llm_returns_valid_payload(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ARCHETYPE_LEARNING", "1")
    monkeypatch.setenv("GSO_PROVISIONAL_SYNTHESIS_LLM", "1")
    from genie_space_optimizer.optimization import archetype_learning as al
    from genie_space_optimizer.optimization.archetype_learning_state import reset_state

    reset_state("run_T3")

    fake_payload = {
        "name": "brand_dimension_disambiguation_provisional",
        "applicable_rca_kinds": ["SYNONYM_OR_ENTITY_MATCH_MISSING"],
        "required_grounding_tokens": ["snack_brand", "beverage_brand"],
        "evidence_predicates": [],
        "default_priority_step": "repair_kit",
        "expected_causal_effect_template": (
            "Add column-level synonyms for brand-axis dimensions; "
            "rewrites WHERE clauses to use canonical column."
        ),
        "rationale": "synthesised from sigA",
    }
    with patch.object(
        al, "_call_llm_for_provisional_archetype_synthesis",
        return_value=fake_payload,
    ):
        out = al.synthesize_provisional_archetype(
            run_id="run_T3", candidate=_candidate(), iteration=2,
        )

    assert isinstance(out, ProvisionalArchetype)
    assert out.signature_hash == "sigA"
    assert out.synthesis_iteration == 2
    assert out.provenance == "provisional_archetype"
    assert out.lifecycle_state == "provisional"


def test_synthesize_returns_none_when_llm_returns_none(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ARCHETYPE_LEARNING", "1")
    monkeypatch.setenv("GSO_PROVISIONAL_SYNTHESIS_LLM", "1")
    from genie_space_optimizer.optimization import archetype_learning as al
    from genie_space_optimizer.optimization.archetype_learning_state import reset_state

    reset_state("run_T3b")
    with patch.object(
        al, "_call_llm_for_provisional_archetype_synthesis", return_value=None,
    ):
        out = al.synthesize_provisional_archetype(
            run_id="run_T3b", candidate=_candidate(), iteration=2,
        )
    assert out is None


def test_synthesize_skips_when_master_flag_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_ARCHETYPE_LEARNING", raising=False)
    from genie_space_optimizer.optimization import archetype_learning as al
    from genie_space_optimizer.optimization.archetype_learning_state import reset_state

    reset_state("run_T3c")
    out = al.synthesize_provisional_archetype(
        run_id="run_T3c", candidate=_candidate(), iteration=1,
    )
    assert out is None


def test_synthesize_skips_when_llm_subflag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ARCHETYPE_LEARNING", "1")
    monkeypatch.delenv("GSO_PROVISIONAL_SYNTHESIS_LLM", raising=False)
    from genie_space_optimizer.optimization import archetype_learning as al
    from genie_space_optimizer.optimization.archetype_learning_state import reset_state

    reset_state("run_T3d")
    out = al.synthesize_provisional_archetype(
        run_id="run_T3d", candidate=_candidate(), iteration=1,
    )
    assert out is None


def test_synthesize_honours_per_iteration_cap(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ARCHETYPE_LEARNING", "1")
    monkeypatch.setenv("GSO_PROVISIONAL_SYNTHESIS_LLM", "1")
    monkeypatch.setenv("GSO_PROVISIONAL_SYNTHESIS_MAX_PER_ITERATION", "1")
    from genie_space_optimizer.optimization import archetype_learning as al
    from genie_space_optimizer.optimization.archetype_learning_state import (
        get_state,
        reset_state,
    )

    reset_state("run_T3e")
    fake_payload = {
        "name": "x_provisional",
        "applicable_rca_kinds": ["SYNONYM_OR_ENTITY_MATCH_MISSING"],
        "required_grounding_tokens": ["snack_brand"],
        "evidence_predicates": [],
        "default_priority_step": "repair_kit",
        "expected_causal_effect_template": "x",
        "rationale": "x",
    }
    with patch.object(
        al, "_call_llm_for_provisional_archetype_synthesis",
        return_value=fake_payload,
    ):
        first = al.synthesize_provisional_archetype(
            run_id="run_T3e", candidate=_candidate(), iteration=1,
        )
        second = al.synthesize_provisional_archetype(
            run_id="run_T3e", candidate=_candidate(), iteration=1,
        )
    assert first is not None
    assert second is None  # cap reached
    assert get_state("run_T3e").synthesis_calls_this_iteration == 1


def test_call_llm_helper_invokes_traced_llm_call_and_parses_payload(monkeypatch) -> None:
    """The Tier 3 helper must delegate to optimizer._traced_llm_call,
    parse the response via evaluation._extract_json, and return the
    parsed dict when it carries all required keys."""
    from unittest.mock import MagicMock, patch

    from genie_space_optimizer.optimization import archetype_learning as al

    fake_payload = {
        "name": "x_provisional",
        "applicable_rca_kinds": ["SYNONYM_OR_ENTITY_MATCH_MISSING"],
        "required_grounding_tokens": ["snack_brand"],
        "evidence_predicates": [],
        "default_priority_step": "repair_kit",
        "expected_causal_effect_template": "x",
        "rationale": "x",
    }
    import json as _json
    raw_text = _json.dumps(fake_payload)

    def _fake_traced_llm_call(*args, **kwargs):
        return raw_text, MagicMock()

    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        side_effect=_fake_traced_llm_call,
    ):
        out = al._call_llm_for_provisional_archetype_synthesis(
            candidate=_candidate(),
            counterfactual_examples=(),
            w=None,
        )
    assert out == fake_payload


def test_call_llm_helper_returns_none_when_llm_declines() -> None:
    """A declined response (``{"declined": true}``) translates to None."""
    from unittest.mock import MagicMock, patch

    from genie_space_optimizer.optimization import archetype_learning as al

    def _fake_traced_llm_call(*args, **kwargs):
        return '{"declined": true}', MagicMock()

    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        side_effect=_fake_traced_llm_call,
    ):
        out = al._call_llm_for_provisional_archetype_synthesis(
            candidate=_candidate(),
        )
    assert out is None


def test_call_llm_helper_returns_none_when_payload_missing_required_keys() -> None:
    """Payloads missing any of the seven required keys return None so
    Tier 3 surfaces a synthesis_declined record rather than building a
    half-formed provisional."""
    from unittest.mock import MagicMock, patch

    from genie_space_optimizer.optimization import archetype_learning as al

    incomplete_payload = '{"name": "x", "rationale": "missing keys"}'

    def _fake_traced_llm_call(*args, **kwargs):
        return incomplete_payload, MagicMock()

    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        side_effect=_fake_traced_llm_call,
    ):
        out = al._call_llm_for_provisional_archetype_synthesis(
            candidate=_candidate(),
        )
    assert out is None


def test_call_llm_helper_returns_none_on_llm_exception() -> None:
    """Any exception from the underlying LLM call short-circuits to
    None — the synthesis-declined path handles the failure cleanly
    without bubbling exceptions up through the iteration loop."""
    from unittest.mock import patch

    from genie_space_optimizer.optimization import archetype_learning as al

    def _raise(*args, **kwargs):
        raise RuntimeError("simulated LLM 503")

    with patch(
        "genie_space_optimizer.optimization.optimizer._traced_llm_call",
        side_effect=_raise,
    ):
        out = al._call_llm_for_provisional_archetype_synthesis(
            candidate=_candidate(),
        )
    assert out is None
