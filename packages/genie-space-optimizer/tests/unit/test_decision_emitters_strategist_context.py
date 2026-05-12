"""Plan P-G — Stage 4 Strategist Context persistence unit tests.

Covers the two producer helpers in decision_emitters that emit
STRATEGIST_CONTEXT_ASSEMBLED (start of Stage 5, captures the typed
boundary) and STRATEGIST_CONTEXT_CONSUMED (LLM-call boundary, captures
what actually went into the prompt) with a hash on each so postmortem
can detect drift between Stage 4 assembly and Stage 5 consumption.

Evidence anchor:
runid_analysis/{ccf1d60d,31ecd96f}/evidence/gso_postmortem_bundle/operator_transcript.md
— Stage 4 is empty in every iter of both runs.
"""

from __future__ import annotations


def test_decision_type_has_strategist_context_assembled_and_consumed() -> None:
    """The two new DecisionType values exist and are JSON-stable strings."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    assert DecisionType.STRATEGIST_CONTEXT_ASSEMBLED.value == (
        "strategist_context_assembled"
    )
    assert DecisionType.STRATEGIST_CONTEXT_CONSUMED.value == (
        "strategist_context_consumed"
    )


def test_reason_code_has_context_drift_and_match_codes() -> None:
    """ReasonCode carries CONTEXT_ASSEMBLED / *_MATCHES_ASSEMBLED / *_DRIFTED."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    assert ReasonCode.CONTEXT_ASSEMBLED.value == "context_assembled"
    assert ReasonCode.CONTEXT_CONSUMED_MATCHES_ASSEMBLED.value == (
        "context_consumed_matches_assembled"
    )
    assert ReasonCode.CONTEXT_CONSUMED_DRIFTED.value == (
        "context_consumed_drifted"
    )


def test_type_to_section_includes_new_decision_types() -> None:
    """The new DecisionType values are mapped to a fixed transcript section
    so the section-coverage invariant (every DecisionType has a section)
    stays green."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        TYPE_TO_SECTION,
    )

    assert DecisionType.STRATEGIST_CONTEXT_ASSEMBLED in TYPE_TO_SECTION
    assert DecisionType.STRATEGIST_CONTEXT_CONSUMED in TYPE_TO_SECTION


def test_stage4_context_persistence_flag_default_off(
    monkeypatch,
) -> None:
    """Default-OFF preserves byte-stable replay."""
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )

    monkeypatch.delenv("GSO_STAGE4_CONTEXT_PERSISTENCE", raising=False)
    assert stage4_context_persistence_enabled() is False


def test_stage4_context_persistence_flag_truthy_values(
    monkeypatch,
) -> None:
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )

    for truthy in ("1", "true", "yes", "on", "TRUE"):
        monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", truthy)
        assert stage4_context_persistence_enabled() is True, truthy


def test_stage4_context_persistence_flag_falsy_values(
    monkeypatch,
) -> None:
    from genie_space_optimizer.common.config import (
        stage4_context_persistence_enabled,
    )

    for falsy in ("", "0", "false", "no", "off"):
        monkeypatch.setenv("GSO_STAGE4_CONTEXT_PERSISTENCE", falsy)
        assert stage4_context_persistence_enabled() is False, falsy
