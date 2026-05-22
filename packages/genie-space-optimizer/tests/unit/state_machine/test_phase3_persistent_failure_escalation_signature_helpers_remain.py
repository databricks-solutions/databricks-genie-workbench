"""Phase 3 PR 3.4: persistent_failure_escalation surface stays trimmed.

The file is already a pure data-projection module (no LLM call, no
applier call, no proposal generator). The plan's regression goal was
to assert that the file remains trimmed — i.e., no legacy LLM-runner
functions get added back over time. This test locks that property.

Note on plan deviation: the plan's regression test mentioned helpers
named ``compute_forbidden_signature`` and ``TerminalSignature`` as
required surfaces. Inspection of the actual file shows it uses
different identifiers (``compute_human_required_escalations``,
``HumanRequiredCase``, etc.) — the file is already in the "trimmed"
shape the plan targeted. We assert against the actual surface here.
"""
from genie_space_optimizer.optimization import persistent_failure_escalation as pfe


def test_module_exposes_human_required_accumulator():
    """The data-projection surface — used by the trajectory/postmortem
    layer to compute escalations — must remain stable."""
    assert hasattr(pfe, "HumanRequiredCase")
    assert hasattr(pfe, "compute_human_required_escalations")
    assert hasattr(pfe, "case_to_delta_row")


def test_module_does_not_expose_direct_llm_runner():
    """Phase 3 contract: no LLM runner, no applier, no proposal
    generator surfaces in this module. The escalation ladder owns
    those concerns now."""
    forbidden_names = (
        "run_persistent_escalation", "invoke_escalation_llm",
        "apply_persistent_escalation", "synthesize_persistent_repair",
    )
    for name in forbidden_names:
        assert not hasattr(pfe, name), (
            f"{name} must not exist on persistent_failure_escalation — "
            f"the escalation_ladder transformer owns LLM-driven retry."
        )
