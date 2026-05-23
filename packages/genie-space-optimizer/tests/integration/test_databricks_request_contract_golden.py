"""PR-2B — CI golden test: every LLM call site's wire envelope must
pass the ``DatabricksEndpointRequestContract``.

This is the "full provider request contract" gate the cross-analyst
review explicitly asked for. The dc89d1a9 / 98ec8950 lever-loop
failures (every Plan 11 Stage 1 call returning 400 with
``tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$``) would have been
caught here at PR-review time, weeks before deploy.

Two enumerations, both designed to fail loudly when a new contract
violation is introduced:

  1. **Plan 11 stage outputs** wrapped in ``AbstainableEnvelope[T]``
     — every Plan 11 stage that uses structured output.
  2. **Legacy top-level outputs** passed bare to
     ``build_response_format`` — every Lever / strategist / cluster /
     repair / critique / RCA call site that uses structured output.

For each, build the ``call_kwargs`` shape the optimizer would dispatch
(model + canonical messages + ``response_format``), feed it to
``DEFAULT_CONTRACT.validate(...)``, and assert the violation list is
empty.

The test runs in well under 5 seconds — pure schema construction, no
network, no LLM.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.databricks_request_contract import (
    DEFAULT_CONTRACT,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
)
from genie_space_optimizer.optimization.prompt_io import build_response_format

# Plan 11 stage output schemas (wrapped in AbstainableEnvelope[T] by
# every Plan 11 stage handler).
from genie_space_optimizer.skills.plan11_cluster.output_schema import (
    Plan11ClusterOutput,
)
from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
    Plan11DiagnoseOutput,
)
from genie_space_optimizer.skills.plan11_narrow.output_schema import (
    Plan11NarrowOutput,
)
from genie_space_optimizer.skills.plan11_repair.output_schema import (
    Plan11RepairOutput,
)
from genie_space_optimizer.skills.plan11_synthesize.output_schema import (
    Plan11SynthesizeOutput,
)

# Legacy / non-Plan-11 top-level output schemas. These are passed bare
# (no envelope wrap) to ``build_response_format`` at their respective
# call sites in ``optimizer.py``.
from genie_space_optimizer.optimization.prompt_io import (
    AdaptiveStrategistOutput,
    Lever1RcaBridgeOutput,
    Lever4JoinDiscoveryOutput,
    Lever5aInstructionsOutput,
    Lever5bExampleSqlOutput,
    Lever5InstructionOutput,
    Lever6SqlExpressionOutput,
    Lever12ColumnOutput,
    Stage1DiscoveryOutput,
    StrategistDetailOutput,
    StrategistTriageOutput,
    TeachingKitOutput,
)
from genie_space_optimizer.skills._reference_smoke_test.output_schema import (
    ReferenceSmokeTestOutput,
)
from genie_space_optimizer.skills.candidate_critique.output_schema import (
    LlmCritiqueVerdictOutput,
)
from genie_space_optimizer.skills.failure_clustering.output_schema import (
    LlmClusterOutput,
    LlmClusterSetOutput,
)
from genie_space_optimizer.skills.rca_evidence_extraction.output_schema import (
    PerQidRcaEvidenceOutput,
)
from genie_space_optimizer.skills.repair_intent_synthesis.output_schema import (
    LlmRepairProposalOutput,
)
from genie_space_optimizer.skills.rollback_learning.output_schema import (
    LlmNextAttemptHypothesisOutput,
)


# Plan 11 stages: every handler wraps its output schema in
# ``AbstainableEnvelope[T]`` (so the LLM can decline cleanly). Pin the
# id to the SM stage name so a regression points at the exact stage.
PLAN11_ENVELOPED_OUTPUTS = [
    pytest.param(Plan11DiagnoseOutput, id="plan11_diagnose"),
    pytest.param(Plan11ClusterOutput, id="plan11_cluster"),
    pytest.param(Plan11SynthesizeOutput, id="plan11_synthesize"),
    pytest.param(Plan11RepairOutput, id="plan11_repair"),
    pytest.param(Plan11NarrowOutput, id="plan11_narrow"),
]


# Legacy call-site outputs: passed bare to ``build_response_format``
# (no envelope). One id per active LLM call site so a regression is
# self-localising.
LEGACY_BARE_OUTPUTS = [
    pytest.param(Stage1DiscoveryOutput, id="stage1_discovery"),
    pytest.param(Lever1RcaBridgeOutput, id="lever1_rca_bridge"),
    pytest.param(Lever12ColumnOutput, id="lever12_column"),
    pytest.param(Lever4JoinDiscoveryOutput, id="lever4_join_discovery"),
    pytest.param(Lever5InstructionOutput, id="lever5_instruction"),
    pytest.param(Lever5aInstructionsOutput, id="lever5a_instructions"),
    pytest.param(Lever5bExampleSqlOutput, id="lever5b_example_sql"),
    pytest.param(Lever6SqlExpressionOutput, id="lever6_sql_expression"),
    pytest.param(TeachingKitOutput, id="teaching_kit"),
    pytest.param(AdaptiveStrategistOutput, id="adaptive_strategist"),
    pytest.param(StrategistTriageOutput, id="strategist_triage"),
    pytest.param(StrategistDetailOutput, id="strategist_detail"),
    pytest.param(LlmClusterOutput, id="llm_cluster"),
    pytest.param(LlmClusterSetOutput, id="llm_cluster_set"),
    pytest.param(LlmCritiqueVerdictOutput, id="llm_critique_verdict"),
    pytest.param(LlmRepairProposalOutput, id="llm_repair_proposal"),
    pytest.param(LlmNextAttemptHypothesisOutput, id="llm_next_attempt_hypothesis"),
    pytest.param(PerQidRcaEvidenceOutput, id="per_qid_rca_evidence"),
    pytest.param(ReferenceSmokeTestOutput, id="reference_smoke_test"),
]


def _canonical_call_kwargs(response_format: dict) -> dict:
    """Build a minimal Plan-11-shaped ``call_kwargs`` for golden
    validation. The ``messages`` and ``max_tokens`` are pinned to
    representative-but-small values so the canonical run never trips
    the budget rules — those have their own coverage in the
    contract's own unit tests."""
    return {
        "model": "databricks-claude-opus-4-6",
        "messages": [
            {"role": "system", "content": "canonical system msg"},
            {"role": "user", "content": "canonical user prompt"},
        ],
        "temperature": 0.1,
        "max_tokens": 4096,
        "response_format": response_format,
    }


@pytest.mark.parametrize("result_cls", PLAN11_ENVELOPED_OUTPUTS)
def test_plan11_enveloped_call_site_passes_contract(result_cls) -> None:
    """For every Plan 11 stage, the ``response_format`` payload an
    SM-driven handler dispatches must pass every contract rule.

    Failure modes pinned here:
      * Schema name regex (the dc89d1a9 / 98ec8950 wire bug).
      * Unsupported JSON-Schema keyword regression (the dc89d1a9
        ``_flatten_nullable_anyof`` precursor).
    """
    envelope_cls = AbstainableEnvelope[result_cls]
    rf = build_response_format(envelope_cls)
    kwargs = _canonical_call_kwargs(rf)
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert violations == [], (
        f"{result_cls.__name__} dispatch envelope violates contract: "
        f"{[(v.field, v.constraint) for v in violations]}"
    )


@pytest.mark.parametrize("result_cls", LEGACY_BARE_OUTPUTS)
def test_legacy_bare_call_site_passes_contract(result_cls) -> None:
    """Every legacy (non-Plan-11) structured-output call site must
    also pass the contract. The bare schemas don't carry the
    AbstainableEnvelope generic-alias name hazard, but they share the
    same schema-keyword and tool-name surface area — pin all of them
    so a future LLMOutputContract subclass that accidentally adds an
    ``anyOf`` (or a $ref, etc.) lights up here."""
    rf = build_response_format(result_cls)
    kwargs = _canonical_call_kwargs(rf)
    violations = DEFAULT_CONTRACT.validate(kwargs)
    assert violations == [], (
        f"{result_cls.__name__} dispatch envelope violates contract: "
        f"{[(v.field, v.constraint) for v in violations]}"
    )


def test_golden_test_enumerates_at_least_all_known_plan11_stages() -> None:
    """A safety net: if anyone adds a 6th Plan 11 stage, this test
    fails until the new stage is added to ``PLAN11_ENVELOPED_OUTPUTS``
    above. Keeps the golden test's coverage in lockstep with the
    optimizer.

    The check enumerates direct subclasses of ``LLMOutputContract`` in
    ``genie_space_optimizer.skills`` whose module path starts with
    ``plan11_`` and whose class name ends in ``Output``."""
    import importlib
    import pkgutil

    import genie_space_optimizer.skills as skills_pkg
    from genie_space_optimizer.optimization.prompt_io import (
        LLMOutputContract,
    )

    expected: set[type] = set()
    for _, modname, ispkg in pkgutil.walk_packages(
        skills_pkg.__path__, prefix="genie_space_optimizer.skills.",
    ):
        if not ispkg and modname.endswith("output_schema"):
            parts = modname.split(".")
            if len(parts) >= 4 and parts[3].startswith("plan11_"):
                mod = importlib.import_module(modname)
                for attr in dir(mod):
                    obj = getattr(mod, attr)
                    if (
                        isinstance(obj, type)
                        and issubclass(obj, LLMOutputContract)
                        and obj is not LLMOutputContract
                        and attr.endswith("Output")
                        # top-level class only; skip nested item classes
                        and not attr.startswith("_")
                        and (
                            attr.startswith("Plan11")
                            or attr == "Plan11NarrowOutput"
                        )
                    ):
                        expected.add(obj)

    enumerated = {p.values[0] for p in PLAN11_ENVELOPED_OUTPUTS}
    missing = expected - enumerated
    assert not missing, (
        f"Plan 11 stage(s) {sorted(c.__name__ for c in missing)} added "
        f"without extending PLAN11_ENVELOPED_OUTPUTS in this golden test."
    )
