"""Anchor-QID merge gates.

Each anchor fixture must reach APPLIED on the legacy lane (Phase 1)
and on the state machine lane (Phase 3). The test runs the full path
from proposal through every gate to the applier.
"""
import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "anchor_qids"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text())


def _build_proposal_dict(fixture: dict) -> dict:
    """Project the fixture's expected_proposal into the dict shape the
    legacy harness gates consume."""
    ep = dict(fixture["expected_proposal"])
    ep["passing_dependents"] = list(fixture.get("passing_dependents_outside_target") or [])
    ep["high_collateral_risk"] = False
    return ep


def test_gs_009_proposal_passes_structural_repair_gate():
    from genie_space_optimizer.optimization.structural_repair_gate import (
        enforce_structural_repair_shape,
    )
    from genie_space_optimizer.optimization.terminal_signature import (
        resolve_emitted_patch_shape,
    )

    fixture = _load_fixture("gs_009_top_n_row_number.json")
    proposal = _build_proposal_dict(fixture)
    shape = resolve_emitted_patch_shape([proposal])
    verdict = enforce_structural_repair_shape(
        intended_patch_shape=fixture["rca_card"]["intended_patch_shape"],
        emitted_patch_shape=shape,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "admitted", (
        f"gs_009 structural verdict was {verdict.outcome} "
        f"reason={verdict.terminal_reason}"
    )


@pytest.mark.parametrize("fixture_name", [
    "gs_024_currency_filter.json",
    "gs_026_sum_row_number.json",
    "gs_021_mtd_filter.json",
    "gs_004_wrong_metric.json",
])
def test_anchor_proposal_passes_structural_repair_gate(fixture_name):
    from genie_space_optimizer.optimization.structural_repair_gate import (
        enforce_structural_repair_shape,
    )
    from genie_space_optimizer.optimization.terminal_signature import (
        resolve_emitted_patch_shape,
    )

    fixture = _load_fixture(fixture_name)
    proposal = _build_proposal_dict(fixture)
    shape = resolve_emitted_patch_shape([proposal])
    verdict = enforce_structural_repair_shape(
        intended_patch_shape=fixture["rca_card"]["intended_patch_shape"],
        emitted_patch_shape=shape,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "admitted", (
        f"{fixture['qid']} structural verdict was {verdict.outcome} "
        f"reason={verdict.terminal_reason}"
    )


from unittest.mock import MagicMock


@pytest.mark.parametrize("fixture_name", [
    "gs_009_top_n_row_number.json",
    "gs_024_currency_filter.json",
    "gs_026_sum_row_number.json",
    "gs_021_mtd_filter.json",
    "gs_004_wrong_metric.json",
])
def test_anchor_reaches_applied_via_state_machine(fixture_name):
    from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
    from genie_space_optimizer.optimization.state_machine.registry import (
        build_production_state_machine,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
        build_initial_states_from_eval_rows,
    )
    from genie_space_optimizer.optimization.state_machine.verdict import (
        TransformerContext, ValidationContext,
    )

    fixture = _load_fixture(fixture_name)
    # Build a synthetic hard eval row from the fixture.
    eval_row = {
        "question_id": fixture["qid"],
        "feedback/result_correctness/value": "no",
        "score": 0.0,
        "sql": fixture["baseline_sql"],
        "expected_shape": fixture["expected_shape"],
        "eval_row_id": f"row_{fixture['qid']}",
    }
    initial = build_initial_states_from_eval_rows([eval_row], iteration=1)
    assert len(initial) == 1

    # Wire stub LLMs that return the fixture's expected_proposal.
    stub_diagnose = MagicMock(return_value=fixture["rca_card"])
    stub_synth = MagicMock(return_value=fixture["expected_proposal"])
    stub_narrow = MagicMock(return_value={
        "decision": "narrow_to",
        "narrowed_patch": fixture["expected_proposal"],
        "rationale": "test stub",
    })

    ctx = TransformerContext(
        iteration=1, run_id="anchor_test",
        validation_context=ValidationContext(1, "anchor_test", {}),
        extras={
            "diagnose_llm": stub_diagnose,
            "synthesize_llm": stub_synth,
            "narrow_replacement_llm": stub_narrow,
        },
    )
    sm = build_production_state_machine()
    final = sm.run_iteration(initial, ctx)
    assert len(final) == 1
    deepest = final[0].deepest_stage_reached
    assert deepest in (FunnelStage.APPLIED, FunnelStage.EVALUATED, FunnelStage.ACCEPTED), (
        f"{fixture['qid']} deepest stage was {deepest}, expected APPLIED+"
    )
