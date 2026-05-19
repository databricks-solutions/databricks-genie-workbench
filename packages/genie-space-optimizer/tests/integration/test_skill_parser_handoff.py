"""Smoke test: the gso-postmortem skill's parser invocation contract
must remain importable from the documented module path. Failure of
this test means a skill has drifted from the parser API."""

from pathlib import Path


_SKILL_DIR = Path(__file__).resolve().parents[2] / "docs" / "skills"


def test_postmortem_skill_references_parser_module():
    skill = (_SKILL_DIR / "gso-postmortem" / "SKILL.md").read_text()
    assert "tools.lever_loop_stdout_parser" in skill
    assert "parse_lever_loop_stdout" in skill


def test_analysis_skill_references_lever_loop_mechanics_health():
    """The analysis skill must declare the Lever-Loop Mechanics Health
    checklist and enumerate the 6-stage process spine so postmortems
    can map evidence to the first broken stage. Plan 8 T10 retired
    the legacy named diagnostic codes (ACCEPTANCE_TARGET_BLIND etc.)
    in favour of stage-key based diagnostics; the test pins the
    stage-key enumeration instead."""
    skill = (_SKILL_DIR / "gso-lever-loop-run-analysis" / "SKILL.md").read_text()
    assert "Lever-Loop Mechanics Health" in skill
    # Stage-key checklist (process spine) must remain documented:
    assert "evaluation_state" in skill
    assert "rca_evidence" in skill
    assert "cluster_formation" in skill
    assert "action_group_selection" in skill
    assert "acceptance_decision" in skill


def test_parser_module_imports_and_exposes_documented_api():
    from genie_space_optimizer.tools.lever_loop_stdout_parser import (
        LeverLoopStdoutView,
        parse_lever_loop_stdout,
    )
    assert callable(parse_lever_loop_stdout)
    view = parse_lever_loop_stdout("")
    assert isinstance(view, LeverLoopStdoutView)
    assert view.optimization_run_summary is None


def test_canonical_example_reference_present():
    """The canonical example run_id is preserved in gso-postmortem so
    operators have a reproducible target to anchor postmortems
    against. Plan 8 T10 retired the inline UUID reference from
    gso-lever-loop-run-analysis (the analysis skill now references
    runs by job_id+run_id supplied at invocation time, not a
    hardcoded example), so this assertion is scoped to gso-postmortem
    only."""
    skill = (_SKILL_DIR / "gso-postmortem" / "SKILL.md").read_text()
    assert "0ade1a99-9406-4a68-a3bc-8c77be78edcb" in skill
