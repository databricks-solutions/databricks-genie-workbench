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
    skill = (_SKILL_DIR / "gso-lever-loop-run-analysis" / "SKILL.md").read_text()
    assert "Lever-Loop Mechanics Health" in skill
    assert "ACCEPTANCE_TARGET_BLIND" in skill
    assert "PATCH_CAP_RCA_BLIND_RANKING" in skill
    assert "BLAST_RADIUS_OVERDROP_ON_NONSEMANTIC" in skill


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
    for skill_name in ("gso-postmortem", "gso-lever-loop-run-analysis"):
        skill = (_SKILL_DIR / skill_name / "SKILL.md").read_text()
        assert "0ade1a99-9406-4a68-a3bc-8c77be78edcb" in skill, skill_name
