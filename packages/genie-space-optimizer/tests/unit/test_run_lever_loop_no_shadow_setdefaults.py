"""Trial-4 follow-up grep-guard — once trial-4 fixtures landed
(commit 9d380a89), the three shadow-flag setdefaults in
``run_lever_loop.py`` became 2x LLM-cost overhead with no benefit
(no fixtures need to be regenerated routinely; the byte-stability
gate runs on committed fixtures in CI).

This guard pins their absence so they are not silently re-added by
a future "I need to regen fixtures" change. The legitimate
re-introduction path is: set the shadow flag for ONE trial run via
an explicit edit to databricks.yml base_parameters (which leaves
git-visible breadcrumbs), then revert.
"""
from __future__ import annotations

import pathlib

NOTEBOOK_PATH = pathlib.Path(
    "src/genie_space_optimizer/jobs/run_lever_loop.py"
)


def _notebook_text() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def test_lever5_shadow_setdefault_absent() -> None:
    """The Plan 2 shadow flag must NOT be set by the notebook on
    import. Production trials must run with shadow off (single LLM
    call per L5 site)."""
    text = _notebook_text()
    assert 'setdefault("GSO_LEVER5_SHADOW_V1"' not in text, (
        "GSO_LEVER5_SHADOW_V1 setdefault re-introduced in "
        "run_lever_loop.py — this doubles L5 LLM cost in every "
        "production run. If you need shadow mode for a one-off "
        "fixture-regen trial, set it via databricks.yml "
        "base_parameters for that trial only."
    )


def test_three_stage_shadow_setdefault_absent() -> None:
    """The Plan 3 shadow flag must NOT be set by the notebook on
    import. Plan 3 is now default-on (post-trial-4) so the shadow
    path no longer serves a purpose — the production path IS the
    three-stage pipeline."""
    text = _notebook_text()
    assert 'setdefault("GSO_THREE_STAGE_SHADOW_V1"' not in text, (
        "GSO_THREE_STAGE_SHADOW_V1 setdefault re-introduced in "
        "run_lever_loop.py. Plan 3 is default-on; shadow mode "
        "would now run the legacy strategist alongside the pipeline "
        "for every iteration, doubling strategist LLM cost."
    )


def test_raw_evidence_shadow_setdefault_absent() -> None:
    """The Plan 4 shadow flag must NOT be set by the notebook on
    import. Plan 4 is default-on; shadow mode would double the
    Stage-2 dispatch LLM cost (raw-evidence-on AND raw-evidence-off
    paths run for every dispatch)."""
    text = _notebook_text()
    assert 'setdefault("GSO_RAW_EVIDENCE_SHADOW_V1"' not in text, (
        "GSO_RAW_EVIDENCE_SHADOW_V1 setdefault re-introduced in "
        "run_lever_loop.py. Plan 4 is default-on; shadow mode "
        "would double Stage-2 dispatch LLM cost."
    )


def test_no_environ_setdefault_for_any_gso_shadow_flag() -> None:
    """Catch-all: any future shadow flag added to common/config.py
    must NOT show up as a setdefault in this notebook unless an
    explicit follow-up plan removes this assertion."""
    text = _notebook_text()
    forbidden_substrings = (
        '_os.environ.setdefault("GSO_LEVER5_SHADOW_V1"',
        '_os.environ.setdefault("GSO_THREE_STAGE_SHADOW_V1"',
        '_os.environ.setdefault("GSO_RAW_EVIDENCE_SHADOW_V1"',
        # Production-affecting V1 flags are also forbidden — they
        # should be controlled via databricks.yml base_parameters,
        # not the notebook source.
        '_os.environ.setdefault("GSO_THREE_STAGE_V1"',
        '_os.environ.setdefault("GSO_LEVER5_SPLIT_V1"',
        '_os.environ.setdefault("GSO_RAW_EVIDENCE_V1"',
        '_os.environ.setdefault("GSO_RCA_CONTRACT_NARROW_V1"',
    )
    found = [s for s in forbidden_substrings if s in text]
    assert not found, (
        f"Forbidden GSO env-var setdefaults found in "
        f"run_lever_loop.py: {found}. Move flag overrides to "
        f"databricks.yml base_parameters so they are git-visible "
        f"per-deployment and do not silently double LLM cost."
    )
