"""Bundle-status wiring grep-guard.

After the wiring fix, the contract-health emission must run AFTER the
Phase H bundle assembly block — otherwise it reads bundle-assembly
payloads before they're populated and reports a false `bundle_status`.

This grep-guard pins the source-level ordering. It is a *source* test;
it does not exercise harness behaviour at runtime. The companion test
``test_bundle_status_payload_accumulation.py`` covers the behavioural
contract.
"""
from __future__ import annotations

import pathlib


HARNESS_PATH = pathlib.Path(
    "src/genie_space_optimizer/optimization/harness.py"
)


def _harness_text() -> str:
    return HARNESS_PATH.read_text(encoding="utf-8")


def test_contract_health_emission_runs_after_phase_h_block() -> None:
    """The `_emit_contract_health_summary(...)` call site that consumes
    the bundle-assembly payloads must follow the
    `bundle_assembly_incomplete_marker` import in the Phase H block.
    Anchors:
      - ``from genie_space_optimizer.optimization.run_analysis_contract import (`
        followed by ``bundle_assembly_incomplete_marker as _incomplete_marker,``
        — marks the Phase H post-upload completeness-check region.
      - ``_emit_contract_health_summary(\n            optimization_run_id=run_id``
        — marks the contract-health emission call site.
    """
    text = _harness_text()
    incomplete_anchor = (
        "bundle_assembly_incomplete_marker as _incomplete_marker"
    )
    emit_anchor = (
        "_emit_contract_health_summary(\n            optimization_run_id=run_id"
    )
    assert incomplete_anchor in text, (
        "Phase H post-upload completeness check anchor not found; "
        "harness layout changed unexpectedly"
    )
    assert emit_anchor in text, (
        "contract-health emission call site anchor not found"
    )
    incomplete_idx = text.find(incomplete_anchor)
    emit_idx = text.find(emit_anchor)
    assert emit_idx > incomplete_idx, (
        f"contract-health emission must run AFTER the Phase H "
        f"bundle-assembly block; got emit_idx={emit_idx}, "
        f"incomplete_anchor_idx={incomplete_idx} — the relocation in "
        f"docs/2026-05-12-bundle-status-wiring-fix-plan.md has been "
        f"reverted or never landed"
    )


def test_contract_health_emission_runs_after_both_bundle_marker_sites() -> None:
    """Both the incomplete-marker and the failed-marker emission sites
    must precede the contract-health emission, so both payload lists
    are visible to ``locals().get(...)`` at the contract-health site.
    """
    text = _harness_text()
    failed_marker_print = "print(_bundle_assembly_failed_marker("
    emit_anchor = (
        "_emit_contract_health_summary(\n            optimization_run_id=run_id"
    )
    assert failed_marker_print in text
    assert emit_anchor in text
    # Last occurrence of the failed-marker print (there are two — one
    # for upload-failed inside ``if _phase_h_anchor_run_id:``, one for
    # render-failed in the outer except). Both must precede the emit.
    failed_idx = text.rfind(failed_marker_print)
    emit_idx = text.find(emit_anchor)
    assert emit_idx > failed_idx, (
        "contract-health emission must run after ALL "
        "`bundle_assembly_failed_marker` print sites"
    )


def test_no_lingering_pre_phase_h_contract_health_emission() -> None:
    """Belt-and-braces: ensure the OLD emission site (inside the
    convergence try/except, before the Phase H block) has been
    removed. If the wiring is re-introduced there, the bug returns.
    """
    text = _harness_text()
    conv_anchor = "print(convergence_marker("
    emit_anchor = (
        "_emit_contract_health_summary(\n            optimization_run_id=run_id"
    )
    assert conv_anchor in text
    assert emit_anchor in text
    conv_idx = text.find(conv_anchor)
    emit_idx = text.find(emit_anchor)
    # The emission must be FAR from the convergence marker — well
    # beyond the ~5000 chars the OLD RCO-2a grep-guard allowed. 10000
    # is conservative; the real distance is ~17000+ chars (the Phase H
    # block is ~500 lines).
    assert (emit_idx - conv_idx) > 10000, (
        f"contract-health emission is suspiciously close to "
        f"convergence_marker ({emit_idx - conv_idx} chars). The old "
        f"convergence-time emission was deleted by this plan; if it "
        f"is back, the bundle-status wiring bug is back too."
    )
