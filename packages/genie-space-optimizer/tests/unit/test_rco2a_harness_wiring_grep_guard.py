"""RCO-2a — grep-guard that the end-of-run emission helper is wired.

We don't run the full harness (it requires a Spark/Databricks env);
this grep-guard ensures the call site exists and lives in the same
end-of-run try/except neighborhood as ``convergence_marker`` so
reviewers cannot accidentally relocate it into a code path that's
only reached on success.
"""
from __future__ import annotations

import pathlib


def test_emit_contract_health_summary_call_site_exists() -> None:
    harness = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert "_emit_contract_health_summary(" in harness
    # After the 2026-05-12 wiring fix
    # (docs/2026-05-12-bundle-status-wiring-fix-plan.md), the
    # emission lives AFTER the Phase H bundle assembly block — not
    # in the convergence try/except — so that locals().get(...) sees
    # populated _bundle_assembly_{incomplete,failed}_payloads and a
    # populated _phase_h_marker_payload. The anchor for the new
    # position is the outer Phase H render-failed except clause.
    render_failed_anchor = 'except Exception as _phase_h_render_exc:'
    emit_anchor = (
        "_emit_contract_health_summary(\n            optimization_run_id=run_id"
    )
    assert render_failed_anchor in harness, (
        "Phase H render-failed anchor missing; harness layout drifted"
    )
    assert emit_anchor in harness, "contract-health emission missing"
    render_idx = harness.find(render_failed_anchor)
    emit_idx = harness.find(emit_anchor)
    assert emit_idx > render_idx, (
        "contract-health emission must follow the Phase H render-"
        "failed except; the wiring fix has been reverted"
    )
    # Within 2000 chars of the render-failed except (it should be the
    # very next try/except block after Phase H wraps up).
    assert emit_idx - render_idx < 2000, (
        f"contract-health emission is suspiciously far from the "
        f"Phase H block ({emit_idx - render_idx} chars). It should "
        f"be the next try/except after Phase H."
    )
