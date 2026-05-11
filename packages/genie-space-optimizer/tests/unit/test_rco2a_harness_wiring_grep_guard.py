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
    # The call site must follow convergence_marker so it fires on every
    # run-end path (normal + plateau + divergence) within the same outer
    # try/except block.
    conv_idx = harness.find("print(convergence_marker(")
    emit_idx = harness.find(
        "_emit_contract_health_summary(\n            optimization_run_id=run_id"
    )
    assert conv_idx != -1, "convergence_marker emission missing"
    assert emit_idx != -1, "contract-health emission missing"
    assert emit_idx > conv_idx, "contract-health must follow convergence"
    # Within 5000 chars (same try/except block neighborhood). The
    # convergence + V1 manifest + V2 manifest block is ~90 lines × ~55
    # chars = ~5000 chars, so this bound keeps emission anchored to the
    # convergence try/except rather than letting it drift to Phase H or
    # beyond.
    assert emit_idx - conv_idx < 5000
