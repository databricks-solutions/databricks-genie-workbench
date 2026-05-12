"""Plan P-A — grep-guard pinning the harness call into the new
per-iter writer. The writer must be invoked from within the Phase H
upload block (so we have a stable anchor) and must follow the
existing parent-bundle uploads (so the writer has a chance to fail
without breaking the parent bundle render path).
"""
from __future__ import annotations

import pathlib

HARNESS_PATH = pathlib.Path(
    "src/genie_space_optimizer/optimization/harness.py"
)


def _harness_text() -> str:
    return HARNESS_PATH.read_text(encoding="utf-8")


def test_harness_declares_three_per_iter_accumulators() -> None:
    """The accumulator names must be visible at the same scope as
    ``_iter_traces`` / ``_iter_summaries`` so the terminate-path
    writer can read them via ``locals().get(...)`` if needed."""
    text = _harness_text()
    assert "_iter_rca_ledgers: dict[int, dict] = {}" in text
    assert "_iter_proposal_inventories: dict[int, dict] = {}" in text
    assert "_iter_journey_reports: dict[int, dict] = {}" in text


def test_terminate_path_calls_materialize_per_iter() -> None:
    """The materializer must be invoked from inside the Phase H
    upload block — the same scope where ``_phase_h_anchor_run_id``
    is known to be a real anchor."""
    text = _harness_text()
    assert "_materialize_per_iter_contract_paths(" in text


def test_materialize_call_follows_parent_bundle_uploads() -> None:
    """Source ordering: the parent-bundle uploads (manifest, run_summary,
    artifact_index, operator_transcript, decision_trace_all, etc.)
    must be called BEFORE the per-iter writer so a writer failure
    does not block the parent bundle from landing.
    """
    text = _harness_text()
    parent_call_idx = text.find('artifact_file=_paths["failure_buckets"]')
    # Find the CALL site (not the def site, which is the module-level
    # helper). The call passes ``client=_client_phase_h`` as its first
    # kwarg; the def signature uses ``client: Any``.
    perit_call_idx = text.find("client=_client_phase_h,")
    assert parent_call_idx >= 0
    assert perit_call_idx >= 0, (
        "Could not find _materialize_per_iter_contract_paths invocation "
        "with client=_client_phase_h — the wiring may have drifted."
    )
    assert parent_call_idx < perit_call_idx, (
        "_materialize_per_iter_contract_paths must be invoked AFTER the "
        "parent-bundle upload block so a per-iter writer failure does "
        "not block the parent bundle landing."
    )
