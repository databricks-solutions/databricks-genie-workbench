"""Phase 2 invariant: state machine never overclaims progress vs legacy lane.

Scaffolded here for future activation. The equivalence harness needs the
legacy iteration entry point to be callable in a test fixture context;
that work lives in PR 4.5 (the deployed-run smoke test). Until then, the
unit-level mocked end-to-end (test_anchor_gs_009_end_to_end_reaches_applied.py)
covers the per-anchor contract.
"""
import pytest


@pytest.mark.skip(
    reason=(
        "Equivalence harness lands in PR 4.5; legacy-runner stub is "
        "environment-specific. Re-enable when the legacy iteration entry "
        "point is callable from tests."
    ),
)
def test_state_machine_never_overclaims_progress():
    """Will fail-loud if state machine reports APPLIED but legacy ledger
    has no applied patches. Phase 2 ships a unit-level mocked end-to-end
    that proves the contract per-anchor; the full equivalence harness
    against the legacy lane is deferred to PR 4.5 alongside the
    deployed-smoke gate the user explicitly chose to defer."""
    pass
