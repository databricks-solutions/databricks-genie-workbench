"""Phase 6 — Plan 11 hard-QID parity gate contract.

Trial 12 (run ``dc89d1a9-...``) postmortem JSON reported, per iteration:

    "missing_from_plan11_by_iteration": [
        {"iteration": 1, "qids": ["7now_..._gs_021", "7now_..._gs_026"]},
        ...
    ]

The harness saw the full hard-QID set, but the Plan 11 dispatch lane
projected a strict subset. Today the parity contract at
:func:`genie_space_optimizer.optimization.input_projection_contract
.assert_input_projection_parity` is **fail-loud only on total starvation**
(both Plan 11 and SM sets empty). Partial drift — i.e. Plan 11 sees a
strict subset while the SM sees the full set — is observable in
``GSO_INPUT_PROJECTION_PARITY_V1`` but not fatal.

That gap let dc89's partial-drift iteration silently run Stage 1 against
the wrong QID subset for four iterations in a row. This test pins the
target contract: partial drift must raise
:class:`InputProjectionContractViolation` so the lever-loop aborts
rather than degrading to a smaller projection.

Two layers, mirroring Phases 3-5:

* :func:`test_partial_drift_emits_observable_parity_marker_today` —
  pins the current marker shape. Passes today; guards against
  accidental marker stripping when the gate lands.
* :func:`test_partial_drift_should_fail_closed_with_typed_violation` —
  target contract. Fails today (XFAIL strict). The follow-up PR widens
  the parity contract to raise on partial drift; the marker becomes
  the canonical attribution channel.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest

from genie_space_optimizer.optimization.input_projection_contract import (
    InputProjectionContractViolation,
    assert_input_projection_parity,
)
from tests.integration.sm_forward_fixtures import (
    parse_markers,
)


# Mirror of dc89's iteration-1 drift shape (run dc89d1a9-..., task
# 383718083419518): the SM admitted the full hard set; Plan 11 projected
# a strict subset; the parity marker fired with non-empty
# ``missing_from_plan11`` and the run continued silently.
HARNESS_HARD_QIDS: tuple[str, ...] = (
    "domain_b_gs_001",
    "domain_b_gs_013",
    "domain_b_gs_021",
    "domain_b_gs_026",
    "domain_b_gs_009",
    "domain_b_gs_017",
)
PLAN11_HARD_QIDS: tuple[str, ...] = (
    "domain_b_gs_001",
    "domain_b_gs_013",
    "domain_b_gs_009",
    "domain_b_gs_017",
)
SM_HARD_QIDS: tuple[str, ...] = HARNESS_HARD_QIDS
EXPECTED_MISSING_FROM_PLAN11: tuple[str, ...] = (
    "domain_b_gs_021",
    "domain_b_gs_026",
)


def _drive_parity_check() -> tuple[str, BaseException | None]:
    """Call the parity contract with the dc89-shaped drift and capture
    both stdout and any exception raised.

    Returns ``(stdout, exc)`` where ``exc`` is the raised exception or
    ``None`` if the call returned cleanly. Both today's and the target
    contract tests share this driver.
    """
    buf = io.StringIO()
    exc: BaseException | None = None
    with redirect_stdout(buf):
        try:
            assert_input_projection_parity(
                iteration=1,
                harness_hard_qids=HARNESS_HARD_QIDS,
                plan11_hard_qids=PLAN11_HARD_QIDS,
                state_machine_hard_qids=SM_HARD_QIDS,
            )
        except BaseException as raised:
            exc = raised
    return buf.getvalue(), exc


# ── Today's contract (passes today) ───────────────────────────────────


def test_partial_drift_emits_observable_parity_marker_today() -> None:
    """Partial drift must surface in the ``GSO_INPUT_PROJECTION_PARITY_V1``
    marker today: ``missing_from_plan11`` must list the QIDs the Plan
    11 projection dropped, and ``missing_from_sm`` must be empty (the
    SM saw the full set).

    Pinning the marker keeps postmortems' attribution channel stable
    when the follow-up PR widens the contract — engineers should be
    able to read off "Plan 11 dropped X, Y" from the same key.
    """
    stdout, exc = _drive_parity_check()

    assert exc is None, (
        f"Partial drift unexpectedly raised today: {exc!r}. "
        "The contract used to fail-loud only on total starvation; "
        "if this changed without the matching xfail marker being "
        "removed, the target contract is now live and the xfail "
        "marker below must be deleted in the same diff."
    )

    parity = parse_markers(stdout, "GSO_INPUT_PROJECTION_PARITY_V1")
    assert len(parity) == 1, (
        f"Expected exactly one parity marker; got {len(parity)}: "
        f"{parity!r}"
    )
    payload = parity[0]
    assert sorted(payload.get("missing_from_plan11") or []) == sorted(
        EXPECTED_MISSING_FROM_PLAN11
    ), (
        f"missing_from_plan11 drifted from the expected dc89 shape. "
        f"got={payload.get('missing_from_plan11')!r}, "
        f"expected={list(EXPECTED_MISSING_FROM_PLAN11)!r}"
    )
    assert payload.get("missing_from_sm") == [], (
        f"missing_from_sm should be empty (the SM saw the full set); "
        f"got {payload.get('missing_from_sm')!r}"
    )


# ── Target contract (XFAIL today; flips green when the gate widens) ───


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Today's parity contract only fails closed on TOTAL starvation "
        "(both authoritative consumers empty). Partial drift "
        "(Plan 11 ⊂ SM) was silently observable in dc89 across four "
        "iterations, producing zero applied patches. The follow-up PR "
        "must widen assert_input_projection_parity to raise "
        "InputProjectionContractViolation on any non-empty "
        "missing_from_plan11. Remove this xfail marker once the gate "
        "widens."
    ),
)
def test_partial_drift_should_fail_closed_with_typed_violation() -> None:
    """Target contract — any partial drift between the harness and the
    Plan 11 projection must raise
    :class:`InputProjectionContractViolation` so the lever-loop's
    ``except Exception`` aborts the iteration rather than running
    Stage 1 against the smaller QID set.
    """
    _, exc = _drive_parity_check()
    assert isinstance(exc, InputProjectionContractViolation), (
        f"Expected InputProjectionContractViolation on partial drift; "
        f"got {exc!r}. dc89 ran for four iterations with "
        f"missing_from_plan11={list(EXPECTED_MISSING_FROM_PLAN11)} "
        f"because this gate did not fire."
    )
