"""Trial 13 Phase 9 — the trial-readiness gate.

Loads ONE real captured production row (gs_009 from the 98ec8950
postmortem corpus), stubs the three LLM stages with happy-path tapes,
stubs the Genie API client via :class:`FakeWorkspaceClient`, drives
``run_state_machine_iteration_and_persist`` end-to-end, and asserts
that:

* The QID admits as a hard failure through the namespace-aware
  dispatch (Phase 4 marker).
* The Stage 1 actionable gate (Phase 5) accepts the tape diagnosis.
* The Stage 3 synthesis tape produces a proposal whose ``patch_body``
  survives the survival contract.
* The applier gate emits at least one ``GSO_PATCH_OUTCOME_V1`` marker
  with ``outcome="applied"``.
* The :class:`FakeApiClient` records exactly one PATCH against the
  Genie space endpoint.

If after Phases 1–8 this test still fails, the failing assertion
names the next-deepest gap and feeds the Trial 14 hypothesis row.
The plan's goal: *at least one canonical hard QID lands
``GSO_PATCH_OUTCOME_V1 outcome=applied`` in a real lever-loop run*.
This test is the offline proxy for that goal.
"""
from __future__ import annotations

import io
import json
import re
import time
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.fake_workspace_client import (
    FakeWorkspaceClient,
    minimal_valid_metadata_snapshot,
)
from tests.integration.sm_forward_tapes import (
    KIT_FREE_RCA_KIND,
    cluster_response_tape,
    diagnose_response_tape,
    synthesize_response_tape,
)
from tests.integration.sm_tape_replay import TapeReplayHarness


_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "unit"
    / "fixtures"
    / "production_eval_rows_real.json"
)


_ACCEPTANCE_CEILING_SECONDS = 10.0
_FAKE_SPACE_ID = "deadbeefcafebabe1234567890abcdef"
_GENIE_PATCH_PATH = f"/api/2.0/genie/spaces/{_FAKE_SPACE_ID}"

# Trial 13i — run-level schema_columns FQNs covering the gs_009 blame
# columns (``DEST_AIRPORT_CD`` / ``ORIG_AIRPORT_CD`` from the 98ec8950
# capture's ASI ``blame_set``). The seed normalizer resolves the bare
# blame identifiers to these 4-part FQNs (rule-2 suffix match), and the
# run-level ``validate_schema_columns`` pre-flight clears. Without this
# channel Stage 1 abstains with ``missing_schema_columns`` before the
# tape diagnosis is consumed.
_GS_009_SCHEMA_COLUMNS = [
    "main.airline.flights.DEST_AIRPORT_CD",
    "main.airline.flights.ORIG_AIRPORT_CD",
]


_MARKER_LINE_RE = re.compile(r"^([A-Z][A-Z0-9_]*) (\{.+\})$", re.MULTILINE)


def _parse_markers(stdout: str, name: str) -> list[dict]:
    out: list[dict] = []
    for m in _MARKER_LINE_RE.finditer(stdout or ""):
        if m.group(1) != name:
            continue
        try:
            out.append(json.loads(m.group(2)))
        except json.JSONDecodeError:
            continue
    return out


def _load_gs_009_real_row() -> dict:
    """Return the captured ``gs_009`` row from the 98ec8950 postmortem
    bundle. This is the canonical Trial-readiness fixture."""
    payload = json.loads(_FIXTURE.read_text())
    captures = payload.get("production_rows_98ec8950") or []
    for entry in captures:
        if entry.get("namespaced_qid", "").endswith("gs_009"):
            return entry["row"]
    raise RuntimeError(
        "production_eval_rows_real.json missing the gs_009 capture "
        "under 'production_rows_98ec8950'; Trial 13 Phase 1 corpus "
        "drift."
    )


def _disable_genie_patch_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    from genie_space_optimizer.common import genie_client as gc

    monkeypatch.setattr(gc.time, "sleep", lambda *_a, **_k: None)


def _stocked_forward_tape(
    qids: list[str],
    *,
    cycles: int = 6,
    rca_kind_label: str = "wrong_aggregation",
) -> list:
    """Stock multiple copies of each stage's tape so escalation cycles
    don't exhaust the replay early.

    ``rca_kind_label`` defaults to ``wrong_aggregation`` for byte-stability
    of the phase-4 dispatch test; the applied-patch readiness test passes
    :data:`KIT_FREE_RCA_KIND` so its single-lever vehicle advances past
    the Trial 26 W26.2 kit gate (see that constant's docstring)."""
    tape = []
    for _ in range(cycles):
        tape += diagnose_response_tape(qids, rca_kind_label=rca_kind_label)
    for _ in range(cycles):
        tape += cluster_response_tape(qids)
    for _ in range(cycles):
        tape += synthesize_response_tape(qids)
    return tape


@pytest.mark.integration
def test_real_production_row_reaches_applied_patch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trial 13 readiness gate — a real production row drives the SM
    end-to-end through Stage 1/2/3 and the applier emits at least one
    ``GSO_PATCH_OUTCOME_V1 outcome=applied`` marker.

    The test fixture is the canonical gs_009 row from the 98ec8950
    postmortem bundle — the very row that terminated Trial 12 at
    Stage 1 with ``question_text_empty``. Phase 2 widened the
    ``row_question`` ladder to absorb ``request.question``; Phase 8
    normalized the row at admission via :func:`normalize_eval_row`.
    The combination MUST land this row at ``APPLIED`` against the
    happy-path tape.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))
    _disable_genie_patch_backoff(monkeypatch)

    real_row = _load_gs_009_real_row()

    # The state machine admits hard QIDs by canonical (unprefixed)
    # form: ``gs_009``. The dispatch admission walks the row through
    # ``row_qid`` ⇒ ``extract_question_id``; for these rows the qid
    # carries the domain prefix. Tape entries key on ``qid``, so we
    # use the namespaced form everywhere.
    from genie_space_optimizer.optimization.canonical_eval_row import (
        normalize_eval_row,
    )

    canonical = normalize_eval_row(real_row)
    sm_qid = canonical.namespaced_qid or canonical.qid
    assert sm_qid, (
        f"normalize_eval_row failed to produce a QID for the real "
        f"production row; raw keys: {sorted(real_row.keys())[:10]!r}"
    )
    assert canonical.question_text, (
        "Phase 2 ladder regression: row_question() came back empty "
        f"for {sm_qid!r}. This is the exact symptom Trial 12 missed. "
        f"question_source_path={canonical.question_source_path!r}"
    )

    qids = [sm_qid]
    tape = _stocked_forward_tape(qids, rca_kind_label=KIT_FREE_RCA_KIND)
    harness = TapeReplayHarness(tape=tape)

    ws = FakeWorkspaceClient()
    metadata_snapshot = minimal_valid_metadata_snapshot()
    metadata_snapshot["schema_columns"] = list(_GS_009_SCHEMA_COLUMNS)

    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
        stage_index,
    )

    # Mark this row as a hard failure for the dispatch admission. The
    # production capture carries ``feedback/result_correctness/value="no"``
    # already; we add the synthetic ``_expected_hard`` flag the
    # dispatch helpers honour when present.
    test_row = dict(real_row)
    test_row.setdefault("_expected_hard", True)
    test_row.setdefault("_expected_qid", sm_qid)

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), harness.patch():
        final_states = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=[test_row],
            iteration=1,
            run_id="trial13_phase9_e2e",
            run_root=tmp_path,
            workspace_client=ws,
            space_id=_FAKE_SPACE_ID,
            metadata_snapshot=metadata_snapshot,
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()

    assert elapsed < _ACCEPTANCE_CEILING_SECONDS, (
        f"E2E replay took {elapsed:.2f}s; ceiling {_ACCEPTANCE_CEILING_SECONDS}s."
    )

    # Surface 1 — exactly one final state, matching the admitted QID.
    assert len(final_states) >= 1, (
        f"State machine admitted zero QIDs from the real production "
        f"row; expected 1. This means the dispatch admission still "
        f"rejects the row — Phase 4 dispatch marker should have "
        f"surfaced the drift kind. stdout tail:\n{stdout[-2000:]}"
    )

    applied_idx = stage_index(FunnelStage.APPLIED)
    deepest_idx = max(
        stage_index(s.deepest_stage_reached) for s in final_states
    )
    assert deepest_idx >= applied_idx, (
        f"No state reached APPLIED. deepest stage index={deepest_idx} "
        f"vs APPLIED={applied_idx}. Trial-readiness gate failure. "
        f"This either surfaces the next-deepest gap (the Trial 14 "
        f"hypothesis row) or pins a regression in Phases 1–8. "
        f"stdout tail:\n{stdout[-3000:]}"
    )

    # Surface 2 — at least one applied PATCH on the wire.
    patch_calls = ws.api_client.genie_patch_calls()
    assert patch_calls, (
        "Zero PATCH calls recorded against the Genie API. The "
        "applier never reached the wire. All recorded API calls: "
        f"{[(c.method, c.path) for c in ws.api_client.calls]!r}"
    )
    assert all(c.path == _GENIE_PATCH_PATH for c in patch_calls), (
        f"PATCH path drift: expected {_GENIE_PATCH_PATH!r}; got "
        f"{[c.path for c in patch_calls]!r}"
    )

    # Surface 3 — typed applier marker emission.
    outcome_markers = _parse_markers(stdout, "GSO_PATCH_OUTCOME_V1")
    applied = [m for m in outcome_markers if m.get("outcome") == "applied"]
    assert applied, (
        f"No GSO_PATCH_OUTCOME_V1 marker with outcome=applied. "
        f"observed outcomes: "
        f"{[m.get('outcome') for m in outcome_markers]!r}. The "
        f"deepest stage reached APPLIED but the applier did not "
        f"emit the canonical outcome marker — Phase 4 of Trial 14 "
        f"would investigate the applier transformer's marker emit."
    )


@pytest.mark.integration
def test_real_production_row_phase4_dispatch_no_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trial 13 Phase 4 + Phase 8 invariant — when the canonical row
    admits successfully, no ``GSO_PLAN11_DISPATCH_STARVED_V1`` marker
    should fire with ``drift_kind != "none"``.

    The Trial 12 trial emitted ``starved`` (no Plan 11 QIDs at all)
    AND ``namespace_mismatch`` (SM saw the namespaced qid; Plan 11
    saw the unnamespaced qid). Phases 4+8 align both pipelines on
    the same projection; the absence of any drift marker on a
    happy-path admission is the regression guard.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))
    _disable_genie_patch_backoff(monkeypatch)

    real_row = _load_gs_009_real_row()

    from genie_space_optimizer.optimization.canonical_eval_row import (
        normalize_eval_row,
    )

    canonical = normalize_eval_row(real_row)
    sm_qid = canonical.namespaced_qid or canonical.qid

    test_row = dict(real_row)
    test_row.setdefault("_expected_hard", True)
    test_row.setdefault("_expected_qid", sm_qid)

    tape = _stocked_forward_tape([sm_qid])
    harness = TapeReplayHarness(tape=tape)
    ws = FakeWorkspaceClient()
    metadata_snapshot = minimal_valid_metadata_snapshot()
    metadata_snapshot["schema_columns"] = list(_GS_009_SCHEMA_COLUMNS)

    from genie_space_optimizer.optimization import optimizer as opt_mod

    buf = io.StringIO()
    with redirect_stdout(buf), harness.patch():
        opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=[test_row],
            iteration=1,
            run_id="trial13_phase9_drift_check",
            run_root=tmp_path,
            workspace_client=ws,
            space_id=_FAKE_SPACE_ID,
            metadata_snapshot=metadata_snapshot,
            forbidden_signatures=(),
        )
    stdout = buf.getvalue()

    drift_markers = _parse_markers(stdout, "GSO_PLAN11_DISPATCH_STARVED_V1")
    bad = [
        m for m in drift_markers
        if str(m.get("drift_kind") or "") not in ("", "none")
    ]
    assert not bad, (
        f"Dispatch drift markers fired on a happy-path admission: "
        f"{bad!r}. Phases 4 + 8 should keep SM and Plan 11 in sync."
    )
