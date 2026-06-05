"""Forward-pipeline applied-boundary test.

Extends the harness one stage further: ``APPLYABLE → APPLIED``. The
preceding test (``test_sm_forward_pipeline_to_applyable``) ran with
``workspace_client=None``, which forced ``applier_gate`` to reject
every proposal at the boundary. This test replaces ``None`` with a
:class:`FakeWorkspaceClient` that records the structured PATCH
payload the applier would have sent and returns a canned success
response. Two paths are exercised:

  * **Apply succeeds**: every hard QID reaches
    :class:`FunnelStage.APPLIED`, the fake records the
    ``/api/2.0/genie/spaces/{space_id}`` PATCHes, and the
    decoded ``serialized_space`` body carries the synthesized
    example SQL the Stage 3 tape produced.

  * **Apply fails at the wire**: the fake's ``on_request`` handler
    returns a ``RuntimeError``; ``applier_gate`` surfaces a typed
    ``ProposalAttempt`` with ``outcome="applyability_rejected"`` and
    a ``GSO_GATE_REASONING_V1`` line carrying the exception text.
    No QID reaches APPLIED.

These two paths cover the deterministic surface of the apply step
without a Databricks dependency. The ``EVALUATED`` and ``ACCEPTED``
stages downstream are intentionally **not** covered here — they
require Genie API + warehouse + judge tape harnesses that are the
subject of Step 3.

Aligned with the ``fast-optimizer-testing`` plan Step 2: "asserted
apply boundary via FakeWorkspaceClient" — the second of the three
steps that extends the harness toward full-loop local testing.
"""
from __future__ import annotations

import io
import time
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.fake_workspace_client import (
    FakeWorkspaceClient,
    minimal_valid_metadata_snapshot,
)
from tests.integration.sm_forward_fixtures import (
    expected_hard_qids,
    forward_metadata_snapshot,
    load_production_hydration_rows,
    parse_gate_reasoning_markers,
    parse_patch_outcome_markers,
    parse_qstate_transitions,
    states_by_qid,
)
from tests.integration.sm_forward_tapes import (
    cluster_response_tape,
    diagnose_response_tape,
    synthesize_response_tape,
)
from tests.integration.sm_tape_replay import TapeReplayHarness


_ACCEPTANCE_CEILING_SECONDS = 5.0

# A 32-char lowercase hex space_id satisfies any Genie-side ID-format
# validators the applier may chain through. Stable per-test value so
# assertions can match the PATCH path exactly.
_FAKE_SPACE_ID = "deadbeefcafebabe1234567890abcdef"
_GENIE_PATCH_PATH = f"/api/2.0/genie/spaces/{_FAKE_SPACE_ID}"


def _stock_forward_tape(qids, *, cycles: int = 10):
    """Build a forward tape with ``cycles`` copies per stage per QID.

    The applier failure path cycles back to PROPOSED on rejection and
    the state machine may re-enter Stage 3 for a retry; stocking
    multiple copies prevents tape-exhaustion noise during escalation.
    """
    tape = []
    for _ in range(cycles):
        tape += diagnose_response_tape(qids)
    for _ in range(cycles):
        tape += cluster_response_tape(qids)
    for _ in range(cycles):
        tape += synthesize_response_tape(qids)
    return tape


def _disable_genie_patch_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """No-op the retry backoff inside ``patch_space_config``.

    ``patch_space_config`` sleeps ``retry_delay * attempt`` seconds
    (default 5s × {1, 2} = 15s) between PATCH retries. When the
    applier-failure test wires the FakeApiClient to raise on every
    PATCH, that 15-second tax accumulates per escalation cycle and
    swamps the acceptance ceiling. Stubbing ``time.sleep`` to a
    no-op only inside the genie_client module preserves the retry
    *count* contract (the applier still attempts ``max_retries+1``
    times) without paying real wall time.
    """
    from genie_space_optimizer.common import genie_client as gc

    monkeypatch.setattr(gc.time, "sleep", lambda *_a, **_k: None)


@pytest.mark.integration
def test_apply_via_fake_workspace_client_records_genie_patch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real applier run against a :class:`FakeWorkspaceClient` must
    advance every hard QID to ``APPLIED`` and record at least one
    Genie PATCH whose payload carries the synthesized example SQL.

    Locks in four observable surfaces:

      1. ``deepest_stage_reached >= APPLIED`` for every admitted hard
         QID. This is the proof the applier-gate predicate succeeded
         end-to-end.

      2. ``FakeApiClient.genie_patch_calls()`` is non-empty and every
         call targets ``/api/2.0/genie/spaces/{space_id}``. The
         applier built and shipped a PATCH; the harness sees the
         exact wire payload it would have sent to Databricks.

      3. The decoded PATCH body's ``instructions.example_question_sqls``
         carries the synthesized ``example_sql`` substring. This is
         the canonical proof the Stage 3 tape's ``patch_body``
         survived canonicalisation and survived the applier's
         strict-validate pass — not just that *some* PATCH was sent.

      4. The applier-gate ``GSO_PATCH_OUTCOME_V1`` marker emits at
         least one ``outcome="applied"`` payload, and the qstate
         transition log includes ``applyable → applied`` for every
         hard QID. Both surfaces are what postmortems read to
         attribute applies; their absence here would be the same
         silent regression that haunted earlier trials.

    The state may terminate downstream of APPLIED because the
    ``evaluated_gate`` requires eval kwargs that this harness
    intentionally does not wire — that is Step 3's surface, not
    Step 2's. The assertions therefore key on ``deepest_stage_reached``
    rather than ``current_stage``.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))
    _disable_genie_patch_backoff(monkeypatch)

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    assert qids, "production fixture must declare hard hydration QIDs"

    tape = _stock_forward_tape(qids)
    harness = TapeReplayHarness(tape=tape)

    ws = FakeWorkspaceClient()
    metadata_snapshot = minimal_valid_metadata_snapshot()
    # Trial 13i — supply the run-level schema_columns channel (derived
    # from the rows' ASI blame refs) alongside the serialized_space so
    # Stage 1 clears the ``missing_schema_columns`` pre-flight. The
    # ``SerializedSpace`` model is ``extra="allow"`` so this extra key is
    # inert for applier validation, mirroring how production snapshots
    # carry it.
    metadata_snapshot["schema_columns"] = forward_metadata_snapshot(rows)[
        "schema_columns"
    ]

    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
        stage_index,
    )

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), harness.patch():
        final_states = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="applied-success",
            run_root=tmp_path,
            workspace_client=ws,
            space_id=_FAKE_SPACE_ID,
            metadata_snapshot=metadata_snapshot,
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()

    assert elapsed < _ACCEPTANCE_CEILING_SECONDS, (
        f"Apply-success replay took {elapsed:.2f}s; ceiling is "
        f"{_ACCEPTANCE_CEILING_SECONDS}s. A slow apply test would "
        f"defeat the purpose of bypassing the Databricks deploy."
    )

    # Surface 1: deepest stage reached APPLIED for every hard QID.
    by = states_by_qid(final_states)
    assert set(by) == set(qids), (
        f"SM admitted {sorted(by)!r}; expected {sorted(qids)!r}."
    )
    applied_idx = stage_index(FunnelStage.APPLIED)
    for qid in qids:
        s = by[qid]
        actual_idx = stage_index(s.deepest_stage_reached)
        assert actual_idx >= applied_idx, (
            f"qid={qid!r} deepest_stage_reached="
            f"{s.deepest_stage_reached.value!r} (index {actual_idx}); "
            f"expected APPLIED (index {applied_idx}) or deeper. "
            f"terminal={s.terminal!r}. The applier-gate predicate "
            f"failed even though the FakeApiClient was wired."
        )

    # Surface 2: at least one Genie PATCH was recorded, targeting the
    # right space_id.
    patch_calls = ws.api_client.genie_patch_calls()
    assert patch_calls, (
        "FakeApiClient recorded zero Genie PATCH calls — the applier "
        "never reached the wire surface. all calls="
        f"{[(c.method, c.path) for c in ws.api_client.calls]!r}"
    )
    for c in patch_calls:
        assert c.method == "PATCH", (
            f"non-PATCH method recorded at Genie path: {c.method!r}"
        )
        assert c.path == _GENIE_PATCH_PATH, (
            f"PATCH path mismatch: expected {_GENIE_PATCH_PATH!r}, "
            f"got {c.path!r}. Bad space_id threading?"
        )
        assert c.body is not None and "serialized_space" in c.body, (
            f"PATCH body missing 'serialized_space' field; got keys "
            f"{sorted(c.body.keys()) if c.body else None!r}"
        )

    # Surface 3: the decoded PATCH body carries the synthesized
    # example SQL the Stage 3 tape produced. We expect at least one
    # PATCH whose ``instructions.example_question_sqls`` contains the
    # tape's example_sql substring (the applier may canonicalise the
    # surrounding shape — wrap question into a list, sort, etc. — so
    # we match on the SQL substring rather than the wrapper).
    tape_example_sql_fragment = "SELECT order_id, SUM(amount) AS total FROM orders"
    matched = False
    for c in patch_calls:
        if c.body_json is None:
            continue
        instructions = c.body_json.get("instructions") or {}
        eqs = instructions.get("example_question_sqls") or []
        for entry in eqs:
            sql = entry.get("sql") or entry.get("query") or ""
            sql_str = sql if isinstance(sql, str) else " ".join(
                str(s) for s in (sql if isinstance(sql, list) else [sql])
            )
            if tape_example_sql_fragment in sql_str:
                matched = True
                break
        if matched:
            break
    assert matched, (
        f"No PATCH body carried the tape-supplied example SQL "
        f"({tape_example_sql_fragment!r}). Either the applier dropped "
        f"the patch silently or the example_question_sqls path moved. "
        f"first patch body_json keys: "
        f"{sorted((patch_calls[0].body_json or {}).keys())!r}"
    )

    # Surface 4: applier_gate emitted ``outcome=applied`` markers and
    # the qstate transition log shows ``applyable → applied`` per QID.
    outcome_markers = parse_patch_outcome_markers(stdout)
    applied_outcomes = [
        m for m in outcome_markers if m.get("outcome") == "applied"
    ]
    assert applied_outcomes, (
        "applier_gate emitted no GSO_PATCH_OUTCOME_V1 with "
        f"outcome='applied'. all outcomes: {outcome_markers!r}"
    )
    by_qid_steps: dict[str, list[tuple[str, str]]] = {}
    for t in parse_qstate_transitions(stdout):
        by_qid_steps.setdefault(t["qid"], []).append(
            (t["from_stage"], t["to_stage"])
        )
    for qid in qids:
        steps = by_qid_steps.get(qid, [])
        assert ("applyable", "applied") in steps, (
            f"qid={qid!r} missing applyable → applied transition; "
            f"observed steps: {steps!r}."
        )

    # Sanity: ``add_example_sql`` does not fire UC DDL, so the
    # statement-execution recorder must be empty. If a future patch
    # type does fire UC DDL, the recorder will grow — that surface
    # is intentionally captured so the test fails noisily rather
    # than silently leaking writes.
    assert not ws.statement_execution.statements, (
        f"FakeStatementExecution unexpectedly captured "
        f"{len(ws.statement_execution.statements)} UC statement(s) "
        f"for a forward tape that should only fire Genie PATCHes. "
        f"sample: {ws.statement_execution.statements[:2]!r}"
    )


@pytest.mark.integration
def test_apply_failure_cycles_back_with_typed_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the Genie PATCH fails at the wire, ``applier_gate`` must
    surface a typed ``ProposalAttempt`` with
    ``outcome="applyability_rejected"`` carrying the wire error text,
    and the state must NOT advance to APPLIED.

    Locks in three surfaces:

      1. ``deepest_stage_reached`` stops at ``APPLYABLE`` (or earlier
         after the escalation cycle gives up) — never ``APPLIED``.

      2. At least one ``GSO_GATE_REASONING_V1`` line fires with
         ``gate=applier_gate verdict=rejected`` and the configured
         error message appears in ``reason``.

      3. The corresponding QID's state has at least one
         ``ProposalAttempt`` with
         ``outcome="applyability_rejected"`` so postmortems can
         attribute the failure to the applier without parsing
         stdout.

    Models the canonical production failure shape: the Genie API
    rejects the PATCH and the applier propagates the exception
    text into the apply_log's ``patch_error`` field.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))
    _disable_genie_patch_backoff(monkeypatch)

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)

    tape = _stock_forward_tape(qids)
    harness = TapeReplayHarness(tape=tape)

    # The applier surfaces ``patch_error`` from the raised exception;
    # picking a recognisable token lets the assertion match on a
    # substring without binding to the full SDK error class hierarchy.
    error_token = "fake_genie_patch_rejected_payload_too_large"

    def on_request(method: str, path: str, body):
        if method == "PATCH" and path.startswith("/api/2.0/genie/spaces/"):
            return RuntimeError(error_token)
        return None

    ws = FakeWorkspaceClient.with_handler(on_request)
    metadata_snapshot = minimal_valid_metadata_snapshot()
    # Trial 13i — supply the run-level schema_columns channel (derived
    # from the rows' ASI blame refs) alongside the serialized_space so
    # Stage 1 clears the ``missing_schema_columns`` pre-flight. The
    # ``SerializedSpace`` model is ``extra="allow"`` so this extra key is
    # inert for applier validation, mirroring how production snapshots
    # carry it.
    metadata_snapshot["schema_columns"] = forward_metadata_snapshot(rows)[
        "schema_columns"
    ]

    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
        stage_index,
    )

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), harness.patch():
        final_states = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="applied-failure",
            run_root=tmp_path,
            workspace_client=ws,
            space_id=_FAKE_SPACE_ID,
            metadata_snapshot=metadata_snapshot,
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()

    assert elapsed < _ACCEPTANCE_CEILING_SECONDS, (
        f"Apply-failure replay took {elapsed:.2f}s; ceiling is "
        f"{_ACCEPTANCE_CEILING_SECONDS}s."
    )

    # Surface 1: no QID reaches APPLIED.
    by = states_by_qid(final_states)
    applied_idx = stage_index(FunnelStage.APPLIED)
    leaked_applies = [
        qid for qid, s in by.items()
        if stage_index(s.deepest_stage_reached) >= applied_idx
    ]
    assert not leaked_applies, (
        f"applier_gate was wired to fail every PATCH but the "
        f"following QIDs leaked through to APPLIED: {leaked_applies!r}. "
        f"FakeApiClient calls so far: "
        f"{len(ws.api_client.genie_patch_calls())}"
    )

    # The fake must have observed at least one PATCH attempt — proof
    # the state actually reached the applier-gate predicate.
    assert ws.api_client.genie_patch_calls(), (
        "FakeApiClient recorded zero PATCH attempts even though the "
        "Stage 3 tape produced a contract-passing proposal. The "
        "state machine did not reach the applier-gate predicate."
    )

    # Surface 2: typed gate-reasoning marker carrying the error token.
    gate_markers = parse_gate_reasoning_markers(stdout)
    applier_rejects = [
        m for m in gate_markers
        if m["gate"] == "applier_gate" and m["verdict"] == "rejected"
    ]
    assert applier_rejects, (
        "applier_gate emitted no rejection marker even though every "
        f"PATCH raised. gate_markers={gate_markers!r}"
    )
    matched_reason = [m for m in applier_rejects if error_token in m["reason"]]
    assert matched_reason, (
        f"No applier_gate rejection marker carries the configured "
        f"error token {error_token!r} in its reason. "
        f"applier_rejects={applier_rejects[:2]!r}"
    )

    # Surface 3: typed PatchOutcome with outcome=applyability_rejected on
    # at least one QID. Trial 16 RC3 changed ``applier_gate`` to TERMINATE
    # the QID with a typed ``forbidden_signature`` instead of cycling back
    # to PROPOSED (the old escalation loop re-attempted the same dead-end
    # proposal up to 32× per qid). Because the terminal path runs through
    # ``state.terminate(...)`` rather than ``state.advance(...,
    # proposals=...+(rejected,))``, the rejected ``ProposalAttempt`` is no
    # longer appended to ``state.proposals`` — it is emitted as a
    # ``GSO_PATCH_OUTCOME_V1`` marker, which ``patch_outcome_marker_from_attempt``
    # documents as the canonical source-of-truth surface postmortems read
    # (state machine emits the marker at the same moment it builds the
    # attempt). Assert on that marker rather than the (now-unpopulated)
    # state.proposals tuple.
    patch_outcomes = parse_patch_outcome_markers(stdout)
    rejected_qids = {m["qid"] for m in applier_rejects}
    for qid in rejected_qids:
        rejected_outcomes = [
            o for o in patch_outcomes
            if o.get("qid") == qid
            and o.get("outcome") == "applyability_rejected"
        ]
        assert rejected_outcomes, (
            f"qid={qid!r} has no GSO_PATCH_OUTCOME_V1 marker with "
            f"outcome='applyability_rejected' despite the applier-gate "
            f"rejection marker. patch_outcomes for qid="
            f"{[o for o in patch_outcomes if o.get('qid') == qid]!r}"
        )
