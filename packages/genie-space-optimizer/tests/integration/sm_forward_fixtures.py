"""Shared fixtures and assertion helpers for forward-pipeline SM tests.

These helpers wrap the production-shape eval rows captured in the
hydration sweep so every forward test loads the same data and
verifies the same funnel progress invariants. They are deliberately
test-only: the only production imports are typed optimizer/state
surfaces the helpers must reference.

See ``fast-optimizer-testing`` plan, Task 1, for the rationale.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
    stage_index,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)


_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "unit"
    / "fixtures"
    / "production_eval_rows.json"
)


def load_production_hydration_rows() -> list[dict]:
    """Return the five **synthetic shape-ladder** eval rows.

    Despite the name (preserved for back-compat), these rows are NOT a
    faithful snapshot of the production hard-QID lane. Each carries
    ``question`` text, expected SQL, generated SQL, judge rationale,
    and a full ASI metadata block — every variant trivially passes
    :class:`Stage1InputEvidenceContract` by construction. They prove
    "if a row carries the question, the SM/builders advance"; they do
    NOT prove "production rows reach DIAGNOSED".

    Tests that want the latter must call
    :func:`tests.integration.replay_row_loader.load_production_case`
    against the sanitized corpus under
    ``tests/integration/fixtures/production_replay/``. Production rows
    carry only ``question_id`` / ``result_correctness`` / ``arbiter``
    and produce a ``question_text_empty`` violation today.
    """
    payload = json.loads(_FIXTURE_PATH.read_text())
    rows = payload.get("hydration_rows") or []
    if not rows:
        raise RuntimeError(
            f"production_eval_rows.json missing 'hydration_rows'; "
            f"forward-pipeline tests rely on the synthetic shape-ladder "
            f"rows the Stage 1 hydration sweep introduced."
        )
    return [dict(r) for r in rows]


def expected_hard_qids(rows: Iterable[dict] | None = None) -> tuple[str, ...]:
    """Return the QID set tests expect to be admitted as hard failures."""
    rows = list(rows) if rows is not None else load_production_hydration_rows()
    return tuple(
        str(r.get("_expected_qid"))
        for r in rows
        if r.get("_expected_hard") and r.get("_expected_qid")
    )


def _row_blame_refs(row: dict) -> list[str]:
    """Return the blame column refs a single hydration row references.

    Reads the ASI ``blame_set`` (and the structured mirror's ``ref``
    tokens) from the row's ``arbiter/metadata`` block — the exact
    tokens the Stage 1 seed normalizer
    (:func:`schema_columns._normalize_seeds_to_fqn`) consumes. Returns
    them verbatim (no FQN expansion): the normalizer resolves a seed
    only by rule-1 passthrough (literal membership in ``schema_columns``)
    or rule-2 bare-identifier suffix match. The synthetic shape-ladder
    rows emit dotted 2-part refs (``table.col_a``) which are neither
    bare nor 4-part, so the run-level ``schema_columns`` channel must
    carry them literally for the seeds to survive normalization.
    """
    md = row.get("arbiter/metadata") or {}
    refs: list[str] = []
    for b in md.get("blame_set") or ():
        token = str(b or "").strip()
        if token:
            refs.append(token)
    for entry in md.get("blame_set_structured") or ():
        if isinstance(entry, dict):
            token = str(entry.get("ref") or "").strip()
            if token:
                refs.append(token)
    return refs


def forward_metadata_snapshot(rows: Iterable[dict] | None = None) -> dict:
    """Return a ``metadata_snapshot`` carrying the run-level ``schema_columns``.

    The forward/tape SM tests drive
    ``optimizer.run_state_machine_iteration_and_persist`` directly; that
    function derives run-level ``schema_columns`` from
    ``metadata_snapshot["schema_columns"]``
    (:func:`schema_columns._derive_schema_columns`, priority 1). Without
    this channel the Trial 13i pre-flight contract
    (:meth:`Stage1InputEvidenceContract.validate_schema_columns`)
    short-circuits every QID with
    ``evidence_card_empty:missing_schema_columns`` before any tape entry
    is consumed — the input-contract drift behind Bucket A.

    The ``schema_columns`` value is *derived from the rows* (their ASI
    blame refs) rather than hardcoded, so the same tokens the per-card
    blame_set_seed normalizer needs are guaranteed present. Returns a
    bare ``{"schema_columns": [...]}`` dict; callers that also feed the
    applier merge these keys into their ``serialized_space`` snapshot
    (``SerializedSpace`` is ``extra="allow"`` so the extra key is inert
    for validation, exactly as production snapshots carry it).
    """
    rows = list(rows) if rows is not None else load_production_hydration_rows()
    seen: set[str] = set()
    schema_columns: list[str] = []
    for row in rows:
        for ref in _row_blame_refs(row):
            if ref not in seen:
                seen.add(ref)
                schema_columns.append(ref)
    return {"schema_columns": schema_columns}


# ── Marker parsing ────────────────────────────────────────────────────


_MARKER_LINE_RE = re.compile(r"^([A-Z][A-Z0-9_]*) (\{.+\})$", re.MULTILINE)


def parse_markers(stdout: str, marker_name: str) -> list[dict]:
    """Return all JSON payloads emitted for ``marker_name`` in ``stdout``.

    Markers are emitted via :func:`run_analysis_contract.marker_line`
    as ``<NAME> <json>`` per line. Some markers (e.g. legacy
    ``GSO_PLAN_V3_CANARY_V1``) use ``key=value`` instead of JSON; this
    parser ignores those and only returns the structured payloads.
    """
    out: list[dict] = []
    for match in _MARKER_LINE_RE.finditer(stdout or ""):
        if match.group(1) != marker_name:
            continue
        try:
            out.append(json.loads(match.group(2)))
        except json.JSONDecodeError:
            continue
    return out


def parse_stage1_diagnosis_markers(stdout: str) -> list[dict]:
    return parse_markers(stdout, "GSO_PLAN11_STAGE1_DIAGNOSIS_V1")


def parse_stage2_clustering_markers(stdout: str) -> list[dict]:
    return parse_markers(stdout, "GSO_PLAN11_STAGE2_CLUSTERING_V1")


def parse_stage3_synthesis_markers(stdout: str) -> list[dict]:
    return parse_markers(stdout, "GSO_PLAN11_STAGE3_SYNTHESIS_V1")


def parse_qstate_transitions(stdout: str) -> list[dict]:
    return parse_markers(stdout, "GSO_QSTATE_TRANSITION_V1")


def parse_stage1_input_card_empty_markers(stdout: str) -> list[dict]:
    return parse_markers(stdout, "GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1")


# ``GSO_GATE_REASONING_V1`` is a space-separated ``key=value`` line that
# ends in a JSON-encoded ``predicate_inputs=<json>`` tail. It is NOT a
# ``<NAME> {json}`` payload so :func:`parse_markers` cannot consume it.
# This regex captures the four flat keys and the JSON tail in one pass;
# the parser then ``json.loads`` only the ``predicate_inputs`` value.
_GATE_REASONING_RE = re.compile(
    r"^GSO_GATE_REASONING_V1 "
    r"gate=(?P<gate>\S+) "
    r"qid=(?P<qid>\S+) "
    r"verdict=(?P<verdict>\S+) "
    r"reason=(?P<reason>.*?) "
    r"predicate_inputs=(?P<predicate_inputs>\{.*\})$",
    re.MULTILINE,
)


def parse_gate_reasoning_markers(stdout: str) -> list[dict]:
    """Return one dict per ``GSO_GATE_REASONING_V1`` line in ``stdout``.

    Each dict has flat string fields ``gate``, ``qid``, ``verdict``,
    ``reason`` and a parsed ``predicate_inputs`` mapping. ``reason``
    keeps the raw production-shaped string so substring assertions
    (``"blast_radius_exceeds_threshold" in m["reason"]``) work the same
    way they do against real trial logs.
    """
    out: list[dict] = []
    for m in _GATE_REASONING_RE.finditer(stdout or ""):
        d = m.groupdict()
        try:
            d["predicate_inputs"] = json.loads(d["predicate_inputs"])
        except json.JSONDecodeError:
            d["predicate_inputs"] = {}
        out.append(d)
    return out


def parse_patch_outcome_markers(stdout: str) -> list[dict]:
    """Return all ``GSO_PATCH_OUTCOME_V1`` payloads emitted in ``stdout``.

    These are emitted by ``applier_gate`` on both success
    (``outcome="applied"``) and rejection
    (``outcome="applyability_rejected"``). Their presence proves the
    state machine reached :class:`FunnelStage.APPLYABLE` and the
    applier-gate predicate ran — the canonical applyability boundary.
    """
    return parse_markers(stdout, "GSO_PATCH_OUTCOME_V1")


# ── Funnel assertion helpers ──────────────────────────────────────────


def assert_stage_reached(
    final_states: tuple[QuestionStateInIteration, ...],
    qid: str,
    stage: FunnelStage,
) -> None:
    """Assert that ``qid`` reached at least ``stage`` (by funnel index).

    Uses ``deepest_stage_reached`` so a clean cycle back to PROPOSED
    does not falsely fail this assertion. TERMINATED is never counted
    as "reached" because it is an absorbing state, not a depth.
    """
    matching = [s for s in final_states if s.qid == qid]
    if not matching:
        raise AssertionError(
            f"qid={qid!r} not present in final_states; got "
            f"{sorted(s.qid for s in final_states)!r}. The SM probably "
            f"failed to admit this QID as a hard row."
        )
    state = matching[0]
    actual = state.deepest_stage_reached
    if actual == FunnelStage.TERMINATED:
        raise AssertionError(
            f"qid={qid!r} deepest_stage_reached should never be "
            f"TERMINATED (it is an absorbing state). Inspect "
            f"state.transitions for the funnel progress before "
            f"termination."
        )
    if stage_index(actual) < stage_index(stage):
        terminal_reason = (
            state.terminal.reason if state.terminal else "<no terminal>"
        )
        raise AssertionError(
            f"qid={qid!r} reached {actual.value!r}; expected at "
            f"least {stage.value!r}. terminal_reason="
            f"{terminal_reason!r}. Latest transition: "
            f"{state.transitions[-1] if state.transitions else None!r}"
        )


def assert_no_terminal_reason(
    final_states: tuple[QuestionStateInIteration, ...],
    reason_fragment: str,
) -> None:
    """Assert no QID terminated with a reason matching ``reason_fragment``.

    ``reason_fragment`` is a case-sensitive substring match against
    ``TerminalRecord.reason``. Use specific fragments like
    ``"diagnose_returned_empty"`` or ``"missing_schema_context"`` so
    a regression of a known failure shape lights up immediately.
    """
    offenders = []
    for state in final_states:
        if state.terminal is None:
            continue
        if reason_fragment in state.terminal.reason:
            offenders.append((state.qid, state.terminal.reason))
    if offenders:
        rendered = "\n  ".join(
            f"qid={qid} reason={reason}" for qid, reason in offenders
        )
        raise AssertionError(
            f"{len(offenders)} QIDs terminated with "
            f"{reason_fragment!r}:\n  {rendered}"
        )


def assert_no_terminal_qids(
    final_states: tuple[QuestionStateInIteration, ...],
    qids: Iterable[str],
) -> None:
    """Assert none of ``qids`` terminated via ``TerminalRecord``.

    A QID may cycle back to PROPOSED on a structural reject and still
    be "live" — this helper only fails if a QID hit a terminal record.
    """
    qids_set = set(qids)
    offenders = [
        (s.qid, s.terminal.kind, s.terminal.reason)
        for s in final_states
        if s.qid in qids_set and s.terminal is not None
    ]
    if offenders:
        rendered = "\n  ".join(
            f"qid={qid} kind={kind} reason={reason}"
            for qid, kind, reason in offenders
        )
        raise AssertionError(
            f"{len(offenders)} of {len(qids_set)} expected-live QIDs "
            f"terminated:\n  {rendered}"
        )


def states_by_qid(
    states: tuple[QuestionStateInIteration, ...],
) -> dict[str, QuestionStateInIteration]:
    return {s.qid: s for s in states}


__all__ = [
    "assert_no_terminal_qids",
    "assert_no_terminal_reason",
    "assert_stage_reached",
    "expected_hard_qids",
    "forward_metadata_snapshot",
    "load_production_hydration_rows",
    "parse_gate_reasoning_markers",
    "parse_markers",
    "parse_patch_outcome_markers",
    "parse_qstate_transitions",
    "parse_stage1_diagnosis_markers",
    "parse_stage1_input_card_empty_markers",
    "parse_stage2_clustering_markers",
    "parse_stage3_synthesis_markers",
    "states_by_qid",
]
