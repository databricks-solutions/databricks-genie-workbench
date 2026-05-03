# Unified Trace And Operator Transcript Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Phase B's canonical optimizer decision trace so replay, fixtures, persistence, and stdout all derive from one typed `OptimizationTrace` model grounded in RCA evidence.

**Architecture:** Extend the existing `optimization/rca_decision_trace.py` module into the Phase B contract owner instead of creating a second trace system. `DecisionRecord` is the canonical row model; `OptimizationTrace` owns journey events, decision records, validation projections, and transcript sections; replay and fixture export preserve `iterations[N].decision_records`; the operator transcript renderer is a deterministic projection over the same trace and is the only standard stdout path for optimizer diagnosis.

**Tech Stack:** Python 3.11+, dataclasses, enums, deterministic JSON, pytest, existing GSO replay fixtures, existing `QuestionJourneyEvent` / `JourneyValidationReport` contracts.

---

## File Structure

### Create

- `packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py`
  - Contract tests for `DecisionRecord`, `OptimizationTrace`, deterministic serialization, transcript rendering, and cross-check violations.

### Modify

- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py`
  - Becomes the canonical Phase B trace module.
  - Keeps existing `summarize_patch_for_trace`, `patch_cap_decision_rows`, and `format_patch_inventory` APIs.
  - Adds `DecisionRecord`, `DecisionType`, `DecisionOutcome`, `ReasonCode`, `OptimizationTrace`, deterministic serializers, transcript renderer, RCA/evidence fields, and journey/decision cross-checks.

- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/journey_fixture_exporter.py`
  - Allows `decision_records` in each iteration snapshot.
  - Initializes `decision_records` in `begin_iteration_capture`.
  - Counts `decision_records` in `summarize_replay_fixture`.

- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py`
  - Extends `ReplayResult` with `decision_records`, `canonical_decision_json`, `operator_transcript`, and `decision_validation`.
  - Loads fixture decision records when present.
  - Returns an empty decision trace when legacy fixtures do not yet contain decision records.

- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
  - Captures patch-cap `DecisionRecord` dictionaries into `_current_iter_inputs["decision_records"]`.
  - Logs per-iteration transcript and decision trace artifacts to MLflow when an active run exists.
  - Keeps existing Delta decision persistence by converting typed decision records back into legacy row dictionaries.
  - Treats existing direct print blocks as migration shims; new operator-visible diagnostics must flow through trace records and the transcript renderer.

- `packages/genie-space-optimizer/tests/unit/test_journey_fixture_exporter.py`
  - Adds pass-through, initialization, and summary tests for `decision_records`.

- `packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py`
  - Adds replay assertions for canonical decision JSON, operator transcript, and journey/decision cross-checks.

- `packages/genie-space-optimizer/tests/unit/test_rca_decision_trace.py`
  - Updates patch-cap tests to assert both legacy decision rows and new typed decision records.

---

## Design Rules

1. `DecisionRecord` is the source-of-truth model. Legacy Delta rows are adapters.
2. Deterministic serialization sorts records and omits volatile fields.
3. Fixture export preserves `decision_records` byte-for-byte except for stripping unknown keys inside each record.
4. Replay remains fast and offline; no Genie, LLM, warehouse, Delta, or MLflow calls.
5. The first implementation slice is contract-first: no scoreboard and no deeper `harness.py` extraction.
6. No optimizer decision is valid unless it can be traced from evidence -> RCA -> causal target qids -> proposed patch -> gate rationale -> applied/skipped outcome -> observed eval result -> next action.
7. Standard output is a deterministic projection of `OptimizationTrace`, not a separate logging path. New observability must add typed trace fields or renderer sections, not ad hoc print blocks in `harness.py`.

## Required Decision Fields

Every applicable `DecisionRecord` must support these RCA-grounded fields:

- `evidence_refs`: trace IDs, eval-row references, judge/ASI IDs, SQL-diff IDs, or replay fixture references.
- `rca_id`: stable identifier for the RCA card/theme/plan.
- `root_cause`: normalized RCA class being acted on.
- `target_qids`: benchmark qids the decision claims to help.
- `expected_effect`: specific behavior the patch/decision expects to change.
- `observed_effect`: post-eval result observed after the decision.
- `regression_qids`: out-of-target qids harmed or accepted as regression debt.
- `reason_code`: stable enum for replay assertions and dashboards.
- `next_action`: operator or optimizer follow-up implied by the result.

---

## Task 1: Add the Typed Decision Trace Contract

**Files:**
- Create: `packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py`

- [ ] **Step 1: Write failing tests for model serialization and stable sort**

Add this file:

```python
from __future__ import annotations

import json


def test_decision_record_to_dict_uses_stable_json_safe_shape() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    rec = DecisionRecord(
        run_id="run_1",
        iteration=2,
        decision_type=DecisionType.GATE_DECISION,
        outcome=DecisionOutcome.DROPPED,
        reason_code=ReasonCode.PATCH_CAP_DROPPED,
        question_id="q2",
        cluster_id="H002",
        ag_id="AG1",
        proposal_id="P002",
        patch_id="P002#1",
        gate="patch_cap",
        reason_detail="lower_causal_rank",
        affected_qids=("q2",),
        evidence_refs=("eval:q2", "rca:H002"),
        root_cause="wrong_column",
        target_qids=("q2",),
        expected_effect="Patch should correct q2's column mapping.",
        observed_effect="Patch was dropped before apply.",
        regression_qids=("q9",),
        next_action="Inspect lower-ranked RCA patch before relaxing cap.",
        source_cluster_ids=("H002",),
        proposal_ids=("P002",),
        metrics={"rank": 2, "relevance_score": 0.42},
    )

    assert rec.to_dict() == {
        "run_id": "run_1",
        "iteration": 2,
        "decision_type": "gate_decision",
        "outcome": "dropped",
        "reason_code": "patch_cap_dropped",
        "question_id": "q2",
        "cluster_id": "H002",
        "ag_id": "AG1",
        "proposal_id": "P002",
        "patch_id": "P002#1",
        "gate": "patch_cap",
        "reason_detail": "lower_causal_rank",
        "affected_qids": ["q2"],
        "evidence_refs": ["eval:q2", "rca:H002"],
        "root_cause": "wrong_column",
        "target_qids": ["q2"],
        "expected_effect": "Patch should correct q2's column mapping.",
        "observed_effect": "Patch was dropped before apply.",
        "regression_qids": ["q9"],
        "next_action": "Inspect lower-ranked RCA patch before relaxing cap.",
        "source_cluster_ids": ["H002"],
        "proposal_ids": ["P002"],
        "metrics": {"rank": 2, "relevance_score": 0.42},
    }


def test_canonical_decision_json_is_order_independent() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
        canonical_decision_json,
    )

    later = DecisionRecord(
        run_id="run_1",
        iteration=2,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode.PATCH_APPLIED,
        question_id="q2",
        proposal_id="P002",
    )
    earlier = DecisionRecord(
        run_id="run_1",
        iteration=1,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.HARD_FAILURE,
        question_id="q1",
    )

    left = canonical_decision_json([later, earlier])
    right = canonical_decision_json([earlier, later])

    assert left == right
    assert json.loads(left)[0]["iteration"] == 1
    assert json.loads(left)[1]["decision_type"] == "patch_applied"
```

- [ ] **Step 2: Run the failing tests**

Run from the repository root:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py -q
```

Expected: FAIL with an import error for `DecisionRecord`, `DecisionType`, `DecisionOutcome`, or `ReasonCode`.

- [ ] **Step 3: Add the minimal typed contract**

Insert this block near the top of `rca_decision_trace.py`, after the imports:

```python
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Sequence


class DecisionType(str, Enum):
    EVAL_CLASSIFIED = "eval_classified"
    CLUSTER_SELECTED = "cluster_selected"
    RCA_FORMED = "rca_formed"
    STRATEGIST_AG_EMITTED = "strategist_ag_emitted"
    PROPOSAL_GENERATED = "proposal_generated"
    GATE_DECISION = "gate_decision"
    PATCH_APPLIED = "patch_applied"
    PATCH_SKIPPED = "patch_skipped"
    ACCEPTANCE_DECIDED = "acceptance_decided"
    QID_RESOLUTION = "qid_resolution"


class DecisionOutcome(str, Enum):
    INFO = "info"
    ACCEPTED = "accepted"
    DROPPED = "dropped"
    APPLIED = "applied"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"


class ReasonCode(str, Enum):
    NONE = "none"
    ALREADY_PASSING = "already_passing"
    HARD_FAILURE = "hard_failure"
    SOFT_SIGNAL = "soft_signal"
    GT_CORRECTION = "gt_correction"
    CLUSTERED = "clustered"
    RCA_GROUNDED = "rca_grounded"
    RCA_UNGROUNDED = "rca_ungrounded"
    STRATEGIST_SELECTED = "strategist_selected"
    PROPOSAL_EMITTED = "proposal_emitted"
    NO_CAUSAL_TARGET = "no_causal_target"
    PATCH_CAP_SELECTED = "patch_cap_selected"
    PATCH_CAP_DROPPED = "patch_cap_dropped"
    PATCH_APPLIED = "patch_applied"
    PATCH_SKIPPED = "patch_skipped"
    MISSING_TARGET_QIDS = "missing_target_qids"
    NO_APPLIED_PATCHES = "no_applied_patches"
    POST_EVAL_HOLD_PASS = "post_eval_hold_pass"
    POST_EVAL_FAIL_TO_PASS = "post_eval_fail_to_pass"
    POST_EVAL_HOLD_FAIL = "post_eval_hold_fail"
    POST_EVAL_PASS_TO_FAIL = "post_eval_pass_to_fail"


def _enum_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value or "")


def _clean_str_tuple(values: Sequence[Any] | None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(v) for v in (values or ()) if str(v)))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


@dataclass(frozen=True)
class DecisionRecord:
    run_id: str = ""
    iteration: int = 0
    decision_type: DecisionType = DecisionType.EVAL_CLASSIFIED
    outcome: DecisionOutcome = DecisionOutcome.INFO
    reason_code: ReasonCode = ReasonCode.NONE
    question_id: str = ""
    cluster_id: str = ""
    rca_id: str = ""
    ag_id: str = ""
    proposal_id: str = ""
    patch_id: str = ""
    gate: str = ""
    reason_detail: str = ""
    affected_qids: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    root_cause: str = ""
    target_qids: tuple[str, ...] = ()
    expected_effect: str = ""
    observed_effect: str = ""
    regression_qids: tuple[str, ...] = ()
    next_action: str = ""
    source_cluster_ids: tuple[str, ...] = ()
    proposal_ids: tuple[str, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "run_id": str(self.run_id),
            "iteration": int(self.iteration),
            "decision_type": self.decision_type.value,
            "outcome": self.outcome.value,
            "reason_code": self.reason_code.value,
        }
        optional = {
            "question_id": self.question_id,
            "cluster_id": self.cluster_id,
            "rca_id": self.rca_id,
            "ag_id": self.ag_id,
            "proposal_id": self.proposal_id,
            "patch_id": self.patch_id,
            "gate": self.gate,
            "reason_detail": self.reason_detail,
            "root_cause": self.root_cause,
            "expected_effect": self.expected_effect,
            "observed_effect": self.observed_effect,
            "next_action": self.next_action,
        }
        for key, value in optional.items():
            if value:
                row[key] = str(value)
        if self.affected_qids:
            row["affected_qids"] = list(self.affected_qids)
        if self.evidence_refs:
            row["evidence_refs"] = list(self.evidence_refs)
        if self.target_qids:
            row["target_qids"] = list(self.target_qids)
        if self.regression_qids:
            row["regression_qids"] = list(self.regression_qids)
        if self.source_cluster_ids:
            row["source_cluster_ids"] = list(self.source_cluster_ids)
        if self.proposal_ids:
            row["proposal_ids"] = list(self.proposal_ids)
        if self.metrics:
            row["metrics"] = _json_safe(dict(self.metrics))
        return row

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "DecisionRecord":
        return cls(
            run_id=str(row.get("run_id") or ""),
            iteration=_as_int(row.get("iteration")),
            decision_type=DecisionType(str(row.get("decision_type") or "eval_classified")),
            outcome=DecisionOutcome(str(row.get("outcome") or "info")),
            reason_code=ReasonCode(str(row.get("reason_code") or "none")),
            question_id=str(row.get("question_id") or ""),
            cluster_id=str(row.get("cluster_id") or ""),
            rca_id=str(row.get("rca_id") or ""),
            ag_id=str(row.get("ag_id") or ""),
            proposal_id=str(row.get("proposal_id") or ""),
            patch_id=str(row.get("patch_id") or ""),
            gate=str(row.get("gate") or ""),
            reason_detail=str(row.get("reason_detail") or ""),
            affected_qids=_clean_str_tuple(row.get("affected_qids") or ()),
            evidence_refs=_clean_str_tuple(row.get("evidence_refs") or ()),
            root_cause=str(row.get("root_cause") or ""),
            target_qids=_clean_str_tuple(row.get("target_qids") or ()),
            expected_effect=str(row.get("expected_effect") or ""),
            observed_effect=str(row.get("observed_effect") or ""),
            regression_qids=_clean_str_tuple(row.get("regression_qids") or ()),
            next_action=str(row.get("next_action") or ""),
            source_cluster_ids=_clean_str_tuple(row.get("source_cluster_ids") or ()),
            proposal_ids=_clean_str_tuple(row.get("proposal_ids") or ()),
            metrics=dict(row.get("metrics") or {}),
        )


def _decision_sort_key(rec: DecisionRecord) -> tuple:
    return (
        int(rec.iteration),
        rec.decision_type.value,
        rec.question_id,
        rec.cluster_id,
        rec.ag_id,
        rec.proposal_id,
        rec.patch_id,
        rec.gate,
        rec.reason_code.value,
    )


def canonical_decision_json(records: Sequence[DecisionRecord]) -> str:
    rows = [r.to_dict() for r in sorted(records, key=_decision_sort_key)]
    return json.dumps(rows, sort_keys=True, separators=(",", ":"))
```

- [ ] **Step 4: Run the tests**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py \
  packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py
git commit -m "$(cat <<'EOF'
Add typed decision trace contract.

EOF
)"
```

---

## Task 2: Add `OptimizationTrace` And Transcript Rendering

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py`

- [ ] **Step 1: Add failing tests for trace container and transcript**

Append to `test_decision_trace_contract.py`:

```python
def test_optimization_trace_serializes_decisions_and_renders_transcript() -> None:
    from genie_space_optimizer.optimization.question_journey import QuestionJourneyEvent
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
    )

    trace = OptimizationTrace(
        journey_events=(
            QuestionJourneyEvent(question_id="q1", stage="evaluated"),
            QuestionJourneyEvent(
                question_id="q1",
                stage="clustered",
                cluster_id="H001",
                root_cause="missing_filter",
            ),
        ),
        decision_records=(
            DecisionRecord(
                run_id="run_1",
                iteration=1,
                decision_type=DecisionType.CLUSTER_SELECTED,
                outcome=DecisionOutcome.INFO,
                reason_code=ReasonCode.CLUSTERED,
                question_id="q1",
                cluster_id="H001",
                evidence_refs=("eval:q1",),
                root_cause="missing_filter",
                target_qids=("q1",),
                expected_effect="Cluster should receive a targeted filter patch.",
                next_action="Generate proposals for H001.",
                reason_detail="missing_filter",
            ),
        ),
    )

    assert "cluster_selected" in trace.canonical_decision_json()
    transcript = trace.render_operator_transcript(iteration=1)
    assert "OPERATOR TRANSCRIPT  iteration=1" in transcript
    assert "Decision records: 1" in transcript
    assert "cluster_selected" in transcript
    assert "q1" in transcript
    assert "missing_filter" in transcript
    assert "Generate proposals for H001." in transcript


def test_operator_transcript_has_fixed_diagnostic_sections() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
        render_operator_transcript,
    )

    transcript = render_operator_transcript(
        trace=OptimizationTrace(),
        iteration=3,
    )

    for heading in [
        "Iteration Summary",
        "Hard Failures And QID State",
        "RCA Cards With Evidence",
        "AG Decisions And Rationale",
        "Proposal Survival And Gate Drops",
        "Applied Patches And Acceptance",
        "Observed Results And Regressions",
        "Unresolved QID Buckets",
        "Next Suggested Action",
    ]:
        assert heading in transcript
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py::test_optimization_trace_serializes_decisions_and_renders_transcript -q
```

Expected: FAIL with `ImportError: cannot import name 'OptimizationTrace'`.

- [ ] **Step 3: Add `OptimizationTrace` and transcript renderer**

Append below `canonical_decision_json` in `rca_decision_trace.py`:

```python
@dataclass(frozen=True)
class OptimizationTrace:
    journey_events: tuple[Any, ...] = ()
    decision_records: tuple[DecisionRecord, ...] = ()
    validation_by_iteration: Mapping[int, Mapping[str, Any]] = field(default_factory=dict)

    def canonical_decision_json(self) -> str:
        return canonical_decision_json(self.decision_records)

    def render_operator_transcript(self, *, iteration: int) -> str:
        return render_operator_transcript(trace=self, iteration=iteration)


def render_operator_transcript(
    *,
    trace: OptimizationTrace,
    iteration: int,
) -> str:
    records = [
        r for r in trace.decision_records
        if int(r.iteration) == int(iteration)
    ]
    bar = "-" * 100
    lines = [
        f"+{bar}",
        f"|  OPERATOR TRANSCRIPT  iteration={iteration}",
        f"+{bar}",
        "|  Iteration Summary",
        f"|  Decision records: {len(records)}",
        "|",
        "|  Hard Failures And QID State",
        "|",
        "|  RCA Cards With Evidence",
        "|",
        "|  AG Decisions And Rationale",
        "|",
        "|  Proposal Survival And Gate Drops",
        "|",
        "|  Applied Patches And Acceptance",
        "|",
        "|  Observed Results And Regressions",
        "|",
        "|  Unresolved QID Buckets",
        "|",
        "|  Next Suggested Action",
    ]
    by_type: dict[str, list[DecisionRecord]] = {}
    for rec in sorted(records, key=_decision_sort_key):
        by_type.setdefault(rec.decision_type.value, []).append(rec)
    for dtype in sorted(by_type):
        lines.append(f"|")
        lines.append(f"|  {dtype}")
        for rec in by_type[dtype]:
            qids = list(rec.affected_qids) or ([rec.question_id] if rec.question_id else [])
            target = ",".join(qids) if qids else "-"
            parts = [
                f"outcome={rec.outcome.value}",
                f"reason={rec.reason_code.value}",
                f"qid={target}",
            ]
            if rec.cluster_id:
                parts.append(f"cluster={rec.cluster_id}")
            if rec.ag_id:
                parts.append(f"ag={rec.ag_id}")
            if rec.proposal_id:
                parts.append(f"proposal={rec.proposal_id}")
            if rec.gate:
                parts.append(f"gate={rec.gate}")
            if rec.reason_detail:
                parts.append(f"detail={rec.reason_detail}")
            if rec.root_cause:
                parts.append(f"root={rec.root_cause}")
            if rec.expected_effect:
                parts.append(f"expected={rec.expected_effect}")
            if rec.observed_effect:
                parts.append(f"observed={rec.observed_effect}")
            if rec.next_action:
                parts.append(f"next={rec.next_action}")
            lines.append("|    - " + "  ".join(parts))
    lines.append(f"+{bar}")
    return "\n".join(lines)
```

- [ ] **Step 4: Run the test**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py \
  packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py
git commit -m "$(cat <<'EOF'
Add optimization trace transcript renderer.

EOF
)"
```

---

## Task 3: Convert Patch-Cap Audit Decisions To `DecisionRecord`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_rca_decision_trace.py`

- [ ] **Step 1: Add failing tests for patch-cap decision records**

Append to `test_rca_decision_trace.py`:

```python
def test_patch_cap_decision_records_use_phase_b_contract() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
        patch_cap_decision_records,
    )

    records = patch_cap_decision_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG1",
        decisions=[
            {
                "proposal_id": "P015",
                "decision": "selected",
                "selection_reason": "highest_causal_relevance",
                "rank": 1,
                "relevance_score": 1.0,
                "lever": 5,
                "patch_type": "update_instruction_section",
                "rca_id": "rca_q028_function_routing",
                "target_qids": ["q028"],
            },
            {
                "proposal_id": "P001",
                "decision": "dropped",
                "selection_reason": "lower_causal_rank",
                "rank": 2,
                "relevance_score": 0.55,
                "lever": 5,
                "patch_type": "update_instruction_section",
                "rca_id": "rca_q012_store_count",
                "target_qids": ["q012"],
            },
        ],
    )

    assert [r.decision_type for r in records] == [
        DecisionType.GATE_DECISION,
        DecisionType.GATE_DECISION,
    ]
    assert [r.outcome for r in records] == [
        DecisionOutcome.ACCEPTED,
        DecisionOutcome.DROPPED,
    ]
    assert records[0].reason_code == ReasonCode.PATCH_CAP_SELECTED
    assert records[1].reason_code == ReasonCode.PATCH_CAP_DROPPED
    assert records[0].affected_qids == ("q028",)
    assert records[1].proposal_id == "P001"
    assert records[1].gate == "patch_cap"
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_rca_decision_trace.py::test_patch_cap_decision_records_use_phase_b_contract -q
```

Expected: FAIL with `ImportError: cannot import name 'patch_cap_decision_records'`.

- [ ] **Step 3: Implement typed patch-cap records and keep legacy row adapter**

Insert this function above `patch_cap_decision_rows`:

```python
def patch_cap_decision_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    decisions: list[dict[str, Any]],
) -> list[DecisionRecord]:
    records: list[DecisionRecord] = []
    for decision in decisions:
        proposal_id = str(decision.get("proposal_id") or "")
        selected = decision.get("decision") == "selected"
        target_qids = _clean_str_tuple(decision.get("target_qids") or ())
        rca_id = str(decision.get("rca_id") or "")
        root_cause = str(decision.get("root_cause") or "")
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.GATE_DECISION,
                outcome=DecisionOutcome.ACCEPTED if selected else DecisionOutcome.DROPPED,
                reason_code=(
                    ReasonCode.PATCH_CAP_SELECTED
                    if selected else ReasonCode.PATCH_CAP_DROPPED
                ),
                question_id=target_qids[0] if len(target_qids) == 1 else "",
                rca_id=rca_id,
                root_cause=root_cause,
                ag_id=ag_id,
                proposal_id=proposal_id,
                gate="patch_cap",
                reason_detail=str(decision.get("selection_reason") or ""),
                evidence_refs=_clean_str_tuple(decision.get("evidence_refs") or ()),
                affected_qids=target_qids,
                target_qids=target_qids,
                expected_effect=str(decision.get("expected_effect") or ""),
                observed_effect=(
                    "Selected for apply" if selected else "Dropped by patch cap"
                ),
                regression_qids=_clean_str_tuple(decision.get("regression_qids") or ()),
                next_action=(
                    "Apply selected patch and evaluate target qids"
                    if selected else "Inspect lower-ranked patch if target remains unresolved"
                ),
                proposal_ids=(proposal_id,) if proposal_id else (),
                metrics={
                    "selection_reason": decision.get("selection_reason"),
                    "rank": decision.get("rank"),
                    "relevance_score": _as_float(decision.get("relevance_score")),
                    "lever": _as_int(decision.get("lever"), 5),
                    "patch_type": decision.get("patch_type"),
                    "rca_id": rca_id,
                    "root_cause": root_cause,
                    "target_qids": list(target_qids),
                    "parent_proposal_id": str(decision.get("parent_proposal_id") or ""),
                    "expanded_patch_id": str(decision.get("expanded_patch_id") or ""),
                },
            )
        )
    return records
```

Then replace the body of `patch_cap_decision_rows` with:

```python
    rows: list[dict[str, Any]] = []
    for idx, record in enumerate(
        patch_cap_decision_records(
            run_id=run_id,
            iteration=iteration,
            ag_id=ag_id,
            decisions=decisions,
        ),
        start=1,
    ):
        row = record.to_dict()
        rows.append({
            "run_id": row["run_id"],
            "iteration": row["iteration"],
            "ag_id": row.get("ag_id"),
            "decision_order": idx,
            "stage_letter": "I",
            "gate_name": row.get("gate", ""),
            "decision": (
                "accepted"
                if record.outcome == DecisionOutcome.ACCEPTED else "dropped"
            ),
            "reason_code": (
                None
                if record.outcome == DecisionOutcome.ACCEPTED
                else row.get("reason_detail")
            ),
            "reason_detail": row.get("reason_detail"),
            "affected_qids": row.get("affected_qids", []),
            "source_cluster_ids": row.get("source_cluster_ids", []),
            "proposal_ids": row.get("proposal_ids", []),
            "proposal_to_patch_map": None,
            "metrics": row.get("metrics", {}),
        })
    return rows
```

- [ ] **Step 4: Run regression tests**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_rca_decision_trace.py -q
```

Expected: PASS, including the pre-existing legacy row test.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py \
  packages/genie-space-optimizer/tests/unit/test_rca_decision_trace.py
git commit -m "$(cat <<'EOF'
Adapt patch-cap audit to decision records.

EOF
)"
```

---

## Task 4: Preserve `iterations[N].decision_records` In Fixtures

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/journey_fixture_exporter.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_journey_fixture_exporter.py`

- [ ] **Step 1: Add failing exporter tests**

Append to `test_journey_fixture_exporter.py`:

```python
def test_exporter_passes_decision_records_through() -> None:
    import json
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [_sample_iteration_input()]
    iterations_data[0]["decision_records"] = [
        {
            "run_id": "run_1",
            "iteration": 1,
            "decision_type": "gate_decision",
            "outcome": "dropped",
            "reason_code": "patch_cap_dropped",
            "question_id": "q_002",
            "ag_id": "AG_1",
            "proposal_id": "p1",
            "gate": "patch_cap",
            "affected_qids": ["q_002"],
                "evidence_refs": ["eval:q_002"],
                "root_cause": "missing_filter",
                "target_qids": ["q_002"],
                "expected_effect": "Patch should resolve q_002.",
                "observed_effect": "Patch was dropped before apply.",
                "regression_qids": [],
                "next_action": "Inspect cap ranking.",
            "metrics": {"rank": 2},
            "_volatile": "strip-me",
        },
    ]

    parsed = json.loads(serialize_replay_fixture(
        fixture_id="decision_records_v1",
        iterations_data=iterations_data,
    ))

    assert parsed["iterations"][0]["decision_records"] == [
        {
            "run_id": "run_1",
            "iteration": 1,
            "decision_type": "gate_decision",
            "outcome": "dropped",
            "reason_code": "patch_cap_dropped",
            "question_id": "q_002",
            "ag_id": "AG_1",
            "proposal_id": "p1",
            "gate": "patch_cap",
            "affected_qids": ["q_002"],
            "evidence_refs": ["eval:q_002"],
            "root_cause": "missing_filter",
            "target_qids": ["q_002"],
            "expected_effect": "Patch should resolve q_002.",
            "observed_effect": "Patch was dropped before apply.",
            "regression_qids": [],
            "next_action": "Inspect cap ranking.",
            "metrics": {"rank": 2},
        },
    ]


def test_begin_iteration_capture_initializes_decision_records() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        begin_iteration_capture,
    )

    iters: list[dict] = []
    snap = begin_iteration_capture(iterations_data=iters, iteration=1)

    assert snap["decision_records"] == []
    assert iters[0]["decision_records"] == []
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
python -m pytest \
  packages/genie-space-optimizer/tests/unit/test_journey_fixture_exporter.py::test_exporter_passes_decision_records_through \
  packages/genie-space-optimizer/tests/unit/test_journey_fixture_exporter.py::test_begin_iteration_capture_initializes_decision_records \
  -q
```

Expected: FAIL because `decision_records` is stripped and not initialized.

- [ ] **Step 3: Modify the exporter**

In `journey_fixture_exporter.py`, add `"decision_records"` to `_ALLOWED_ITERATION_KEYS`:

```python
    "journey_validation",
    "decision_records",
```

Add this tuple after `_ALLOWED_PATCH_KEYS`:

```python
_ALLOWED_DECISION_RECORD_KEYS = (
    "run_id",
    "iteration",
    "decision_type",
    "outcome",
    "reason_code",
    "question_id",
    "cluster_id",
    "rca_id",
        "root_cause",
    "ag_id",
    "proposal_id",
    "patch_id",
    "gate",
    "reason_detail",
    "affected_qids",
        "evidence_refs",
        "target_qids",
        "expected_effect",
        "observed_effect",
        "regression_qids",
        "next_action",
    "source_cluster_ids",
    "proposal_ids",
    "metrics",
)
```

Add this block in `_strip_iteration` before `return out`:

```python
    if "decision_records" in out:
        out["decision_records"] = [
            _strip_dict(r, _ALLOWED_DECISION_RECORD_KEYS)
            for r in (out.get("decision_records") or [])
        ]
```

Add `"decision_records": []` to the snapshot in `begin_iteration_capture`:

```python
        "journey_validation": None,
        "decision_records": [],
```

Add this count inside `summarize_replay_fixture`:

```python
                "decision_records": len(it.get("decision_records") or []),
```

- [ ] **Step 4: Run exporter tests**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_journey_fixture_exporter.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/journey_fixture_exporter.py \
  packages/genie-space-optimizer/tests/unit/test_journey_fixture_exporter.py
git commit -m "$(cat <<'EOF'
Preserve decision records in replay fixtures.

EOF
)"
```

---

## Task 5: Extend Replay With Decision Trace Outputs

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py`
- Modify: `packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py`

- [ ] **Step 1: Add failing replay tests**

Append to `test_lever_loop_replay.py`:

```python
def test_run_replay_exposes_decision_trace_outputs() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = {
        "fixture_id": "decision_replay_v1",
        "iterations": [
            {
                "iteration": 1,
                "eval_rows": [
                    {
                        "question_id": "q1",
                        "result_correctness": "no",
                        "arbiter": "ground_truth_correct",
                    }
                ],
                "clusters": [
                    {
                        "cluster_id": "H001",
                        "root_cause": "missing_filter",
                        "question_ids": ["q1"],
                    }
                ],
                "soft_clusters": [],
                "strategist_response": {
                    "action_groups": [
                        {
                            "id": "AG1",
                            "affected_questions": ["q1"],
                            "patches": [
                                {
                                    "proposal_id": "P001",
                                    "patch_type": "add_sql_snippet_filter",
                                    "target_qids": ["q1"],
                                    "cluster_id": "H001",
                                }
                            ],
                        }
                    ]
                },
                "ag_outcomes": {"AG1": "accepted"},
                "post_eval_passing_qids": ["q1"],
                "decision_records": [
                    {
                        "run_id": "fixture",
                        "iteration": 1,
                        "decision_type": "proposal_generated",
                        "outcome": "accepted",
                        "reason_code": "proposal_emitted",
                        "question_id": "q1",
                "rca_id": "rca_q1_missing_filter",
                "root_cause": "missing_filter",
                        "ag_id": "AG1",
                        "proposal_id": "P001",
                "evidence_refs": ["eval:q1", "cluster:H001"],
                "target_qids": ["q1"],
                "expected_effect": "The proposed filter should make q1 pass.",
                "observed_effect": "q1 passed post-eval.",
                "regression_qids": [],
                "next_action": "Keep patch if no regressions are observed.",
                        "affected_qids": ["q1"],
                    }
                ],
            }
        ],
    }

    result = run_replay(fixture)

    assert result.decision_records[0].proposal_id == "P001"
    assert "proposal_generated" in result.canonical_decision_json
    assert "OPERATOR TRANSCRIPT  iteration=1" in result.operator_transcript
    assert result.decision_validation == []
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_exposes_decision_trace_outputs -q
```

Expected: FAIL because `ReplayResult` has no decision trace fields.

- [ ] **Step 3: Extend replay result and load fixture decisions**

In `lever_loop_replay.py`, add imports:

```python
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionRecord,
    OptimizationTrace,
    canonical_decision_json,
    render_operator_transcript,
    validate_decisions_against_journey,
)
```

Change `ReplayResult` to:

```python
@dataclass(frozen=True)
class ReplayResult:
    events: list[QuestionJourneyEvent]
    canonical_json: str
    validation: JourneyValidationReport
    decision_records: list[DecisionRecord]
    canonical_decision_json: str
    operator_transcript: str
    decision_validation: list[str]
```

Add this helper above `run_replay`:

```python
def _decision_records_from_iteration(it: dict) -> list[DecisionRecord]:
    return [
        DecisionRecord.from_dict(r)
        for r in (it.get("decision_records") or [])
    ]
```

Inside `run_replay`, initialize:

```python
    decision_records: list[DecisionRecord] = []
    transcript_parts: list[str] = []
```

Inside the iteration loop, after `events.extend(iter_events)`, add:

```python
        iter_decisions = _decision_records_from_iteration(it)
        decision_records.extend(iter_decisions)
        if iter_decisions:
            trace = OptimizationTrace(
                journey_events=tuple(iter_events),
                decision_records=tuple(iter_decisions),
            )
            transcript_parts.append(
                render_operator_transcript(
                    trace=trace,
                    iteration=int(it.get("iteration") or 0),
                )
            )
```

Before returning, add:

```python
    decision_validation = validate_decisions_against_journey(
        records=decision_records,
        events=events,
    )
```

Return the new fields:

```python
        decision_records=decision_records,
        canonical_decision_json=canonical_decision_json(decision_records),
        operator_transcript="\n".join(transcript_parts),
        decision_validation=decision_validation,
```

- [ ] **Step 4: Run replay tests**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py -q
```

Expected: FAIL only because `validate_decisions_against_journey` is not implemented. If other failures appear, fix the exact signature mismatch before continuing.

- [ ] **Step 5: Commit after Task 6 passes**

Do not commit this task until Task 6 implements `validate_decisions_against_journey`; this keeps the branch green at every commit.

---

## Task 6: Add Journey/Decision Cross-Checks

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py`
- Test: `packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py`

- [ ] **Step 1: Add failing cross-check tests**

Append to `test_decision_trace_contract.py`:

```python
def test_validate_decisions_against_journey_catches_missing_proposed_event() -> None:
    from genie_space_optimizer.optimization.question_journey import QuestionJourneyEvent
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
        validate_decisions_against_journey,
    )

    violations = validate_decisions_against_journey(
        records=[
            DecisionRecord(
                iteration=1,
                decision_type=DecisionType.PROPOSAL_GENERATED,
                outcome=DecisionOutcome.ACCEPTED,
                reason_code=ReasonCode.PROPOSAL_EMITTED,
                question_id="q1",
                evidence_refs=("eval:q1", "cluster:H001"),
                rca_id="rca_q1_missing_filter",
                root_cause="missing_filter",
                target_qids=("q1",),
                expected_effect="Proposal should produce a targeted filter patch.",
                next_action="Emit proposal journey event before applying.",
                proposal_id="P001",
            )
        ],
        events=[QuestionJourneyEvent(question_id="q1", stage="evaluated")],
    )

    assert violations == [
        "decision proposal_generated qid=q1 proposal=P001 has no matching journey stage proposed"
    ]


def test_validate_decisions_against_journey_accepts_matching_post_eval_resolution() -> None:
    from genie_space_optimizer.optimization.question_journey import QuestionJourneyEvent
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
        validate_decisions_against_journey,
    )

    violations = validate_decisions_against_journey(
        records=[
            DecisionRecord(
                iteration=1,
                decision_type=DecisionType.QID_RESOLUTION,
                outcome=DecisionOutcome.RESOLVED,
                reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
                question_id="q1",
                evidence_refs=("post_eval:q1",),
                rca_id="rca_q1_missing_filter",
                root_cause="missing_filter",
                target_qids=("q1",),
                expected_effect="q1 should pass after patch.",
                observed_effect="q1 passed after patch.",
                next_action="Keep the accepted patch.",
            )
        ],
        events=[
            QuestionJourneyEvent(question_id="q1", stage="evaluated"),
            QuestionJourneyEvent(question_id="q1", stage="post_eval", is_passing=True),
        ],
    )

    assert violations == []
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py -q
```

Expected: FAIL with missing `validate_decisions_against_journey`.

- [ ] **Step 3: Implement cross-checks**

Append to `rca_decision_trace.py`:

```python
def _record_qids(record: DecisionRecord) -> tuple[str, ...]:
    if record.affected_qids:
        return record.affected_qids
    if record.question_id:
        return (record.question_id,)
    return ()


def _has_event(
    *,
    events: Sequence[Any],
    qid: str,
    stage: str,
    proposal_id: str = "",
) -> bool:
    for ev in events:
        if getattr(ev, "question_id", "") != qid:
            continue
        if getattr(ev, "stage", "") != stage:
            continue
        if proposal_id and getattr(ev, "proposal_id", "") != proposal_id:
            continue
        return True
    return False


def validate_decisions_against_journey(
    *,
    records: Sequence[DecisionRecord],
    events: Sequence[Any],
) -> list[str]:
    violations: list[str] = []
    rca_required = {
        DecisionType.CLUSTER_SELECTED,
        DecisionType.RCA_FORMED,
        DecisionType.STRATEGIST_AG_EMITTED,
        DecisionType.PROPOSAL_GENERATED,
        DecisionType.GATE_DECISION,
        DecisionType.PATCH_APPLIED,
        DecisionType.PATCH_SKIPPED,
        DecisionType.ACCEPTANCE_DECIDED,
        DecisionType.QID_RESOLUTION,
    }
    stage_requirements = {
        DecisionType.PROPOSAL_GENERATED: "proposed",
        DecisionType.PATCH_APPLIED: "applied",
        DecisionType.QID_RESOLUTION: "post_eval",
    }
    for record in records:
        if record.decision_type in rca_required:
            if not record.evidence_refs:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no evidence_refs"
                )
            if not record.rca_id and record.decision_type not in {DecisionType.EVAL_CLASSIFIED}:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no rca_id"
                )
            if not record.root_cause and record.decision_type not in {DecisionType.EVAL_CLASSIFIED}:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no root_cause"
                )
            if not record.target_qids and not record.reason_code == ReasonCode.MISSING_TARGET_QIDS:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no target_qids"
                )
        required_stage = stage_requirements.get(record.decision_type)
        if not required_stage:
            continue
        for qid in _record_qids(record):
            if _has_event(
                events=events,
                qid=qid,
                stage=required_stage,
                proposal_id=record.proposal_id if required_stage in {"proposed", "applied"} else "",
            ):
                continue
            violations.append(
                "decision "
                f"{record.decision_type.value} qid={qid} "
                f"proposal={record.proposal_id or '-'} "
                f"has no matching journey stage {required_stage}"
            )
    return violations
```

- [ ] **Step 4: Run unit and replay tests**

Run:

```bash
python -m pytest \
  packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py \
  packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_exposes_decision_trace_outputs \
  -q
```

Expected: PASS.

- [ ] **Step 5: Commit Tasks 5 and 6 together**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever_loop_replay.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_decision_trace.py \
  packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py \
  packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py
git commit -m "$(cat <<'EOF'
Replay decision traces with journey cross-checks.

EOF
)"
```

---

## Task 7: Capture Patch-Cap Decision Records In The Harness Fixture

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_diminishing_returns_and_budget.py`

- [ ] **Step 1: Add a static wiring test**

Append to `test_diminishing_returns_and_budget.py`:

```python
def test_harness_captures_patch_cap_decision_records_in_fixture() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    src = (
        root
        / "src"
        / "genie_space_optimizer"
        / "optimization"
        / "harness.py"
    ).read_text()

    assert "patch_cap_decision_records" in src
    assert '_current_iter_inputs.setdefault("decision_records", [])' in src
    assert "phase_b/decision_trace" in src
    assert "phase_b/operator_transcript" in src
```

- [ ] **Step 2: Run the failing static test**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_diminishing_returns_and_budget.py::test_harness_captures_patch_cap_decision_records_in_fixture -q
```

Expected: FAIL because the harness only calls `patch_cap_decision_rows`.

- [ ] **Step 3: Capture typed records at the existing patch-cap persistence site**

At the existing import block around `harness.py` patch-cap persistence, replace:

```python
                from genie_space_optimizer.optimization.rca_decision_trace import (
                    patch_cap_decision_rows,
                )
```

with:

```python
                from genie_space_optimizer.optimization.rca_decision_trace import (
                    OptimizationTrace,
                    patch_cap_decision_records,
                    patch_cap_decision_rows,
                    render_operator_transcript,
                )
```

Immediately before `_write_decisions(...)`, add:

```python
                _patch_cap_records = patch_cap_decision_records(
                    run_id=run_id,
                    iteration=iteration_counter,
                    ag_id=ag_id,
                    decisions=_patch_cap_decisions,
                )
                _current_iter_inputs.setdefault("decision_records", []).extend(
                    [r.to_dict() for r in _patch_cap_records]
                )
```

Change the existing `patch_cap_decision_rows(...)` call to keep the same legacy persistence:

```python
                    patch_cap_decision_rows(
                        run_id=run_id,
                        iteration=iteration_counter,
                        ag_id=ag_id,
                        decisions=_patch_cap_decisions,
                    ),
```

- [ ] **Step 4: Log Phase B MLflow artifacts at iteration end**

In `harness.py`, after the Phase A journey-validation MLflow block, add:

```python
        try:
            from genie_space_optimizer.optimization.rca_decision_trace import (
                DecisionRecord,
                OptimizationTrace,
                canonical_decision_json,
                render_operator_transcript,
                validate_decisions_against_journey,
            )

            _decision_records = [
                DecisionRecord.from_dict(r)
                for r in (_current_iter_inputs.get("decision_records") or [])
            ]
            if _decision_records:
                _decision_validation = validate_decisions_against_journey(
                    records=_decision_records,
                    events=_journey_events,
                )
                _trace = OptimizationTrace(
                    journey_events=tuple(_journey_events),
                    decision_records=tuple(_decision_records),
                )
                _transcript = render_operator_transcript(
                    trace=_trace,
                    iteration=iteration_counter,
                )
                print(_transcript)
                import mlflow as _mlflow_trace  # type: ignore[import-not-found]
                if _mlflow_trace.active_run() is not None:
                    _mlflow_trace.log_text(
                        canonical_decision_json(_decision_records),
                        artifact_file=(
                            f"phase_b/decision_trace/"
                            f"iter_{iteration_counter}.json"
                        ),
                    )
                    _mlflow_trace.log_text(
                        _transcript,
                        artifact_file=(
                            f"phase_b/operator_transcript/"
                            f"iter_{iteration_counter}.txt"
                        ),
                    )
                    _mlflow_trace.set_tags({
                        f"decision_trace.iter_{iteration_counter}.records": (
                            str(len(_decision_records))
                        ),
                        f"decision_trace.iter_{iteration_counter}.violations": (
                            str(len(_decision_validation))
                        ),
                    })
        except Exception:
            logger.debug(
                "Phase B: decision trace persistence skipped (non-fatal)",
                exc_info=True,
            )
```

- [ ] **Step 5: Run the static test**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_diminishing_returns_and_budget.py::test_harness_captures_patch_cap_decision_records_in_fixture -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
  packages/genie-space-optimizer/tests/unit/test_diminishing_returns_and_budget.py
git commit -m "$(cat <<'EOF'
Capture patch-cap decision records in harness fixtures.

EOF
)"
```

---

## Task 8: Pin Replay Snapshot Behavior For Decision Records

**Files:**
- Modify: `packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py`
- Modify: `packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json`

- [ ] **Step 1: Add snapshot tests that skip until fixture baseline exists**

Append to `test_lever_loop_replay.py`:

```python
def test_airline_real_v1_replay_decision_trace_is_byte_stable() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = _load("airline_real_v1.json")
    expected = fixture.get("expected_canonical_decisions")
    if not expected:
        pytest.skip(
            "expected_canonical_decisions not yet recorded; seed it after "
            "Phase B decision_records are present in airline_real_v1.json."
        )
    result = run_replay(fixture)
    assert result.canonical_decision_json == expected


def test_airline_real_v1_operator_transcript_is_byte_stable() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = _load("airline_real_v1.json")
    expected = fixture.get("expected_operator_transcript")
    if not expected:
        pytest.skip(
            "expected_operator_transcript not yet recorded; seed it after "
            "Phase B decision_records are present in airline_real_v1.json."
        )
    result = run_replay(fixture)
    assert result.operator_transcript == expected
```

- [ ] **Step 2: Run replay tests**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py -q
```

Expected: PASS with the two new tests skipped until a refreshed fixture contains decision records.

- [ ] **Step 3: Refresh the real fixture after the next real run**

After a real Databricks run emits `decision_records`, update `airline_real_v1.json` by adding:

```json
"expected_canonical_decisions": "<output from result.canonical_decision_json>",
"expected_operator_transcript": "<output from result.operator_transcript>"
```

Use a small Python helper rather than manual editing when the real fixture is available:

```bash
python - <<'PY'
import json
from pathlib import Path
from genie_space_optimizer.optimization.lever_loop_replay import run_replay

path = Path("packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json")
fixture = json.loads(path.read_text())
result = run_replay(fixture)
fixture["expected_canonical_decisions"] = result.canonical_decision_json
fixture["expected_operator_transcript"] = result.operator_transcript
path.write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
PY
```

- [ ] **Step 4: Run replay tests after fixture refresh**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py -q
```

Expected: PASS with no skips for the two Phase B snapshot tests after `airline_real_v1.json` contains decision records and expected outputs.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py \
  packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json
git commit -m "$(cat <<'EOF'
Pin decision trace replay snapshots.

EOF
)"
```

---

## Task 9: Update Phase B Documentation Cross-References

**Files:**
- Modify: `packages/genie-space-optimizer/docs/2026-05-01-burn-down-to-merge-roadmap.md`
- Modify: `packages/genie-space-optimizer/docs/canonical-schema.md`

- [ ] **Step 1: Update the roadmap link**

In `2026-05-01-burn-down-to-merge-roadmap.md`, replace the placeholder Phase B unified-trace row with:

```markdown
| [`2026-05-02-unified-trace-and-operator-transcript-plan.md`](./2026-05-02-unified-trace-and-operator-transcript-plan.md) | Ready | B |
```

- [ ] **Step 2: Add trace vocabulary to canonical schema**

Append these rows to the canonical vocabulary table in `canonical-schema.md`:

```markdown
| Optimizer trace container | `OptimizationTrace` | Canonical in-memory container for journey events, decision records, validation reports, and operator transcript projections. | `decision log`, `trace rows` when used as a schema name | `optimization/rca_decision_trace.py` |
| Optimizer decision row | `DecisionRecord` | Canonical record for a Lever Loop choice with evidence refs, RCA, root cause, causal target qids, expected effect, observed effect, regression qids, reason code, and next action. | `decision audit row` outside Delta persistence, `gate row` outside gate-specific adapters | `optimization/rca_decision_trace.py` |
| Operator transcript | `operator_transcript` | Deterministic pretty stdout projection rendered from `OptimizationTrace`. | ad hoc print sections, scoreboard prose | `optimization/rca_decision_trace.py` |
```

- [ ] **Step 3: Run documentation lint check**

Run:

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py -q
```

Expected: PASS. This command verifies the trace vocabulary referenced by the docs exists in code.

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/docs/2026-05-01-burn-down-to-merge-roadmap.md \
  packages/genie-space-optimizer/docs/canonical-schema.md
git commit -m "$(cat <<'EOF'
Document unified trace contract.

EOF
)"
```

---

## Final Verification

- [ ] **Run focused Phase B tests**

```bash
python -m pytest \
  packages/genie-space-optimizer/tests/unit/test_decision_trace_contract.py \
  packages/genie-space-optimizer/tests/unit/test_rca_decision_trace.py \
  packages/genie-space-optimizer/tests/unit/test_journey_fixture_exporter.py \
  packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py \
  -q
```

Expected: PASS. The two `airline_real_v1` Phase B snapshot tests may SKIP until a refreshed real fixture contains `decision_records`.

- [ ] **Run static harness wiring tests**

```bash
python -m pytest packages/genie-space-optimizer/tests/unit/test_diminishing_returns_and_budget.py -q
```

Expected: PASS.

- [ ] **Inspect the fixture summary path manually**

Run:

```bash
python - <<'PY'
from genie_space_optimizer.optimization.journey_fixture_exporter import (
    begin_iteration_capture,
    summarize_replay_fixture,
)

iters = []
snap = begin_iteration_capture(iterations_data=iters, iteration=1)
snap["decision_records"].append({
    "run_id": "run_1",
    "iteration": 1,
    "decision_type": "gate_decision",
    "outcome": "accepted",
    "reason_code": "patch_cap_selected",
})
print(summarize_replay_fixture(iterations_data=iters))
PY
```

Expected output includes:

```text
'decision_records': 1
```

---

## Self-Review

### Spec Coverage

- Defines `DecisionRecord`, `DecisionType`, `DecisionOutcome`, `ReasonCode`, and `OptimizationTrace`: Tasks 1 and 2.
- Requires RCA-grounded decision fields (`evidence_refs`, `rca_id`, `root_cause`, `target_qids`, `expected_effect`, `observed_effect`, `regression_qids`, `next_action`): Tasks 1, 3, 4, 5, and 6.
- Enforces stdout as a centralized deterministic `OptimizationTrace` projection with fixed sections: Task 2 and Task 7.
- Adds deterministic serialization and stable sort order: Task 1.
- Adds `iterations[N].decision_records` to fixture export/replay: Tasks 4 and 5.
- Adds minimal operator transcript renderer: Task 2 and Task 7 runtime artifacting.
- Adapts patch-cap decision audit path into the new model: Task 3 and Task 7.
- Adds cross-checks that decision records agree with journey events: Task 6.

### Placeholder Scan

This plan contains no unresolved placeholder tokens, no empty implementation steps, and no "write tests for the above" steps without concrete test code.

### Type Consistency

The same class names and functions are used throughout:

- `DecisionRecord`
- `DecisionType`
- `DecisionOutcome`
- `ReasonCode`
- `OptimizationTrace`
- `canonical_decision_json`
- `render_operator_transcript`
- `patch_cap_decision_records`
- `validate_decisions_against_journey`

---

## Execution Handoff

Plan complete and saved to `packages/genie-space-optimizer/docs/2026-05-02-unified-trace-and-operator-transcript-plan.md`. Two execution options:

1. **Subagent-Driven (recommended)** - Dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints.

---

## Postmortem Follow-up (run 894992655057610)

The first deployment of the Phase B unified-trace contract produced a
diagnostic gap: when proposals never reach the patch-cap (the only
DecisionRecord producer), the iteration produces zero records and Phase
B persistence is a silent no-op. Cycle-9 reality (5 iters, 0 levers
accepted) demonstrated this: no MLflow artifacts, no operator
transcript visible via Jobs CLI, no `iterations[N].decision_records` in
the local fixture. See
[`runid_analysis/1036606061019898_894992655057610_analysis.md`](./runid_analysis/1036606061019898_894992655057610_analysis.md)
for the full evidence.

The follow-up plan at
`~/.claude/plans/groovy-gathering-fountain.md` (kept locally because
it was authored under a planning session) closes the gap. Summary of
what landed in code:

* **5 new DecisionRecord producers** in
  `optimization/decision_emitters.py`: EVAL_CLASSIFIED,
  CLUSTER_SELECTED, STRATEGIST_AG_EMITTED, ACCEPTANCE_DECIDED,
  QID_RESOLUTION. Wired into `harness.py` adjacent to existing
  journey-emit hooks.
* **`PHASE_B_CONTRACT_VERSION = "v1"`** constant, stamped on the
  active MLflow run as a tag at lever-loop start. Lets the analyzer
  tell "deploy is stale" (no tag) from "deploy current, 0 records"
  (tag present, manifest shows zero).
* **No-records diagnostic** — when an iteration produces 0 records,
  `GSO_PHASE_B_NO_RECORDS_V1` marker + MLflow tag carries a stable
  `NoRecordsReason` enum value (no_clusters / no_ags_emitted /
  all_ags_dropped_at_grounding / patch_cap_did_not_fire /
  producer_exception / unknown).
* **`GSO_PHASE_B_END_V1` marker** at every lever-loop terminate path
  with per-iter record + violation counts and the
  `no_records_iterations` list.
* **`loop_out["phase_b"]` manifest** in the lever-loop return dict —
  the CLI-truth surface for the postmortem analyzer because
  `databricks jobs get-run-output` for the lever_loop task exposes
  only the `dbutils.notebook.exit(...)` JSON, not stdout.
  `run_lever_loop.py:548-563` allowlists `"phase_b"` so the manifest
  survives the filter.
* **POST_EVAL_HOLD_PASS rca-exempt cross-check** — held-pass qids
  carry no `rca_id` (they were never clustered); the cross-checker
  now exempts them.
* **`skipped_pre_ag_snapshot_failed` AG outcome captured** — the path
  that previously discarded the AG with no `ag_outcomes` write now
  records the outcome so ACCEPTANCE_DECIDED sees it.

Manifest schema (the CLI-truth surface):

```python
loop_out["phase_b"] = {
    "contract_version": "v1",
    "decision_records_total": int,
    "iter_record_counts": list[int],
    "iter_violation_counts": list[int],
    "no_records_iterations": list[int],
    "artifact_paths": list[str],
    "producer_exceptions": dict[str, int],
    "target_qids_missing_count": int,
    "total_violations": int,
}
```

Out-of-scope (deliberately deferred to follow-up PRs):

* **Cross-checker tightening** — every STRATEGIST_AG_EMITTED must have
  a matching ACCEPTANCE_DECIDED; CLUSTER_SELECTED.target_qids must
  equal cluster.question_ids; counted-violation taxonomy. Lands once
  PR 1 is deployed and a real cycle 9 fixture exposes the rule edges
  against real data.
* **`dropped_at_<gate>` DecisionRecord producers** for proposals
  dropped at grounding/normalize/applyability/alignment/reflection
  gates. The journey events exist; this PR's scope is "observable on
  every iteration" not "fully covered."
* **Postmortem-analyzer skill changes** to consume the new manifest.
  Skill-side change separate from this trace plan.

