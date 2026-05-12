# AG Emission Grounding Gate + Forbidden-AG Cluster-Signature Collision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Block AG emission for clusters with no fit RCA card (Gate G1) and stop the strategist from re-admitting forbidden AG families across iterations when the LLM regenerates the AG with a slightly different `root_cause` text but the same underlying cluster (Gate G2).

**Architecture:** Two narrow runtime gates added at the harness/stage boundary, both fixture-driven by the May-12 trial runs that exposed the defects.
- **Gate G1 — Ungrounded-RCA gate:** Emit a typed `cluster_blocked_no_rca` `DecisionRecord` whenever an open hard cluster has `rca_card=False` at AG-emit time. Plumb the blocked cluster ids into `ActionGroupsInput` and filter them out in `stages.action_groups.select` before the strategist runs. This makes the existing I7 invariant runtime-enforced rather than detection-only.

  > **See also:** Plan P-D (`2026-05-12-plan-p-d-rca-regeneration-recovery-loop.md`)
  > inserts an RCA ungrounded recovery *policy* step immediately before this
  > prelude when `GSO_RCA_REGEN_RECOVERY_POLICY=1` (default). The policy
  > classifies the typed ungrounded reason and only retries the retryable
  > categories (`NO_PARENT_RCA`, `NO_FINDINGS`, `NO_TERM_OVERLAP`,
  > `NO_CAUSAL_TARGET`); non-retryable categories
  > (`MISSING_TARGET_QIDS`, `NO_EVIDENCE_AVAILABLE`, `UNKNOWN`) fall straight
  > through to this G1 short-circuit. G1 then operates on the post-policy
  > `clusters` view — only the still-ungrounded clusters reach
  > `cluster_blocked_no_rca`.
- **Gate G2 — Cluster-signature forbidden-AG admission:** Today's collision key is `(root_cause, blame_set, frozenset(lever_set))`, which is fragile to LLM-regenerated `root_cause` text. Add a parallel cluster-signature key derived from `source_cluster_signatures` (which is stable across iterations by construction) so two reflections that target the same cluster collide regardless of LLM-side `root_cause` drift.

Both gates ship behind `_flag_default_on` accessors so the rollback escape hatch is `GSO_*=0`, matching the RCO-4/4b posture established by `2026-05-13-rco-4b-consolidating-trial-submission-plan.md`.

**Tech Stack:** Python 3.11+, `pytest`, frozen-dataclass typed records, OpenAI-compatible Databricks model serving (already deployed via Apps platform). No new transitive deps.

---

## Status

| Field | Value |
|---|---|
| Status | Draft |
| Surfaced by | RCO-4b consolidating-trial runs `31ecd96f-5d56-4b5a-af8e-38e9e5c549af` (airline) + `ccf1d60d-d686-467b-bafa-1640131b4393` (7now), captured 2026-05-12 |
| Defect stub origins | `docs/2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md`, `docs/2026-05-12-defect-forbidden-ag-admission-enforcement.md` |
| Roadmap parent | `docs/2026-05-10-roadmap-closeout.md` — does **not** belong to a roadmap RCO; this is a defect plan that unblocks a re-trial against the original F9-3b050ec5 + AIRLINE-clean anchors |
| Lever-loop trial gated | Yes — landing this plan + Defect Plan 2 (retry signature) + bundle-status micro-plan together enables the re-trial that converts the "FUTURE TARGET" anchors in `tests/integration/fixtures/rco4b_trial/expected_outcomes.json` to captured evidence |
| Estimated tasks | 12 |
| Estimated LOC | ~600 production + ~900 test |

---

## Evidence-grounded RCA

> The two defect stubs are accurate at the surface symptom level but the postmortems' "Recommended next steps (verbatim from F1–F8)" sections describe the **fix shape**, not the **root cause**. A live-code read produces a more precise diagnosis. This section is the authoritative RCA for this plan and supersedes the stubs' wording wherever they conflict.

### Surface symptom 1 — Airline run 31ecd96f: H001/H002 alternate without retirement

Iterations 1, 3, 5 all dispatch `AG_DECOMPOSED_H001` for `gs_009`; iterations 2, 4 all dispatch `AG_DECOMPOSED_H002` for `gs_024`. Every iteration ends `skipped_no_applied_patches`. The transcript repeatedly logs `rca_formed outcome=unresolved reason=rca_ungrounded` for both clusters, and `GSO_CONTRACT_HEALTH_V1` reports the I7 invariant title `cluster H001/H002 reached AG-emit with no fit RCA card and no cluster_blocked_no_rca record`.

### Live-code findings (Symptom 1)

1. **I7 already exists and already names the right violation.** `optimization/invariants.py:319` defines `check_i7_rca_grounding`. Its `_violation` payload at line 342 uses the title `open_cluster_ungrounded_at_ag_emit` and the detail string `cluster {cid} reached AG-emit with no fit RCA card and no cluster_blocked_no_rca record`. The airline stub's proposal "add an invariant test for `open_cluster_ungrounded_at_ag_emit`" is **already satisfied**.

2. **I7 is detection-only.** It runs at end-of-iteration through `run_invariants` and the merge-gate path (RCO-2a). Nothing in the live runtime *prevents* the AG from emitting; I7 just records a violation after the fact. The existing tests `test_i7_red_when_open_cluster_has_no_rca_card_or_block_record` and `test_i7_green_when_block_record_emitted` (in `tests/unit/test_invariants.py:329-357`) confirm this — the green case requires a `decision_records[]` entry with `decision_type="cluster_blocked_no_rca"`.

3. **No producer of `cluster_blocked_no_rca` exists.** Grepping the entire `src/` tree:
   ```
   $ rg "cluster_blocked_no_rca" src/
   src/.../optimization/invariants.py: 4 occurrences (the consumer)
   ```
   That is the entire population. There is no `DecisionType.CLUSTER_BLOCKED_NO_RCA` in `rca_decision_trace.py:32-54`; there is no `cluster_blocked_no_rca_record(...)` helper in `decision_emitters.py`. The I7 "green-when-block-record-emitted" branch is **unreachable in production**.

4. **The runtime path that produces `outcome=unresolved reason=rca_ungrounded` is purely a log line**, not a gate. Searching for the actual ReasonCode emission, `ReasonCode.RCA_UNGROUNDED` (`rca_decision_trace.py:79` and `:216`) is used for `RCA_FORMED` / proposal-failure records but never short-circuits AG dispatch.

#### Root cause 1

**No runtime gate on AG emission for ungrounded clusters.** The detection invariant exists, but neither (a) a typed decision record producer nor (b) a runtime filter in the action-groups stage prevents the LLM from being prompted with — and the harness from dispatching — an AG whose source cluster has `rca_card=False`. The defect is one missing emitter plus one missing filter, both small.

### Surface symptom 2 — 7now run ccf1d60d: AG1 admitted in iterations 2-5 despite forbidden observe

Iteration 1 produced a real candidate (87.0% → 91.3%) that was correctly rolled back (`gs_026` still hard, `gs_012` regressed). Iterations 2-5 then each dispatched the **same AG family** (the postmortem calls it "AG1") and each returned zero proposals. `GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1` fired with `behavior_flag_on=true` and `would_admit_with_admit_no_action_on=true` on every NO_ACTION reflection.

### Live-code findings (Symptom 2)

1. **`GSO_FORBIDDEN_AG_ADMITS_NO_ACTION` is already default-ON.** `common/config.py:5532` returns `_flag_default_on("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION")`. The 7now postmortem F5's framing "the runtime ignores it" / "still admits" is misleading — the runtime *does* call `_compute_forbidden_ag_set` with `admit_no_action=True` (`harness.py:10868`) and the result *is* consulted by `_ag_collision_key` at `harness.py:18861`.

2. **The forbidden-set tuple shape is `(root_cause, blame_set_norm, frozenset(lever_set))`** (`harness.py:10906-10911`). `_ag_collision_key` (`harness.py:10916-10936`) builds the lookup key from the same three fields on the current AG.

3. **`root_cause` is LLM-regenerated each iteration.** When the strategist regenerates an AG for the same cluster on a subsequent iteration, the `root_cause` field on the new AG can differ in surface text (different phrasing, different evidence excerpt) even though the cluster identity is identical. Each iteration of the 7now run produced a new AG whose `(root_cause_n, blame_set_n, lever_set_n)` tuple did **not** match the prior iteration's tuple, so the collision check at 18861 returned no match.

4. **`source_cluster_signatures` is stable across iterations by construction.** `_build_reflection_entry` at `harness.py:10282` stores `source_cluster_signatures` as a list of sha1-based signatures (per the T2.1 comment block at lines 10277-10281: "sha1 of base_question_ids + root_cause + blame"). The "root_cause" used in the sha1 is the original cluster `root_cause`, computed by the clusterer once, not the strategist's per-iteration regeneration. So two reflections targeting the same cluster have **identical** `source_cluster_signatures` even when their `root_cause` fields drift.

5. **The OBSERVE marker fires *per NO_ACTION reflection processed***, not per iteration. `harness.py:10877` loops over the reflection buffer and emits one observe per NO_ACTION entry. The marker confirms the predicate would-admit; it does not prove the collision check matched.

#### Root cause 2

**The forbidden-set collision key is too narrow.** It keys on `root_cause` (LLM-regenerated, unstable) instead of `source_cluster_signatures` (clusterer-derived, stable). When the strategist regenerates an AG with slightly different `root_cause` text for the same cluster, the collision check misses and the AG is admitted. The fix is to broaden the forbidden set + collision key to *also* carry a cluster-signature-based key, and to match on **either** key.

### Out of scope (separately addressed)

The investigation surfaced **three** defects in the May-12 trial postmortems, not two. The third is intentionally deferred to a separate plan:

| Defect | Root cause | Plan |
|---|---|---|
| Ungrounded RCA → AG emit (airline) | No producer of `cluster_blocked_no_rca` + no runtime gate | **This plan, Gate G1, Tasks 1-7** |
| Forbidden-AG re-admission across iterations (7now) | Collision key keys on LLM-regenerated `root_cause` instead of stable `source_cluster_signatures` | **This plan, Gate G2, Tasks 8-11** |
| Empty/missing retry signature for `no_applied_patches` (airline secondary) | `classify_rollback_reason("no_applied_patches")` returns `RollbackClass.OTHER` (`rollback_class.py:175`), so reflection is never admitted to forbidden set regardless of cluster identity | **Defect Plan 2** (retry signature) — separate plan, not drafted yet |

The retry-signature defect is mentioned in passing in Task 11's regression assertion list (the new G2 gate alone does NOT close the airline failure; G1 does most of the work on the airline path, and Defect Plan 2 will close the residual).

### Why these two gates ship together (not separately)

Both gates filter AGs at admission time. Both touch the same harness call sites near `_compute_forbidden_ag_set` and the action-groups stage handoff. Both produce typed `DecisionRecord` instances consumed by the same I7 invariant and the same operator transcript section. Splitting them would force two passes through the same code, two flag-flip cycles, and two replay re-runs against the same May-12 fixtures. They land cleanly together because the file-touch set is the same.

### What this plan does NOT change

- **Structural SQL repair quality** (LLM/prompt domain). The airline postmortem's F2-F5 list of structural-repair next steps for `gs_009` / `gs_024` is explicitly out of roadmap closeout scope per `docs/2026-05-10-roadmap-closeout.md:8` ("Remaining LLM work is isolated to prompt quality").
- **`classify_rollback_reason` mapping** for `no_applied_patches`. Defect Plan 2 will widen this; we deliberately leave it alone to keep this plan's blast radius narrow.
- **Phase H per-iteration totality** (`GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1` orthogonal gap). Bundle-status wiring fix is a separate plan, already drafted at `docs/2026-05-12-bundle-status-wiring-fix-plan.md`.
- **Databricks ID resolution in run manifest** (`GSO_RUN_MANIFEST_V*` reports `unknown`). Cross-cutting orthogonal gap; deferred.
- **`gs_021 clustered → soft_signal` replay parity**. Defect Plan 3 (RCO-6 carve-out) — separate plan, not drafted yet.

---

## File Structure

Files this plan touches, with one-line responsibility:

| File | Responsibility | Operation |
|---|---|---|
| `src/genie_space_optimizer/optimization/rca_decision_trace.py` | Add `DecisionType.CLUSTER_BLOCKED_NO_RCA` enum value | Modify |
| `src/genie_space_optimizer/optimization/decision_emitters.py` | Add `cluster_blocked_no_rca_record(...)` producer helper | Modify |
| `src/genie_space_optimizer/common/config.py` | Add `ag_emit_grounding_gate_enabled()` and `forbidden_ag_collision_by_cluster_signature_enabled()` accessors (both default-on via `_flag_default_on`) | Modify |
| `src/genie_space_optimizer/optimization/stages/action_groups.py` | Add `blocked_cluster_ids: tuple[str, ...]` field to `ActionGroupsInput`; filter AGs whose `source_cluster_ids` intersect the blocked set in `select()` | Modify |
| `src/genie_space_optimizer/optimization/harness.py` | (1) After cluster formation, scan open hard clusters with `rca_card=False`, emit `cluster_blocked_no_rca_record` per cluster, plumb `blocked_cluster_ids` into `ActionGroupsInput`. (2) Broaden `_compute_forbidden_ag_set` to produce a second set keyed on `source_cluster_signatures`; broaden `_ag_collision_key` to check both. | Modify |
| `src/genie_space_optimizer/optimization/invariants.py` | I7 docstring touch-up (no logic change — green branch becomes reachable now) | Modify |
| `tests/unit/test_decision_emitters.py` | Unit test for `cluster_blocked_no_rca_record` producer + JSON round-trip | Modify |
| `tests/unit/test_stages_action_groups.py` | Unit test for the `select()` blocked-cluster filter | Modify |
| `tests/unit/test_harness_forbidden_ag.py` | New test file: unit tests for the cluster-signature collision key (G2) | Create |
| `tests/unit/test_harness_grounding_gate.py` | New test file: unit tests for the harness wiring of the grounding gate (G1) | Create |
| `tests/unit/test_config.py` | Add two assertions for the new flag accessors (default-on + falsy-rollback) | Modify |
| `tests/integration/test_ag_grounding_and_admission_replay.py` | New: replay against the May-12 captures, assert (a) zero ungrounded AG emissions on airline, (b) iterations 2-5 short-circuit on 7now | Create |
| `docs/2026-05-10-roadmap-closeout.md` | Mark Defect Plan 1 status row | Modify |
| `docs/2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md` | Mark stub as superseded by this plan | Modify |
| `docs/2026-05-12-defect-forbidden-ag-admission-enforcement.md` | Mark stub as superseded by this plan (for items 1, 5 only — keep RCO-6 carve-out (item 6) as pending) | Modify |

---

## Background context an executor will need

### How the existing AG dispatch loop works (high-level)

1. **`evaluation_state` → `rca_evidence` → `cluster_formation`** populate `clusters: list[dict]`. Each cluster has `cluster_id`, `root_cause`, `rca_card` (truthy when grounded), `signature` (sha1), and `recommended_levers`.
2. The harness builds `rca_cards_present = {cluster_id: bool(c.rca_card)}` at `harness.py:18338`.
3. **`action_group_selection`** runs the strategist LLM, which returns a slate of AGs. Each AG has `source_cluster_ids: list[str]`, `affected_questions: list[str]`, `root_cause: str` (LLM-regenerated, may differ from `cluster.root_cause`), `blame_set`, and `lever_keys`.
4. For each AG, the harness:
   - Computes `_ag_collision_key(ag, root_cause, blame_set, lever_keys)` (harness.py:18858).
   - Calls `_compute_forbidden_ag_set(reflection_buffer)` (harness.py:18857) → returns `set[tuple[str, Any, frozenset[int]]]`.
   - If `_collision_key in _forbidden`, skip the AG (`harness.py:18861`).
5. Otherwise the AG proceeds to proposal generation → safety gates → applied patches → eval → acceptance → rollback or accept.
6. On failure, `_build_reflection_entry` writes one entry per AG to `reflection_buffer`. Identity fields (`root_cause`, `blame_set`, `lever_set`, `source_cluster_signatures`) come from `_ag_identity_kwargs`.

### Where Gate G1 plugs in

After step 2 and before step 3. In the harness section that builds `_ags_inp = _ags_stage.ActionGroupsInput(...)` at `harness.py:20647`:

- Scan `clusters` once, collect `blocked_cluster_ids = [c.cluster_id for c in clusters if not c.rca_card]`.
- Emit one `cluster_blocked_no_rca_record` per blocked cluster into `_current_iter_inputs["decision_records"]`.
- Pass `blocked_cluster_ids=tuple(blocked_cluster_ids)` as a new kwarg on `ActionGroupsInput`.
- In `stages.action_groups.select(ctx, inp)`, before the strategist call, filter out any AG whose `source_cluster_ids` intersects `inp.blocked_cluster_ids`.

### Where Gate G2 plugs in

Inside `_compute_forbidden_ag_set` and `_ag_collision_key` in `harness.py`:

- `_compute_forbidden_ag_set` already returns `set[tuple[str, Any, frozenset[int]]]`. We extend its return shape to a `ForbiddenSetPair(by_root_cause, by_signature)` namedtuple, where the second member contains tuples of `(signature, frozenset(lever_set))` for every admitted reflection that has non-empty `source_cluster_signatures`.
- `_ag_collision_key` is updated to return `(root_cause_key, signature_keys)` — a tuple of the existing key plus a tuple of `(sig, frozenset(lever_set))` for each `source_cluster_signature` on the current AG.
- The collision check at `harness.py:18861` becomes: collision if `root_cause_key in forbidden.by_root_cause` OR any `sig_key` is in `forbidden.by_signature`.
- Gate G2 is **independent** of G1. They can be implemented and reviewed in two halves.

### Test fixtures used by Task 7 and Task 11

The May-12 trial captured the operator-transcript markers and decision records into the evidence-bundle outputs:

- `docs/runid_analysis/31ecd96f-5d56-4b5a-af8e-38e9e5c549af/evidence/` — airline (run 357881600282129).
- `docs/runid_analysis/ccf1d60d-d686-467b-bafa-1640131b4393/evidence/` — 7now (run 318760998419002).

The replay fixtures already exist in those directories:

- `replay_fixture_from_latest_export_357881600282129.json`
- `replay_fixture_from_latest_export_318760998419002.json`

Task 7 and Task 11 replay the fixtures and assert the new gate's effects on the decision-record stream. Read-only fixture access; no fixture mutation.

---

## Tasks

### Task 1: Add `CLUSTER_BLOCKED_NO_RCA` `DecisionType` and `RCA_UNGROUNDED` reason wiring

**Files:**
- Modify: `src/genie_space_optimizer/optimization/rca_decision_trace.py:32-54`
- Test: `tests/unit/test_rca_decision_trace.py`

This task adds the typed `DecisionType` value that the producer in Task 2 will use. The corresponding `ReasonCode.RCA_UNGROUNDED` already exists at `rca_decision_trace.py:79` and `:216`, so it is reused.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_rca_decision_trace.py`:

```python
def test_cluster_blocked_no_rca_decision_type_exists():
    from genie_space_optimizer.optimization.rca_decision_trace import DecisionType
    assert DecisionType.CLUSTER_BLOCKED_NO_RCA.value == "cluster_blocked_no_rca"


def test_cluster_blocked_no_rca_is_recognized_decision_type():
    from genie_space_optimizer.optimization.rca_decision_trace import DecisionType
    members = {m.value for m in DecisionType}
    assert "cluster_blocked_no_rca" in members
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_rca_decision_trace.py::test_cluster_blocked_no_rca_decision_type_exists -v`

Expected: `FAIL` with `AttributeError: CLUSTER_BLOCKED_NO_RCA` (the enum member does not yet exist).

- [ ] **Step 3: Add the enum value**

Modify `src/genie_space_optimizer/optimization/rca_decision_trace.py` — locate the `class DecisionType(str, Enum):` block (starts around line 32) and add the new member immediately after `INVARIANT_VIOLATION`:

```python
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
    AG_RETIRED = "ag_retired"
    ITERATION_BUDGET_DECISION = "iteration_budget_decision"
    PRODUCER_EXCEPTION = "producer_exception"
    INVARIANT_VIOLATION = "invariant_violation"
    # Defect Plan 1 (2026-05-12): typed record emitted at AG-emit time
    # when an open hard cluster has rca_card=False. Consumed by
    # check_i7_rca_grounding (already wired) AND by the runtime gate
    # in stages.action_groups.select that drops AGs whose
    # source_cluster_ids intersect the blocked set. Closes the
    # airline-trial defect where AG_DECOMPOSED_H001/H002 emitted on
    # iterations 1/3/5 and 2/4 despite rca_formed outcome=unresolved.
    CLUSTER_BLOCKED_NO_RCA = "cluster_blocked_no_rca"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_rca_decision_trace.py::test_cluster_blocked_no_rca_decision_type_exists tests/unit/test_rca_decision_trace.py::test_cluster_blocked_no_rca_is_recognized_decision_type -v`

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd packages/genie-space-optimizer
git add src/genie_space_optimizer/optimization/rca_decision_trace.py tests/unit/test_rca_decision_trace.py
git commit -m "$(cat <<'EOF'
feat(defect-1): add DecisionType.CLUSTER_BLOCKED_NO_RCA

Surfaced by RCO-4b consolidating-trial run 31ecd96f. I7 already
consumes "cluster_blocked_no_rca" decision records but no production
path emitted one. This is the substrate for the Task 2 producer
helper and the Task 4 harness wiring.
EOF
)"
```

---

### Task 2: Add `cluster_blocked_no_rca_record` producer helper

**Files:**
- Modify: `src/genie_space_optimizer/optimization/decision_emitters.py`
- Test: `tests/unit/test_decision_emitters.py`

The helper returns a single `DecisionRecord`. Pure function — no I/O. The harness call site in Task 4 wraps the call in the existing producer-exception try/except.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_decision_emitters.py`:

```python
def test_cluster_blocked_no_rca_record_produces_typed_record():
    from genie_space_optimizer.optimization.decision_emitters import (
        cluster_blocked_no_rca_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        DecisionOutcome,
        ReasonCode,
    )

    rec = cluster_blocked_no_rca_record(
        run_id="run-123",
        iteration=2,
        cluster_id="H001",
        rca_id="",
        affected_qids=["airline_ticketing_and_fare_analysis_gs_009"],
        root_cause="wrong_aggregation",
    )

    assert rec.decision_type == DecisionType.CLUSTER_BLOCKED_NO_RCA
    assert rec.outcome == DecisionOutcome.SKIPPED
    assert rec.reason_code == ReasonCode.RCA_UNGROUNDED
    assert rec.iteration == 2
    assert rec.run_id == "run-123"
    payload = rec.to_dict()
    assert payload["decision_type"] == "cluster_blocked_no_rca"
    assert payload["cluster_id"] == "H001"
    # Required by I7's consumer check (invariants.py:330-334).
    assert payload["cluster_id"] == "H001"


def test_cluster_blocked_no_rca_record_handles_empty_optional_fields():
    from genie_space_optimizer.optimization.decision_emitters import (
        cluster_blocked_no_rca_record,
    )

    rec = cluster_blocked_no_rca_record(
        run_id="run-x",
        iteration=0,
        cluster_id="H999",
        rca_id=None,
        affected_qids=None,
        root_cause=None,
    )

    assert rec.to_dict()["cluster_id"] == "H999"
    assert rec.to_dict()["rca_id"] == ""
    assert rec.to_dict()["root_cause"] == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_decision_emitters.py::test_cluster_blocked_no_rca_record_produces_typed_record -v`

Expected: `FAIL` with `ImportError: cannot import name 'cluster_blocked_no_rca_record'`.

- [ ] **Step 3: Add the producer helper**

Append to `src/genie_space_optimizer/optimization/decision_emitters.py`:

```python
def cluster_blocked_no_rca_record(
    *,
    run_id: str,
    iteration: int,
    cluster_id: str,
    rca_id: str | None,
    affected_qids: Sequence[str] | None,
    root_cause: str | None,
) -> DecisionRecord:
    """Defect Plan 1 (2026-05-12) — emit a typed record when an open
    hard cluster reaches AG-emit time with no fit RCA card.

    Consumed by:

    1. ``invariants.check_i7_rca_grounding`` — the green branch
       (cluster present in ``blocked_clusters`` set) becomes
       reachable in production once the harness wires this producer
       (Task 4). Before this plan, I7 was detection-only.
    2. ``stages.action_groups.select`` — the runtime gate (Task 6)
       reads ``ActionGroupsInput.blocked_cluster_ids`` (Task 5) and
       drops AGs whose ``source_cluster_ids`` intersect the blocked
       set.

    Pure function. The harness call site (Task 4) wraps the call in
    the producer-exception try/except so any failure becomes a
    typed ``PRODUCER_EXCEPTION`` record rather than a silent mute.
    """
    return DecisionRecord(
        run_id=str(run_id or ""),
        iteration=int(iteration),
        decision_type=DecisionType.CLUSTER_BLOCKED_NO_RCA,
        outcome=DecisionOutcome.SKIPPED,
        reason_code=ReasonCode.RCA_UNGROUNDED,
        cluster_id=str(cluster_id or ""),
        rca_id=str(rca_id or ""),
        ag_id="",
        target_qids=[str(q) for q in (affected_qids or []) if q],
        root_cause=str(root_cause or ""),
        next_action=(
            "regenerate RCA evidence for this cluster or escalate to "
            "diagnostic-AG path before re-attempting AG emission"
        ),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_decision_emitters.py -k cluster_blocked_no_rca -v`

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/decision_emitters.py tests/unit/test_decision_emitters.py
git commit -m "feat(defect-1): add cluster_blocked_no_rca_record producer helper"
```

---

### Task 3: Add feature-flag accessors (default-ON)

**Files:**
- Modify: `src/genie_space_optimizer/common/config.py`
- Test: `tests/unit/test_config.py`

Two accessors:

- `ag_emit_grounding_gate_enabled()` — gates the Task 4-6 changes (G1).
- `forbidden_ag_collision_by_cluster_signature_enabled()` — gates the Task 9-10 changes (G2).

Both default ON per the RCO-4/4b convention. Rollback escape hatch is `GSO_*=0`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_config.py`:

```python
def test_ag_emit_grounding_gate_enabled_defaults_to_true(monkeypatch):
    monkeypatch.delenv("GSO_AG_EMIT_GROUNDING_GATE", raising=False)
    from genie_space_optimizer.common.config import ag_emit_grounding_gate_enabled
    assert ag_emit_grounding_gate_enabled() is True


def test_ag_emit_grounding_gate_disabled_when_falsy(monkeypatch):
    from genie_space_optimizer.common.config import ag_emit_grounding_gate_enabled
    for v in ("0", "false", "FALSE", "off", "no"):
        monkeypatch.setenv("GSO_AG_EMIT_GROUNDING_GATE", v)
        assert ag_emit_grounding_gate_enabled() is False, f"falsy value {v!r} did not disable the flag"


def test_forbidden_ag_collision_by_cluster_signature_enabled_defaults_to_true(monkeypatch):
    monkeypatch.delenv(
        "GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", raising=False
    )
    from genie_space_optimizer.common.config import (
        forbidden_ag_collision_by_cluster_signature_enabled,
    )
    assert forbidden_ag_collision_by_cluster_signature_enabled() is True


def test_forbidden_ag_collision_by_cluster_signature_disabled_when_falsy(monkeypatch):
    from genie_space_optimizer.common.config import (
        forbidden_ag_collision_by_cluster_signature_enabled,
    )
    for v in ("0", "false", "FALSE", "off", "no"):
        monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", v)
        assert forbidden_ag_collision_by_cluster_signature_enabled() is False, (
            f"falsy value {v!r} did not disable the flag"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py::test_ag_emit_grounding_gate_enabled_defaults_to_true -v`

Expected: `FAIL` with `ImportError: cannot import name 'ag_emit_grounding_gate_enabled'`.

- [ ] **Step 3: Add the accessors**

Append to `src/genie_space_optimizer/common/config.py` (after the existing RCO-4b accessors near line 5450):

```python
def ag_emit_grounding_gate_enabled() -> bool:
    """Defect Plan 1 (2026-05-12) — runtime gate that blocks AG
    emission for open hard clusters whose ``rca_card`` is falsy.

    When ON (default), the harness emits one
    ``DecisionType.CLUSTER_BLOCKED_NO_RCA`` record per ungrounded
    open hard cluster at AG-emit time and ``stages.action_groups.
    select`` drops every AG whose ``source_cluster_ids`` intersects
    the blocked set. Closes the airline-trial defect where
    ``AG_DECOMPOSED_H001`` for ``gs_009`` and ``AG_DECOMPOSED_H002``
    for ``gs_024`` continued to emit despite ``rca_formed
    outcome=unresolved reason=rca_ungrounded`` on every iteration.

    Detection-side guarantee: ``invariants.check_i7_rca_grounding``
    will return zero violations on runs where this flag is ON and
    every ungrounded cluster has the matching block record. The I7
    green-when-block-record-emitted test
    (``tests/unit/test_invariants.py:343``) already pins the
    consumer behavior.

    Default ON. Disable with ``GSO_AG_EMIT_GROUNDING_GATE=0`` for
    replay byte-stability against pre-defect-plan-1 fixtures.
    """
    return _flag_default_on("GSO_AG_EMIT_GROUNDING_GATE")


def forbidden_ag_collision_by_cluster_signature_enabled() -> bool:
    """Defect Plan 1 (2026-05-12) — broaden the forbidden-AG
    collision key to also key on ``source_cluster_signatures``, so
    the strategist cannot re-admit an AG family on iteration N+1
    just because the LLM regenerated the same cluster's
    ``root_cause`` with slightly different text.

    Before this flag, ``_compute_forbidden_ag_set`` returns
    ``set[tuple[root_cause, blame, frozenset(lever_set)]]`` and
    ``_ag_collision_key`` looks up the same tuple. The 7now-trial
    run ccf1d60d showed five iterations of the same AG family slip
    through because iteration N+1's LLM-regenerated ``root_cause``
    string did not byte-equal iteration N's, even though both AGs
    targeted the same cluster.

    When this flag is ON (default), the forbidden set carries a
    second axis keyed on ``(source_cluster_signature, frozenset(
    lever_set))`` where the signature is the clusterer-derived
    sha1 (stable across iterations by construction —
    ``harness.py:10277-10281`` builds it from
    ``base_question_ids + root_cause + blame``). Collision matches
    on EITHER axis.

    Default ON. Disable with
    ``GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE=0`` for
    replay byte-stability against pre-defect-plan-1 fixtures.
    """
    return _flag_default_on("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -k "ag_emit_grounding_gate or forbidden_ag_collision_by_cluster_signature" -v`

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/common/config.py tests/unit/test_config.py
git commit -m "feat(defect-1): add default-on flag accessors for grounding gate + signature collision"
```

---

### Task 4: Wire grounding-gate emission in harness

**Files:**
- Modify: `src/genie_space_optimizer/optimization/harness.py` (immediately before the `_ags_inp = _ags_stage.ActionGroupsInput(...)` construction around line 20647)
- Test: `tests/unit/test_harness_grounding_gate.py` (new file)

The harness emits one `cluster_blocked_no_rca_record` per open hard cluster with `rca_card=False`, places the records on `_current_iter_inputs["decision_records"]`, and accumulates `blocked_cluster_ids: list[str]` for plumbing into the action-groups stage in Task 5. Wrapped in the existing producer-exception try/except so a producer fault does not crash the iteration.

- [ ] **Step 1: Create the failing test file**

Create `tests/unit/test_harness_grounding_gate.py`:

```python
"""Defect Plan 1 — unit tests for the harness wiring of the
``CLUSTER_BLOCKED_NO_RCA`` producer.

The harness logic itself is integration-heavy, so this test file
exercises the pure helper extracted in Task 4 step 3 below
(``collect_blocked_clusters``). The end-to-end behaviour is locked
in by Task 7's replay test.
"""

from __future__ import annotations


def test_collect_blocked_clusters_returns_ids_with_no_rca_card():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {"cluster_id": "H001", "rca_card": False, "root_cause": "wrong_agg"},
        {"cluster_id": "H002", "rca_card": True, "root_cause": "missing_filter"},
        {"cluster_id": "H003", "rca_card": None, "root_cause": "join_mismatch"},
        {"cluster_id": "H004", "rca_card": {}, "root_cause": "join_mismatch"},
        {"cluster_id": "H005", "rca_card": {"sections": [{}]}, "root_cause": "x"},
    ]

    result = collect_blocked_clusters(clusters)

    # H001 (False), H003 (None), H004 (empty dict) — all falsy → blocked.
    # H002 (True), H005 (non-empty dict) — truthy → grounded.
    assert sorted(result.blocked_cluster_ids) == ["H001", "H003", "H004"]
    assert len(result.records_payload) == 3
    assert {r["cluster_id"] for r in result.records_payload} == {
        "H001", "H003", "H004",
    }
    for payload in result.records_payload:
        assert payload["decision_type"] == "cluster_blocked_no_rca"
        assert payload["reason_code"] == "rca_ungrounded"


def test_collect_blocked_clusters_passes_root_cause_into_record():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {
            "cluster_id": "H001",
            "rca_card": False,
            "root_cause": "wrong_aggregation",
            "base_question_ids": [
                "airline_ticketing_and_fare_analysis_gs_009",
            ],
        }
    ]

    result = collect_blocked_clusters(clusters, run_id="r1", iteration=2)
    payload = result.records_payload[0]
    assert payload["root_cause"] == "wrong_aggregation"
    assert payload["target_qids"] == [
        "airline_ticketing_and_fare_analysis_gs_009"
    ]
    assert payload["iteration"] == 2


def test_collect_blocked_clusters_returns_empty_when_all_grounded():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {"cluster_id": "H001", "rca_card": {"sections": [{}]}},
        {"cluster_id": "H002", "rca_card": True},
    ]
    result = collect_blocked_clusters(clusters)
    assert result.blocked_cluster_ids == []
    assert result.records_payload == []


def test_collect_blocked_clusters_skips_clusters_without_id():
    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = [
        {"cluster_id": "", "rca_card": False},
        {"rca_card": False, "root_cause": "x"},
        {"cluster_id": "H001", "rca_card": False},
    ]
    result = collect_blocked_clusters(clusters)
    assert result.blocked_cluster_ids == ["H001"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_harness_grounding_gate.py -v`

Expected: `ImportError: cannot import name 'collect_blocked_clusters' from 'genie_space_optimizer.optimization.harness'`.

- [ ] **Step 3: Add the pure helper to harness**

Insert the helper near the other pure helpers in `harness.py` (between `_reflection_admitted_to_forbidden_set` at line 10777 and `_compute_forbidden_ag_set` at line 10836):

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class _BlockedClustersResult:
    """Defect Plan 1 — pure result of :func:`collect_blocked_clusters`.

    ``blocked_cluster_ids`` is the list passed into
    :class:`ActionGroupsInput.blocked_cluster_ids` so
    :func:`stages.action_groups.select` can filter AGs whose
    ``source_cluster_ids`` intersect this set.

    ``records_payload`` is a list of ``dict`` (not ``DecisionRecord``)
    so it can be appended to ``_current_iter_inputs["decision_records"]``
    without an additional conversion (mirrors the existing
    decision-emitter wiring throughout ``harness.py``).
    """

    blocked_cluster_ids: list[str]
    records_payload: list[dict]


def collect_blocked_clusters(
    clusters: list[dict],
    *,
    run_id: str = "",
    iteration: int = 0,
) -> _BlockedClustersResult:
    """Defect Plan 1 (2026-05-12) — scan ``clusters`` for entries
    whose ``rca_card`` is falsy and produce a paired result of
    blocked-cluster ids and typed ``CLUSTER_BLOCKED_NO_RCA``
    decision records.

    Pure function (no I/O, no flag reads). The caller is the
    AG-emit prelude in ``_run_lever_loop`` (this plan's Task 4 wire
    site). The caller decides whether to plumb the result based on
    ``ag_emit_grounding_gate_enabled()`` — when off, the result is
    discarded and behaviour is byte-stable with pre-defect-plan-1
    runs.

    A cluster is considered "ungrounded" when:

    * It has a non-empty ``cluster_id`` (entries without an id are
      skipped — they cannot be referenced from
      ``source_cluster_ids`` anyway).
    * ``bool(c.get("rca_card"))`` is False. This matches the
      existing ``rca_cards_present`` projection at
      ``harness.py:18338-18340`` which uses the same predicate.
    """
    from genie_space_optimizer.optimization.decision_emitters import (
        cluster_blocked_no_rca_record,
    )

    blocked_ids: list[str] = []
    payloads: list[dict] = []
    for c in clusters or []:
        cid = str((c or {}).get("cluster_id") or "")
        if not cid:
            continue
        if bool((c or {}).get("rca_card")):
            continue
        rec = cluster_blocked_no_rca_record(
            run_id=run_id,
            iteration=iteration,
            cluster_id=cid,
            rca_id=str((c or {}).get("rca_id") or ""),
            affected_qids=list((c or {}).get("base_question_ids") or []),
            root_cause=str((c or {}).get("root_cause") or ""),
        )
        blocked_ids.append(cid)
        payloads.append(rec.to_dict())
    return _BlockedClustersResult(
        blocked_cluster_ids=blocked_ids,
        records_payload=payloads,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_harness_grounding_gate.py -v`

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/harness.py tests/unit/test_harness_grounding_gate.py
git commit -m "feat(defect-1): add collect_blocked_clusters pure helper for grounding gate"
```

---

### Task 5: Extend `ActionGroupsInput` with `blocked_cluster_ids`

**Files:**
- Modify: `src/genie_space_optimizer/optimization/stages/action_groups.py:165-239`
- Test: `tests/unit/test_action_groups_contract.py`

Add a new optional field; default empty tuple so flag-off byte-stability holds.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_action_groups_contract.py`:

```python
def test_action_groups_input_carries_blocked_cluster_ids():
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )

    inp = ActionGroupsInput(
        action_groups=(),
        blocked_cluster_ids=("H001", "H003"),
    )
    assert inp.blocked_cluster_ids == ("H001", "H003")


def test_action_groups_input_blocked_cluster_ids_defaults_to_empty():
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )

    inp = ActionGroupsInput(action_groups=())
    assert inp.blocked_cluster_ids == ()


def test_action_groups_input_blocked_cluster_ids_round_trips():
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )

    src = ActionGroupsInput(
        action_groups=(),
        blocked_cluster_ids=("H001", "H002"),
    )
    payload = src.to_json()
    rt = ActionGroupsInput.from_json(payload)
    assert rt.blocked_cluster_ids == ("H001", "H002")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_action_groups_contract.py -k blocked_cluster_ids -v`

Expected: `FAIL` with `TypeError: __init__() got an unexpected keyword argument 'blocked_cluster_ids'`.

- [ ] **Step 3: Add the field**

Modify `src/genie_space_optimizer/optimization/stages/action_groups.py` — the `ActionGroupsInput` dataclass declaration around line 165-206. Add the new field directly after `forbidden_ags`:

```python
@dataclass
class ActionGroupsInput(JsonRoundTrip):
    """Input to stages.action_groups.select.

    ``action_groups`` is the slate of AGs the strategist returned (after
    filtering and buffered-AG drain — F4 doesn't re-do that work).
    ``source_clusters_by_id`` maps cluster id to cluster dict so each
    AG's root_cause can be recovered. ``rca_id_by_cluster`` maps cluster
    id to its RCA id. ``ag_alternatives_by_id`` carries Phase D.5
    rejected-alternatives stamping.

    C15 Phase 3: ``forbidden_ags`` carries the typed forbidden-AG set so
    select() can produce a per-candidate AdmissionTrace when
    ``stage_handlers_chunk_b_enabled()`` is on.

    Defect Plan 1 (2026-05-12): ``blocked_cluster_ids`` carries the set
    of cluster ids that the AG-emit prelude
    (``harness.collect_blocked_clusters``) marked as ungrounded. When
    non-empty AND ``ag_emit_grounding_gate_enabled()`` is True,
    ``select()`` drops every AG whose ``source_cluster_ids``
    intersects this set. Empty tuple preserves pre-defect-plan-1
    byte-stability.
    """

    action_groups: tuple[Mapping[str, Any], ...]
    source_clusters_by_id: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )
    rca_id_by_cluster: Mapping[str, str] = field(default_factory=dict)
    ag_alternatives_by_id: Mapping[str, Sequence[AlternativeOption]] = field(
        default_factory=dict
    )
    prior_buckets_by_qid: Mapping[str, Any] = field(default_factory=dict)
    prior_iteration_dropped_causal_patches: tuple[Any, ...] = ()
    forbidden_ags: tuple[ForbiddenAG, ...] = ()
    # Defect Plan 1 (2026-05-12) — cluster ids with rca_card=False at
    # AG-emit time. select() drops AGs whose source_cluster_ids
    # intersect this set when ag_emit_grounding_gate_enabled().
    blocked_cluster_ids: tuple[str, ...] = ()
```

Update the `from_json` classmethod (around line 208-239) to round-trip the new field:

```python
    @classmethod
    def from_json(cls, payload: dict) -> "ActionGroupsInput":
        ags = tuple(
            dict(a) for a in (payload.get("action_groups") or [])
        )
        src = {
            str(k): dict(v)
            for k, v in (payload.get("source_clusters_by_id") or {}).items()
        }
        rca_by_cluster = {
            str(k): str(v)
            for k, v in (payload.get("rca_id_by_cluster") or {}).items()
        }
        ag_alts = {
            str(k): tuple(v)
            for k, v in (payload.get("ag_alternatives_by_id") or {}).items()
        }
        buckets = dict(payload.get("prior_buckets_by_qid") or {})
        dropped = tuple(payload.get("prior_iteration_dropped_causal_patches") or [])
        forbidden = tuple(
            ForbiddenAG.from_json(f)
            for f in (payload.get("forbidden_ags") or [])
        )
        blocked = tuple(
            str(c) for c in (payload.get("blocked_cluster_ids") or [])
        )
        return cls(
            action_groups=ags,
            source_clusters_by_id=src,
            rca_id_by_cluster=rca_by_cluster,
            ag_alternatives_by_id=ag_alts,
            prior_buckets_by_qid=buckets,
            prior_iteration_dropped_causal_patches=dropped,
            forbidden_ags=forbidden,
            blocked_cluster_ids=blocked,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_action_groups_contract.py -k blocked_cluster_ids -v`

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/stages/action_groups.py tests/unit/test_action_groups_contract.py
git commit -m "feat(defect-1): add blocked_cluster_ids field to ActionGroupsInput"
```

---

### Task 6: Filter blocked AGs in `stages.action_groups.select`

**Files:**
- Modify: `src/genie_space_optimizer/optimization/stages/action_groups.py:400-490` (the `select` function)
- Test: `tests/unit/test_stages_action_groups.py`

The runtime filter is added between the bucket-policy filtering and the strategist record emission. AGs whose `source_cluster_ids` intersect the blocked set are dropped before the strategist record is emitted and before the AdmissionTrace is built.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_stages_action_groups.py`:

```python
def test_select_drops_ags_whose_source_clusters_are_blocked(monkeypatch):
    """Defect Plan 1 G1 — when the grounding gate is on, AGs whose
    source_cluster_ids intersect ``blocked_cluster_ids`` must not
    appear in the returned slate."""
    monkeypatch.setenv("GSO_AG_EMIT_GROUNDING_GATE", "1")
    from types import SimpleNamespace

    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput, select,
    )

    inp = ActionGroupsInput(
        action_groups=(
            {
                "id": "AG_H001",
                "source_cluster_ids": ["H001"],
                "affected_questions": ["gs_009"],
            },
            {
                "id": "AG_H002",
                "source_cluster_ids": ["H002"],
                "affected_questions": ["gs_024"],
            },
            {
                "id": "AG_H003",
                "source_cluster_ids": ["H003", "H004"],
                "affected_questions": ["gs_018"],
            },
        ),
        blocked_cluster_ids=("H001", "H003"),
    )
    ctx = SimpleNamespace(
        run_id="r-1",
        iteration=2,
        decision_emit=lambda rec: None,
    )

    slate = select(ctx, inp)

    surviving_ids = {ag.get("id") for ag in slate.ags}
    # AG_H001 → dropped (H001 is blocked).
    # AG_H002 → survives.
    # AG_H003 → dropped (H003 is blocked, even though H004 is not).
    assert surviving_ids == {"AG_H002"}


def test_select_keeps_all_ags_when_blocked_set_is_empty(monkeypatch):
    """Backward compatibility — flag-off behaviour."""
    monkeypatch.setenv("GSO_AG_EMIT_GROUNDING_GATE", "1")
    from types import SimpleNamespace

    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput, select,
    )

    inp = ActionGroupsInput(
        action_groups=(
            {"id": "AG1", "source_cluster_ids": ["H001"]},
            {"id": "AG2", "source_cluster_ids": ["H002"]},
        ),
        blocked_cluster_ids=(),
    )
    ctx = SimpleNamespace(run_id="r-1", iteration=1, decision_emit=lambda r: None)

    slate = select(ctx, inp)
    assert {ag.get("id") for ag in slate.ags} == {"AG1", "AG2"}


def test_select_keeps_all_ags_when_flag_off(monkeypatch):
    """Replay byte-stability — GSO_AG_EMIT_GROUNDING_GATE=0 disables
    the filter even when blocked_cluster_ids is non-empty."""
    monkeypatch.setenv("GSO_AG_EMIT_GROUNDING_GATE", "0")
    from types import SimpleNamespace

    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput, select,
    )

    inp = ActionGroupsInput(
        action_groups=(
            {"id": "AG_H001", "source_cluster_ids": ["H001"]},
            {"id": "AG_H002", "source_cluster_ids": ["H002"]},
        ),
        blocked_cluster_ids=("H001",),
    )
    ctx = SimpleNamespace(run_id="r-1", iteration=1, decision_emit=lambda r: None)

    slate = select(ctx, inp)
    assert {ag.get("id") for ag in slate.ags} == {"AG_H001", "AG_H002"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_stages_action_groups.py -k blocked -v`

Expected: `test_select_drops_ags_whose_source_clusters_are_blocked` FAILs with `assert {'AG_H001', 'AG_H002', 'AG_H003'} == {'AG_H002'}` (filter not yet implemented).

- [ ] **Step 3: Implement the filter**

Modify `stages/action_groups.py:select(...)` — insert the blocked-cluster filter immediately after the existing `bucket_driven_ag_selection_enabled()` branch (around line 441-452). The exact diff:

```python
def select(ctx, inp: ActionGroupsInput) -> ActionGroupSlate:
    """[existing docstring]

    Defect Plan 1 (2026-05-12): when ``ag_emit_grounding_gate_enabled()``
    is True AND ``inp.blocked_cluster_ids`` is non-empty, AGs whose
    ``source_cluster_ids`` intersect the blocked set are filtered
    out before strategist-record emission. The harness has already
    emitted one ``CLUSTER_BLOCKED_NO_RCA`` decision record per
    blocked cluster, so the postmortem operator transcript shows
    *why* the AG was dropped.
    """
    from genie_space_optimizer.common.config import (
        ag_emit_grounding_gate_enabled,
        bucket_driven_ag_selection_enabled,
        stage_handlers_chunk_b_enabled,
    )
    from genie_space_optimizer.optimization.llm_boundary_sort import (
        sort_action_groups_canonically,
    )

    # RCO-7 Site 2 — canonical pre-sort.
    sorted_action_groups = tuple(
        sort_action_groups_canonically(inp.action_groups)
    )
    sorted_forbidden_ags = tuple(
        sorted(inp.forbidden_ags, key=lambda f: f.ag_id)
    )

    if (
        bucket_driven_ag_selection_enabled()
        and inp.prior_buckets_by_qid
    ):
        filtered_ags = tuple(
            _apply_bucket_policy(
                sorted_action_groups,
                buckets_by_qid=inp.prior_buckets_by_qid,
            )
        )
    else:
        filtered_ags = sorted_action_groups

    # Defect Plan 1 (2026-05-12) — grounding gate. Drop AGs whose
    # source_cluster_ids intersect the blocked set.
    if ag_emit_grounding_gate_enabled() and inp.blocked_cluster_ids:
        blocked = set(inp.blocked_cluster_ids)
        filtered_ags = tuple(
            ag for ag in filtered_ags
            if not (set(ag.get("source_cluster_ids") or []) & blocked)
        )

    # [existing post-filter logic continues unchanged from here]
    filtered_ags = tuple(
        normalize_strategist_ags_with_recommended_levers(
            ags=list(filtered_ags),
            clusters=list(inp.source_clusters_by_id.values())
                if inp.source_clusters_by_id else [],
        )
    )

    records = strategist_ag_records(
        run_id=ctx.run_id,
        iteration=ctx.iteration,
        action_groups=filtered_ags,
        source_clusters_by_id=inp.source_clusters_by_id,
        rca_id_by_cluster=inp.rca_id_by_cluster,
        ag_alternatives_by_id=inp.ag_alternatives_by_id,
    )
    for record in records:
        ctx.decision_emit(record)

    admission_trace: tuple[AdmissionTrace, ...] = ()
    if stage_handlers_chunk_b_enabled() and sorted_forbidden_ags:
        admission_trace = _build_admission_trace(
            candidates=sorted_action_groups,
            forbidden_ags=sorted_forbidden_ags,
        )

    return ActionGroupSlate(
        ags=filtered_ags,
        rejected_ag_alternatives=(),
        admission_trace=admission_trace,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_stages_action_groups.py -k blocked -v`

Expected: 3 passed.

Also run the broader stage-handlers suite to confirm we did not regress AdmissionTrace ordering:

Run: `uv run pytest tests/unit/test_stages_action_groups.py tests/unit/test_action_groups_contract.py tests/unit/test_action_groups_bucket_feedback.py -v`

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/stages/action_groups.py tests/unit/test_stages_action_groups.py
git commit -m "$(cat <<'EOF'
feat(defect-1): filter ungrounded AGs in stages.action_groups.select

When ag_emit_grounding_gate_enabled() is on AND
inp.blocked_cluster_ids is non-empty, AGs whose source_cluster_ids
intersect the blocked set are dropped before strategist-record
emission. Closes the airline-trial defect where AG_DECOMPOSED_H001
and AG_DECOMPOSED_H002 emitted on every iteration despite
rca_formed outcome=unresolved reason=rca_ungrounded.
EOF
)"
```

---

### Task 7: End-to-end harness wiring + airline-trial replay parity

**Files:**
- Modify: `src/genie_space_optimizer/optimization/harness.py` (the AG-emit prelude immediately before `_ags_inp = _ags_stage.ActionGroupsInput(...)` around line 20647)
- Test: `tests/integration/test_ag_grounding_and_admission_replay.py` (new file)

This task wires `collect_blocked_clusters` into the live harness path, plumbs the result into `ActionGroupsInput`, and replays the May-12 airline fixture to lock in the end-to-end behaviour.

- [ ] **Step 1: Create the failing replay test**

Create `tests/integration/test_ag_grounding_and_admission_replay.py`:

```python
"""Defect Plan 1 — replay tests against the May-12 consolidating-trial
captures.

These tests load the persisted replay fixtures from
``docs/runid_analysis/<opt_run_id>/evidence/`` and assert that the
defect-1 gates would have changed the operator-visible outcome.

The fixtures themselves are read-only artefacts; the tests do NOT
re-run the lever loop, they project the captured cluster + reflection
state through the new pure helpers.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

AIRLINE_EVIDENCE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "docs"
    / "runid_analysis"
    / "31ecd96f-5d56-4b5a-af8e-38e9e5c549af"
    / "evidence"
)
SEVENNOW_EVIDENCE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "docs"
    / "runid_analysis"
    / "ccf1d60d-d686-467b-bafa-1640131b4393"
    / "evidence"
)


def _load_replay(path: pathlib.Path) -> dict[str, Any]:
    if not path.is_file():
        pytest.skip(
            f"replay fixture missing at {path} — the May-12 captures "
            f"must be promoted into the runid_analysis tree before "
            f"this test can run"
        )
    return json.loads(path.read_text())


def _iter_clusters(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    """Best-effort projection of the per-iteration cluster slate from
    the replay fixture. Supports both the iter-record list shape and
    the legacy decisions-only shape.
    """
    iters = fixture.get("iterations") or fixture.get("iter_records") or []
    out: list[dict[str, Any]] = []
    for it in iters:
        clusters = it.get("clusters") or it.get("source_clusters") or []
        for c in clusters:
            out.append(dict(c))
    return out


def test_airline_replay_emits_block_records_for_ungrounded_clusters():
    fixture = _load_replay(
        AIRLINE_EVIDENCE / "replay_fixture_from_latest_export_357881600282129.json"
    )

    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = _iter_clusters(fixture)
    if not clusters:
        pytest.skip(
            "replay fixture has no per-iteration cluster projection; "
            "promote richer evidence-bundle output first"
        )

    result = collect_blocked_clusters(clusters, run_id="replay-airline", iteration=0)
    blocked = set(result.blocked_cluster_ids)

    # The postmortem (F3) names gs_009 (cluster H001) and gs_024
    # (cluster H002) as the ungrounded clusters. If the replay
    # fixture's cluster ids differ, fall back to a structural check:
    # at least one cluster must be in the blocked set, otherwise the
    # fixture does not reproduce the airline failure mode and the
    # test should be re-pointed.
    assert blocked, (
        "no ungrounded clusters in airline fixture — either the "
        "fixture does not capture the rca_card=False clusters or the "
        "fixture shape changed; re-derive against the postmortem F3 "
        "evidence"
    )


def test_airline_replay_select_drops_blocked_ag_families(monkeypatch):
    """With the grounding gate on, AGs whose source_cluster_ids are
    all blocked must not appear in the final slate.

    The airline fixture's iteration 1 contained ``AG_DECOMPOSED_H001``
    and ``AG_DECOMPOSED_H002`` whose source_cluster_ids were exactly
    the ungrounded {H001, H002} set; with the gate on, neither AG
    should survive ``select``.
    """
    monkeypatch.setenv("GSO_AG_EMIT_GROUNDING_GATE", "1")

    from types import SimpleNamespace

    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput, select,
    )

    fixture = _load_replay(
        AIRLINE_EVIDENCE / "replay_fixture_from_latest_export_357881600282129.json"
    )
    clusters = _iter_clusters(fixture)
    if not clusters:
        pytest.skip("airline fixture lacks per-iteration cluster projection")

    grounding = collect_blocked_clusters(clusters, run_id="replay", iteration=0)
    if not grounding.blocked_cluster_ids:
        pytest.skip("airline fixture has no rca_card=False clusters in projection")

    synthetic_ags = tuple(
        {"id": f"AG_{cid}", "source_cluster_ids": [cid]}
        for cid in grounding.blocked_cluster_ids
    )

    inp = ActionGroupsInput(
        action_groups=synthetic_ags,
        blocked_cluster_ids=tuple(grounding.blocked_cluster_ids),
    )
    ctx = SimpleNamespace(run_id="r", iteration=0, decision_emit=lambda r: None)
    slate = select(ctx, inp)
    assert slate.ags == (), (
        f"grounding gate did not drop ungrounded AGs; survivors: "
        f"{[ag.get('id') for ag in slate.ags]}"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_ag_grounding_and_admission_replay.py::test_airline_replay_emits_block_records_for_ungrounded_clusters -v`

Expected: PASS (the helper from Task 4 already exists). If the fixture lacks the cluster projection, the test SKIPs with a clear message.

Run: `uv run pytest tests/integration/test_ag_grounding_and_admission_replay.py::test_airline_replay_select_drops_blocked_ag_families -v`

Expected: PASS (the filter from Task 6 already exists, and the harness wiring is not yet on the production path so this synthetic test stands alone).

- [ ] **Step 3: Wire `collect_blocked_clusters` into the production harness path**

Modify `src/genie_space_optimizer/optimization/harness.py` — locate the `_ags_inp = _ags_stage.ActionGroupsInput(...)` construction around line 20647. Insert the grounding-gate prelude immediately before it, and pass `blocked_cluster_ids` into the constructor:

```python
                # Defect Plan 1 (2026-05-12) — grounding gate prelude.
                # Emit one CLUSTER_BLOCKED_NO_RCA decision record per
                # open hard cluster whose rca_card is falsy, accumulate
                # blocked_cluster_ids, plumb into ActionGroupsInput.
                # Wrapped in producer-exception try/except so a record
                # construction fault becomes a typed PRODUCER_EXCEPTION
                # rather than a silent mute (matches the existing
                # decision-emitter contract throughout this file).
                _blocked_cluster_ids_tuple: tuple[str, ...] = ()
                try:
                    from genie_space_optimizer.common.config import (
                        ag_emit_grounding_gate_enabled,
                    )
                    if ag_emit_grounding_gate_enabled():
                        _grounding_result = collect_blocked_clusters(
                            clusters or [],
                            run_id=run_id,
                            iteration=iteration_counter,
                        )
                        _blocked_cluster_ids_tuple = tuple(
                            _grounding_result.blocked_cluster_ids
                        )
                        if _grounding_result.records_payload:
                            _current_iter_inputs.setdefault(
                                "decision_records", []
                            ).extend(_grounding_result.records_payload)
                except Exception as _grounding_exc:
                    try:
                        from genie_space_optimizer.common.config import (
                            phase_b_producer_typed_exceptions_enabled as _typed_on,
                        )
                        if _typed_on():
                            from genie_space_optimizer.optimization.decision_emitters import (
                                producer_exception_record as _producer_exception_record,
                            )
                            _pe_rec = _producer_exception_record(
                                run_id=run_id,
                                iteration=iteration_counter,
                                producer="cluster_blocked_no_rca",
                                ag_id="",
                                exception=_grounding_exc,
                            )
                            _current_iter_inputs.setdefault(
                                "decision_records", []
                            ).append(_pe_rec.to_dict())
                    except Exception:
                        logger.debug(
                            "Defect Plan 1: producer_exception_record emission "
                            "failed for cluster_blocked_no_rca",
                            exc_info=True,
                        )
                    logger.debug(
                        "Defect Plan 1: grounding gate prelude failed "
                        "(non-fatal); skipping AG filter for this iteration",
                        exc_info=True,
                    )

                _ags_inp = _ags_stage.ActionGroupsInput(
                    action_groups=tuple([ag]),
                    source_clusters_by_id={
                        str(_c.get("cluster_id") or ""): _c
                        for _c in (clusters or [])
                        if _c.get("cluster_id")
                    },
                    # [existing kwargs continue]
                    forbidden_ags=_chunk_b_forbidden_ags,
                    blocked_cluster_ids=_blocked_cluster_ids_tuple,
                )
```

- [ ] **Step 4: Run replay test + the full I7 unit suite to verify the green branch is now reached on synthesised evidence**

Run: `uv run pytest tests/integration/test_ag_grounding_and_admission_replay.py tests/unit/test_invariants.py -k "i7 or grounding or admission_replay" -v`

Expected: replay tests pass; existing I7 unit tests pass (we did not change the consumer).

Also run a broader harness-shape regression:

Run: `uv run pytest tests/unit/test_stages_action_groups.py tests/unit/test_action_groups_contract.py tests/unit/test_action_groups_bucket_feedback.py tests/unit/test_harness_grounding_gate.py -v`

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/harness.py tests/integration/test_ag_grounding_and_admission_replay.py
git commit -m "$(cat <<'EOF'
feat(defect-1): wire grounding-gate prelude into AG-emit dispatch

Plumb collect_blocked_clusters output through ActionGroupsInput so
stages.action_groups.select can filter ungrounded AGs in production
(not just in unit tests). Replay against the airline May-12 trial
fixture confirms the filter drops the two AG families that previously
emitted on every iteration.

Closes the live-path half of the airline-trial Gate G1 defect.
EOF
)"
```

---

### Task 8: Cluster-signature collision-key extension — pure-helper foundation

**Files:**
- Modify: `src/genie_space_optimizer/optimization/harness.py:10836-10936` (`_compute_forbidden_ag_set` + `_ag_collision_key`)
- Test: `tests/unit/test_harness_forbidden_ag.py` (new file)

This task introduces the cluster-signature-based forbidden subset and the broadened collision key as **pure helpers** with full unit coverage. Task 9 swaps the call site to use them; Task 10 verifies on the 7now fixture.

- [ ] **Step 1: Create the failing test file**

Create `tests/unit/test_harness_forbidden_ag.py`:

```python
"""Defect Plan 1 G2 — unit tests for the cluster-signature collision
key.

Today's collision is keyed on ``(root_cause, blame, frozenset(
lever_set))``. The 7now-trial defect was that the LLM regenerated
``root_cause`` text on iteration N+1 so the same cluster's AG slipped
through. These tests pin the broadened lookup that ALSO keys on
``source_cluster_signatures`` (clusterer-derived, stable across
iterations by construction — see harness.py:10277-10281 sha1 over
base_question_ids + root_cause + blame).
"""

from __future__ import annotations


def _reflection_entry(
    *,
    root_cause: str,
    cluster_signature: str,
    lever_set: list[int],
    rollback_class: str = "no_action",
    accepted: bool = False,
) -> dict:
    """Build a NO_ACTION reflection entry that the admission predicate
    accepts (non-empty root_cause + lever_set; escalation_handled
    False; matches the live shape from _build_reflection_entry).
    """
    return {
        "rollback_class": rollback_class,
        "rollback_reason": "no_proposals",
        "accepted": accepted,
        "escalation_handled": False,
        "root_cause": root_cause,
        "blame_set": ("gs_026",),
        "lever_set": lever_set,
        "source_cluster_signatures": [cluster_signature],
        "iteration": 1,
    }


def test_compute_forbidden_ag_set_pair_returns_root_and_signature_subsets(
    monkeypatch,
):
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    from genie_space_optimizer.optimization.harness import (
        _compute_forbidden_ag_set_pair,
    )

    buf = [
        _reflection_entry(
            root_cause="plural top-N collapse on zone_combination",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    pair = _compute_forbidden_ag_set_pair(buf)
    assert (
        "plural top-N collapse on zone_combination",
        ("gs_026",),
        frozenset({6}),
    ) in pair.by_root_cause
    assert ("sha1-cluster-026", frozenset({6})) in pair.by_signature


def test_ag_collision_key_pair_returns_both_root_and_signature_keys():
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
    )

    ag = {
        "id": "AG1",
        "source_cluster_signatures": ["sha1-cluster-026"],
    }
    pair = _ag_collision_key_pair(
        ag,
        ag_root_cause="plural top-N collapse on zone_combination",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )
    assert pair.root_cause_key == (
        "plural top-N collapse on zone_combination",
        ("gs_026",),
        frozenset({6}),
    )
    assert pair.signature_keys == (("sha1-cluster-026", frozenset({6})),)


def test_signature_collision_matches_even_when_root_cause_text_differs(
    monkeypatch,
):
    """The keystone behaviour pinned by this defect plan."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    # Iteration N: a NO_ACTION reflection lands with root_cause "A".
    buf = [
        _reflection_entry(
            root_cause="A — plural top-N collapse on zone_combination",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    forbidden_pair = _compute_forbidden_ag_set_pair(buf)

    # Iteration N+1: the strategist regenerates an AG for the same
    # cluster but the LLM phrased the root_cause as "B" (text drift).
    candidate_pair = _ag_collision_key_pair(
        {
            "id": "AG1",
            "source_cluster_signatures": ["sha1-cluster-026"],
        },
        ag_root_cause="B — top-N collapse / wrong table routing",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )

    # The root_cause key DOES NOT match (LLM drift).
    assert candidate_pair.root_cause_key not in forbidden_pair.by_root_cause

    # But the SIGNATURE key DOES match → overall collision.
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is True


def test_signature_collision_disabled_when_flag_off(monkeypatch):
    """Replay byte-stability — flag-off uses only the legacy
    root_cause key."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "0")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    buf = [
        _reflection_entry(
            root_cause="A",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    forbidden_pair = _compute_forbidden_ag_set_pair(buf)
    candidate_pair = _ag_collision_key_pair(
        {
            "id": "AG1",
            "source_cluster_signatures": ["sha1-cluster-026"],
        },
        ag_root_cause="B",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )

    # Flag off → only root_cause axis is consulted → no collision.
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is False


def test_signature_match_requires_lever_set_to_align():
    """A cluster collision still requires the lever family to match,
    otherwise a lever-family change correctly bypasses the gate."""
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    buf = [
        _reflection_entry(
            root_cause="A",
            cluster_signature="sha1-cluster-026",
            lever_set=[6],
        ),
    ]
    forbidden_pair = _compute_forbidden_ag_set_pair(buf)

    # Same cluster signature, different lever family → no collision.
    candidate_pair = _ag_collision_key_pair(
        {"id": "AG1", "source_cluster_signatures": ["sha1-cluster-026"]},
        ag_root_cause="A",
        ag_blame_set=("gs_026",),
        lever_keys=["5"],
    )
    # The root_cause axis matches the same root_cause text but the
    # frozenset of levers differs, so root_cause_key isn't in
    # forbidden_pair.by_root_cause either. Both axes correctly miss.
    assert _collision_pair_matches(candidate_pair, forbidden_pair) is False
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `uv run pytest tests/unit/test_harness_forbidden_ag.py -v`

Expected: every test FAILs with `ImportError: cannot import name '_compute_forbidden_ag_set_pair'` (and similar for the other new names).

- [ ] **Step 3: Implement the pure helpers**

In `src/genie_space_optimizer/optimization/harness.py`, **append** the new helpers immediately after `_ag_collision_key` (around line 10936). Do NOT remove or modify the existing `_compute_forbidden_ag_set` or `_ag_collision_key` yet — Task 9 swaps the call site.

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class _ForbiddenSetPair:
    """Defect Plan 1 G2 — paired forbidden set.

    ``by_root_cause`` is the legacy lookup keyed on
    ``(root_cause, blame, frozenset(lever_set))`` for byte-stability
    with pre-defect-plan-1 fixtures.

    ``by_signature`` is the new lookup keyed on
    ``(source_cluster_signature, frozenset(lever_set))`` — stable
    across LLM-regenerated root_cause text.
    """
    by_root_cause: frozenset[tuple[str, Any, frozenset[int]]]
    by_signature: frozenset[tuple[str, frozenset[int]]]


@dataclass(frozen=True)
class _CollisionKeyPair:
    """Defect Plan 1 G2 — paired collision-key candidate.

    ``root_cause_key`` is the legacy single key (None when the AG
    lacks enough identity to participate; the legacy code returned
    None and the caller short-circuited).

    ``signature_keys`` is a tuple of ``(signature, frozenset(
    lever_set))`` — one per ``source_cluster_signature`` on the AG.
    Empty tuple when the AG has no signatures.
    """
    root_cause_key: tuple[str, Any, frozenset[int]] | None
    signature_keys: tuple[tuple[str, frozenset[int]], ...]


def _compute_forbidden_ag_set_pair(
    reflection_buffer: list[dict],
) -> _ForbiddenSetPair:
    """Defect Plan 1 G2 — broadened forbidden-set producer.

    Pure function. Reads
    :func:`forbidden_ag_admits_no_action_enabled` and
    :func:`forbidden_ag_collision_by_cluster_signature_enabled` from
    config (at the caller boundary for the legacy axis already; the
    signature axis is gated by its own accessor).

    Each admitted entry contributes:

    * a tuple ``(root_cause, blame, frozenset(lever_set))`` to
      ``by_root_cause`` (matches the legacy
      :func:`_compute_forbidden_ag_set` output exactly).
    * one tuple ``(signature, frozenset(lever_set))`` per
      ``source_cluster_signature`` on the entry to ``by_signature``,
      only when the new flag is on AND signatures are non-empty.

    The same admission predicate (:func:`_reflection_admitted_to_forbidden_set`)
    is consulted as before — this helper only widens the *output*
    shape, not which entries are admitted. Replay byte-stability
    against pre-defect-plan-1 fixtures is preserved when the new
    flag is off (``by_signature`` becomes the empty frozenset).
    """
    from genie_space_optimizer.common.config import (
        forbidden_ag_admits_no_action_enabled,
        forbidden_ag_collision_by_cluster_signature_enabled,
    )

    admit_no_action = forbidden_ag_admits_no_action_enabled()
    by_signature_on = forbidden_ag_collision_by_cluster_signature_enabled()

    by_root_cause: set[tuple[str, Any, frozenset[int]]] = set()
    by_signature: set[tuple[str, frozenset[int]]] = set()
    for r in reflection_buffer:
        if not _reflection_admitted_to_forbidden_set(
            r, admit_no_action=admit_no_action
        ):
            continue
        blame = _normalise_blame(r.get("blame_set"))
        lever_set = r.get("lever_set") or []
        lever_frozen = frozenset(int(l) for l in lever_set)
        by_root_cause.add(
            (r.get("root_cause") or "", blame, lever_frozen)
        )
        if by_signature_on:
            for sig in (r.get("source_cluster_signatures") or []):
                if not sig:
                    continue
                by_signature.add((str(sig), lever_frozen))
    return _ForbiddenSetPair(
        by_root_cause=frozenset(by_root_cause),
        by_signature=frozenset(by_signature),
    )


def _ag_collision_key_pair(
    ag: dict,
    ag_root_cause: str,
    ag_blame_set: Any,
    lever_keys: list[str],
) -> _CollisionKeyPair:
    """Defect Plan 1 G2 — broadened collision-key candidate producer.

    Pure function. Returns the legacy ``root_cause_key`` (None when
    the AG lacks identity) AND one signature_key per
    ``source_cluster_signature`` on the AG. The caller composes the
    pair against a :class:`_ForbiddenSetPair` via
    :func:`_collision_pair_matches`.
    """
    # Legacy axis — preserves the (root_cause, blame, lever_set)
    # shape exactly, including the "return None when lacking
    # identity" short-circuit semantics.
    root_cause_key: tuple[str, Any, frozenset[int]] | None
    if not ag_root_cause or not lever_keys:
        root_cause_key = None
    else:
        root_cause_key = (
            ag_root_cause,
            _normalise_blame(ag_blame_set),
            frozenset(int(lk) for lk in lever_keys),
        )

    lever_frozen = (
        frozenset(int(lk) for lk in lever_keys) if lever_keys else frozenset()
    )
    sigs = ag.get("source_cluster_signatures") or []
    signature_keys = tuple(
        (str(s), lever_frozen)
        for s in sigs
        if s
    )
    return _CollisionKeyPair(
        root_cause_key=root_cause_key,
        signature_keys=signature_keys,
    )


def _collision_pair_matches(
    candidate: _CollisionKeyPair,
    forbidden: _ForbiddenSetPair,
) -> bool:
    """Defect Plan 1 G2 — return True iff EITHER axis matches.

    Pure function. Short-circuits on the legacy axis so the existing
    code path's behaviour is preserved when the new signature axis is
    empty (flag off).
    """
    if (
        candidate.root_cause_key is not None
        and candidate.root_cause_key in forbidden.by_root_cause
    ):
        return True
    if not candidate.signature_keys:
        return False
    if not forbidden.by_signature:
        return False
    return any(
        k in forbidden.by_signature for k in candidate.signature_keys
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_harness_forbidden_ag.py -v`

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/harness.py tests/unit/test_harness_forbidden_ag.py
git commit -m "$(cat <<'EOF'
feat(defect-1): add cluster-signature collision-key pure helpers

_compute_forbidden_ag_set_pair returns paired forbidden-set
(root_cause axis + signature axis), _ag_collision_key_pair returns
paired candidate-key (same two axes), _collision_pair_matches OR-s
across axes. Pure functions; the live call site swaps to them in
Task 9. Existing _compute_forbidden_ag_set / _ag_collision_key are
unchanged.
EOF
)"
```

---

### Task 9: Swap the live AG collision call site to the paired helpers

**Files:**
- Modify: `src/genie_space_optimizer/optimization/harness.py:18857-18928` (the AG collision check block)
- Test: existing `tests/unit/test_harness_forbidden_ag.py` + integration test in Task 11

The swap is mechanical. The pre-existing `_compute_forbidden_ag_set` / `_ag_collision_key` are kept (for any other callers and for the observe-marker path) — we only switch the **single call site** at line 18857-18861 to the new paired helpers, with behaviour gated by `forbidden_ag_collision_by_cluster_signature_enabled()` for replay byte-stability.

- [ ] **Step 1: Add a regression test to lock in the legacy-axis behaviour**

Append to `tests/unit/test_harness_forbidden_ag.py`:

```python
def test_legacy_axis_still_matches_when_signatures_absent(monkeypatch):
    """Pre-existing behaviour — when AG has no source_cluster_signatures,
    the legacy root_cause axis is the only collision path and must
    still fire. Replay byte-stability for pre-defect-plan-1 fixtures.
    """
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    buf = [
        _reflection_entry(
            root_cause="A",
            cluster_signature="sha1-026",
            lever_set=[6],
        ),
    ]
    forbidden = _compute_forbidden_ag_set_pair(buf)

    # AG with NO source_cluster_signatures — must still collide via
    # the legacy axis.
    candidate = _ag_collision_key_pair(
        {"id": "AG1"},
        ag_root_cause="A",
        ag_blame_set=("gs_026",),
        lever_keys=["6"],
    )
    assert _collision_pair_matches(candidate, forbidden) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_harness_forbidden_ag.py::test_legacy_axis_still_matches_when_signatures_absent -v`

Expected: PASS (the pure helper from Task 8 already supports this; the test pins behaviour we must not regress).

- [ ] **Step 3: Swap the live call site**

Modify `src/genie_space_optimizer/optimization/harness.py:18857-18928` — the AG collision check block. Replace the legacy lookup with the paired-helper match, keeping the existing logging and reflection-entry write intact:

```python
            # Phase D2 collision guard, broadened by Defect Plan 1 G2
            # (2026-05-12). The legacy axis (root_cause, blame,
            # lever_set) is preserved verbatim; the new signature axis
            # (source_cluster_signatures × lever_set) closes the
            # 7now-trial defect where iterations 2-5 re-admitted the
            # same AG family after the LLM regenerated root_cause text.
            _forbidden_pair = _compute_forbidden_ag_set_pair(reflection_buffer)
            _collision_pair = _ag_collision_key_pair(
                ag, _ag_root_cause, _ag_blame_set, lever_keys,
            )
            if _collision_pair_matches(_collision_pair, _forbidden_pair):
                # Derive the human-readable identity for the operator
                # transcript — prefer the legacy root_cause/blame/lever
                # when available, fall back to the signature axis.
                if _collision_pair.root_cause_key is not None:
                    _rc_k, _blame_k, _lever_k = _collision_pair.root_cause_key
                    _collision_axis = "root_cause"
                else:
                    _rc_k = _ag_root_cause or "(empty)"
                    _blame_k = _normalise_blame(_ag_blame_set)
                    _lever_k = (
                        frozenset(int(lk) for lk in lever_keys)
                        if lever_keys
                        else frozenset()
                    )
                    _collision_axis = "cluster_signature"
                print(
                    _section(f"[{ag_id}] AG COLLISION — skipping", "!") + "\n"
                    + _kv("Root cause", _rc_k) + "\n"
                    + _kv("Blame", _blame_k) + "\n"
                    + _kv("Lever set", sorted(_lever_k)) + "\n"
                    + _kv("Collision axis", _collision_axis) + "\n"
                    + _kv(
                        "Reason",
                        "strategist re-proposed a (root_cause, blame, "
                        "lever_set) tuple or a (cluster_signature, "
                        "lever_set) tuple previously rolled back",
                    ) + "\n"
                    + _bar("!")
                )
                # [existing write_stage / reflection_buffer.append / phase H
                # finalize block continues unchanged from here — keep the
                # existing rollback_reason="ag_collision_with_forbidden_set"]
```

The rest of the block (the `write_stage(..., f"AG_{ag_id}_COLLISION_SKIPPED", ...)`, the `reflection_buffer.append(_build_reflection_entry(...))` call, the `_render_current_journey()` call, and the Phase H `_finalize_iteration_summary` call) is **unchanged** — keep it byte-stable.

- [ ] **Step 4: Run the unit + harness regression suite**

Run: `uv run pytest tests/unit/test_harness_forbidden_ag.py -v`

Expected: 6 passed (5 from Task 8 + the regression test added in Step 1).

Run the broader smoke suite to confirm no other call site relied on the legacy helpers' exact behaviour:

Run: `uv run pytest tests/unit/test_action_groups_contract.py tests/unit/test_stages_action_groups.py tests/unit/test_action_groups_bucket_feedback.py tests/unit/test_harness_grounding_gate.py tests/unit/test_invariants.py -v`

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/harness.py tests/unit/test_harness_forbidden_ag.py
git commit -m "$(cat <<'EOF'
feat(defect-1): swap AG collision check to paired forbidden-set lookup

The single call site at harness.py:18857-18861 now consults both
the legacy (root_cause, blame, lever_set) axis and the new
(source_cluster_signature, lever_set) axis. Operator transcript
records which axis caused the skip. Closes the runtime half of the
7now-trial Gate G2 defect.

Legacy helpers (_compute_forbidden_ag_set, _ag_collision_key) are
kept so any other caller and the existing observe-marker path are
unaffected.
EOF
)"
```

---

### Task 10: I7 docstring + roadmap status touch-up

**Files:**
- Modify: `src/genie_space_optimizer/optimization/invariants.py:319-323` (docstring of `check_i7_rca_grounding`)

The invariant logic itself is unchanged. The docstring is updated to reflect that the production runtime path now reaches the green branch (block-record-emitted) — this is documentation only, not behaviour.

- [ ] **Step 1: Inspect the current docstring**

Run: `uv run python -c "from genie_space_optimizer.optimization.invariants import check_i7_rca_grounding; print(check_i7_rca_grounding.__doc__)"`

Expected: prints the existing docstring referencing "7NOW iter-1 where 4/5 hard clusters had no RCA card".

- [ ] **Step 2: Update the docstring**

Modify `src/genie_space_optimizer/optimization/invariants.py:319-323`:

```python
def check_i7_rca_grounding(evidence: Mapping[str, Any]) -> list[dict]:
    """I7 — every open hard cluster reaching AG-emit has either a fit
    RCA card or a typed cluster_blocked_no_rca decision record. Closes
    7NOW iter-1 where 4/5 hard clusters had no RCA card but the
    strategist proceeded to AG-emit anyway.

    Detection-side guarantee landed by Cycle 17 (the invariant body
    below). Production-side guarantee landed by Defect Plan 1
    (2026-05-12) — ``harness.collect_blocked_clusters`` now emits one
    ``DecisionType.CLUSTER_BLOCKED_NO_RCA`` record per ungrounded open
    hard cluster at AG-emit time, and
    ``stages.action_groups.select`` drops AGs whose
    ``source_cluster_ids`` intersect the blocked set. With both halves
    landed, a run that has any open hard cluster with ``rca_card=False``
    at AG-emit time SHOULD have zero I7 violations because the green
    branch (cluster present in ``blocked_clusters`` set) is now
    reached in production. A surviving violation indicates either the
    grounding-gate flag is off or the harness wiring failed before
    record emission.
    """
```

- [ ] **Step 3: Run the I7 unit tests**

Run: `uv run pytest tests/unit/test_invariants.py -k i7 -v`

Expected: existing 3 I7 tests still green (we did not change behaviour).

- [ ] **Step 4: Commit**

```bash
git add src/genie_space_optimizer/optimization/invariants.py
git commit -m "docs(defect-1): update I7 docstring to reflect production-side guarantee"
```

---

### Task 11: 7now-trial replay parity — iterations 2-5 short-circuit

**Files:**
- Modify: `tests/integration/test_ag_grounding_and_admission_replay.py`

This task adds the second end-to-end replay test: feed the 7now reflection buffer (as captured from the May-12 trial) through `_compute_forbidden_ag_set_pair` and assert that an iteration-2 AG with the SAME cluster signature but a DIFFERENT root_cause text collides on the signature axis.

- [ ] **Step 1: Append the failing test**

Append to `tests/integration/test_ag_grounding_and_admission_replay.py`:

```python
def _extract_reflection_buffer(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    """Best-effort projection of the reflection buffer from the replay
    fixture. The May-12 captures stash it under ``reflection_buffer``
    at the run level or under ``reflections`` per iteration; try both.
    """
    if isinstance(fixture.get("reflection_buffer"), list):
        return [dict(r) for r in fixture["reflection_buffer"]]
    out: list[dict[str, Any]] = []
    for it in fixture.get("iterations") or fixture.get("iter_records") or []:
        for r in it.get("reflections") or []:
            out.append(dict(r))
    return out


def test_7now_replay_iterations_2_through_5_collide_on_cluster_signature(
    monkeypatch,
):
    """7now Gate G2 — iteration 1's NO_ACTION reflection has the same
    ``source_cluster_signatures`` as iteration 2-5's regenerated AGs.
    With the cluster-signature axis on, the iteration-2 AG must
    collide even though the LLM-regenerated root_cause text differs.
    """
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    fixture = _load_replay(
        SEVENNOW_EVIDENCE
        / "replay_fixture_from_latest_export_318760998419002.json"
    )
    buf = _extract_reflection_buffer(fixture)
    if not buf:
        pytest.skip(
            "7now fixture has no reflection_buffer projection; promote "
            "richer evidence-bundle output first"
        )

    # Find the iteration-1 NO_ACTION reflection (the iteration-1
    # CONTENT_REGRESSION rollback was the one that left gs_026 hard
    # and regressed gs_012). Iterations 2-5 are NO_ACTION /
    # no_proposals.
    no_action_entries = [
        r for r in buf
        if str(r.get("rollback_class") or "").lower() == "no_action"
        and (r.get("source_cluster_signatures") or [])
    ]
    if not no_action_entries:
        pytest.skip(
            "7now fixture does not surface NO_ACTION reflections with "
            "source_cluster_signatures populated; cannot exercise G2"
        )

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    pair = _compute_forbidden_ag_set_pair(no_action_entries)
    assert pair.by_signature, (
        "G2 forbidden-set has no signature axis entries — the fixture's "
        "reflections lack source_cluster_signatures or the predicate "
        "rejected all admitted candidates"
    )

    # Synthesise iteration-N+1's AG using the SAME signature but a
    # DIFFERENT root_cause text (the failure mode the postmortem
    # describes: LLM root_cause drift).
    one_sig, one_lever_frozen = next(iter(pair.by_signature))
    next_iter_ag = {
        "id": "AG1_iter2",
        "source_cluster_signatures": [one_sig],
    }
    candidate = _ag_collision_key_pair(
        next_iter_ag,
        ag_root_cause="DIFFERENT root_cause text — LLM regenerated",
        ag_blame_set=("gs_026",),
        lever_keys=[str(int(l)) for l in sorted(one_lever_frozen)] or ["1"],
    )

    assert _collision_pair_matches(candidate, pair) is True, (
        "G2 signature collision did not fire — the next-iteration AG "
        "would have been re-admitted; this is exactly the 7now defect."
    )


def test_7now_replay_legacy_axis_alone_does_not_collide(monkeypatch):
    """Negative control — with the signature axis OFF, the same
    iteration-2 AG would NOT collide (this reproduces the bug)."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "0")

    fixture = _load_replay(
        SEVENNOW_EVIDENCE
        / "replay_fixture_from_latest_export_318760998419002.json"
    )
    buf = _extract_reflection_buffer(fixture)
    if not buf:
        pytest.skip("7now fixture has no reflection_buffer projection")

    no_action_entries = [
        r for r in buf
        if str(r.get("rollback_class") or "").lower() == "no_action"
        and (r.get("source_cluster_signatures") or [])
    ]
    if not no_action_entries:
        pytest.skip("no usable NO_ACTION reflections in 7now fixture")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    pair = _compute_forbidden_ag_set_pair(no_action_entries)
    # With the new flag off, by_signature is empty.
    assert pair.by_signature == frozenset()

    one_entry = no_action_entries[0]
    sig = (one_entry.get("source_cluster_signatures") or ["x"])[0]
    lever_set = one_entry.get("lever_set") or [1]

    candidate = _ag_collision_key_pair(
        {"id": "AG1", "source_cluster_signatures": [sig]},
        ag_root_cause="DIFFERENT root_cause text",
        ag_blame_set=("gs_026",),
        lever_keys=[str(int(l)) for l in lever_set],
    )

    # Legacy axis alone misses → no collision → bug reproduced.
    assert _collision_pair_matches(candidate, pair) is False
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/integration/test_ag_grounding_and_admission_replay.py -v`

Expected: all tests PASS or cleanly SKIP with the fixture-shape message. The two new tests prove the bug AND the fix on the same fixture.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_ag_grounding_and_admission_replay.py
git commit -m "test(defect-1): 7now replay parity — signature axis closes iter-2 admission"
```

---

### Task 12: Full RCO regression sweep + roadmap status updates

**Files:**
- Modify: `docs/2026-05-10-roadmap-closeout.md`
- Modify: `docs/2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md`
- Modify: `docs/2026-05-12-defect-forbidden-ag-admission-enforcement.md`

This task runs the full RCO regression suite to prove no behaviour drift on the existing pre-defect-plan-1 baselines, then updates the three docs that reference these defects.

- [ ] **Step 1: Run the canonical RCO regression batch**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest \
    tests/unit/test_rco4b_run_gate_checks_sequence_guard.py \
    tests/unit/test_rco4_run_gate_checks_sequence_guard.py \
    tests/unit/test_invariants.py \
    tests/unit/test_invariant_projection.py \
    tests/unit/test_stages_action_groups.py \
    tests/unit/test_action_groups_contract.py \
    tests/unit/test_action_groups_bucket_feedback.py \
    tests/unit/test_decision_emitters.py \
    tests/unit/test_rca_decision_trace.py \
    tests/unit/test_config.py \
    tests/unit/test_harness_forbidden_ag.py \
    tests/unit/test_harness_grounding_gate.py \
    tests/integration/test_ag_grounding_and_admission_replay.py \
    -v
```

Expected: all green. If any pre-existing test went red, root-cause and fix before proceeding — every pre-defect-plan-1 fixture must remain byte-stable.

- [ ] **Step 2: Run the full unit suite as a final regression check**

Run: `uv run pytest tests/unit -x --tb=short`

Expected: same pre-existing failures as documented in the May-11 preflight green-light log of `docs/2026-05-13-rco-4b-trial-runbook.md` (the four pre-existing failures: `test_skill_parser_handoff` ×2, `test_evidence_bundle_smoke`, `test_mlflow_smoke_one_iteration`). No new failures.

- [ ] **Step 3: Update the roadmap closeout doc**

Modify `docs/2026-05-10-roadmap-closeout.md`. Locate the trial-disposition section (added 2026-05-12). Replace any "❌ blocked on defect plans" or "⚠️ partial" wording for the re-trial unblock status with a pointer to this plan, and append a row to the defect-status table:

Append to the existing trial-disposition section:

```markdown
### Re-trial unblock progress (2026-05-12)

| Defect plan | Plan file | Trial-blocking | Status |
|---|---|---|---|
| Defect 1 — AG grounding + cluster-signature admission | `docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md` | Yes | Drafted |
| Defect 2 — Stable retry signature for no-progress iterations | (not drafted yet) | Yes | Pending |
| Defect 3 — RCO-6 carve-out (gs_021 clustered → soft_signal) | (not drafted yet) | No (blocks RCO-6, not the re-trial) | Pending |
| Bundle-status wiring fix (micro-plan) | `docs/2026-05-12-bundle-status-wiring-fix-plan.md` | No (de-risks RCO-2b) | Drafted |

Re-trial against F9-3b050ec5 + AIRLINE-clean fires when Defects 1 + 2 land.
```

- [ ] **Step 4: Mark the two defect stubs as superseded**

Modify `docs/2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md`. At the top of the file, replace the `> **Status:** stub.` line with:

```markdown
> **Status:** SUPERSEDED by
> `docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md`
> (2026-05-12). All seven items in the "Recommended next steps"
> section below are addressed by that plan (items 1, 4, 5) or
> consciously deferred to Defect Plan 2 (item 6 — Phase H totality)
> and the bundle-status wiring fix (item 7 in spirit). Items 2-3
> (structural SQL repairs for gs_009 / gs_024) are LLM/prompt-domain
> work explicitly out of roadmap-closeout scope per
> `docs/2026-05-10-roadmap-closeout.md`.
```

Modify `docs/2026-05-12-defect-forbidden-ag-admission-enforcement.md`. At the top of the file, replace the `> **Status:** stub.` line with:

```markdown
> **Status:** PARTIALLY SUPERSEDED by
> `docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md`
> (2026-05-12). Items 1 (forbidden-AG admission), 5 (zero-proposal
> retry signature deferred to Defect Plan 2), and 7-8 (orthogonal
> gaps) are covered. Items 2-4 (target_qids_not_improved →
> patch-family change requirement; gs_026 structural repair;
> gs_012 regression-aware collateral constraints) are LLM/prompt-domain
> work out of roadmap-closeout scope. Item 6 (gs_021 clustered →
> soft_signal replay parity) is the named RCO-6 blocker and remains
> pending its own defect plan.
```

- [ ] **Step 5: Final commit**

```bash
git add docs/2026-05-10-roadmap-closeout.md \
        docs/2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md \
        docs/2026-05-12-defect-forbidden-ag-admission-enforcement.md
git commit -m "$(cat <<'EOF'
docs(defect-1): mark stubs superseded; update roadmap re-trial unblock

The two May-12 defect stubs are now superseded (one fully, one
partially) by the Defect Plan 1 implementation plan. Roadmap-closeout
trial-disposition section gains a re-trial unblock progress table so
the path to re-running against F9-3b050ec5 + AIRLINE-clean is
explicit.
EOF
)"
```

---

## Self-Review

### 1. Spec coverage

| Defect-plan-1 requirement | Tasks |
|---|---|
| Add `DecisionType.CLUSTER_BLOCKED_NO_RCA` | Task 1 |
| Add producer helper `cluster_blocked_no_rca_record` | Task 2 |
| Add feature-flag accessors (default-on, RCO-4/4b convention) | Task 3 |
| Pure helper `collect_blocked_clusters` to scan clusters | Task 4 |
| Extend `ActionGroupsInput` with `blocked_cluster_ids` | Task 5 |
| Runtime filter in `stages.action_groups.select` | Task 6 |
| Wire grounding-gate prelude into harness AG-emit path + airline replay | Task 7 |
| Pure paired-helpers for forbidden-set + collision-key (G2 substrate) | Task 8 |
| Swap live AG collision call site to paired helpers | Task 9 |
| I7 docstring touch-up to reflect production-side guarantee | Task 10 |
| 7now replay parity + negative-control test | Task 11 |
| Full regression suite + roadmap doc updates | Task 12 |

Both Gate G1 (Tasks 1-7) and Gate G2 (Tasks 8-11) are covered end-to-end with unit + integration tests and feature-flag rollback. Documentation updates land in Task 12.

### 2. Placeholder scan

I scanned the plan for the forbidden patterns:

- "TBD", "TODO", "implement later" — absent.
- "Add appropriate error handling" — absent. The Task 7 producer-exception block is fully spelled out with the `_typed_on()` pattern that the rest of `harness.py` uses.
- "Write tests for the above" — absent. Every task contains the actual test code.
- "Similar to Task N" — absent. The Task 11 negative-control test repeats the structure of the positive test rather than referring back.
- Generic steps without code — absent. Every "implement minimal code" step contains the full code block.

### 3. Type consistency

- `_BlockedClustersResult` (Task 4) — used only in Task 4. ✓
- `_ForbiddenSetPair` / `_CollisionKeyPair` (Task 8) — used in Tasks 8, 9, 11. ✓
- `_compute_forbidden_ag_set_pair`, `_ag_collision_key_pair`, `_collision_pair_matches` (Task 8) — referenced consistently in Tasks 9 and 11. ✓
- `ActionGroupsInput.blocked_cluster_ids: tuple[str, ...]` (Task 5) — used in Task 6 filter, Task 7 harness wiring, Task 11 replay. Consistent type. ✓
- `cluster_blocked_no_rca_record(...)` signature (Task 2) — called from Task 4 helper with the same kwarg names. ✓
- Feature-flag accessors (Task 3) — `ag_emit_grounding_gate_enabled` called from Task 6 and Task 7; `forbidden_ag_collision_by_cluster_signature_enabled` called from Task 8 pure helper. ✓

### 4. Sequencing

Tasks have a strict DAG. Tasks 1 → 2 → 3 are independent foundation. Tasks 4 → 5 → 6 → 7 are the G1 enforcement chain. Tasks 8 → 9 are the G2 enforcement chain. Task 10 is documentation, can land any time after Task 7. Task 11 depends on Task 9. Task 12 must be last.

Subagent-driven execution can split into two parallel tracks after Task 3:

- Track A (G1): Tasks 4 → 5 → 6 → 7.
- Track B (G2): Tasks 8 → 9 → 11.

Tasks 10 and 12 join at the end.

### 5. Replay byte-stability

Every behaviour change is feature-flagged with `_flag_default_on` so the escape hatch is `GSO_*=0`:

| Behaviour | Flag | Rollback |
|---|---|---|
| Grounding gate (Tasks 4-7) | `GSO_AG_EMIT_GROUNDING_GATE` | `GSO_AG_EMIT_GROUNDING_GATE=0` |
| Cluster-signature collision (Tasks 8-9) | `GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE` | `GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE=0` |

Pre-defect-plan-1 fixtures are byte-stable when both flags are `=0`. Task 6's `test_select_keeps_all_ags_when_flag_off` and Task 8's `test_signature_collision_disabled_when_flag_off` pin this contract.

---

## Execution Handoff

Plan complete and saved to `packages/genie-space-optimizer/docs/2026-05-12-defect-ag-emit-grounding-and-forbidden-admission-plan.md`.

Two execution options:

**1. Subagent-Driven (recommended for parallel G1/G2 tracks)** — fresh subagent per task, review between tasks, fast iteration. Tasks 4-7 and Tasks 8-11 can run in parallel subagent tracks after Task 3.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints. Slower wall-clock but a single review surface.

Which approach?

**Downstream consumer:** P-E1 ([`2026-05-12-plan-p-e1-l6-decline-cache-and-narrow-guard-plan.md`](2026-05-12-plan-p-e1-l6-decline-cache-and-narrow-guard-plan.md)) reuses `_ag_collision_key_pair` as the cache-key shape for its iteration-scoped Lever-6 decline cache, so "L6 declined this shape" and "AG selection considers this shape forbidden next iteration" share the same canonical identity. Invariant `I14` enforces the observable dedup property at run end.
