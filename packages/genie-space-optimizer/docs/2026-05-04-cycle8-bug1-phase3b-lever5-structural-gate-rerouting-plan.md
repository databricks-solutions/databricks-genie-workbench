# Cycle 8 Bug 1 Phase 3b — Lever 5 Structural Gate Rerouting Mini-Plan

> **Status:** Ready for implementation. Supersedes Phase 3 (hypotheses 3.1–3.4) of
> [`2026-05-02-cycle8-bug1-target-qids-diagnosis-and-plan.md`](./2026-05-02-cycle8-bug1-target-qids-diagnosis-and-plan.md),
> which was blocked on applier-decision data.
>
> **Status update (Phase 3c authored 2026-05-05):** the cycle 9 fixture
> (`run_id=d6a7faeb...`) shows the Lever 5 structural gate is **not firing**
> for `AG_DECOMPOSED_H001` or `AG_DECOMPOSED_H002`. The actual blockers are
> the instruction-scope gate's `split_child_missing_passing_dependents` check
> and the blast-radius gate's `high_collateral_risk_flagged` check, both
> upstream of any lever-routing decision. Phase 3b Tasks A and B (visibility)
> are still safe to land. Phase 3b Task C is a no-op against the current
> codebase (the diagnostic AGs already arrive with both Lever 5 and Lever 6
> patches via cluster-driven synthesis) but safe and idempotent. The headline
> acceptance criterion ("applied Lever 6 patch on gs_024") is now owned by
> [`2026-05-05-cycle8-bug1-phase3c-blast-radius-instruction-scope-deadlock-plan.md`](./2026-05-05-cycle8-bug1-phase3c-blast-radius-instruction-scope-deadlock-plan.md).
>
> **Authored after:** Cycle 8 stderr now provides the missing data — the applier
> never sees `AG_DECOMPOSED_H001`'s patches because the **Lever 5 structural gate**
> at `optimizer.py:13961` drops the entire instruction-only proposal upstream.
> The stderr line is unmistakable:
>
> ```
> [AG_DECOMPOSED_H001] Lever 5 structural gate: dropping instruction-only
> proposal. Dominant cluster root cause(s) ['wrong_aggregation'] are SQL-shape;
> no example_sql attached. Expected structural fix via cluster-driven synthesis
> or a different lever.
> ```
>
> The decomposed AG carried only a Lever 5 directive (per
> `_DIAGNOSTIC_AG_DIRECTIVES["wrong_aggregation"] = {"lever": "5", "kind":
> "sql_shape"}` at `control_plane.py:248`), no `example_sqls`, and no companion
> Lever 6 path. The gate fired correctly — the AG was simply born unable to
> survive it.
>
> **This is a mini-plan** — three TDD-driven tasks scoped to make the gate visible,
> auditable, and self-healing for SQL-shape clusters. Roughly 200 lines of plan,
> targeting ~150 lines of production code and ~6 unit tests.

---

## Acceptance criterion

**Single, measurable, observed in cycle 10:**

> Cycle 10 (the next lever-loop run after this plan ships) produces **at least one
> applied Lever 6 patch** on `airline_ticketing_and_fare_analysis_gs_024`
> (or its decomposed-AG successor with the same `wrong_aggregation` root cause).

Secondary signals confirming the new wiring works end-to-end:

* The cycle 10 iteration banner shows `lever5_text_only_blocked > 0` (Task A — the
  counter is now visible to the writer).
* The cycle 10 operator transcript's "Proposal Survival And Gate Drops" section
  contains at least one `gate=lever5_structural_gate / outcome=DROPPED /
  reason_code=RCA_UNGROUNDED` row (Task B — the gate now emits a decision).
* The cycle 10 raw replay fixture shows `AG_DECOMPOSED_H001.lever_directives`
  with both keys `"5"` and `"6"` populated (Task C — the diagnostic AG is now
  born with a Lever 6 escape hatch).

If even one of these tertiary signals does not appear, the test plan failed —
the mini-plan is incomplete; do not declare success.

---

## Background — what changed since the parent plan

The parent plan listed four candidate hypotheses (3.1 schema-mismatch, 3.2
dry-run failure, 3.3 idempotency rejection, 3.4 patch-DSL validation). All four
assumed the rejection happened **inside** `applier.apply_patches`. The cycle 8
stderr now proves otherwise: the rejection is **upstream** of the applier, at the
Lever 5 structural gate in `propose_action_groups` (`optimizer.py:13961`). The
applier never gets called for `AG_DECOMPOSED_H001`'s instruction proposal.

The structural gate itself is **not the bug**. It correctly enforces a real
contract: SQL-shape failures (`wrong_aggregation`, `missing_filter`,
`wrong_join`, etc.) cannot be repaired by prose instructions alone — the fix
must change SQL structure, which means an `example_sql` attached to the Lever 5
directive OR a Lever 6 (`sql_snippet_*`) directive on the same AG. The bug is
that the **diagnostic AG production path** (`diagnostic_action_group_for_cluster`
in `control_plane.py:261-295`) hands SQL-shape clusters a Lever 5–only
directive set, so the AG is born already failing the gate it must clear.

The fix is therefore upstream of the gate, not at the gate.

---

## Tasks

### Task A — Surface `lever5_text_only_blocked` in the iteration banner

**Goal:** the existing `_BUG4_COUNTERS["lever5_text_only_blocked"]` counter is
incremented at `optimizer.py:13962` every time the gate fires, but
`_merge_bug4_counters` (`harness.py:770-810`) does **not** fold it into
`eval_result`. So `write_iteration` and the iteration banner never see it.
Future cycles can hit the gate hundreds of times without operators knowing.

**TDD shape (write the test first):**

* New test: `tests/unit/test_merge_bug4_counters_lever5.py`
  * Test 1: `_merge_bug4_counters({})` after manually bumping
    `optimizer._BUG4_COUNTERS["lever5_text_only_blocked"] = 3` returns a dict
    where `eval_result["lever5_text_only_blocked"] == 3`.
  * Test 2: the same call **also** resets the counter back to 0 (call
    `_merge_bug4_counters` twice and assert the second call returns 0).
  * Test 3: when the key is already present in `eval_result` (e.g. from a
    slice-eval scope), values are **summed**, not overwritten — mirrors the
    existing behaviour for `secondary_mining_blocked` at `harness.py:797-800`.

**Production change:** in `_merge_bug4_counters` (`harness.py:790-810`), add
two lines mirroring the `secondary_mining_blocked` block:

```python
eval_result.setdefault("lever5_text_only_blocked", 0)
eval_result["lever5_text_only_blocked"] = (
    int(eval_result.get("lever5_text_only_blocked") or 0)
    + int(snapshot.get("lever5_text_only_blocked", 0) or 0)
)
```

Place the block immediately above the `firewall_rejection_count_by_type` block
so the existing `reset_bug4_counters()` call at line 809 still resets all
keys uniformly.

**No further wiring needed** — `write_iteration` already persists every
`eval_result` key, and the iteration banner reads
`run_summary.<iteration>.lever5_text_only_blocked` lazily via the catch-all
counter renderer if it is non-zero.

**Acceptance:** the unit tests pass; cycle 10 banner emits a non-zero value
when the gate fires.

**Estimated change:** 4 lines of production code, 1 new test file (~50 lines).

---

### Task B — Emit a `DecisionRecord` at the gate-drop site

**Goal:** Today the gate at `optimizer.py:13961-13971` silently zeros
`instruction_sections` and `instruction_guidance`, then logs a warning. Phase B's
operator transcript renders nothing for that drop because no `DecisionRecord`
is emitted. The "Proposal Survival And Gate Drops" section of the transcript
shows blast-radius drops (cycle 9 T6) and patch-cap drops, but **not**
Lever 5 structural-gate drops — the most common drop on `gs_024`.

This task adds a producer that mirrors `blast_radius_decision_records` from
`decision_emitters.py:776-852` exactly.

**TDD shape (write the tests first):**

* New test: `tests/unit/test_lever5_structural_gate_records.py`
  * Test 1: calling `lever5_structural_gate_records(...)` with one drop entry
    returns a single `DecisionRecord` with
    `decision_type=GATE_DECISION`, `outcome=DROPPED`,
    `reason_code=RCA_UNGROUNDED`, `gate="lever5_structural_gate"`.
  * Test 2: the record's `metrics["root_causes"]` carries the sorted tuple of
    SQL-shape root causes that triggered the gate (e.g.
    `("wrong_aggregation",)`); `metrics["target_lever"] == 5`;
    `metrics["had_example_sqls"] is False`.
  * Test 3: `target_qids` and `affected_qids` mirror the AG's
    `affected_questions` (so the cross-projection contract from Phase B
    Task 10 still holds — every gate-decision DecisionRecord covers the same
    qids the matching `JourneyEvent` did).
  * Test 4: `next_action` is the human-readable string
    `"Re-route via Lever 6 (sql_snippet) or attach example_sql via cluster-driven synthesis"`.
  * Test 5: when called with `dropped=()`, returns `[]` (no spurious records).

* New test (validator-side): `tests/unit/test_validator_lever5_gate.py`
  * Re-use the rca-grounding contract test pattern from
    `test_validator_applied_stage_family.py`. Assert the record passes the
    cross-checker (validator does not flag a missing `rca_id` or
    `root_cause` — both fields are populated when present, OR the
    `RCA_FORMED + RCA_UNGROUNDED` exemption already added in Phase C T7
    covers this `GATE_DECISION + RCA_UNGROUNDED` shape too if the cluster
    had no RCA finding). If the existing exemption covers only
    `RCA_FORMED`, the validator may need a sibling exemption for
    `(GATE_DECISION, RCA_UNGROUNDED)` — extend the test to cover both
    populated and empty `rca_id` cases and add the exemption only if the
    populated-id case is not the dominant scenario.

**Production change — three sub-steps:**

1. **Side-channel collector in `optimizer.py`** (mirrors `_BUG4_COUNTERS`):

   ```python
   # near _BUG4_COUNTERS (~optimizer.py:7791)
   _LEVER5_GATE_DROPS: list[dict] = []

   def reset_lever5_gate_drops() -> None:
       _LEVER5_GATE_DROPS.clear()

   def get_lever5_gate_drops() -> list[dict]:
       return list(_LEVER5_GATE_DROPS)
   ```

   At the gate-drop site (`optimizer.py:13961-13971`), append a record after
   the `_incr_bug4_counter` line:

   ```python
   _LEVER5_GATE_DROPS.append({
       "ag_id": str(ag_id),
       "source_clusters": tuple(str(s) for s in source_clusters),
       "root_causes": tuple(sorted(_ag_structural_root_causes)),
       "target_lever": 5,
       "had_example_sqls": bool(example_sqls_list),
       "instruction_sections_dropped": isinstance(instruction_sections, dict)
                                        and bool(instruction_sections),
       "instruction_guidance_dropped": bool(instruction_guidance),
   })
   ```

   Keep the existing `instruction_sections = None` and
   `instruction_guidance = ""` lines — they remain the active
   silencing — but the drop is now also captured for downstream emit.

2. **Producer in `decision_emitters.py`** — mirror
   `blast_radius_decision_records` (lines 776-852) exactly. Signature:

   ```python
   def lever5_structural_gate_records(
       *,
       run_id: str,
       iteration: int,
       ag_id: str,
       rca_id: str,
       root_cause: str,
       target_qids: Sequence[str],
       drops: Sequence[Mapping[str, Any]],
   ) -> list[DecisionRecord]: ...
   ```

   Use `decision_type=GATE_DECISION`, `outcome=DROPPED`,
   `reason_code=RCA_UNGROUNDED`, `gate="lever5_structural_gate"`.
   Stash `target_lever`, `root_causes`, `had_example_sqls`,
   `instruction_sections_dropped`, `instruction_guidance_dropped` under
   `metrics`. `next_action` per Test 4 above. Per the cycle 9 wiring pattern,
   `evidence_refs` = `(f"ag:{ag_id}", "lever5_structural_gate")`.

3. **Harness wire-up** — mirror the blast-radius wire-up at
   `harness.py:14781-14831`. After the propose-loop returns (i.e. after
   `optimizer.generate_proposals_from_strategy` is called for the AG, somewhere
   between the proposal return and the proposal→patch step), do a
   `get_lever5_gate_drops()` snapshot, filter to entries whose
   `ag_id == ag.id`, build records via the producer, extend
   `_current_iter_inputs["decision_records"]` with them, then call
   `reset_lever5_gate_drops()` once at the end of the iteration (alongside
   the existing `reset_bug4_counters()` call). Wrap in `try/except` matching
   the existing pattern; consult `is_strict_mode()` for re-raise behaviour.

   Use the same `_iter_rca_id_by_cluster` lookup that blast-radius records use
   (Phase B delta T1) to populate `rca_id` and the cluster's `root_cause`.

**Acceptance:**

* All five Task B unit tests pass.
* The cross-projection completeness test
  (`test_cross_projection_completeness.py` from Phase B T10) still passes —
  no new `JourneyEvent` ↔ `DecisionRecord` mismatches.
* On cycle 10, the operator transcript's "Proposal Survival And Gate Drops"
  section renders ≥1 `lever5_structural_gate` row for every iteration that
  produced a Lever 5 drop. Visually inspect the transcript to confirm.

**Estimated change:** ~30 lines in `optimizer.py` (collector + drop append),
~50 lines in `decision_emitters.py` (new producer), ~25 lines in `harness.py`
(wire-up), ~150 lines across two new test files.

---

### Task C — Augment `diagnostic_action_group_for_cluster` to add Lever 6 for SQL-shape clusters

**Recommendation: Option (a)** — extend `diagnostic_action_group_for_cluster`,
not `next_action_for_rejection`.

**Rationale (why (a) over (b)):**

* (a) fixes the cluster's AG **at construction time**, so the gate never has
  cause to fire on the next iteration. The fix lands in cycle 10 — same
  iteration the cluster reappears.
* (b) requires the gate drop to convert to a synthetic `rollback_reason`,
  thread through the reflection buffer, and reach the strategist's
  next-iteration prompt. That is more plumbing for a one-iteration delay.
* (b) also overloads `next_action_for_rejection`'s contract: today it maps
  **rollback** reasons to next actions. The Lever 5 structural gate is a
  **proposal-time** gate, not a rollback. Co-locating the two would muddy
  the seam.
* (a) parallels the existing precedent at
  `_DIAGNOSTIC_AG_DIRECTIVES["wrong_filter_condition"] = {"lever": "6", ...}`
  and `..._DIAGNOSTIC_AG_DIRECTIVES["missing_scd_filter"] = {"lever": "6", ...}`
  — Lever 6 already wins for those SQL-shape causes; the inconsistency is
  that aggregation/dimension/grouping causes still route to Lever 5–only.

**TDD shape (write the tests first):**

* Extend `tests/unit/test_decompose_overbroad_ag.py` (or its sibling test for
  `diagnostic_action_group_for_cluster`):
  * Test 1: a cluster with `root_cause="wrong_aggregation"` produces an AG with
    `lever_directives` containing **both** keys `"5"` and `"6"`.
  * Test 2: the Lever 5 directive keeps `kind="sql_shape"` and the existing
    `target_qids` / `guidance` shape (no regression on existing producers).
  * Test 3: the Lever 6 directive carries `kind="sql_snippet"`, the same
    `target_qids` and `root_cause`, plus a guidance string of the form
    `"Emit a sql_snippet patch demonstrating the correct {root} structure"`.
  * Test 4: a cluster with `root_cause="column_disambiguation"` produces an AG
    with **only** key `"1"` (no Lever 6 sibling — Lever 1 is the column
    metadata lever, not SQL-shape; the augmentation must be scoped to
    SQL-shape causes).
  * Test 5: a cluster with `root_cause="wrong_filter_condition"` (already
    routed to Lever 6 today) produces an AG with **only** key `"6"` (no
    duplicate Lever 5 directive — augmentation must be a no-op when the
    primary lever is already 6).
  * Test 6: end-to-end via `decompose_overbroad_ag` — when an over-broad
    parent AG has two source clusters with `root_cause="wrong_aggregation"`
    and `root_cause="missing_aggregation"`, the two `AG_DECOMPOSED_*` children
    each carry both Levers 5 and 6.

**Production change** — in
`control_plane.diagnostic_action_group_for_cluster` (~line 261-295), after
the existing `_DIAGNOSTIC_AG_DIRECTIVES[root]` lookup populates
`lever_directives`, add a single conditional augmentation block:

```python
# When the diagnostic AG's primary lever is 5 with kind sql_shape, add a
# sibling Lever 6 sql_snippet directive so the AG carries an escape hatch
# past the Lever 5 structural gate (optimizer.py:13961). Otherwise the AG
# is born unable to survive the gate (cycle 8 Bug 1 Phase 3b).
if (
    "5" in lever_directives
    and lever_directives["5"].get("kind") == "sql_shape"
    and "6" not in lever_directives
):
    lever_directives["6"] = {
        "kind": "sql_snippet",
        "root_cause": root,
        "guidance": (
            f"Emit a sql_snippet patch that demonstrates the correct "
            f"{root.replace('_', ' ')} structure for the affected qids."
        ),
        "target_qids": qids,
    }
```

The block is intentionally narrow — it triggers **only** on the precise
shape today's gate gates against (Lever 5 + sql_shape kind), and **only**
when no Lever 6 directive already exists. Both guards keep existing AGs
untouched.

**Source list (for the SQL-shape causes that benefit):** every `root_cause`
in `_DIAGNOSTIC_AG_DIRECTIVES` (`control_plane.py:241-258`) where `lever ==
"5"` and `kind == "sql_shape"`:
`plural_top_n_collapse`, `missing_temporal_filter`, `time_window_pivot`,
`missing_filter`, `wrong_aggregation`, `missing_aggregation`,
`missing_dimension`, `wrong_grouping`, `wrong_join_type`. All nine causes
will gain a sibling Lever 6 directive automatically when the dispatcher
runs.

**Acceptance:**

* All six Task C unit tests pass.
* On cycle 10, the raw replay fixture for any decomposed AG built from a
  `wrong_aggregation` cluster carries `lever_directives` with both `"5"` and
  `"6"` keys (visually inspect a fresh fixture or extend the
  `test_replay_fixture_targets` test if one exists).
* On cycle 10, **at least one** Lever 6 patch on `gs_024` reaches the
  applier and is applied. This is the headline acceptance criterion of the
  whole plan.

**Estimated change:** ~12 lines of production code in `control_plane.py`,
~120 lines across the new and updated test files.

---

## Test plan summary

| Layer | New / extended tests | File |
|---|---|---|
| Unit (counter) | 3 tests | `tests/unit/test_merge_bug4_counters_lever5.py` |
| Unit (producer) | 5 tests | `tests/unit/test_lever5_structural_gate_records.py` |
| Unit (validator) | 1-2 tests | `tests/unit/test_validator_lever5_gate.py` |
| Unit (dispatcher) | 6 tests | extend `tests/unit/test_decompose_overbroad_ag.py` |
| Cross-projection | 0 new (must keep passing) | `tests/replay/test_cross_projection_completeness.py` |

All tests are deterministic, no MLflow / no Genie API. Run order:

1. Task A tests (counter merge) — cheapest, confirms persistence path.
2. Task C tests (dispatcher) — highest leverage; if they fail, no Lever 6
   directive ships at all and Task B's emit produces drops nobody acts on.
3. Task B tests (gate emit) — depends on the producer existing in
   `decision_emitters.py`; can be developed in parallel with Task C.

---

## Sequencing and commit boundaries

| Order | Task | Why |
|---|---|---|
| 1 | Task A | One small commit, low risk, lands the operator-visibility piece. Cycle-10-ready by itself. |
| 2 | Task C | Larger commit — changes the diagnostic AG dispatcher. Lands the structural fix. |
| 3 | Task B | Depends on the gate still firing in some scenarios (e.g. clusters whose root_cause is `select_star` or `over_filtered_dimension` and whose primary lever is **not** Lever 5 + sql_shape — Task C does not augment those). Wires the visibility gate-decision producer for the residual cases. |

All three tasks land before cycle 10's lever-loop run. Each is an independent
commit on the existing branch (`fix/gso-lossless-contract-replay-gate` or its
successor); none of them touches the strategist prompt, the applier, or the
patch-DSL.

---

## Risks and rollback

| Risk | Mitigation |
|---|---|
| Task C's auto-augmented Lever 6 directive produces a low-quality `sql_snippet` patch that the blast-radius gate or applier rejects. | Acceptable — the goal is to make `gs_024` *progress* (applied or rolled back) rather than silently stagnate. A rolled-back Lever 6 patch still emits a `JourneyEvent` and a `DecisionRecord` and feeds the reflection buffer for the next iteration. |
| Task C changes the cycle 9 raw replay fixture's `lever_directives` shape. | The replay fixture is regenerated on every cycle by design. If a frozen replay test is keyed on the cycle 8 / cycle 9 shape, update it to expect both `"5"` and `"6"` keys for SQL-shape clusters. The cross-projection completeness test (Phase B T10) does not depend on the directive shape — it depends on the qid set, which is unchanged. |
| Task B's new producer fires for AGs that Task C now repairs (i.e. the Lever 5 path drops but the Lever 6 sibling succeeds). | Desired — the operator transcript should show **both** the drop on Lever 5 and the success on Lever 6. The `Proposal Survival And Gate Drops` section is intended to surface gate fires regardless of whether the AG ultimately produced an applied patch. |
| Task A double-counts when slice + full evals both run. | Mitigated by the existing `_merge_bug4_counters` design: slice/p0 evals deliberately skip the merge (`harness.py:6278`), so only the `full` write folds the counter. |

If Cycle 10 still produces zero applied Lever 6 patches on `gs_024` despite
all three tasks landing, the Phase 3b mini-plan has failed and a follow-on
investigation is needed. The most likely follow-on cause would be the cluster-
driven synthesis path (`cluster_driven_synthesis.py`) refusing to emit a
Lever 6 `sql_snippet` for the `wrong_aggregation` shape — a genuinely
different bug, not a re-occurrence of the gate problem. That investigation
gets its own diagnosis plan.

---

## Cross-references

* Parent plan (this mini-plan supersedes its Phase 3): [`2026-05-02-cycle8-bug1-target-qids-diagnosis-and-plan.md`](./2026-05-02-cycle8-bug1-target-qids-diagnosis-and-plan.md)
* Phase B decision-trace plan (cross-projection contract this plan must respect): [`2026-05-03-phase-b-decision-trace-completion-plan.md`](./2026-05-03-phase-b-decision-trace-completion-plan.md)
* Cycle 9 plan (introduced `blast_radius_decision_records` — pattern this plan mirrors): [`2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md`](./2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md)
* Long-term roadmap (Phase A burn-down, Phase B decision-trace contract): [`2026-05-01-burn-down-to-merge-roadmap.md`](./2026-05-01-burn-down-to-merge-roadmap.md)
* Gate site: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:13961-13971`
* Counter declaration: `optimizer.py:7780-7791`
* Counter merge: `harness.py:770-810`
* Diagnostic AG dispatcher: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py:241-295`
* Decomposition path: `control_plane.py:459-533` (`decompose_overbroad_ag`)
* Producer pattern this mini-plan mirrors: `decision_emitters.py:776-852` (`blast_radius_decision_records`)
