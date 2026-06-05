# Harness Control-Flow Sites (Canonical)

This document is the **canonical reference** for stable, replay-reachable
insertion points inside `harness._run_lever_loop`. Future plans that need
to wire decisions into the iter body MUST reference sites by name (e.g.
`pre_lever6_proposal_collect`) rather than by raw line number.

## Scope and non-goals

This is a **navigability artifact**, not a refactor proposal. The
`_run_lever_loop` function remains a single ~15,840-line body (lines
17528–33369). The named sites below let plans target the function by
*semantic* role without owning that complexity.

This document is regenerated empirically: run
`scripts/audit_harness_control_flow.py` to refresh the audit sidecars
at `docs/harness/audit/`, then update this doc's line ranges from the
audit output if they have drifted.

## How to keep this in sync

* After any change to `_run_lever_loop`: rerun the audit script and
  update line ranges below. The regression test in
  `tests/replay/active/test_harness_control_flow_sites_regression.py`
  will fail loudly if a named site stops being reachable under both
  anchor tapes.
* When you intentionally remove a named site (refactored away):
  remove its row from the table below AND from the regression test's
  expected-name set, in the same commit.
* When you add a new named site: add its row below AND a paragraph
  in the per-site contracts section. The audit script does NOT
  invent names — naming is a human-curation decision.

## Named-site table

The table below is parsed by the regression test. Format is strict:
columns are pipe-separated; row order is top-to-bottom in iter-body
control flow (pre-loop → iter body → post-loop teardown). Each row's
line range covers the semantic boundary; the regression test asserts
at least one line in the range was executed under the expected
anchor(s).

Only **reachable** sites appear in this table. Sites that are
syntactically present in the harness but never execute under either
anchor tape (the "Phase 5 dead zone") are documented in the
**Unreachable sites** section below — they're real code paths
future plans might want to target, but they cannot be wired without
an anchor tape that triggers proposal acceptance.

<!-- BEGIN CANONICAL SITES TABLE -->
| name | start_lineno | end_lineno | reach_airline | reach_7now |
|------|--------------|------------|---------------|------------|
| phase_2_pre_loop_setup | 18605 | 18615 | YES | YES |
| phase_3_adaptive_lever_loop | 18712 | 18720 | YES | YES |
| pre_loop_setup_complete | 18900 | 19055 | YES | YES |
| iter_loop_start | 19056 | 19070 | YES | YES |
| iter_counter_increment | 19613 | 19625 | YES | YES |
| strategist_orchestration_start | 20800 | 21000 | YES | YES |
| pending_buffered_ags_consume | 21080 | 21300 | YES | YES |
| phase_b_strategist_emit | 22390 | 22420 | YES | YES |
| strategist_response_received | 22390 | 22420 | YES | YES |
| lever_per_iter_setup | 23600 | 23740 | YES | YES |
| lever6_invocation_site | 23745 | 23810 | YES | YES |
| post_lever6_proposal_collect | 23810 | 24450 | YES | YES |
| proposal_generation_empty_record | 24490 | 24520 | YES | YES |
| proposal_generation_empty_marker | 26214 | 26243 | YES | YES |
| proposal_generation_empty_continue | 26244 | 26244 | YES | YES |
| post_loop_replay_fixture_emit | 34196 | 34247 | YES | YES |
| post_loop_mlflow_artifact | 34249 | 34266 | YES | YES |
| post_loop_final_summary | 33548 | 33769 | YES | YES |
<!-- END CANONICAL SITES TABLE -->

## Per-site semantic contracts

For each named site, this section documents:

* **Bound variables**: what's available at this point.
* **Stages run**: what work has already executed.
* **Stages not yet run**: what still lies ahead.
* **Safe to mutate**: which structures can be modified without
  breaking downstream stages.
* **Anchor reachability**: confirmed reachability under each anchor
  tape (must match the table above).

### phase_2_pre_loop_setup

Pre-loop section marker (`# ── Phase 2: Pre-Loop Setup ──`). Marks the
start of the per-run setup before the iteration for-loop.

* **Bound variables**: function arguments only (`w`, `spark`,
  `run_id`, `space_id`, `domain`, `benchmarks`, `levers`, etc.).
* **Stages run**: function entry, Phase 1 inline enrichment.
* **Stages not yet run**: per-iter setup, eval, cluster, strategist,
  lever calls, action_groups stage, full eval, arbiter, teardown.
* **Safe to mutate**: function-scope locals; do NOT mutate inputs.
* **Anchor reachability**: airline=YES, 7now=YES.

### phase_3_adaptive_lever_loop

Section marker (`# ── Phase 3: Adaptive Lever Loop ──`). Marks the
start of the iteration for-loop's enclosing block.

* **Bound variables**: function-scope accumulators
  (`_phase_b_producer_exceptions`, `_iter_traces`,
  `pending_action_groups`, etc.).
* **Stages run**: Phase 2 pre-loop setup complete.
* **Stages not yet run**: every per-iter stage.
* **Safe to mutate**: per-run accumulators only.
* **Anchor reachability**: airline=YES, 7now=YES.

### pre_loop_setup_complete

Last reachable point before the for-loop header. All run-scoped
accumulators are bound; no iter-scoped state exists yet.

* **Bound variables**: every function-scope local declared above
  line ~18900.
* **Stages run**: Phase 2 + Phase 3 markers passed; trial-3 capture
  registry initialized.
* **Stages not yet run**: every iter-body stage.
* **Safe to mutate**: per-run accumulators (additive).
* **Anchor reachability**: airline=YES, 7now=YES.

### iter_loop_start

Top of the iteration for-loop body (`for _iter_num in range(1,
max_iterations + 1):`). Earliest stable insertion point for per-iter
reset logic.

* **Bound variables**: `_iter_num`, plus everything from
  `pre_loop_setup_complete`.
* **Stages run**: function-level setup; tape-replay binding hook
  fires here (line 19065: `_tape_binding_set_iteration(_iter_num - 1)`).
* **Stages not yet run**: eval, cluster, RCA card build, strategist,
  lever calls, action_groups stage.
* **Safe to mutate**: per-iter locals (declared inside the loop body
  immediately after this line). Do NOT mutate iter-scoped state from
  the prior iteration without explicit reset.
* **Anchor reachability**: airline=YES, 7now=YES.

### iter_counter_increment

`iteration_counter += 1` — happens early in each iter body. After this
line, `iteration_counter` is the canonical "current iteration" value
used by decision records, markers, and the recorder binding.

* **Bound variables**: `iteration_counter` (post-increment),
  `_iter_num` (outer for-loop variable, may differ from
  iteration_counter under resume / retry).
* **Stages run**: iter setup; eval not yet run.
* **Stages not yet run**: eval, cluster, RCA card build, strategist.
* **Safe to mutate**: nothing iter-scoped yet (we're at the very
  start of the iter body).
* **Anchor reachability**: airline=YES, 7now=YES.

### strategist_orchestration_start

Approximate start of the strategist-orchestration block (~line 21400).
Eval rows and cluster compute have run; the strategist call is being
prepared.

* **Bound variables**: `eval_rows`, `clusters`, `rca_cards_by_cluster`,
  `_iter_source_clusters_by_id`, plus the iteration-scoped
  accumulators.
* **Stages run**: eval (`evaluate_questions`), cluster compute
  (`cluster_failures`), Plan P-D RCA recovery (when enabled and
  blocked clusters exist), Phase 2.2 provisional RCA from soft signals.
* **Stages not yet run**: strategist LLM call, lever invocations,
  action_groups stage.
* **Safe to mutate**: per-iter intermediate state. Avoid mutating
  `clusters` — downstream stages assume the cluster set is stable.
* **Anchor reachability**: airline=YES, 7now=YES.

### pending_buffered_ags_consume

Re-validation block for buffered action groups carried over from the
previous iteration's strategist response (lines ~21900-22090, the
`while pending_action_groups` loop).

* **Bound variables**: `pending_action_groups` (input),
  `pending_strategy` (input), `_live_cluster_signatures` (computed
  here), `ag` (possibly bound to a re-validated buffered AG).
* **Stages run**: cluster compute, RCA card build, Plan P-D, Phase
  2.2.
* **Stages not yet run**: strategist LLM call (only fires if no
  buffered AG survives re-validation), lever invocations.
* **Safe to mutate**: `pending_action_groups` (pop entries),
  `_dropped_for_drift` audit row.
* **Anchor reachability**: airline=YES, 7now=YES.

### phase_b_strategist_emit

Phase B strategist-AG emit area (lines ~22130-22180). The block
where the strategist's returned AGs are stamped with provenance and
emitted as STRATEGIST_AG_EMITTED decision records.

* **Bound variables**: `action_groups`, `strategy`, `ag` (if
  selected), `_phase_b_target_qids_missing_count`.
* **Stages run**: strategist LLM call (response received),
  pending-AG revalidation.
* **Stages not yet run**: lever invocations, action_groups stage,
  forced synthesis.
* **Safe to mutate**: per-iter emit state.
* **Anchor reachability**: airline=YES, 7now=YES.

### strategist_response_received

Strategist response action_groups appended to `_current_iter_inputs`
(~line 22288). Marks the boundary between strategist call and
per-AG processing.

* **Bound variables**: `strategy` (full strategist response),
  `action_groups`, `_current_iter_inputs["strategist_response"]`.
* **Stages run**: strategist LLM call complete.
* **Stages not yet run**: per-AG proposal generation, action_groups
  stage (which dies before lever-6 emission on the anchor tapes).
* **Safe to mutate**: `_current_iter_inputs["strategist_response"]`
  (additive only).
* **Anchor reachability**: airline=YES, 7now=YES.

### lever_per_iter_setup

Lever-invocation setup area (lines ~23500-23640). Per-lever
configuration before the actual lever calls.

* **Bound variables**: `ag`, `action_groups`, lever-specific
  `levers` list, `_iter_source_clusters_by_id`.
* **Stages run**: strategist; pending-AG revalidation; Phase B
  emit.
* **Stages not yet run**: lever-1..6 calls; action_groups stage.
* **Safe to mutate**: lever-call kwargs; AG metadata (additive).
* **Anchor reachability**: airline=YES, 7now=YES.

### lever6_invocation_site

`_generate_lever6_proposal` invocation block (lines 23636-23700).
This is where the historic-tape replay's cross-cluster
`affected_questions` guard rejection fires under the airline anchor.

* **Bound variables**: `ag`, `_force_cluster`,
  `_iter_source_clusters_by_id[cid]`, strategist hints,
  `metadata_snapshot`.
* **Stages run**: everything up to lever-5; the cluster's
  pre-resolved AFS + RCA card; tape-replay binding pointed at
  `(stage=lever6_llm, iter, ag, cluster)`.
* **Stages not yet run**: post-lever-6 proposal aggregation,
  action_groups stage, full eval, arbiter.
* **Safe to mutate**: nothing — the call is the action; bindings
  feed into the historic_inject replay key.
* **Anchor reachability**: airline=YES, 7now=YES.

### post_lever6_proposal_collect

Post-lever-6 proposal collection + lever-rotation + per-AG outcome
block (lines ~23700-24300). On the airline / 7now anchors, lever-6
returns no proposals (cross-cluster guard), so this block runs the
empty-proposal accounting path.

* **Bound variables**: `proposals` (list of accepted lever-6
  proposals; possibly empty), `all_proposals` (aggregate),
  `_lever_rotation_records`.
* **Stages run**: lever-6 returned.
* **Stages not yet run**: proposal compatibility gate, action_groups
  stage, forced synthesis.
* **Safe to mutate**: `all_proposals` (append-only),
  `_lever_rotation_records`.
* **Anchor reachability**: airline=YES, 7now=YES.

### proposal_generation_empty_record

`proposal_generation_empty_record` typed-record emission block
(~lines 24380-24410). Fires when the per-iter proposal aggregation
produced zero proposals. The record carries the closed-vocabulary
`reason_code` + `next_action` for Plan P-F.

* **Bound variables**: `_empty_rec` (the DecisionRecord),
  `failure_mode="proposal_generation_empty"`.
* **Stages run**: lever-6 returned with empty proposals.
* **Stages not yet run**: proposal compat gate, action_groups
  stage, forced synthesis.
* **Safe to mutate**: the record's `metrics` (additive only) before
  it's appended to `decision_records`.
* **Anchor reachability**: airline=YES, 7now=YES.

### proposal_generation_empty_marker

`iteration_no_candidate_marker(terminal_reason=
"proposal_generation_empty", …)` stdout marker emission block (~lines
26214-26243). Postmortems grep `GSO_ITERATION_NO_CANDIDATE_V1` to
classify the iteration's terminal state.

* **Bound variables**: `_iter_terminal_emitted`,
  `_iter_terminal_reason="proposal_generation_empty"`,
  `_iter_ag_id_for_ledger`, `_iter_cluster_ids_for_ledger`,
  `_ag_cids_for_ledger`.
* **Stages run**: proposal aggregation complete (empty).
* **Stages not yet run**: action_groups stage, forced synthesis,
  full eval, arbiter, acceptance.
* **Safe to mutate**: marker payload (must remain JSON-parseable).
* **Anchor reachability**: airline=YES, 7now=YES.

### proposal_generation_empty_continue

The `continue` at line 26244 that short-circuits the rest of the iter
body under the airline + 7now anchors. **This is the line that ends
the reachable iter-body span**; everything downstream (the
action_groups stage, admission consume, grounding gate, forced
synthesis dispatch) is unreachable under both anchor tapes.

* **Bound variables**: same as `proposal_generation_empty_marker`.
* **Stages run**: empty-proposal handling complete.
* **Stages not yet run**: nothing in this iter (control returns to
  the for-loop header for the next iter).
* **Safe to mutate**: nothing — `continue` is the exit.
* **Anchor reachability**: airline=YES, 7now=YES.

### post_loop_replay_fixture_emit

Phase A replay-fixture stderr emission block (lines ~34196-34247).
Under `GSO_REPLAY_FIXTURE_DUAL_EMIT` (default-ON from Phase 5 WU-5),
this calls `emit_dual_fixture` to write both plain-JSON and base64
marker pairs.

* **Bound variables**: `_replay_fixture_json`,
  `_replay_fixture_iterations`.
* **Stages run**: iter for-loop completed (all max_iterations
  iterations or early break).
* **Stages not yet run**: MLflow artifact upload (next site), final
  summary print, function return.
* **Safe to mutate**: nothing — the fixture is published.
* **Anchor reachability**: airline=YES, 7now=YES.

### post_loop_mlflow_artifact

MLflow artifact upload block (lines ~34249-34266). Best-effort log
of the replay fixture as an MLflow artifact when an active run is
attached; silently no-ops otherwise.

* **Bound variables**: `_replay_fixture_json`, MLflow active-run
  state.
* **Stages run**: replay fixture stderr emission.
* **Stages not yet run**: final summary print, function return.
* **Safe to mutate**: nothing.
* **Anchor reachability**: airline=YES, 7now=YES.

### post_loop_final_summary

End-of-run summary + return block (lines ~33548-33769). Emits the
``_emit_contract_health_summary`` marker, performs the Phase-5
post-loop trial-capture upload, and returns the
``_build_loop_out_with_pretty_print`` result. (Range shifted from
33100-33369 → 33548-33769 in Plan 8 as the lever-loop body grew —
audit refreshed 2026-05-19 against airline + 7now anchor tapes.)

* **Bound variables**: every function-scope accumulator.
* **Stages run**: replay fixture emit + MLflow upload.
* **Stages not yet run**: nothing — this is the function exit.
* **Safe to mutate**: nothing — return value is determined here.
* **Anchor reachability**: airline=YES, 7now=YES.

## Unreachable sites (documented for completeness)

The following sites are syntactically present in `_run_lever_loop`
but **never execute** under either anchor tape because the
`proposal_generation_empty_continue` at line 26244 short-circuits
the iter body. They are listed here so future plans know they exist
and so plans wanting to target them know they need a fundamentally
different anchor (one where lever-6 produces an accepted proposal
that survives the compatibility gate).

| name | start_lineno | end_lineno | airline | 7now | semantic role |
|------|--------------|------------|---------|------|---------------|
| post_proposal_compat_gate | 24700 | 24850 | no | no | RCA/patch-type compatibility gate (Task 6A) |
| action_groups_stage_entry | 24960 | 25050 | no | no | F4 action_groups stage call |
| grounding_gate_prelude | 25230 | 25260 | no | no | `collect_blocked_clusters` invocation |
| admission_consume | 25380 | 25450 | no | no | `apply_admission_trace` consumption |

These rows are deliberately **excluded from the regression test**
because the test asserts each named site is reachable under at least
one anchor. When a future anchor tape exercises the post-empty path
(e.g., a fresh capture from a run where lever-6 emits an accepted
proposal), these sites will become reachable and should be moved
into the regression-tested table above with their observed reach
markers.

## Design notes

### Why no inline `# CFL_SITE: <name>` markers in `harness.py`

This audit deliberately publishes the named-site contract in this
doc rather than as inline source comments in `harness.py`. Trade-offs:

* **Pro of inline markers**: name travels with the code; refactors
  that move a block move the marker with it; less drift.
* **Con of inline markers**: invasive change to the production
  file, plan friction during merges, no behavioral effect at
  runtime so easily lost in unrelated edits.

The compromise: the regression test
(`tests/replay/active/test_harness_control_flow_sites_regression.py`)
parses the table above AND uses the audit script's reachability
trace to confirm each site's line range still anchors a
syntactically reachable construct. That gives drift detection
without invasive in-source comments.

If drift becomes a problem, a follow-on plan can revisit and embed
markers.

### Why the unreachable sites are documented but not regression-tested

Listing them in the regression table would fail the
"reach under at least one anchor" assertion. Excluding them from the
table would silently lose the documentation that they exist as real
code paths a future plan might want to target. The compromise is
the separate "Unreachable sites" section above: it preserves the
knowledge that these sites exist and explains the empirical reason
they're currently unreachable.

## How to cite this doc in a future plan

When proposing a wiring change, future plans should:

1. **Cite the target site by name** (e.g., "wire `decide_slate_action`
   at `iter_counter_increment`").
2. **Verify the named site exists** in the table above.
3. **Quote the line range** from this doc, not a raw line number
   from a grep.
4. **Re-run `scripts/audit_harness_control_flow.py`** after landing
   to confirm the new code hasn't broken any other site's
   reachability.
