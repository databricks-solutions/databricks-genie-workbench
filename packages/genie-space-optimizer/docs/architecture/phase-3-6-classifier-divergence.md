# Phase 3.6 — Classifier divergence diagnosis (read-only, post-E4)

**Date:** 2026-05-18
**Branch:** `feat/gso-cycle12` (after commits `604c08c5` E1, `568d8393` E2, `0a7f99ff` E3, `dc248651` E4)
**Status:** read-only diagnosis, no code edits
**Trigger:** after E1-E4 the lever loop traverses all 4 iterations but every iteration's per-qid classifier marks every qid `already_passing`, strategist receives `action_groups=[]`, the loop exits `no_actionable_clusters`. None of the 4 anchor qids (gs_005, gs_009, gs_013, gs_024) reaches AG dispatch.

---

## The actual classifier chain

The audit conflated two distinct stages. The "classifier" symptom is downstream of an earlier failure. Full chain:

```
load_latest_state_iteration().rows_json     ← raw eval rows (E3 stub serves these)
        ↓
_analyze_and_distribute(failure_rows)
        ↓
   row-level filters:
     - is_gt_correction_candidate(row)     → corpus-review queue (NOT a hard failure)
     - row_is_hard_failure(row)            → filtered_failure_rows (eligible to cluster)
     - else                                → soft_signal_rows
        ↓
stages.clustering.form(eval_result_for_clustering={"rows": filtered_failure_rows})
        ↓
optimizer.cluster_failures(eval_results)
   groups by (judge, asi_failure_type, blame_set_str)
        ↓
clusters (all_clusters / soft_signal_clusters)
        ↓
_already_passing_set := eval_qids - (hard_qids ∪ soft_qids ∪ gt_correction_qids)
        ↓
per-qid decision_record emit (decision_type=eval_classified)
```

**The classifier itself is a set complement.** It is correct iff clustering produces the right clusters. With `clusters=[]` and `soft_signal_clusters=[]`, every qid lands in `_already_passing_set`.

## Check 1 — what does clustering actually read?

`cluster_failures` (`optimizer.py:2046`) groups by `(judge, asi_failure_type, blame_set_str)`. Inputs:

| Field | Source per row | Required for grouping? |
|---|---|---|
| `judge` | row's eval-result-judge column | YES (groupby key) |
| `asi_failure_type` | UC `genie_eval_asi_results` table via `read_asi_from_uc` | YES (groupby key) |
| `blame_set` | UC `genie_eval_asi_results` table | YES (groupby key) |
| `question_id` | row's qid field | for cluster.question_ids |

`read_asi_from_uc(spark, run_id, catalog, schema)` reads the `genie_eval_asi_results` Delta table — flagged in the Path 1 audit's pre-E1 completeness check as "wrapped in try/except, graceful empty under MagicMock". That graceful-empty IS the problem here: it returns `[]` silently and clustering gets no ASI metadata per row.

## Check 1 — the export tells us why

The historic `replay_fixture_from_latest_export_*.json` eval rows have ONLY:

```json
{"arbiter": "ground_truth_correct", "question_id": "...gs_009", "result_correctness": "no"}
```

— **3 fields**. Specifically, no `asi_failure_type`, no `blame_set`, no `judge`. The ASI was extracted out-of-band into the `genie_eval_asi_results` Delta table (the table `read_asi_from_uc` consumes). The export does NOT carry per-row ASI metadata.

But the export's `clusters` array carries the **computed** cluster output:

```json
{"asi_failure_type": "...", "cluster_id": "H001", "question_ids": [...], "root_cause": "..."}
```

So the tape has:
- raw eval rows (no ASI metadata)
- computed clusters (with full cluster metadata)
- **no row-level ASI mapping** (the link between qids and ASI is in the Delta table only)

Re-running clustering from raw rows under replay produces 0 clusters because the row-level grouping keys are all empty.

## Check 2 — was the predicate logic changed since the historic runs?

`row_is_hard_failure` (`evaluation.py:3649`) uses `_ARBITER_CORRECT_VERDICTS = {"genie_correct", "both_correct"}`. Git history:

```
abba9639 2026-04-24 12:12:10 -0500 Lever loop consolidated fix: correctness foundation, ...
```

The predicate was committed **2026-04-24**, BEFORE the historic anchor runs (2026-05-09). The classification logic was the same in the historic build as it is now.

For the 4 failing rows:
- **gs_005** (`arbiter=genie_correct`, `rc=no`) → matches `is_gt_correction_candidate` → routed to corpus-review queue (intentional, not a Genie failure).
- **gs_009 / gs_013 / gs_024** (`arbiter=ground_truth_correct`, `rc=no`) → `row_is_hard_failure` = True → `filtered_failure_rows` (eligible to cluster). The predicate correctly tags them as hard failures.

So the predicate works. The downstream failure is the grouping step that needs ASI metadata the tape doesn't have at the row level.

**R2 is excluded.** Behavior of the classifier predicate has not changed since the historic runs.

## Check 3 — is the export missing data, or is this how production wrote it?

`post_eval_passing_qids` across all 4 iterations:

```
iter=1: passing_qids=0, failing_rows=4 (gs_005, gs_009, gs_024, gs_013)
iter=2: passing_qids=0, failing_rows=4 (same 4)
iter=3: passing_qids=0, failing_rows=4 (same 4)
iter=4: passing_qids=0, failing_rows=4 (same 4)
```

`post_eval_passing_qids` is empty across the run. This is the legitimate value for a run where no patches landed (the postmortem confirms 0 patches accepted across all 4 iterations). It is not an export bug — it correctly reflects production's "0 new passing qids per iteration".

The eval rows are consistent across all 4 iterations: 20 passing + 4 failing (the same 4 qids). Tape data is **correct and complete**.

**R3 is excluded.**

## What this leaves — R1 with a structural nuance

The gap is in Check 1: clustering reads per-row ASI from a Delta table the harness doesn't tape-serve. The export carries the COMPUTED clusters but not the per-row ASI mapping that produced them.

Two fix shapes for the next step (no commit on this branch — handing back to operator):

### E5a — narrow stub for `read_asi_from_uc`

Reverse-engineer per-row ASI from the tape's computed clusters: for each qid in each cluster, synthesize an ASI row carrying that cluster's `asi_failure_type`, `root_cause`, and (if present) `blame_set`. Stub `read_asi_from_uc` to return these.

- **Pro:** Same shape as previous stubs (one function, narrow seam, ~30 lines).
- **Pro:** Preserves the harness's clustering pipeline — `cluster_failures` runs with real data and produces real cluster output.
- **Con:** The synthesized ASI is partial. `read_asi_from_uc` returns more fields (`severity`, `confidence`, `counterfactual_fix`, `wrong_clause`, `expected_value`, `actual_value`, `missing_metadata`, `ambiguity_detected`). Clustering might use them indirectly. We'd guess-default them to None / "" and hope nothing downstream reads them critically.

### E5b — bypass clustering: serve computed clusters from `iteration_payloads`

Stub `optimizer.cluster_failures` (or higher: `_analyze_and_distribute`) to return the iteration's pre-computed clusters from `iteration_payloads[<current-iter>].clusters`. The lever loop's downstream consumers receive byte-identical clusters to what production produced.

- **Pro:** Single source of truth — the tape's `clusters` field IS what production used. No reverse-engineering.
- **Pro:** Avoids any chance of "reconstructed ASI ≠ original ASI" drift.
- **Con:** Larger seam — bypasses an entire derivation stage instead of patching one underlying read. The harness's clustering code path is no longer exercised under replay, so future clustering bugs wouldn't be caught.
- **Con:** Requires the stub to know which iteration is "current" via `_RECORDER_BINDING.iteration` so it returns the right per-iteration clusters.

### Recommendation

**E5b**, but framed honestly: this is no longer a "narrow stub" — it's a tape-format extension that moves the replay boundary from "tape carries raw inputs, harness re-derives" to "tape carries pre-computed stage outputs, harness consumes them". This is the architectural choice the user flagged at the original anti-treadmill review.

The case for E5b over E5a: the tape's value is "frozen production behavior". Re-deriving clusters under replay invites drift from frozen behavior (today via missing ASI, tomorrow via some other derivation gap). The harness gets to be the same code in production and replay, but the BOUNDARIES at which the replay tape feeds the harness should be at semantic checkpoints (eval rows in, clusters out, AGs out, decisions out) — not at every intermediate state source.

### Caveats — what would change if E5b lands

- The Phase 3.6 protocol doc gains a "pre-computed stage outputs" section enumerating which derivation stages get tape-served (clustering certainly; possibly RCA-card-builder too if it shows up as the next gap).
- Future Phase 4 work on "is clustering doing the right thing" can NO LONGER use these anchor tapes as regression fixtures — the clustering step is short-circuited. We'd need a different test path for that.
- Tests asserting on the harness's CLUSTERING behavior would need their own non-anchor-tape fixtures.

This is a real trade-off. The user's anti-treadmill flag specifically cited this kind of decision: "the tape was supposed to be a 'frozen LLM responses' artifact; it's become a state-store snapshot. That's not wrong, but it has scaling limits."

E5b extends the tape further along that scaling axis. If we go that direction, we should be explicit about which stages are "production-derived, tape-served" vs which are "harness-derived, replay-exercised". The architectural contract gets more nuanced but not unmanageable.

## What I haven't done

- No code edits.
- No commits beyond the doc updates this turn would carry.
- No selection between E5a and E5b — that's the operator decision.

Branch state: 4 commits since the last review (E1-E4 cadence). Anchor regression tests run after each commit; failure mode shifted significantly across the four commits but never crossed the "loop reaches AG dispatch with non-empty clusters" threshold.
