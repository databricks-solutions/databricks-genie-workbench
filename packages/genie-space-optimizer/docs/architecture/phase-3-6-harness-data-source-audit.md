# Phase 3.6 — Harness Data-Source Audit (Path 1 read-only diagnosis)

**Date:** 2026-05-18
**Branch:** `feat/gso-cycle12` (after commits `1e4bbdad` C1, `0bf0c268` D1)
**Status:** read-only diagnosis, no code edits
**Trigger:** Path 2 probe confirmed H1 — `run_evaluation` is never called during the path that produces `baseline_iter keys=NoneType` at `harness.py:18774`. Phase A reads baseline state from a source the replay harness does not yet stub.

---

## 1. Where Phase A reads `baseline_iter`

**Function:** `load_latest_full_iteration(spark, run_id, catalog, schema)` in `src/genie_space_optimizer/optimization/state.py:1539`.

**Call site (the one Path 2's probe traced to):** `src/genie_space_optimizer/optimization/harness.py:18539`:

```python
baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
```

This is `_run_lever_loop`'s Phase A pre-loop block (around lines 18530-18820) which prepares the `_latest_eval_result` seed used by every iteration's gate evaluation.

**Data-source primitive:** Spark/Delta SQL via `run_query(spark, sql)` in `common/delta_helpers.py:295`:

```python
df = run_query(
    spark,
    f"SELECT * FROM {fqn} WHERE run_id = '{run_id}' AND eval_scope = 'full' "
    f"ORDER BY iteration DESC LIMIT 1",
)
if df.empty:
    return None
```

Underlying Delta table: `<catalog>.<schema>.genie_opt_iterations` (`TABLE_ITERATIONS` constant in `state.py`). Production writes every full-eval iteration row via `write_iteration(...)` in state.py. The baseline iteration row carries `rows_json`, `scores_json`, `soft_signal_qids`, `mlflow_run_id`, `evaluated_count`, and every other piece of per-iteration state the lever loop reads as the seed.

## 2. Why `baseline_iter` returns `None` under replay

The replay harness stubs three production callables: `evaluation.run_evaluation`, `common.genie_client.patch_space_config`, `optimization.state.write_stage`. It does **not** stub `state.load_latest_full_iteration` (or any other `load_*` reader in `state.py`), and it does **not** stub `common.delta_helpers.run_query`.

The Phase 3 anchor smoke tests pass `MagicMock(name="spark_replay")` as the `spark` argument to `_run_lever_loop`. Under that mock:

```python
df = _native_df(spark.sql(sql).toPandas())   # ← returns MagicMock
if df.empty:                                 # ← MagicMock attribute → truthy
    return None                              # ← always taken
```

`MagicMock` returns more `MagicMock` for unknown attribute access; `MagicMock.empty` is truthy by default, so every `load_*` call that uses the `df.empty → return None` early-exit returns `None`. The replay never reaches `df.iloc[0].to_dict()`.

Net effect: `baseline_iter = None` → `_seed_eval_result_from_baseline_iter(None) → {}` → Phase A yields `_latest_eval_result = {}` → the first iteration's gate evaluation sees zero failing questions → strategist gets an empty `failure_clusters` payload → returns `{"action_groups": []}` → iteration 1 exits via `no_actionable_clusters` long before reaching the iterations the postmortems documented.

## 3. The full inventory — every cross-source read the harness performs

Found by `grep -nE "load_run\b|load_stages|load_iterations|load_patches|load_provenance|load_latest_state_iteration|load_all_full_iterations|load_latest_full_iteration" src/genie_space_optimizer/optimization/harness.py`:

| Function | state.py:line | harness.py call sites | What it reads | Phase A? |
|---|---|---|---|---|
| `load_latest_full_iteration` | 1539 | 18539, 7495, 13337, 13421, 16394, 17939, 33633 | Latest `eval_scope='full'` iteration row (rows_json, scores, soft signals, mlflow_run_id) | **YES — primary baseline seed** |
| `load_latest_state_iteration` | 1586 | 17936, 19134, 34363 | Latest iteration reflecting current Genie space state (full + enrichment) | YES — clustering uses it |
| `load_run` | 1476 | 10822, 17979 | Run metadata (created_at, status, space_id) | YES — preflight |
| `load_stages` | 1491 | 34371 | All stage transitions for resume / postmortem | Post-loop only |
| `load_all_full_iterations` | 1632 | 13577, 34390 | All full-eval iterations for convergence detection | Mid-loop |
| `load_provenance` | 1712 | 14105 | Proposal provenance history | Mid-loop |

**Other reads outside `state.py`** (lower priority — narrower scope):

- `MlflowClient().download_artifacts(...)` at harness.py:32531, 32547 (Phase H upload), 33855 (held-out anchor) — fires during Phase H/post-loop artifact bundling, NOT during the iteration body. Currently no replay stub; under `MagicMock` returns MagicMock and the post-loop code handles missing artifacts.
- `spark.sql("USE CATALOG ...")` / `USE SCHEMA` at harness.py:6565-6567, 9602-9606 — sets session state; return value not consumed for correctness. Safe under MagicMock.
- `dbutils.jobs.taskValues.set(...)` in `jobs/run_lever_loop.py` (outer notebook wrapper, not in `_run_lever_loop`). All writers, no readers; not consumed during in-process replay.

**Conclusion:** Six `load_*` reader functions inside `state.py` are called from `_run_lever_loop`. **Each one is a candidate gap.** Phase A is the first one to bite because `baseline_iter` gates the entire first iteration's eval state. The other five would surface as subsequent surprises (clustering would see no state; preflight would see no run row; convergence detection would see no iteration history; etc.) once we get past Phase A.

## 4. Proposed fix shape

Three options considered:

### Option (a) — Phase-A-only stub (surgical, narrowest)

Patch `state.load_latest_full_iteration` in `LeverLoopReplayHarness.__enter__` to read from a new tape field `latest_full_iteration: dict | None`. The capture script populates it from the historic export's last `full`-eval iteration.

- **Pro:** Smallest possible diff; fixes Phase A only.
- **Con:** Doesn't generalize. Each of the other five `load_*` siblings surfaces as the next stop-and-report. Three iterations to date have already followed the "found one gap, surfaced another" pattern; (a) bakes that pattern in.

### Option (b) — Tape side-table for all iteration-row reads (preferred)

Extend the tape format with a new top-level field `iteration_payloads: dict[int, dict]` where each value is the full iteration-row dict from `genie_opt_iterations` (rows_json, scores_json, soft_signal_qids, mlflow_run_id, evaluated_count, eval_scope, rolled_back, etc.). Capture script reads these from `replay_fixture_from_latest_export_*.json` (which already carries them — see `iterations[*]` fields).

Add four stubs in `LeverLoopReplayHarness.__enter__` covering the four `state.py` readers that consume this table family:
- `load_latest_full_iteration` → return `payloads[max_key_below_<before_iteration>]` filtered by `eval_scope='full'` and `rolled_back != True`.
- `load_latest_state_iteration` → same filter, accept `eval_scope in ('full', 'enrichment')`.
- `load_all_full_iterations` → return DataFrame of all `eval_scope='full'` payloads.
- `load_run` → return a synthetic run-metadata dict from tape fields already present (`source_run_id`, `tape_id`, `space_id`).

`load_stages` and `load_provenance` are post-loop / mid-loop secondary; stub them as empty DataFrames in the same harness `__enter__`. They'll surface their own gaps if they bite, but the dominant Phase A blocker is the iteration-row family.

- **Pro:** Single coherent fix covering the dominant source of cross-source reads. The tape's existing `replay_fixture_from_latest_export_*.json` source ALREADY carries this data — capture is a one-script-edit.
- **Pro:** Generalizes to live captures (Phase 3.5) — `_run_lever_loop` can write its in-memory iteration snapshots to the same `iteration_payloads` field on the export, no extra plumbing.
- **Pro:** Contract-assertable: `LeverLoopTape.from_json_file` extends its 0-indexed-key check to `iteration_payloads`.
- **Con:** Larger surface than (a). 4-5 new stubs + capture-script extension + contract assertion. Estimated ~60-90 min for the full sweep.

### Option (c) — Capture-time bundle of the full export JSON

Bundle the entire `replay_fixture_from_latest_export_*.json` alongside the tape as a sibling artifact (`*_export.json`). Replay harness loads both and serves any `state.py` read from the export's per-iteration dicts.

- **Pro:** Preserves the source-of-truth artifact verbatim; future schema changes don't require capture-script updates.
- **Con:** Adds a second on-disk artifact per tape; replay harness has two sources of truth (tape entries vs export); contract assertion gets harder.

**Recommendation: (b).** It's the smallest coherent fix that addresses the pattern, not the specific Phase A failure. The capture script already reads the export to populate `evals_by_iteration` and `clusters_by_iteration`; extending it to also write `iteration_payloads` is two lines in `_read_export_side_tables`. The replay-harness stubs are mechanical (each reads `tape.iteration_payloads[<idx>]` and shapes the result for the consuming caller).

Critically, (b) lets us decide ONCE whether the harness's "tape-served vs live-derivation" boundary is the right one, rather than discovering it five more times.

## 5. Open question for the operator

Three of the secondary loaders (`load_stages`, `load_provenance`, `MlflowClient().download_artifacts`) fire OUTSIDE the iteration body — during Phase H artifact bundling and post-loop summary. The replay harness already tolerates these returning empty/None (the smoke tests pass). Should they be tape-served too, or stubbed as empty DataFrames?

Argument for tape-served: they're the same family of cross-source read; bundling them with the iteration-row stubs gives a uniform contract.
Argument for empty-stub: they don't affect iteration behavior; tape-serving them is premium polish, not blocker removal.

I'd default to empty-stub for these three and tape-serve only the four iteration-row readers. But this is a judgment call — flagged here for explicit decision.

## 6. Branch state

- `1e4bbdad` — C1 (0-indexed side-tables + contract assertion)
- `0bf0c268` — D1 (`"{}"` allowlist payload)
- Probe reverted; `git diff` clean.

No code edits in this audit. Awaiting go/no-go on fix-shape (b) before any further commit.
