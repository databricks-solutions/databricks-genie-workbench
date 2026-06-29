# GSO Optimizer v2 — Implementation Plan & Progress Log

> Living document, maintained by polly + the implementation sub-agents; ships in the implementation PR.
> **Pair this with `GSO_WORKFLOW_REARCHITECTURE_PLAN.md` (the "arch doc")** for code authoring: the arch
> doc owns the **workflow shape** (5-task DAG + the controller-notebook two-mode loop); this doc owns the
> **build plan, code grounding, and progress**. Where they overlap, the arch doc is authoritative on shape.
>
> Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` blocked

---

## 0. Orientation (READ FIRST)

### 0.1 Repo & package layout
- **GSO engine + job:** `packages/genie-space-optimizer/` (own `pyproject.toml` + DABs bundle `databricks.yml`; wheel-deployed). Source `src/genie_space_optimizer/`: `jobs/run_*.py` (notebook entrypoints) · `optimization/` (`harness.py`, `evaluation.py`, `eval_runner.py`, `eval_gates.py`, `eval_budget.py`, `scorers/`, `optimizer.py`, `rca.py`, `applier.py`, `acceptance_policy.py`, `models.py`, `state.py`, `ddl.py`, `benchmarks.py`, `preflight.py`, `labeling.py`) · `common/` (`genie_client.py`, `genie_schema.py`, `config.py`) · `integration/` (`trigger.py`, `levers.py`, `discard.py`) · `ui/`.
- **App backend (FastAPI):** `backend/`; auto-optimize router `backend/routers/auto_optimize.py`; tests `backend/tests/`.
- **Frontend (React+Vite+TS):** `frontend/`; auto-optimize UI `frontend/src/components/auto-optimize/`; TS types `frontend/src/types/index.ts`; API client `frontend/src/lib/api.ts`.

### 0.2 The job (DAG · params · build/deploy)
- **Current** DABs job (`databricks.yml`), serverless, **6 linear `notebook_task`s**: `preflight → baseline_eval → enrichment → lever_loop → finalize → deploy` (`deploy` condition-gated OFF). Entrypoints `jobs/run_<task>.py`.
- **Target shape (arch doc, Phase 7):** a **5-task linear DAG** — `00_intake_and_snapshot → 01_benchmark_qc_and_repair → 02_baseline_eval_and_triage → 03_optimize → publish_and_audit` — with the whole hill-climb as a `while` loop **inside `03_optimize`**. Mapping: `preflight`→`00`+`01` (benchmark repair now inline), `baseline_eval`→`02`, `enrichment`→folded into `03` as the attempt-1 coverage pass, `lever_loop`→`03` while-loop, `finalize`→`publish_and_audit`, `deploy`→**removed (out of scope)**.
- Job params (`databricks.yml`): `run_id, space_id, domain, catalog, schema, apply_mode, levers (default "[1,2,3,4,5,6]"), max_iterations (5), triggered_by, deploy_target, warehouse_id`. (`experiment_name` was removed in the MLflow decommission, Phase 5.) Arch doc adds `target_accuracy` (0.90) and `benchmark_repair_max_tries` (K — §5).
- Build = `apx build` artifact hook → versioned wheel → `.build/genie_space_optimizer-0.0.0-py3-none-any.whl`; deploy via `./scripts/deploy.sh`. **No local dev server — test against a deployed workspace** (`CONTRIBUTING.md`).

### 0.3 Dev workflow & gates (exact commands)
No Makefile / justfile / pre-commit; no repo-wide ruff/black/mypy.
- **GSO Python tests:** `cd packages/genie-space-optimizer && python -m pytest` (unit + integration + replay; integration/replay self-skip when fixtures absent; `conftest.py` mocks workspace/Spark — no live workspace needed). Unit only: `python -m pytest tests/unit`.
- **GSO type-check:** `uv run ty check` (Astral `ty`; the only Python type checker, GSO-scoped).
- **Backend tests:** `./scripts/test.sh` (root pytest scoped to `backend/tests`). GSO via the script: `./scripts/test.sh packages/genie-space-optimizer/tests`.
- **Frontend (npm):** `npm ci`, then `npm run lint` (eslint), `npx tsc -b` (typecheck), `npm test` (vitest), `npm run build`.
- **Definition of done (gates):** GSO `python -m pytest` green · `uv run ty check` 0 NEW diagnostics vs base · frontend `npm run lint` + `npx tsc -b` + `npm test` green.

### 0.4 Levers & authoritative routing maps
`LEVER_NAMES` (`common/config.py:3558`): `0` Proactive Enrichment (now the **attempt-1 coverage pass**, not user-selectable) · `1` Tables&Columns · `2` Metric Views · `3` TVFs · `4` Join Specs · `5` Instructions · `6` SQL Expressions. `DEFAULT_LEVER_ORDER=[1,2,3,4,5,6]`. Descriptions `integration/levers.py:7`.

**Repoint target** = `_RCA_KIND_TO_LEVERS` (`rca.py:101`) + `_RCA_KIND_TO_PATCH_FAMILY` (`rca.py:126`), keyed on RCA *reasons*. The official `assessment_reason → RcaKind` routing is implemented (Phase 3) in `rca._ASSESSMENT_REASON_TO_RCA_KIND` / `rca_kind_for_assessment_reason` / `levers_for_assessment_reasons`. Legacy `_JUDGE_TO_LEVER` (`optimizer.py:448`) / `_ROOT_CAUSE_LEVER_MAP` (`optimizer.py:464`) are the retired inputs.

```python
# rca.py:101  (RcaKind -> recommended levers)  — verified
METRIC_VIEW_ROUTING_CONFUSION: (1, 2, 5)   MEASURE_SWAP: (1, 2, 5, 6)
CANONICAL_DIMENSION_MISSED: (1, 2, 5, 6)    MISSING_REQUIRED_DIMENSION: (1, 5, 6)
EXTRA_DEFENSIVE_FILTER: (5,)                JOIN_SPEC_MISSING_OR_WRONG: (4, 5)
FILTER_LOGIC_MISMATCH: (2, 5, 6)           GRAIN_OR_GROUPING_MISMATCH: (1, 5, 6)
SYNONYM_OR_ENTITY_MATCH_MISSING: (1,)      SQL_EXPRESSION_MISSING: (6,)
EXAMPLE_SQL_SHAPE_NEEDED: (5,)             FUNCTION_OR_TVF_NOT_INVOKED: (3, 5, 6)
FUNCTION_ROUTING_MISMATCH: (3, 5, 6)       TOP_N_CARDINALITY_COLLAPSE: (1, 5, 6)
TIME_WINDOW_LOGIC_MISMATCH: (2, 5, 6)      ASSET_TYPE_ROUTING_MISMATCH: (5,)
UNKNOWN: (5,)
```

### 0.5 Working agreement (guardrails)
- Delegated implementation: **open your own PR**; cross-reviewed by a different-vendor agent; **do not merge** (the human merges).
- **Keep this doc current** — tick the Phase checkboxes and append a one-line §6 entry per phase; the doc ships in the PR.
- **Honor the locked decisions (§2):** no agent/chat-mode abstraction (D4); no double-run eval (D1); reuse the existing routing maps (§0.4) — don't invent; live-space mutation is intentional but **additive-only** with JSON-snapshot rollback (D3).
- **Eval-validity guard (D8 / §3.6):** NEVER seed a scored benchmark question's Q/A into the space's *Example SQL Queries* section (incl. from *passing* rows) — it leaks the answer key and invalidates the API score.
- All §0.3 gates green before marking any phase done.

---

## 1. Goal

Two coordinated re-architectures of the Genie Space Optimizer (GSO), shipped as cross-reviewed PRs the human merges:

1. **Eval / judge / tracking v2 — DONE (Phases 1–6).** Make the official Databricks **Genie Benchmark (Eval-Run) API** the single authoritative evaluation runner; drive lever routing from its per-question `assessment_reasons`; retire the local judges, MLflow, and the Prompt Registry; make Delta the sole tracking/versioning store; migrate the Workbench UI to be assessment-centric.
2. **Orchestration v2 — REMAINING (Phase 7).** Replace the 6-notebook DAG with the **5-task controller-notebook design** in the arch doc: one `03_optimize` `while` loop whose **first attempt is a broad, measured, reversible coverage pass** (enrichment folded in) and whose later attempts are surgical, with benchmark repair inlined into `01` and `deploy` dropped.
3. **Cleanup / tests / docs — LAST (Phase 8).** A single final pass after the orchestration reshape lands, so cleanup sweeps up dead code from BOTH efforts (retired scorers + the superseded notebooks/tasks), backfills tests, and refreshes docs.

---

## 2. Scope decisions (LOCKED)

| # | Decision |
|---|----------|
| D1 | **Official Benchmark API is the SOLE eval runner.** Replace GSO's in-process accuracy scoring; never double-run (avoids 2× eval cost). |
| D2 | **Judges collapse into the Benchmark API — no local scored judges.** Scoring = API verdict (GOOD/BAD/NEEDS_REVIEW); coarse routing = API `assessment_reasons` (25-value taxonomy) + a derived `asset_type_mismatch` annotation on BAD rows (via `detect_asset_type`, `genie_client.py:559`). **Retain GSO's deterministic SQL-shape RCA** (`rca.py` — NOT an LLM judge, no extra eval round-trip) as the *fine* lever sub-router: it resolves what the coarse reasons cannot (the 4 distinct Lever-1 sub-actions, defensive-vs-missing filter, L5-example / L6-expression / L5-instruction priority). |
| D3 | **Delta is the SOLE tracking/versioning store.** Mutate the LIVE space (additive benchmarks + prune invalid/SQL-erroring). Accuracy/scores/config + full patch+provenance trail live in `genie_opt_*` Delta (CDF-versioned) + a per-iteration `config_json`. Rollback = JSON-snapshot re-PATCH from Delta. **MLflow removed from the tracking/versioning path.** |
| D4 | **Chat-mode vs agent-mode differentiation DEFERRED.** No mode abstraction is built (execution-mode is UI-only today; the future API shape is unknown). Revisit when the Benchmark API gains agent-mode. |
| D5 | **Cross-vendor review discipline.** Each PR is cross-reviewed by a different-vendor agent; polly never merges. |
| D6 | **Drop the MLflow Prompt Registry dependency.** It gated startup but never hot-loaded at runtime. Surviving strategist/benchmark/enrichment prompts stay as versioned `config.py` constants; optional best-effort "Linked Prompts" tagging only. |
| D7 | **Remove MLflow from the critical path** — no Review-App human-review (use official `manual_assessment`/`NEEDS_REVIEW` + Genie UI), no UC Model Registry. **Cross-workspace deploy is OUT OF SCOPE**; future approach = the official DAB `genie_space` resource (`docs.databricks.com/aws/en/dev-tools/bundles/resources#genie_space`), not the old MLflow UC-registry path. |
| D8 | **Benchmark working set & eval validity.** Working set = the whole **30–40-question** set; **no train/held-out split** (held-out by nature — Genie is asked the questions and never sees the ground-truth answers). Preflight enforces the window: `>40` ⇒ recommend a prune set in the UI (EXPLAIN-invalid first, then near-duplicates); `<30` ⇒ top-up via synthesis — recommend, never silent auto-delete. **Example-SQL leakage guard (§3.6):** never seed a scored benchmark Q/A (incl. *passing* rows) into *Example SQL Queries*. **Eval-runs are sequential** (no concurrency flag) — the hard 2-hour budget is the sum of all run wall-clocks. **Iteration model (revised per arch doc):** iteration 0 = baseline; **attempt 1 = coverage** (broad enrichment, measured & rolled back if Δacc<0); **attempts 2..N = surgical**. |

---

## 3. Key findings (grounding)

### 3.1 Benchmark / Eval-Run API (databricks-sdk v0.102.0)
Under `/api/2.0/genie/spaces/{space_id}/eval-runs`:
- `genie_create_eval_run(space_id, benchmark_question_ids=None)` — empty/None ⇒ runs all questions in the space.
- `genie_get_eval_run` → `num_correct / num_done / num_needs_review / num_questions` + `eval_run_status`.
- `genie_list_eval_results` → per-question summaries; `genie_get_eval_result_details` → `assessment` (GOOD/BAD/NEEDS_REVIEW), `assessment_reasons` (`ScoreReason` ×25), `actual_response`, `expected_response`, `manual_assessment`.
- Accuracy = `num_correct / num_questions`, server-side.

### 3.2 Enum reference (`databricks.sdk.service.dashboards`)
- **`GenieEvalAssessment`** (3): `GOOD` · `BAD` · `NEEDS_REVIEW`.
- **`ScoreReason`** (25):
  - *Result-diff (8):* `COLUMN_TYPE_DIFFERENCE`, `EMPTY_GOOD_SQL`, `EMPTY_RESULT`, `RESULT_EXTRA_COLUMNS`, `RESULT_EXTRA_ROWS`, `RESULT_MISSING_COLUMNS`, `RESULT_MISSING_ROWS`, `SINGLE_CELL_DIFFERENCE`.
  - *LLM-judge (17):* `LLM_JUDGE_FORMATTING_ERROR`, `LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT`, `LLM_JUDGE_INCORRECT_FUNCTION_USAGE`, `LLM_JUDGE_INCORRECT_METRIC_CALCULATION`, `LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE`, `LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC`, `LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST`, `LLM_JUDGE_MISSING_JOIN`, `LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION`, `LLM_JUDGE_MISSING_OR_INCORRECT_FILTER`, `LLM_JUDGE_MISSING_OR_INCORRECT_JOIN`, `LLM_JUDGE_OTHER`, `LLM_JUDGE_SEMANTIC_ERROR`, `LLM_JUDGE_SYNTAX_ERROR`, `LLM_JUDGE_WRONG_AGGREGATION`, `LLM_JUDGE_WRONG_COLUMNS`, `LLM_JUDGE_WRONG_FILTER`.
- `GenieEvalResponse.response_type ∈ {SQL, TEXT}` (read-only scoring output, **not** a mode selector — basis for deferring D4).

### 3.3 Reason → lever routing (implemented, Phase 3)
`rca._ASSESSMENT_REASON_TO_RCA_KIND` maps all 25 official reasons + the derived `ASSET_TYPE_MISMATCH` to an existing `RcaKind`; `levers_for_assessment_reasons` resolves levers by **reusing** the §0.4 `_RCA_KIND_TO_LEVERS` table (do NOT invent a new map). The deterministic SQL-shape RCA in `rca.py` runs alongside as the fine sub-router; its findings carry confidence `0.8–0.9` vs the coarse reason findings' `0.6`, so the structural diff wins on conflict for the same `(qid, lever)`. `EMPTY_GOOD_SQL` is non-actionable (GT-quality, not a Genie fault).

### 3.4 Eval-run budget (HARD 2-hour job cap)
Native eval-runs are async server jobs, materially slower than the retired in-process scoring: **~15–20 min per 30-question run**, scaling with space complexity, warehouse size, and question count. The DABs job has a **hard 2-hour wall** — total eval wall-clock, NOT iteration count, is the binding constraint.

- **Subset-first 3-gate (implemented, Phase 1):** slice (~5–10 failing-cluster Qs) → P0 (~10–15 priority Qs) → full only after slice+P0 pass. Selectors `eval_gates.select_slice_qids` / `select_p0_qids`; caps `SLICE_GATE_MAX_QUESTIONS=10` / `P0_GATE_MAX_QUESTIONS=15`. Most iterations must NOT run a full benchmark.
- **Budget math:** ≈17 min / 30 Qs ⇒ ~6–7 full runs fit in 2h. Fixed full runs ≈ 3 (baseline + the attempt-1 coverage eval + the publish/finalize full run), leaving ~60–69 min for the loop. `max_iterations` (5) / `max_attempts` is an upper bound, not a target.
- **Budget-aware cap (implemented):** `eval_budget` tracks cumulative official-runner wall-clock and stops the loop before an iteration that can't fund its gate cycle while reserving the final full run. Eval-runs are **sequential** (no API concurrency flag) — assume no parallelism.
- **Bounded working set (30–40 Qs)** keeps a single full run well under budget; preflight enforces the window.

### 3.5 Live-space benchmark mutation — provenance & cleanup (UI requirement)
GSO mutates the **user's live production space** benchmark set (additive PUSH of EXPLAIN-validated questions; PRUNE of invalid/SQL-erroring ones). This is now done in `00_intake_and_snapshot` + `01_benchmark_qc_and_repair` (was `preflight`). It must be **transparent in the Workbench UI**: show the benchmark **diff** (added / removed / changed).
- **Provenance ledger (Delta):** `genie_opt_benchmark_mutations(run_id, question_id, op ∈ {added,removed,changed}, before, after, reason)`, populated at the push/prune (`state.write_benchmark_mutations`). Served to the UI; also reconstructable as (current benchmarks) − (intake snapshot).
- **Cleanup = existing rollback:** the intake snapshot captures the ORIGINAL `serialized_space`. On **discard**, rollback re-PATCHes it (GSO additions removed); on **accept**, the champion config (incl. additions) persists. Merge-only publish never deletes user-authored rows.

### 3.6 Example-SQL leakage guard (EVAL-VALIDITY — hard constraint)
The benchmark is held-out *by nature* (Genie never sees the ground-truth just because the question lives in the space's benchmark config), so there is **no train/held-out split**; the whole set is scored each run. The one real leakage path is **GSO's own config patching**: writing a benchmark question's Q→SQL pair into the space's *Example SQL Queries* section lets Genie read the answer key at inference and fraudulently inflate the score.
- **Hard constraint:** every config write path that touches *Example SQL Queries* — the attempt-1 coverage pass's example-SQL seeding (old Lever 0) and any lever that emits example SQL — MUST exclude examples matching a scored benchmark item (by question-id AND normalized-SQL hash). **Seeding from *passing* rows is itself leakage** and must be excluded.
- **Implemented:** deterministic always-on `LeakageOracle.is_scored_benchmark_qa`; hard-blocks in `is_example_sql_benchmark_leak` regardless of config flags; unit-tested that no scored Q/A reaches the Example SQL config.

---

## 4. Implementation checklist

### Phases 1–6 — Eval / judge / tracking / UI migration — **DONE** ✅
Compressed (full detail in §6 progress log; all gates green, all cross-reviewed and PR'd):
- **[x] Phase 1 — `EvalRunner` swap to the native Benchmark API.** `optimization/eval_runner.py` (`OfficialBenchmarkRunner`: create→poll→list→details; `map_eval_detail_to_row`), behind `USE_OFFICIAL_BENCHMARK_RUNNER` (default ON); subset-first 3-gate + budget guard active; fail-closed after eval creation. (PR #232.)
- **[x] Phase 2 — Benchmark-question lifecycle into the live space.** Additive/merge push at intake (`preflight_push_benchmarks_to_space`), 30–40 window recommendation (`compute_benchmark_window_recommendation`), the §3.6 leakage guard, and the §3.5 provenance ledger.
- **[x] Phase 3 — Judge re-architecture (D2).** Reason→lever routing (§3.3); deterministic SQL-shape RCA retained as the fine sub-router; `asset_type_mismatch` kept as a derived annotation; all 9 scored judges retired; acceptance collapsed to the single API-accuracy gate.
- **[x] Phase 4 — Delta-only config/version tracking (D3).** `config_json` + `is_champion` columns on `genie_opt_iterations`; champion = `idxmax(overall_accuracy)`; rollback/discard stay Delta-based. (PR #237.)
- **[x] Phase 5 — Decommission MLflow + Prompt Registry + human-review (D3/D6/D7).** `models.py` reduced to Delta-only `promote_best_model`; UC-registry + Review-App removed; Prompt-Registry gate dropped; MLflow pointers scrubbed from DDL/state/router/UI/`databricks.yml`. (PR #238.)
- **[x] Phase 6 — UI + backend contract migration.** Assessment-centric end-to-end (`assessment` + `assessment_reasons[]`, NEEDS_REVIEW as a third state); Judges tab/`JudgePassRates` removed; benchmark-changes view; official accuracy denominator. (PR #239.)

### Phase 7 — Orchestration re-architecture (per the arch doc) — **REMAINING**
Carry the (done) v2 eval/RCA/acceptance/tracking logic into the new 5-task shape. Design is authoritative in `GSO_WORKFLOW_REARCHITECTURE_PLAN.md`; this is the build checklist.
- [ ] **Reshape the DABs job** from 6 notebooks to the **5-task linear DAG** (`00_intake_and_snapshot → 01_benchmark_qc_and_repair → 02_baseline_eval_and_triage → 03_optimize → publish_and_audit`); delete the `deploy` task. (`databricks.yml`, `jobs/run_*.py`.)
- [ ] **Split `preflight` → `00_intake_and_snapshot` + `01_benchmark_qc_and_repair`.** `00` = rollback snapshot + run manifest; `01` = validate → **bounded inline repair/prune** (≤ `benchmark_repair_max_tries`) → re-validate → flow unconditionally into `02`; hard-fail only on `BENCHMARK_UNREPAIRABLE`. Reuse the Phase-2 push/window/validate helpers.
- [ ] **Collapse `enrichment` + `lever_loop` into one `03_optimize` controller notebook** running the hill-climb as an in-process `while` loop over plain functions (no `dbutils.notebook.run`, no inter-task task values). The existing `_run_lever_loop` body is the starting point.
- [ ] **Two-mode loop (arch doc §5.1–5.2).** Attempt 1 = coverage mode: reuse the `_run_enrichment`/Lever-0 executor as a measured, reversible first attempt against the frozen baseline (rolled back if Δacc<0). Attempts 2..N = the existing surgical strategist. **Net-new:** a per-attempt breadth/mode parameter threaded into `_call_llm_for_adaptive_strategy` + its prompt, and a controller branch that **bypasses the one-source-cluster-per-action-group invariant for attempt 1 only**.
- [ ] **Per-iteration Delta commit = observability + checkpoint.** Commit one `loop_state`/iteration row at the end of each attempt; heartbeat to driver logs; `03_optimize` resumes from the last committed attempt. Record `attempt_mode` (coverage/surgical).
- [ ] **`finalize` → `publish_and_audit`** — publish champion + audit only on `TARGET_REACHED` / `MAX_ATTEMPTS`.
- [ ] Reconcile the Delta schema: the arch doc's proposed generic `genie_opt_artifacts` table vs the existing Phase-4 `genie_opt_iterations.config_json` + the specific `genie_opt_*` tables — pick one, don't duplicate (see §5).
- [ ] Gates green for the reshaped job.

### Phase 8 — Cleanup, tests, docs (LAST) — **REMAINING**
The final pass once the orchestration reshape lands, so cleanup covers both efforts:
- [ ] Remove dead/retired code paths: leftover scorer modules, and the old per-task notebook entrypoints superseded by the 5-task reshape (notably the standalone `enrichment`, `lever_loop`, and `deploy` tasks).
- [ ] Backfill missing tests: the official runner + reason→lever mapping + question-results state mapping (eval-v2), and the two-mode loop + inline benchmark repair + per-iteration checkpoint/resume (orchestration).
- [ ] Update `docs/07-auto-optimize.md` and cross-link the arch doc.
- [ ] Run gates: test / lint / typecheck green.

---

## 5. Open questions / risks
- [ ] **`benchmark_repair_max_tries` (K) default** for `01`'s inline repair loop (arch doc §13).
- [ ] **Plateau / no-improvement safety stop** in addition to `max_attempts`? If yes, a `break` condition in the `03_optimize` loop (no DAG change).
- [ ] **Empty attempt-1 coverage pass** (warm/well-documented space, nothing to enrich): consume a budget slot, or fall through to surgical without counting against `max_attempts`?
- [ ] **Ownership/scope of the net-new two-mode code** — the breadth/mode parameter + the attempt-1 cluster-agnostic branch.
- [ ] **Delta schema reconciliation** — arch doc `genie_opt_artifacts` vs Phase-4 `config_json` + existing tables (Phase 7 item).
- [x] **Resolved** (D-decisions): no local scored judges; eval latency vs the 2-hour budget (subset-first 3-gate + budget-aware cap + 30–40 window); no train/held-out split (leakage guard instead); eval-runs sequential; Delta-only tracking (D3); drop Prompt Registry (D6); drop MLflow human-review + UC-registry, cross-env deploy out of scope (D7); single-vs-multi PR → per-phase PRs, cross-reviewed; live-space mutation is UI-transparent (§3.5).

---

## 6. Progress log
- **2026-06-24** — Plan finalized & made self-contained: §0 orientation (repo layout, gate commands, verified lever maps), §3 grounding. Spikes confirmed MLflow/Prompt-Registry are non-load-bearing → D3 (Delta-only), D6 (drop Prompt Registry), D7 (drop MLflow human-review/UC-registry; cross-env deploy out of scope, future via DAB `genie_space`). Eval-design locked: 30–40 window, train/held-out split DROPPED + leakage guard, sequential-eval 2-hour budget. (Original iteration relabel v0/v1/v2..N superseded 06-28 by the arch doc's coverage/surgical model.)
- **2026-06-24** — **Phase 1 shipped** + 2 cross-review rounds (PR #232, `claude_code`; Codex review). `eval_runner.py`; subset-first 3-gate made active; fail-closed after eval creation; complete-qid-resolution; non-DONE/empty never reads green. SDK verified against `databricks-sdk==0.102.0` (status field `eval_run_status`).
- **2026-06-24** — **Phase 2 shipped** (benchmark lifecycle into the live space): runner-independent push at preflight, window recommendation, leakage `LeakageOracle`, `genie_opt_benchmark_mutations` ledger.
- **2026-06-24** — **Phase 3 shipped** (judge re-architecture, D2; `polly/phase-3`): `rca._ASSESSMENT_REASON_TO_RCA_KIND` + `levers_for_assessment_reasons`; deterministic SQL-shape RCA retained; 9 judges retired; acceptance = single API-accuracy gate.
- **2026-06-25** — **Phase 4 shipped** + cross-review (PR #237): `config_json` + `is_champion` on `genie_opt_iterations`; atomic champion marking; no ACL/PII in the iteration config whitelist.
- **2026-06-25** — **Phase 5 shipped** + cross-review (PR #238): `models.py` Delta-only; UC-registry + Review-App + Prompt-Registry gate removed; MLflow columns/kwargs scrubbed; 3 dangling-reference fixes (`run_preflight.py`, `gso_lakebase.py`, `models_db.py`).
- **2026-06-25** — **Phase 6 shipped** + cross-review (PR #239): assessment/reasons end-to-end; NEEDS_REVIEW third state; Judges tab + `JudgePassRates` removed; benchmark-changes view; official accuracy denominator fixed.
- **2026-06-28** — Arch doc finalized (5-task DAG + controller-notebook **two-mode loop**: attempt-1 broad coverage measured/reversible, attempts 2+ surgical; benchmark repair inline in `01`; `deploy` out of scope; no MLflow). This doc updated to match, trimmed (Phases 1–6 compressed, progress log condensed), and **Phase 7 (orchestration re-arch)** opened with **cleanup/tests/docs moved to the final Phase 8**. D8 iteration model revised to coverage/surgical.
