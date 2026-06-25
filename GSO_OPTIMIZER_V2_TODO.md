# GSO Optimizer v2 — Implementation Plan & Progress Log

> Living document. Maintained by the orchestrator (polly) and the implementation
> sub-agent across review cycles. This file ships **in the implementation PR**.
>
> Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` blocked

---

## 0. Orientation (READ FIRST — for a cold-start implementer)

### 0.1 Repo & package layout
- **GSO engine + job:** `packages/genie-space-optimizer/` (own `pyproject.toml` + DABs bundle `databricks.yml`; wheel-deployed). Source `src/genie_space_optimizer/`: `jobs/run_*.py` (notebook entrypoints) · `optimization/` (`harness.py`, `evaluation.py`, `scorers/`, `optimizer.py`, `rca.py`, `applier.py`, `acceptance_policy.py`, `models.py`, `state.py`, `ddl.py`, `benchmarks.py`, `preflight.py`) · `common/` (`genie_client.py`, `genie_schema.py`, `config.py`) · `integration/` (`trigger.py`, `levers.py`, `discard.py`) · `ui/`.
- **App backend (FastAPI):** `backend/`; auto-optimize router `backend/routers/auto_optimize.py`; tests `backend/tests/`.
- **Frontend (React+Vite+TS):** `frontend/`; auto-optimize UI `frontend/src/components/auto-optimize/`; TS types `frontend/src/types/index.ts`; API client `frontend/src/lib/api.ts`.

### 0.2 The job (DAG · params · build/deploy)
- DABs job, serverless, 6 `notebook_task`s, strictly linear (`databricks.yml:101-163`): `preflight → baseline_eval → enrichment → lever_loop → finalize → deploy`; entrypoints `jobs/run_<task>.py`. `deploy` is condition-gated OFF (`EQUAL_TO "deploy" "disabled"`).
- Job params (`databricks.yml:76-100`): `run_id, space_id, domain, catalog, schema, apply_mode, levers (default "[1,2,3,4,5,6]"), max_iterations (5), triggered_by, experiment_name, deploy_target, warehouse_id`.
- Build = `apx build` artifact hook (`databricks.yml:26`) → versioned wheel copied to stable `.build/genie_space_optimizer-0.0.0-py3-none-any.whl`. Deploy via `./scripts/deploy.sh`. **No local dev server — do NOT run `uvicorn`; test against a deployed workspace** (`CONTRIBUTING.md:20`).

### 0.3 Dev workflow & gates (exact commands)
No Makefile / justfile / pre-commit; no repo-wide ruff/black/mypy.
- **GSO Python tests:** `cd packages/genie-space-optimizer && python -m pytest` (unit 436 + integration 11 + replay 21; integration/replay self-skip when fixtures absent; `conftest.py` mocks workspace/Spark — no live workspace needed). Unit only: `python -m pytest tests/unit`.
- **GSO type-check:** `uv run ty check` (Astral `ty==0.0.25`; the only Python type checker, GSO-scoped).
- **Backend tests:** `./scripts/test.sh` (root pytest scoped to `backend/tests`). GSO via the script: `./scripts/test.sh packages/genie-space-optimizer/tests`.
- **Frontend (npm):** `npm ci`, then `npm run lint` (eslint), `npx tsc -b` (typecheck), `npm test` (vitest), `npm run build`.
- **Definition of done (gates):** GSO `python -m pytest` green · `uv run ty check` clean · frontend `npm run lint` + `npx tsc -b` + `npm test` green.

### 0.4 Levers & authoritative routing maps
`LEVER_NAMES` (`common/config.py:3558`): `0` Proactive Enrichment (always; enrichment task, not user-selectable) · `1` Tables&Columns · `2` Metric Views · `3` TVFs · `4` Join Specs · `5` Instructions · `6` SQL Expressions. `DEFAULT_LEVER_ORDER=[1,2,3,4,5,6]`. Descriptions `integration/levers.py:7`. (No single id→patch-type dict; patch family comes via the RCA map below.)

**Repoint target** = `_RCA_KIND_TO_LEVERS` (`rca.py:101`) — already keyed on RCA *reasons*, not judge names. v2 maps each official `assessment_reason` → a `RcaKind`, then reuses this table (+ `_RCA_KIND_TO_PATCH_FAMILY`, `rca.py:126`). Legacy judge/string-keyed paths `_JUDGE_TO_LEVER` (`optimizer.py:448`) and `_ROOT_CAUSE_LEVER_MAP` (`optimizer.py:464`) are the OLD inputs being replaced.

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
- Delegated implementation: **open your own PR**; it is cross-reviewed by a different-vendor agent; **do not merge** (the human merges).
- **Keep this doc current** — tick the Phase checkboxes and append to §6 as you go; the doc ships in the PR.
- **Honor the locked decisions (§2):** do NOT build the agent/chat mode abstraction (D4); do NOT double-run eval (D1); reuse the existing routing maps — don't invent (§0.4); live-space mutation is intentional but **additive-only** with JSON-snapshot rollback (D3).
- **Eval-validity guard (D8 / §3.6):** NEVER seed a scored benchmark question's Q/A into the space's *Example SQL Queries* section (incl. seeding from passing benchmark rows) — it leaks the answer key and invalidates the API score.
- All §0.3 gates green before marking Phase 7 done.

---

## 1. Goal

Rework the Genie Space Optimizer (GSO) so the **official Databricks Genie
Benchmark (Eval-Run) API** is the single authoritative evaluation runner for
baseline and lever runs, and so its per-question assessments drive the
optimizer's lever-routing diagnostics. Ship as **one PR**.

---

## 2. Scope decisions (LOCKED)

| # | Decision |
|---|----------|
| D1 | **Official Benchmark API is the SOLE eval runner** (chat-mode). Replace GSO's in-process accuracy scoring. Do **not** double-run (avoids 2× eval cost). |
| D2 | **Judges fully collapse into the Benchmark API — NO local scored judges remain.** Scoring = API verdict (GOOD/BAD/NEEDS_REVIEW); lever routing = API `assessment_reasons` (25-value taxonomy) + a cheap derived `asset_type_mismatch` annotation on BAD rows (reuse `detect_asset_type`, `genie_client.py:559`) to feed Lever 5 guidance. **Retire ALL 9 scored judges**, incl. `asset_routing` (only fires when result already wrong → already covered by API BAD; kept only as derived metadata) and `response_quality` (was threshold-0 diagnostic; revisit with agent-mode). **Refinement (CONFIRMED): retain GSO's deterministic SQL-shape RCA** (`rca.py` — NOT an LLM judge, no extra eval round-trip) as the *fine* lever sub-router, fed by the API's `actual_response`/`expected_response` + result-diff `comparison`. Coarse `assessment_reasons` give the bucket; the deterministic SQL diff resolves what the reasons cannot (the 4 distinct Lever-1 sub-actions, defensive-vs-missing filter, and the L5-example / L6-expression / L5-instruction priority). |
| D3 | **Delta is the SOLE tracking/versioning store (revised).** Mutate the LIVE space, additive benchmarks + prune invalid/SQL-erroring ones. Accuracy/scores/config + full patch+provenance trail live in `genie_opt_*` Delta (CDF-versioned); add a per-iteration `config_json` to close the only gap. Rollback = JSON-snapshot re-PATCH from Delta. **MLflow is removed from the tracking/versioning path** (drop per-mutation runs, `link_eval_scores_to_model`, dead `rollback_to_model`). |
| D4 | **Feature #3 (chat-mode vs agent-mode differentiation) DEFERRED COMPLETELY.** No mode abstraction is built. Rationale: execution-mode selection is UI-only today; the future API shape is unknown. Revisit when the Benchmark API gains agent-mode. |
| D5 | **Single PR**, single implementer, cross-reviewed by a different vendor. polly never merges. |
| D6 | **Drop the MLflow Prompt Registry as a required dependency.** It's a traceability shell around static `config.py` prompt constants (runtime never hot-loads from it) yet gates run/UI/preflight startup. Judge prompts retired; surviving strategist/benchmark/enrichment prompts stay as versioned `config.py` constants. Remove the registration gate; optional best-effort "Linked Prompts" tagging only. |
| D7 | **Remove MLflow from the v2 critical path** — no Review App human-review (official `manual_assessment`/`NEEDS_REVIEW` + Genie UI replace it), no UC Model Registry. **Cross-workspace deploy is OUT OF SCOPE for this PR**; future approach = the official DAB `genie_space` resource (`docs.databricks.com/aws/en/dev-tools/bundles/resources#genie_space`), NOT the old MLflow UC-registry path. Optional: lightweight MLflow tracing of surviving strategist/benchmark LLM calls (non-blocking). |
| D8 | **Benchmark working set & eval validity.** Working benchmark = the **whole 30–40-question set**; **no train/held-out split** (the benchmark is held-out by nature — Genie is asked the questions and never sees the ground-truth answers). Preflight enforces the window: `>40` ⇒ recommend a prune set surfaced in the UI (EXPLAIN-invalid first, then near-duplicates), `<30` ⇒ top-up via synthesis — recommend, never silent auto-delete. **Example-SQL leakage guard (§3.6):** GSO must NEVER seed a scored benchmark question's own Q/A into the space's *Example SQL Queries* section (incl. seeding from *passing* benchmark rows) — that leaks the answer key and invalidates the API score. **Eval-runs are sequential** (no concurrency flag in the API): the 2-hour budget is the sum of all run wall-clocks (§3.4). Iteration numbering: baseline = v0 (iter 0), enrichment = v1 (iter 1), lever loop = iters 2..N. |

---

## 3. Key findings (grounding for the above)

**Benchmark / Eval-Run API (databricks-sdk v0.102.0), under `/api/2.0/genie/spaces/{space_id}/eval-runs`:**
- `genie_create_eval_run(space_id, benchmark_question_ids=None)` — empty list ⇒ runs all questions in the space.
- `genie_get_eval_run` → `num_correct / num_done / num_needs_review / num_questions` + status.
- `genie_list_eval_results` → per-question summaries.
- `genie_get_eval_result_details` → `assessment` (GOOD/BAD/NEEDS_REVIEW), `assessment_reasons` (`ScoreReason` × 25), `actual_response`, `expected_response`, `manual_assessment`.
- Accuracy = `num_correct / num_questions`, server-side.

**`assessment_reasons` → GSO judge mapping (drives D2):**
- Result-diff family (8: `RESULT_*`, `SINGLE_CELL_DIFFERENCE`, `COLUMN_TYPE_DIFFERENCE`, `EMPTY_RESULT`, `EMPTY_GOOD_SQL`) ≈ `result_correctness`.
- `LLM_JUDGE_*` family (17) covers `schema_accuracy` / `logical_accuracy` / `semantic_equivalence` / `completeness` / `syntax_validity`.
- **No official equivalent for `asset_routing`** (kept locally). `response_quality` only weakly covered (`LLM_JUDGE_FORMATTING_ERROR`, `LLM_JUDGE_OTHER`).

**Current GSO eval substrate (to be replaced/retired):**
- Drives the Conversation API directly: `common/genie_client.py:360` `run_genie_query` → `:380` `start_conversation`. Does **not** use the native eval runner today.
- Headline accuracy = arbiter-adjusted result correctness (`optimization/evaluation.py:3911`); 9-judge set `optimization/scorers/__init__.py:93`.
- Live-space mutation via `patch_space_config` → `PATCH /api/2.0/genie/spaces/{space_id}` (`genie_client.py:790`); apply at `applier.py:4289`.
- Rollback = stored prior `serialized_space` JSON re-PATCH (`applier.py:4401`; discard via Delta `genie_opt_runs.config_snapshot` in `integration/discard.py`). `models.py:733 rollback_to_model` is **dead code**.
- Benchmark validation/prune at preflight via EXPLAIN: `benchmarks.py:398 validate_ground_truth_sql`, `preflight.py:2034 preflight_validate_benchmarks`.
- Benchmark publish (merge-only) at finalize: `genie_client.py:1110 publish_benchmarks_to_genie_space`.
- MLflow config versioning exists: `models.py:32 create_genie_model_version` logs `space_config.json` artifact + LoggedModel.

**Agent-mode (Feature #3) — why deferred:**
- No mode selector on Conversation API (`start_conversation`/`create_message` body = `{content}` only), eval-run create request, `update_space`, or the benchmark/`serialized_space` schema (benchmark answer format must be `"SQL"`).
- `response_type=TEXT` / `LLM_JUDGE_*` are read-only chat-mode scoring outputs, not a mode selector. Conclusion: agent-mode execution is UI-only and not driveable headlessly today.

---

### 3.1 `assessment_reasons` → diagnostic concept → lever routing

The 25-value `ScoreReason` taxonomy is the v2 replacement for the retired
per-judge verdicts. Map each reason to the diagnostic concept its old judge
owned, then route to a lever. **Do NOT invent a new lever map** — reuse the
existing judge→lever map (`optimization/optimizer.py:448`) and RCA
recommendations (`optimization/rca.py:101`), repointing the INPUT from
judge-verdicts to these reasons. (The authoritative map is now in §0.4 — `_RCA_KIND_TO_LEVERS`, `rca.py:101`; the "candidate lever" column below is conceptual orientation only.)

**Levers:** `0` Proactive Enrichment (always; the enrichment task) · `1` Tables&Columns ·
`2` Metric Views · `3` Table-Valued Functions · `4` Join Specs · `5` Instructions ·
`6` SQL Expressions. Default order `[1,2,3,4,5,6]` (defined `common/config.py:3558`, `integration/levers.py`).

| `ScoreReason` | Diagnostic concept (retired judge) | Candidate lever(s) |
|---|---|---|
| `RESULT_EXTRA_ROWS` | result_correctness | 6 / 5 (over-loose filter) |
| `RESULT_MISSING_ROWS` | result_correctness | 6 / 5 (over-filter) |
| `RESULT_EXTRA_COLUMNS` | result_correctness / completeness | 1 |
| `RESULT_MISSING_COLUMNS` | result_correctness / completeness | 1 |
| `SINGLE_CELL_DIFFERENCE` | result_correctness | 6 / 2 (calc/aggregation) |
| `COLUMN_TYPE_DIFFERENCE` | result_correctness / schema | 1 |
| `EMPTY_RESULT` | result_correctness / syntax | 5 / 6 |
| `EMPTY_GOOD_SQL` | GT quality (not a Genie fault) | — (exclude / GT-correction) |
| `LLM_JUDGE_SYNTAX_ERROR` | syntax_validity | 6 / 5 |
| `LLM_JUDGE_INCORRECT_FUNCTION_USAGE` | syntax_validity / logical | 6 |
| `LLM_JUDGE_WRONG_COLUMNS` | schema_accuracy | 1 |
| `LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE` | schema_accuracy | 1 (+ 2/3 asset) |
| `LLM_JUDGE_MISSING_JOIN` | schema_accuracy | 4 |
| `LLM_JUDGE_MISSING_OR_INCORRECT_JOIN` | schema_accuracy | 4 |
| `LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION` | logical_accuracy | 6 / 2 |
| `LLM_JUDGE_WRONG_AGGREGATION` | logical_accuracy | 6 / 2 |
| `LLM_JUDGE_MISSING_OR_INCORRECT_FILTER` | logical_accuracy | 6 / 5 |
| `LLM_JUDGE_WRONG_FILTER` | logical_accuracy | 6 / 5 |
| `LLM_JUDGE_INCORRECT_METRIC_CALCULATION` | logical_accuracy | 2 / 6 |
| `LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC` | logical_accuracy | 5 |
| `LLM_JUDGE_SEMANTIC_ERROR` | semantic_equivalence | 5 |
| `LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST` | semantic_equivalence | 5 |
| `LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT` | completeness | 1 / 5 |
| `LLM_JUDGE_FORMATTING_ERROR` | response_quality (dropped) | 5 (low signal) |
| `LLM_JUDGE_OTHER` | catch-all | — (manual / needs-review) |
| *(derived)* `asset_type_mismatch` | asset_routing (retired) | 5 (routing/example-SQL) |

### 3.2 Enum reference (`databricks.sdk.service.dashboards`, sdk v0.102.0)

- **`GenieEvalAssessment`** (3): `GOOD` · `BAD` · `NEEDS_REVIEW`
- **`ScoreReason`** (25):
  - *Result-diff (8):* `COLUMN_TYPE_DIFFERENCE`, `EMPTY_GOOD_SQL`, `EMPTY_RESULT`, `RESULT_EXTRA_COLUMNS`, `RESULT_EXTRA_ROWS`, `RESULT_MISSING_COLUMNS`, `RESULT_MISSING_ROWS`, `SINGLE_CELL_DIFFERENCE`
  - *LLM-judge (17):* `LLM_JUDGE_FORMATTING_ERROR`, `LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT`, `LLM_JUDGE_INCORRECT_FUNCTION_USAGE`, `LLM_JUDGE_INCORRECT_METRIC_CALCULATION`, `LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE`, `LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC`, `LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST`, `LLM_JUDGE_MISSING_JOIN`, `LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION`, `LLM_JUDGE_MISSING_OR_INCORRECT_FILTER`, `LLM_JUDGE_MISSING_OR_INCORRECT_JOIN`, `LLM_JUDGE_OTHER`, `LLM_JUDGE_SEMANTIC_ERROR`, `LLM_JUDGE_SYNTAX_ERROR`, `LLM_JUDGE_WRONG_AGGREGATION`, `LLM_JUDGE_WRONG_COLUMNS`, `LLM_JUDGE_WRONG_FILTER`
- Key `GenieEvalResultDetails` fields: `assessment`, `assessment_reasons[]`, `actual_response[]`, `expected_response[]`, `manual_assessment`. `GenieEvalResponse.response_type ∈ {SQL, TEXT}`.
- Run summary `GenieEvalRunResponse`: `num_correct`, `num_done`, `num_needs_review`, `num_questions`, `eval_run_status`.

### 3.3 v2 job flow (the 6-notebook DAG)

```
trigger ─► integration/trigger.py ─► job_launcher (DABs job)
   │
   ▼  1. PREFLIGHT   validate · create Delta state · fetch serialized_space ·
   │                 gen/load benchmarks ─► EXPLAIN-validate ─► prune ·
   │                 ★PUSH ALL Qs into space.benchmarks (30–40 Qs; NO held-out split)
   │                 (additive/merge) · ★snapshot config to Delta (rollback anchor)
   ▼  2. BASELINE_EVAL (iter 0 = v0)  ★genie_create_eval_run(space, all_qids)→poll→details
   │                 accuracy=num_correct/num_questions · assessment+reasons ·
   │                 derive asset_type_mismatch · = REGRESSION ANCHOR
   │                 ✗ in-process Conversation-API + GT-hash + 9 judges REMOVED
   ▼  3. ENRICHMENT (iter 1 = v1; Lever 0)  descr/joins/example-SQL ─► ★PATCH space ─►
   │                 re-run Benchmark API ─► measured (roll back if Δacc<0) · ★leakage-guard example-SQL
   ▼  4. LEVER_LOOP (iters 2..N; ≤ max_iters)  ── per iteration:
   │       cluster failures (inputs: assessment_reasons + asset_type_mismatch)
   │       ─► strategist LLM → action group across levers 1–6
   │       ─► ★PATCH live space  ─► 3-GATE eval via Benchmark API (slice→P0→full)
   │       ─► accept/rollback on API ACCURACY DELTA (reject ⇒ re-PATCH snapshot)
   │       ─► reflect ─► write Delta iteration (config_json + accuracy + reasons)
   │       converge: accuracy target · plateau · rollback limit · no clusters · maxN
   ▼  5. FINALIZE   ★full-benchmark eval via Benchmark API (+ repeatability) ·
   │                 promote champion (best Delta iteration) · final report
   ▼  6. DEPLOY     condition-gated; real publish app-side after human review (unchanged)

   ★ = new/changed in v2   ✗ = removed.  Live space mutated in place throughout
   (PATCH /api/2.0/genie/spaces/{id}); rollback = re-PATCH prior serialized_space JSON.
```

### 3.4 Eval-run budget (HARD 2-hour job cap)

Native eval-runs are async server jobs, materially slower than the retired
in-process scoring. Measured: **~15–20 min per 30-question run**, scaling with
space complexity, SQL-warehouse size, and **question count**. The DABs job has
a **hard 2-hour wall** (workflow limit) — so total eval wall-clock, NOT
iteration count, is the binding constraint.

Budget math (≈17 min / 30 Qs ⇒ ~6–7 full runs fit in 2h): fixed full runs =
baseline + post-enrichment + finalize ≈ 3 (~51 min), leaving ~60–69
min for the lever loop. A full-benchmark gate every iteration caps the loop at
~3–4 iterations before any slice/P0 cost — too tight.

**Design constraints (Phase 1):**
- **Subset-first 3-gate:** slice (~5–10 failing-cluster Qs) → P0 (~10–15
  priority Qs) → full only on slice+P0 pass (ideally only at acceptance). Most
  iterations must NOT run a full benchmark.
- **Budget-aware iteration cap:** track cumulative eval wall-clock; reserve
  enough for the held-out finalize; stop the loop when the remaining budget
  can't fund another gate cycle. `max_iterations` (5) is an upper bound, not a
  target.
- **Bounded working benchmark (30–40 Qs)** so a single full run stays well
  under budget; preflight enforces the window (prune `>40`, top-up `<30`).
  Latency scales with question count.
- **Eval-runs are sequential** — there is **no concurrency flag** in the API,
  so total eval wall-clock = the sum of every run. Design the loop assuming no
  parallelism; any future concurrency is upside, not a design assumption.

### 3.5 Live-space benchmark mutation — provenance & cleanup (UI requirement)

GSO mutates the **user's live production space** benchmark set: it PUSHES its
EXPLAIN-validated questions (additive) and PRUNES invalid / SQL-erroring ones.
This must be **transparent in the Genie Workbench UI** (user requirement): show
the benchmark **diff** — questions **added**, **removed**, and **changed**.

- **Provenance ledger (Delta, per D3):** record every GSO benchmark mutation in
  Delta (e.g. `genie_opt_benchmark_mutations(run_id, question_id, op ∈ {added,
  removed, changed}, before, after, reason)`), populated at the preflight push +
  prune. The backend serves it to the UI; the diff is also reconstructable as
  (current space benchmarks) − (preflight snapshot).
- **Cleanup model (reuses existing rollback):** the preflight snapshot captures
  the ORIGINAL `serialized_space` (pre-push). On **discard**, rollback
  re-PATCHes that snapshot → GSO-added questions are removed automatically; on
  **accept/finalize**, the champion config (incl. GSO's additions) persists as a
  benefit. No separate cleanup path — merge-only publish still never deletes
  user-authored rows.
- **UI surface (Phase 6):** a "Benchmark changes" view listing added / removed /
  changed questions with provenance, fed by the ledger endpoint.

### 3.6 Example-SQL leakage guard (EVAL-VALIDITY — hard constraint)

The benchmark is held-out *by nature*: Genie is asked each question and must
produce SQL; it never sees the ground-truth answer just because the question
lives in the space's benchmark config. So there is **no train/held-out split** —
the whole 30–40-question set is scored on every eval run.

The one real leakage path is **GSO's own config patching**: if the example-SQL
seeding writes a benchmark question's **Q→SQL pair into the space's "Example SQL
Queries" section**, Genie reads the answer key at inference time and regurgitates
it — fraudulently inflating the API score and making the whole eval meaningless.
(The old train/held-out split was effectively the crude guard against this; a
direct exclusion at the seeding site replaces it.)

**Hard constraint:** every config write path that touches *Example SQL Queries* —
enrichment's example-SQL seeding (Lever 0) and any lever that emits example SQL —
MUST exclude examples matching a scored benchmark item (by question-id AND by
normalized-SQL hash). **Seeding from *passing* benchmark rows is itself leakage**
and must be excluded. If GSO has no benchmark-disjoint example source, example-SQL
seeding from benchmark rows is disabled for the scored set.
**Implementation:** enumerate ALL Example-SQL write sites; add the exclusion
filter; add a unit test asserting no scored benchmark Q/A reaches the Example SQL
Queries config.

---

## 4. Implementation checklist (single PR)

### Phase 1 — `EvalRunner` swap to the native Benchmark API
- [x] Introduce an `EvalRunner` seam; implement `OfficialBenchmarkRunner` over the SDK eval-run methods (create → poll status → list results → get details). → `optimization/eval_runner.py`.
- [x] Map results into GSO's existing per-question result rows (accuracy from `num_correct/num_questions`; verdict from `assessment`; reasons from `assessment_reasons`). → `map_eval_detail_to_row` reuses the flat row dict shape; no parallel schema.
- [x] Wire baseline run and lever-loop 3-gate eval (slice/P0/full) to pass `benchmark_question_ids` subsets. → ACTIVE in Phase 1: when the official runner is the eval path, `_run_gate_checks` runs slice → P0 → full with the capped §3.4 selectors (`eval_gates.select_slice_qids` / `select_p0_qids`, caps `SLICE_GATE_MAX_QUESTIONS=10` / `P0_GATE_MAX_QUESTIONS=15`); the full benchmark runs ONLY after slice + P0 pass (each short-circuits to rollback on regression), so most iterations never pay for a full run. Existing acceptance thresholds are unchanged (only the sequencing is added; threshold rework is Phase 3). Subsets flow through the runner via the `run_evaluation` funnel per `eval_scope`. `eval_gates.run_three_gate` is the standalone tested encapsulation of the same sequence.
- [x] Remove/disable the in-process accuracy scoring path so we never double-run. → feature switch `USE_OFFICIAL_BENCHMARK_RUNNER` (default ON); when the official runner is active it returns before `mlflow.genai.evaluate()` runs. 9 judges left in place but off the critical path (retirement is Phase 3).
- [x] **Eval-run budget guard (§3.4):** subset-first 3-gate (active, above), budget-aware iteration cap against the hard 2-hour wall (reserving the finalize run), and the bounded 30–40-question working-set check. Eval-runs are sequential (no concurrency flag) — budget = sum of run wall-clocks (the loop records the official runner's per-run wall-clock, not the whole-gate elapsed). → `optimization/eval_budget.py`; wired into `_run_lever_loop`. NOTE: `assess_working_set` is the 30–40 *recommendation* helper; the actual window ENFORCEMENT (prune/top-up surfaced in the UI) lands in **Phase 2** with the benchmark push/prune — it is not enforced/surfaced in Phase 1.

### Phase 2 — Benchmark-question lifecycle into the live space
- [x] Ensure GSO's EXPLAIN-validated questions are pushed (additive/merge-only) into `serialized_space.config.benchmarks.questions` before each eval run. Push the **whole 30–40-question set** (NO train/held-out split, D8). *(wired at preflight Step 1d.2 → `preflight_push_benchmarks_to_space`, reuses the merge-only `publish_benchmarks_to_genie_space_with_report`)*
- [x] **Enforce the 30–40 window at preflight:** `>40` ⇒ recommend a prune set surfaced in the UI (EXPLAIN-invalid first, then near-duplicates); `<30` ⇒ top-up via synthesis. Prune is a recommendation, not silent auto-delete. *(`compute_benchmark_window_recommendation`; surfaced via the `PREFLIGHT_BENCHMARK_WINDOW` stage + the push return; `BENCHMARK_WINDOW_MIN/MAX` constants; existing `<TOP_UP_THRESHOLD` synthesis still tops up)*
- [x] **Example-SQL leakage guard (§3.6, D8):** add an exclusion filter on every Example-SQL write path so no scored benchmark Q/A (and no *passing*-row Q/A) is seeded as example SQL; unit-test it. *(deterministic always-on `LeakageOracle.is_scored_benchmark_qa` by question-id / normalized-SQL hash / canonical question; hard-blocks in `is_example_sql_benchmark_leak` regardless of `GSO_EXAMPLE_SQL_FIREWALL_STRICT`; `test_scored_benchmark_qa_exclusion.py`)*
- [x] Confirm prune-invalid behavior still drops SQL-erroring questions before publish. *(validation prune retained + a defensive prune-invalid backstop at the push site; covered by `test_preflight_benchmark_push.py`)*
- [x] **Benchmark provenance ledger (§3.5):** record every push / prune / change to Delta (`genie_opt_benchmark_mutations`) so the UI can render the added/removed/changed diff; keep the preflight snapshot as the discard revert anchor. *(DDL in `ddl.py` + `state.write_benchmark_mutations`; populated at the preflight push/prune; backend endpoint + UI view remain Phase 6)*

### Phase 3 — Judge re-architecture (D2)
- [x] Retire ALL 9 scored judges (`result_correctness`, `arbiter`, `schema_accuracy`, `logical_accuracy`, `semantic_equivalence`, `completeness`, `syntax_validity`, `asset_routing`, `response_quality`). *(retired from the v2 decision path: no judge drives accuracy/routing/acceptance on the active official-runner path; `scorers/__init__.py` docstring + new `RETIRED_JUDGES` mark them retired; the scorer modules survive ONLY behind the legacy in-process fallback and are deleted in Phase 7)*
- [x] Accuracy/correctness from API verdict; lever routing from `assessment_reasons` via the §3 mapping. *(accuracy = official verdict via Phase-1 `build_eval_output_from_official`; routing via the new `_ASSESSMENT_REASON_TO_RCA_KIND` + `rca_kind_for_assessment_reason` / `levers_for_assessment_reasons`, wired into BOTH (a) the RCA-ledger path `extract_rca_findings_from_row` and (b) the ACTIVE clustering + lever-assignment path — `cluster_failures` stamps the aggregated official reasons (+ derived `ASSET_TYPE_MISMATCH`) onto each cluster, `_map_to_lever` takes an `assessment_reasons` arg and prefers reason-derived levers before the legacy judge/root-cause fallbacks (passed from the harness `_mapped_lever` calls + the optimizer natural-lever sites), and `recommended_levers_for_cluster` / `stamp_recommended_levers_on_clusters` derive the strategist's `recommended_levers` from the reason mapping. Legacy/mocked rows (no top-level `assessment_reasons`) keep the legacy routing unchanged)*
- [x] **Retain the deterministic SQL-shape RCA (D2 refinement):** keep `rca.py`'s `actual_response` vs `expected_response` structural diff (`_measures`/`_tables`/`_where_text`/`_equality_filters`/`extract_failed_row_sql_expression_candidates`/`_classify_result_correctness_reason`) as the FINE lever sub-router — it is NOT an LLM judge and adds no eval round-trip. Map each official `assessment_reason` → a `RcaKind`, then reuse `_RCA_KIND_TO_LEVERS` (§0.4). **Enumerate the 6 official `LLM_JUDGE_*` reasons GSO does not yet mirror** (11/17 today) and handle all 25. *(deterministic SQL-shape RCA untouched and still runs alongside reason-derived findings as the fine sub-router; reason findings carry confidence 0.6 < SQL-shape 0.8–0.9 so the fine router wins on `_dedupe_rca_findings` merge. All 25 official ScoreReason values mapped — incl. the 6 previously-unmirrored: `LLM_JUDGE_MISSING_JOIN`, `_SEMANTIC_ERROR`, `_SYNTAX_ERROR`, `_WRONG_AGGREGATION`, `_WRONG_COLUMNS`, `_WRONG_FILTER`. `EMPTY_GOOD_SQL` is mapped but flagged non-actionable, GT-defect not a Genie fault. Verified against installed `databricks-sdk` `ScoreReason` (25 values))*
- [x] Preserve the asset-routing nugget WITHOUT a scored judge: compute `expected_asset_type` / `actual_asset_type` / `asset_type_mismatch` (via `detect_asset_type`, `genie_client.py:559`) as derived annotations on BAD rows → feed Lever 5 routing / example-SQL guidance. (Repo already maps `asset_routing_error → LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE` at `genie_eval_taxonomy.py:136`.) *(`eval_runner._asset_type_annotations` attaches the three fields to BAD/NEEDS_REVIEW rows in `map_eval_detail_to_row`; rca consumes the `asset_type_mismatch` flag → `ASSET_TYPE_ROUTING_MISMATCH` finding (Lever 5). GOOD rows carry no annotation. mv_names unavailable at this layer ⇒ MEASURE()/get_*() SQL-surface detection)*
- [x] Rework acceptance logic (`acceptance_policy`, `all_thresholds_met`) to gate on API accuracy delta — no per-judge thresholds remain. *(`DEFAULT_THRESHOLDS` collapsed to the single API-accuracy gate `{result_correctness: 85.0}` — the legacy accuracy-carrier key, NOT a scored judge; `all_thresholds_met` documented + accepts the `overall_accuracy` alias; per-iteration accept/reject was already pure accuracy-delta in `acceptance_policy.decide_acceptance` (no per-judge logic — unchanged))*

### Phase 4 — Delta-only config/version tracking (D3)
- [x] Add per-iteration full config to Delta: a `config_json` column on `genie_opt_iterations` (or a new `genie_opt_configs(run_id, iteration, config_json)` table). Delta already holds accuracy/scores, run-start `config_snapshot`, and the full patch+provenance trail; CDF gives versioned history. *(chose the **`config_json` column on `genie_opt_iterations`** — matches the dominant wide-table pattern, 1:1 with iteration rows so no join, reuses the existing additive-migration machinery, and inherits the table's already-enabled CDF for free. DDL declared in `_GENIE_OPT_ITERATIONS_DDL` + registered in `ADDITIVE_COLUMN_MIGRATIONS`; written via a new `config_snapshot` kwarg on `state.write_iteration` through a self-contained whitelist/cycle-safe `_project_config_for_iteration` projection — wired at baseline (iter 0), enrichment (iter 1), lever-loop full + slice/p0, and finalize held-out call sites)*
- [x] Champion = best `genie_opt_iterations` row (selection is already Delta-driven); mark it in Delta — no UC model registration. *(new `is_champion BOOLEAN` column on `genie_opt_iterations` + `state.mark_champion_iteration` writer; invoked from `models.promote_best_model` **reusing its existing `idxmax(overall_accuracy)` selection** — placed before the MLflow `model_id` guard so the marker lands even on the Delta-only path with no LoggedModel. No `register_uc_model`/MLflow model-version path added — UC registration decommission stays Phase 5)*
- [x] Rollback stays Delta-based (in-memory `pre_snapshot` for rejected iterations; `genie_opt_runs.config_snapshot` for discard). *(confirmed no-regression: `applier.rollback` still re-PATCHes `apply_log["pre_snapshot"]`; `integration/discard.discard_optimization` still reverts from `genie_opt_runs.config_snapshot`. The new per-iteration `config_json`/`is_champion` live on `genie_opt_iterations` and never enter either rollback path. Asserted in `test_phase4_config_tracking.py`)*

### Phase 5 — Decommission MLflow + Prompt Registry + human-review (D3, D6, D7)
- [x] Remove MLflow config/version logging: `create_genie_model_version`, `link_eval_scores_to_model`, per-mutation runs, and the dead `rollback_to_model` (`models.py`). *(`models.py` gutted to the Delta-only `promote_best_model`; the `model_creation_kwargs` per-mutation-run carrier removed end-to-end; `evaluation.run_evaluation` no longer mints/links a LoggedModel)*
- [x] Remove the UC Model Registry path (`register_uc_model`, `_GenieConfigSnapshot`, `ENABLE_UC_MODEL_REGISTRATION`) and the MLflow-based `run_cross_env_deploy`. Cross-env deploy is OUT OF SCOPE this PR; future = official DAB `genie_space` resource (`docs.databricks.com/aws/en/dev-tools/bundles/resources#genie_space`). *(also removed `_register_uc_version`, `_extract_space_dimensions`, the dead `ensure_deployment_job` + `run_cross_env_deploy.py`/`run_deploy_approval.py` notebooks, and the `UC_REGISTERED_MODEL_TEMPLATE`/`DEPLOYMENT_JOB_NAME_TEMPLATE`/`MODEL_NAME_TEMPLATE` config constants)*
- [x] Remove the Review App labeling session (`labeling_session_url` plumbing in `ddl.py`/router/UI); rely on the official `manual_assessment`/`NEEDS_REVIEW`. *(split `optimization/labeling.py`: removed the MLflow Review App functions, KEPT the Delta-backed `flag_for_human_review`/`get_flagged_questions`/`resolve_stale_flags` → `genie_opt_flagged_questions`, the NEEDS_REVIEW surfacing. Dropped `labeling_run_name`, the harness `create_review_session` block, preflight feedback ingestion, and the `labeling_session_*` columns/fields across ddl/state/engine-backend/app-backend/app-frontend)*
- [x] Drop the Prompt Registry dependency (D6): stop `register_judge_prompts`/registration gating at preflight/startup; keep prompts as `config.py` constants. Optional: best-effort `mlflow.genai` 'Linked Prompts' tagging only (non-blocking). *(removed `register_judge_prompts` + its call, `STRICT_PROMPT_REGISTRATION` env gate, the preflight write-probe `preflight_probe_prompt_registry`, and the env-var setters in run_baseline/run_preflight. `JUDGE_PROMPTS`/`LEVER_PROMPTS`/`BENCHMARK_PROMPTS` stay as `config.py` constants; `register_instruction_version`/`register_benchmark_prompts`/`register_synthesis_prompt` stay as best-effort non-blocking tagging; `common/prompt_registry.py` read-probe KEPT — still used by the app backend's permission UX. Optional 'Linked Prompts' tagging SKIPPED — non-trivial, non-blocking)*
- [x] Scrub now-unused MLflow pointers: `genie_opt_runs.best_model_id`/`experiment_*`/`labeling_session_url`, `genie_opt_iterations.mlflow_run_id`/`model_id`, the `experiment_name` job param, and MLflow `ResourceLinks` in the UI — unless optional tracing is retained. *(removed the columns from the DDL + the `labeling_session_*` additive migrations; dropped the matching kwargs from `create_run`/`update_run_status`/`write_iteration`/`wh_create_run`; removed `experiment_name` from `databricks.yml` + `submit_optimization` + the entrypoint widget reads (preflight self-resolves a deterministic experiment path via `_resolve_experiment_path`, so surviving strategist/benchmark/eval MLflow **tracing** is intact — only the pointer columns/param were scrubbed); removed the MLflow experiment/run/UC `ResourceLinks` from BOTH backends + the app `ResourceLinks.tsx` mlflow category. `genie_opt_iterations.mlflow_run_id` step-detail RESPONSE fields + the engine-UI labeling/mlflow display left for the Phase 6 UI-contract migration — TS-safe, render nothing at runtime; `genie_opt_asi.mlflow_run_id` left as-is — different table, Phase 7)*

### Phase 6 — UI + backend API contract migration (Genie Workbench)
**Backend (`backend/routers/auto_optimize.py`):**
- [ ] `/runs/{id}/question-results`: drop the hardcoded 9-judge list (`:1915`); derive display state from API `assessment` (not `result_correctness`+`arbiter`, `:1902`); return `assessment` + `assessment_reasons[]` instead of `judge_verdicts`.
- [ ] `/runs/{id}/iterations`: keep `overall_accuracy = num_correct/num_questions`; add `num_done` / `num_needs_review`; replace `thresholds_met` with `eval_gate_status` / `api_accuracy_gate_met`.
- [ ] Replace `/runs/{id}/asi-results` per-judge rows with a lightweight official eval-results endpoint (`failure_type` → `assessment_reasons[]`).
- [ ] Surface native eval-run status + URL (reuse the unused `outputs.evaluationRunUrl` hook, `StepDetailContent.tsx:88`).
- [ ] Update baseline step-detail builder (`:515`–`550`) to emit an assessment/reason summary, not per-judge `scores_json`.
- [ ] **Benchmark-changes endpoint (§3.5):** serve the `genie_opt_benchmark_mutations` ledger (added/removed/changed questions + provenance) for a run.

**Frontend (`frontend/src/components/auto-optimize/` + `types/index.ts`):**
- [ ] Retire `JudgePassRates.tsx` + the Judges tab in `PipelineDetailsModal.tsx`; fix the stale "9 evaluation judges" copy.
- [ ] Add **NEEDS_REVIEW as a third per-question state** (not `passed: boolean`) across `QuestionList` / `QuestionDetail` / `QuestionJourney` — prevents mislabeling NEEDS_REVIEW/BAD rows as plain fail.
- [ ] Remap `StepDetailContent` badges → assessment + reason-count summary; wire `evaluationRunUrl`.
- [ ] Repoint `ScoreSummary` / `RunDetailView` / `IterationChart` to official counts (`overall_accuracy` / `num_correct` / `num_needs_review`).
- [ ] Update TS types: `IterationRow` (`scores_json`/`thresholds_met`), question-results (`judge_verdicts`→`assessment`/`reasons`), ASI types.
- [ ] **Benchmark-changes view (§3.5):** a Workbench panel showing questions GSO added / removed / changed in the live space with provenance; add matching TS types for the ledger endpoint.

### Phase 7 — Cleanup, tests, docs
- [ ] Remove dead/retired scorer code paths and update `docs/07-auto-optimize.md`.
- [ ] Unit/integration tests for the new runner + reason→lever mapping + question-results state mapping.
- [ ] Run gates: test / lint / typecheck green.

---

## 5. Open questions / risks
- [x] **Resolved:** drop both `response_quality` and `asset_routing` as scored judges → GSO v2 has **no local scored judges**; asset-type mismatch kept only as a derived annotation feeding Lever 5.
- [x] **Resolved — eval-run latency vs. the hard 2-hour budget (§3.4).** ~15–20 min / 30 questions (scales with space complexity, warehouse size, question count); hard 2-hour job cap. Mitigation: subset-first 3-gate + budget-aware iteration cap + bounded 30–40 working set (§3.4). Eval-runs are **sequential** (no API concurrency flag) — budget = sum of all run wall-clocks.
- [x] **Resolved — no train/held-out split; example-SQL leakage guard instead (D8 / §3.6).** Benchmark is held-out by nature (Genie never sees answers); the whole 30–40-question set is scored each run. Real leakage = GSO seeding a benchmark Q/A into *Example SQL Queries* — guarded by a hard exclusion on all Example-SQL write paths (incl. passing rows). 30–40 window enforced at preflight (prune-recommend, never silent delete).
- [x] **Resolved — iteration numbering & enrichment rollback.** v0 = baseline (iter 0), v1 = enrichment (iter 1), v2..N = lever loop; enrichment is measured and rolled back if its accuracy delta < 0 (no longer assumed always-safe).
- [x] **Resolved — eval-run concurrency.** Not available (no API flag); eval-runs are sequential, so the 2-hour budget is the sum of all run wall-clocks.
- [x] **Resolved — `assessment_reasons` granularity (pi best-practices audit).** Reasons-only is *partially* grounded but **under-serves Lever 1** (one reason `LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE` collapses 4 distinct L1 fixes the official decision matrix separates) and cannot resolve the **defensive-vs-missing filter** opposite-action case or the official **L5-example vs L6-expression vs L5-instruction** priority. GSO does this sub-routing *today* via **deterministic SQL-shape analysis in `rca.py`** (`_measures`/`_tables`/`_where_text`/`_equality_filters`/`extract_failed_row_sql_expression_candidates` + `_classify_result_correctness_reason`), **not** LLM judges. **D2 refinement (CONFIRMED): retain that deterministic SQL-diff RCA**, fed by the API's `actual_response`/`expected_response` + result-diff `comparison` — zero LLM judges, zero extra eval cost (same derived-annotation pattern already approved for asset-type). Caveat: GSO mirrors only 11 of the official 17 `LLM_JUDGE_*` reasons — enumerate the 6 unmapped from the official Benchmark API reference before locking routing.
- [x] **Resolved — live-space benchmark mutation is UI-transparent (§3.5).** Workbench shows added/removed/changed questions; a Delta provenance ledger feeds it; discard reverts via the preflight snapshot, accept persists additions (merge-only never deletes user rows). Phase 2 ledger + Phase 6 view.
- [x] **Resolved — PR structure: Option 1 (single PR).** Engine + backend routers + frontend + TS types ship together (no broken interim UI); the UI migration is in scope (Phase 6).
- [x] **Resolved — D3: Delta-only tracking/versioning.** MLflow removed from the tracking path (D3; Phases 4 + 5).
- [x] **Resolved — D6: drop the Prompt Registry** as a required dependency (Phase 5).
- [x] **Resolved — cross-workspace promotion: drop the MLflow UC-registry path.** Out of scope this PR; future cross-env deploy uses the official DAB `genie_space` resource (D7).
- [x] **Resolved — human review: drop the MLflow Review App.** Rely on the official API's `manual_assessment`/`NEEDS_REVIEW` + Genie UI (D7). Note: with scoring on the official API, GSO stops running `mlflow.genai.evaluate()`, so its eval traces also go away; optional lightweight tracing of surviving strategist/benchmark LLM calls is a non-blocking nice-to-have.

---

## 6. Progress log
- 2026-06-24 — Plan finalized & made self-contained: added §0 orientation (repo layout, exact gate commands, verified lever-routing maps, working agreement), §3.1 reason→lever mapping, §3.2 enum reference, §3.3 job-flow diagram. Lever maps confirmed against code (`rca.py:101`, `optimizer.py:448`).
- 2026-06-24 — MLflow/Delta + Prompt Registry spikes: MLflow config/metric tracking found largely redundant with Delta; Prompt Registry found to be a non-load-bearing traceability shell. Proposed Delta-only tracking (revise D3) + drop Prompt Registry as required dep (new D6); two MLflow pivots opened in §5 (cross-workspace promotion; human-review flow).
- 2026-06-24 — Decisions locked: D3 → Delta-only tracking; D6 → drop Prompt Registry dep; D7 → drop MLflow human-review + UC registry (cross-env deploy out of scope, future via DAB `genie_space`); PR structure → single PR. Added Phase 4 (Delta tracking) + Phase 5 (MLflow/Prompt-Registry/human-review decommission); renumbered UI→Phase 6, Cleanup→Phase 7.
- 2026-06-24 — Open questions addressed: **Q1 latency** → §3.4 eval-run budget (~15–20 min/30 Qs, hard 2-hr cap; subset-first gating + budget-aware iteration cap + bounded full-gate set). **Q3 live-space mutation** → §3.5, made UI-transparent (added/removed/changed diff, Delta provenance ledger; Phase 2 ledger + Phase 6 view). **Q2 reasons granularity** → research spike dispatched (pi) against the official best-practices / tune-quality docs to ground §3.1; pending.
- 2026-06-24 — Q2 best-practices audit (pi) complete: L1–L6 well-grounded in official guidance; gaps = ETL reshaping / column-hiding / monitoring (no lever), L0 only weakly grounded. Reasons-only under-serves **L1** + the L5/L6 priority + defensive-vs-missing filter; recommended refinement = **retain GSO's deterministic SQL-shape RCA** (`rca.py`, not an LLM judge, zero extra eval cost) on the API's returned `actual_response`/`expected_response`. Pending user confirm of the D2 clarification.
- 2026-06-24 — Job-flow & eval-design decisions locked: **iteration relabel** (v0 baseline → v1 enrichment → v2..N levers; enrichment now rollback-able if Δacc<0); **benchmark window 30–40** with preflight prune-recommendation; **train/held-out split DROPPED** (benchmark is held-out by nature) → replaced by the **example-SQL leakage guard** (new §3.6 + D8): never seed a scored benchmark Q/A into *Example SQL Queries*; **eval-runs are sequential** (no API concurrency flag) — budget = sum of run wall-clocks; **D2 deterministic-RCA refinement CONFIRMED**. Doc updated: D2, new D8, §3.3 diagram, §3.4, new §3.6, §0.5, Phases 1–3, §5.
- 2026-06-24 — **Phase 1 implemented** (`claude_code`, branch `gso/optimizer-v2-phase1`). Engine-only Python. SDK methods verified against the installed `databricks-sdk==0.102.0` before coding — one deviation from the plan note: the status field is `eval_run_status` (type `EvaluationStatusType` ∈ {NOT_STARTED, RUNNING, DONE, EVALUATION_CANCELLED, EVALUATION_FAILED, EVALUATION_TIMEOUT}), **not** `status`; `genie_list_eval_results` is paginated (`next_page_token`); `genie_create_eval_run(benchmark_question_ids=None)` omits the field ⇒ runs all.
  - **New modules:** `optimization/eval_runner.py` (the `EvalRunner` Protocol + `OfficialBenchmarkRunner`: create→poll→list(paginated)→details, injectable clock/sleep; `map_eval_detail_to_row` maps each official result into the EXISTING flat per-question row dict — `result_correctness/value` carries the GOOD⇒yes / BAD|NEEDS_REVIEW⇒no verdict under both legacy and `feedback/`-prefixed keys, with native `assessment`/`assessment_reasons` attached for the Phase-3 reason→lever repoint; `build_eval_output_from_official` produces the legacy `run_evaluation` output contract; `resolve_space_benchmark_qids` best-effort maps GSO benchmarks→space-side qids); `optimization/eval_budget.py` (`EvalBudget` cumulative-wall-clock guard reserving the finalize run + `estimate_three_gate_seconds` + `assess_working_set` 30–40 recommendation); `optimization/eval_gates.py` (`run_three_gate` subset-first slice→P0→full sequencing with short-circuit + per-gate budget recording + subset selectors).
  - **Seam / double-run disable (D1):** the official runner is wired at the single `run_evaluation` funnel (`evaluation.py`) behind `USE_OFFICIAL_BENCHMARK_RUNNER` (default ON). It activates only for a real `WorkspaceClient` (mocked test workspaces stay on the legacy path), resolves the scope's subset to space-side qids, runs the official API, and RETURNS before `mlflow.genai.evaluate()` — so the in-process scorers never run alongside it (no double-run). Falls back to the legacy path when the subset can't be resolved (Phase 2 gap) or on any official-path error.
  - **3-gate + budget:** every `eval_scope` (baseline=full; gates=slice/p0/full) now passes its resolved subset through the runner. The budget guard is instantiated in `_run_lever_loop`; each iteration records the gate wall-clock and the loop stops before an iteration when the remaining wall (after the finalize reserve) can't fund another estimated gate cycle — `max_iterations` is an upper bound, not a target.
  - **Gates:** `python -m pytest` → 3862 passed / 14 skipped / 3 xfailed, **4 failures are pre-existing on the base branch** (`test_skill_parser_handoff` ×3, `test_state_migration` ×1 — unrelated to this change); +39 new unit tests (`test_eval_runner.py`, `test_eval_budget.py`, `test_eval_gates.py`). `uv run ty check` → 392 diagnostics, all pre-existing (base branch is not ty-clean); this change adds **0** new diagnostics (0 in the new modules).
  - **Deliberately deferred to Phase 3 (NOT done here):** retiring the 9 scored judges, the reason→lever repoint via `_RCA_KIND_TO_LEVERS`, and reworking `acceptance_policy`/`all_thresholds_met` (the Phase-1 official output keeps `result_correctness`+accuracy-delta acceptance unchanged — only the subset-first SEQUENCING is added in Phase 1). Deferred to **Phase 2:** the guaranteed additive benchmark push + robust space-side qid resolution (Phase 1 resolves best-effort and falls back BEFORE any eval-run is created), the example-SQL leakage guard, the provenance ledger, and the 30–40 working-set ENFORCEMENT (Phase 1 ships only the `assess_working_set` recommendation helper). Per D3/D4 unchanged: rollback semantics, no agent/chat-mode abstraction.
- 2026-06-24 — **Phase 1 cross-review fixes** (`claude_code`, addressing 5 blocking findings from the Codex/OpenAI cross-vendor review of PR #232):
  - **F1 — subset-first 3-gate is now ACTIVE in Phase 1** (was incorrectly skip-to-full). When the official runner is the eval path, `_run_gate_checks` runs slice (capped failing-cluster Qs) → P0 (capped priority Qs) → full, full ONLY after slice+P0 pass, via the capped §3.4 selectors. Acceptance thresholds unchanged. Mocked-workspace tests keep the legacy path (runner is `None`).
  - **F2 — fail-closed after eval creation** (`evaluation.run_evaluation`): split into Phase A (pre-creation: build runner + resolve qids; any failure ⇒ legacy fallback, no eval-run created) and Phase B (post-creation: `run()` + return, NOT wrapped in catch-and-fallback). Once `genie_create_eval_run` is called we never start the legacy evaluator — no double-run.
  - **F3 — complete qid resolution required** (`resolve_space_benchmark_qids`): returns the full resolved list only when EVERY requested benchmark resolves; any unresolved ⇒ `None` ⇒ legacy fallback before creation. No more silent partial runs.
  - **F4 — non-DONE/partial/empty never reads green** (`EvalRunResult.is_complete_success` + `build_eval_output_from_official`): a non-DONE terminal status, `num_done < num_questions`, or an empty set maps to accuracy 0 with every requested id as a failure and `thresholds_met=False`, so the slice/P0/full gates all reject → rollback.
  - **F5 — row-schema legacy aliases** (`map_eval_detail_to_row`): mapped rows now also carry `inputs/question`, `outputs/response`, `inputs/expected_response`, `generated_sql`, `expected_sql` (the keys the active harness/state/feature-mining readers consume), alongside the native `assessment`/`assessment_reasons`. Added a row-schema compat test exercising the real harness readers (`_get_question_text`/`_get_genie_sql`/`_get_expected_sql`/`_get_question_id`) + `row_is_hard_failure`.
  - **Non-blocking:** budget recording now scopes to the official eval-run wall-clock (`eval_runner` accumulator reset/summed per iteration) instead of the whole-gate elapsed; TODO wording corrected so it no longer implies the working-set window is enforced in Phase 1.
  - **Gates after fixes:** `python -m pytest` → 3874 passed / 14 skipped / 3 xfailed, same 4 pre-existing base-branch failures; +12 new tests (51 total across the 3 eval modules). `uv run ty check` → 392 diagnostics, all pre-existing, 0 new.
- 2026-06-24 — **Phase 1 round-2 cross-review fix** (`claude_code`): closed an F4 edge case — `EvalRunResult.is_complete_success` did not enforce non-empty collected rows, so a nominally-DONE run with `num_questions>0` but `rows==[]` (a pagination/listing quirk) read as success and the server-reported accuracy passed the gate green with an empty failure set. Added `len(self.rows) >= self.num_questions` to the property (the official path is 1:1 results→rows on a genuine success; a per-detail mapping failure raises rather than silently shortening, so real successes aren't misclassified). Short/empty row sets now fail closed exactly like a non-DONE status. +2 tests (`test_done_with_empty_rows_fails_closed`, `test_done_with_short_rows_fails_closed`); 53 eval-module tests, full suite 3876 passed (same 4 pre-existing), `ty check` 392 (0 new).
- 2026-06-24 — **Phase 2 shipped** (benchmark-question lifecycle into the live space). Wired the benchmark push at **preflight** (runner-independent — no dependency on the unbuilt Phase-1 EvalRunner seam): new `preflight.preflight_push_benchmarks_to_space` runs as `run_preflight.py` Step 1d.2 (after EXPLAIN validation, before baseline). It (a) pushes the WHOLE EXPLAIN-validated set additive/merge-only into `serialized_space.benchmarks.questions` via the new report-returning `genie_client.publish_benchmarks_to_genie_space_with_report` (the int-returning `publish_benchmarks_to_genie_space` is now a thin wrapper — all existing callers/tests unchanged); (b) enforces the 30–40 window as a **recommendation** through `compute_benchmark_window_recommendation` (>40 ⇒ near-duplicate-first recommended prune; <30 ⇒ synthesis top-up count) surfaced via a `PREFLIGHT_BENCHMARK_WINDOW` stage — never a silent delete; (c) keeps EXPLAIN-invalid pruning and adds a defensive prune-invalid backstop before publish; (d) writes the new `genie_opt_benchmark_mutations` provenance ledger (DDL in `ddl.py`, registered in `_ALL_DDL`; writer `state.write_benchmark_mutations`) with added/removed/changed rows. **Example-SQL leakage guard (§3.6 hard constraint):** enumerated all Example-SQL write sites; every benchmark-derived example-SQL path already routes through `leakage.py` (synthesis via `is_benchmark_leak`, proactive via `is_example_sql_benchmark_leak`). Closed the one relaxable gap — added a deterministic, always-on `LeakageOracle.is_scored_benchmark_qa` (matches by question-id, normalized-SQL hash, OR canonical question text; the corpus is the whole scored set so *passing* rows are covered) and made `is_example_sql_benchmark_leak` hard-block on it regardless of `GSO_EXAMPLE_SQL_FIREWALL_STRICT`. New tests: `test_scored_benchmark_qa_exclusion.py`, `test_preflight_benchmark_push.py`, `test_benchmark_mutations_ledger.py`. Gates: GSO `python -m pytest` green (3844 passed / 17 skipped / 3 xfailed); `uv run ty check` adds 0 net diagnostics (392 pre-existing on the base branch, unchanged). Also repaired one pre-existing stale unit test (`test_state_migration.py::test_rolled_back_entry_is_present_in_real_migrations`, now inspects the real `ADDITIVE_COLUMN_MIGRATIONS` list after the migration-list refactor) and made the fixture-less `test_skill_parser_handoff.py` integration tests self-skip per the documented integration-test contract. Out of scope (untouched): Phases 1, 3–7 — the EvalRunner swap, judge re-arch, Delta/MLflow tracking, and the Phase-6 backend endpoint + UI view for the mutations ledger.
- 2026-06-24 — **Phase 3 shipped** (judge re-architecture, D2; branch `polly/phase-3`). Engine-only Python; no behavior change on the legacy/mocked path (every change is gated on official-runner artifacts). **Verified the 25 `ScoreReason` values against the installed `databricks-sdk` before mapping** (8 result-diff + 17 `LLM_JUDGE_*`).
  - **Reason → lever routing (new, the core deliverable):** `rca._ASSESSMENT_REASON_TO_RCA_KIND` maps all 25 official reasons + the derived `ASSET_TYPE_MISMATCH` to an existing `RcaKind`; `rca_kind_for_assessment_reason` / `levers_for_assessment_reasons` resolve levers by REUSING `_RCA_KIND_TO_LEVERS` + `_RCA_KIND_TO_PATCH_FAMILY` (no new lever map, §0.4). Wired via `_findings_from_assessment_reasons` into `extract_rca_findings_from_row`, **gated on the top-level `assessment_reasons` key that only `OfficialBenchmarkRunner.map_eval_detail_to_row` writes** — so legacy/in-process/mocked rows (which lack it) route exactly as before. The 6 previously-unmirrored `LLM_JUDGE_*` reasons (`MISSING_JOIN`, `SEMANTIC_ERROR`, `SYNTAX_ERROR`, `WRONG_AGGREGATION`, `WRONG_COLUMNS`, `WRONG_FILTER`) are now handled. `EMPTY_GOOD_SQL` is mapped but flagged non-actionable (`_NON_ACTIONABLE_ASSESSMENT_REASONS`) — a GT defect, not a Genie fault → no lever, no finding.
  - **Deterministic SQL-shape RCA retained as the FINE sub-router (D2 refinement):** `rca.py` structural diff is untouched and runs alongside the coarse reason findings. Reason findings carry confidence `0.6` < the SQL-shape findings' `0.8–0.9`; when both fire for the same `(qid, RcaKind)` they share an `rca_id` and `_dedupe_rca_findings` keeps `max(confidence)` + unions levers, so the fine router dominates. Zero extra eval round-trips (no LLM judge).
  - **Asset-type nugget preserved without a scored judge:** `eval_runner._asset_type_annotations` computes `expected_asset_type` / `actual_asset_type` / `asset_type_mismatch` via `detect_asset_type` on BAD/NEEDS_REVIEW rows only (GOOD rows carry none) inside `map_eval_detail_to_row`; rca turns a true `asset_type_mismatch` into an `ASSET_TYPE_ROUTING_MISMATCH` (Lever 5) finding. `mv_names` is unavailable at this layer ⇒ detection uses the authoritative SQL-surface signals (`MEASURE(...)` ⇒ MV, `get_*(...)` ⇒ TVF).
  - **Acceptance reworked to API-accuracy gating (no per-judge thresholds):** `common/config.DEFAULT_THRESHOLDS` collapsed from the 8-judge dict to the single `{result_correctness: 85.0}` API-accuracy gate (`result_correctness` survives ONLY as the accuracy-carrier key the official mapper populates with `num_correct/num_questions`, not as a scored judge). `evaluation.all_thresholds_met` re-documented and now also accepts the `overall_accuracy` alias; its loop naturally gates on accuracy alone. `acceptance_policy.decide_acceptance` was ALREADY pure accuracy-delta (no per-judge logic) — confirmed, left unchanged. The `_informational_judges = {j for j,t in DEFAULT_THRESHOLDS.items() if t==0.0}` derivations degrade to empty sets safely.
  - **9 judges retired from the decision path:** `scorers/__init__.py` docstring marks them retired + new `RETIRED_JUDGES` constant; no decision path (accuracy/routing/acceptance) on the active official-runner path consumes a judge verdict. Scorer modules + `make_all_scorers` are kept ONLY for the legacy in-process fallback and are physically deleted in **Phase 7** (per the existing phasing). `EXPECTED_JUDGE_SET` and the arbiter contract tests are intentionally left intact (legacy-path contract).
  - **Tests:** new `tests/unit/test_phase3_reason_routing.py` (22 tests — full 25-reason coverage incl. the 6 newly-handled, lever-map reuse, non-actionable `EMPTY_GOOD_SQL`, finding extraction, legacy-row isolation, accuracy-only acceptance, retired-judge config asserts) + 3 asset-annotation tests added to `test_eval_runner.py`.
  - **Gates:** GSO full `python -m pytest` → **3934 passed / 17 skipped / 3 xfailed** (0 failures; +25 over the Phase-2 base). `uv run ty check` → **392 diagnostics, all pre-existing, 0 new** (none in the changed files `rca.py` / `eval_runner.py` / `config.py`). Out of scope (untouched): Phases 4–7 — Delta-only tracking, MLflow/Prompt-Registry/human-review decommission, the Phase-6 backend/UI contract migration (still reads the legacy `judge_verdicts`/`thresholds_met` shape), and physical scorer-module deletion.
- 2026-06-25 — **Phase 4 shipped** (Delta-only config/version tracking, D3; branch `polly/phase-4`). Engine-only Python; fully additive — no behavior change on the legacy/mocked path (every new column is written NULL/`false` by default and the new `config_snapshot` kwarg defaults to `None`).
  - **Storage choice — `config_json` STRING column on `genie_opt_iterations`** (NOT a new `genie_opt_configs` table). Rationale: the per-iteration config is 1:1 with the iteration row, so a column needs no join and no new writer; `genie_opt_iterations` is already the project's wide "everything about this eval pass" table (`scores_json`/`rows_json`/`reflection_json` + ~20 additive migrations), so a column matches the dominant convention; the additive-migration machinery (`ADDITIVE_COLUMN_MIGRATIONS` + `_migrate_add_columns`) is purpose-built for exactly this; and the table's already-enabled **CDF** gives versioned config history for free (closing D3's one gap on top of the existing run-start `config_snapshot` + patch/provenance trail). Declared in `_GENIE_OPT_ITERATIONS_DDL` (fresh installs) AND registered in `ADDITIVE_COLUMN_MIGRATIONS` (existing tables); both added to `_REQUIRED_ITERATION_COLUMNS` so the migration self-check catches drift.
  - **Per-iteration config write path:** new `config_snapshot: dict | None` kwarg on `state.write_iteration`. A self-contained `_project_config_for_iteration` normalizes either a raw fetched config (nesting under `_parsed_space`) or an already-parsed `metadata_snapshot` to a whitelisted Genie-domain projection — drops every optimizer-internal `_*` key (`_failure_clusters`/`_data_profile`/`_strategy`/…, which is also where cycles live) and de-cycles defensively, then serializes via the existing `_opt_json` escaper. The projection mirrors `models._SAFE_SPACE_CONFIG_KEYS`/`_project_space_config_for_artifact` but is kept in `state.py` so the Delta write path carries **no MLflow import** (D3: Delta is the sole store; Phase-5-safe). Wired at every iteration write site: baseline iter 0 (`baseline_persist_state` ← threaded from `_build_predict_and_scorers`'s fetched config + `run_baseline.py`), enrichment iter 1 (`config`), lever-loop full + slice + p0 (the POST-apply candidate config — see the 2026-06-25 cross-review fix below; the original commit wrongly passed the pre-patch `metadata_snapshot`), finalize held-out (`_ho_parsed`). Empty/None projections write `NULL`.
  - **Champion marked in Delta:** new `is_champion BOOLEAN DEFAULT false` column + `state.mark_champion_iteration(run_id, iteration, eval_scope)` (a single atomic run-scoped conditional UPDATE — see the 2026-06-25 cross-review fix; the original commit used a non-atomic clear-then-set — scoped to the exact `(run_id, iteration, eval_scope)` row so a same-iteration slice/p0 row is never mismarked; best-effort — a write failure logs, never raises). It is invoked from `models.promote_best_model` **reusing that function's existing Delta-driven selection verbatim** (`full`/`enrichment` scope, rolled-back rows excluded except baseline iter 0, `idxmax(overall_accuracy)`) — no new selection logic. The call is placed BEFORE the MLflow `model_id` early-return guard, so the Delta champion marker is recorded even when there is no LoggedModel (the v2 Delta-only path). **Explicitly NO UC model registration added** (`register_uc_model`/`create_genie_model_version` untouched — that decommission is Phase 5).
  - **Rollback/discard — confirmed no-regression (not rebuilt):** rejected-iteration rollback still re-PATCHes the in-memory `apply_log["pre_snapshot"]` (`applier.rollback`, unchanged); discard still reverts from `genie_opt_runs.config_snapshot` (`integration/discard.discard_optimization`, unchanged). The new `config_json`/`is_champion` live on `genie_opt_iterations` and never enter either path. Asserted by dedicated tests.
  - **Tests:** new `tests/unit/test_phase4_config_tracking.py` (26 tests after the cross-review round — DDL+migration registration, the projection helper incl. cycle-safety + `_parsed_space` preference + non-dict guards, `write_iteration` config_json/NULL/escaping, `mark_champion_iteration` clear-then-set + scope + best-effort, `promote_best_model` champion-marking reusing the existing selection incl. rolled-back/slice exclusion + the no-model_id path + a "no UC registration added" source guard, and the rollback + discard no-regression assertions).
  - **Gates:** GSO full `python -m pytest` → **3965 passed / 17 skipped / 3 xfailed** (0 failures; +22 over the Phase-4 base of 3943, all from the new test file — skips/xfails unchanged ⇒ 0 net-new failures). `uv run ty check` → **392 diagnostics, all pre-existing, 0 new** (verified by per-file diagnostic-count diff against a stashed clean tree; 0 in the touched files `ddl.py`/`state.py`/`models.py`/`harness.py`/`run_baseline.py` and the new test). Out of scope (untouched, later phases): Phase 5 MLflow/Prompt-Registry/human-review/UC-registry decommission (MLflow champion alias in `promote_best_model` left in place — the new Delta marker runs alongside it); Phase 6 backend/UI contract; Phase 7 scorer-module deletion + doc rewrite.
- 2026-06-25 — **Phase 4 cross-review fixes** (`claude_code`, addressing the cross-vendor review of PR #237: 1 blocking + 3 non-blocking).
  - **BLOCKING — lever-loop iteration rows recorded the WRONG config_json (pre-patch, not the evaluated candidate).** The slice/P0/full gate writes in `_run_gate_checks` passed `config_snapshot=metadata_snapshot`, but the caller sets `metadata_snapshot = _pre_ag_snapshot_capture["snapshot"]` BEFORE `apply_patch_set(...)` — that is the pre-patch rollback anchor, and `apply_patch_set` deep-copies its input (it never mutates `metadata_snapshot`) and returns the evaluated candidate as `apply_log["post_snapshot"]`. So iters 2..N persisted the pre-patch config and the rejected-candidate configs Phase 4 must version were lost. **Fix:** compute `_candidate_config_snapshot = apply_log["post_snapshot"]` (guarded — falls back to `metadata_snapshot` only when there is genuinely no post-apply snapshot) once at the top of `_run_gate_checks`, and pass THAT to all three slice/P0/full `write_iteration` calls. **Rollback semantics unchanged:** `metadata_snapshot` remains the rollback anchor everywhere it is legitimately used; only the recorded `config_json` changed. Regression test `test_run_gate_checks_records_post_apply_config_not_pre_patch` drives the REAL gate path (slice gate forced to fail → early return) and asserts the slice row's `config_snapshot` IS `post_snapshot`; `test_all_three_gate_writes_use_candidate_not_pre_patch_snapshot` statically pins all three sites to the candidate var and asserts `config_snapshot=metadata_snapshot` no longer appears.
  - **Non-blocking #1 — atomic champion marking** (`state.mark_champion_iteration`): replaced the two-step clear-then-set (which could leave a run with NO champion if the second UPDATE failed) with a SINGLE run-scoped conditional UPDATE — `SET is_champion = (iteration = <best> [AND eval_scope = <scope>]) WHERE run_id = …` — so a row can never be cleared without the new champion being set in the same statement; on failure the prior state is left intact. Tests updated to assert the single atomic UPDATE.
  - **Non-blocking #2 — whitelist comment drift corrected** (`state._SAFE_ITERATION_CONFIG_KEYS`): the comment now states the set INTENTIONALLY differs from `models._SAFE_SPACE_CONFIG_KEYS` — it extends it with `benchmarks`/`config` (tracking the full effective serialized space, not the MLflow artifact subset) and prefers `_parsed_space`.
  - **Non-blocking #3 — no ACL/PII in Delta** (reviewer suggestion #2): dropped `permissions` / `owner` from the iteration config whitelist (no consumer; exactly the user-identity/ACL data we must not copy into the optimization tables). This is also the guard for a future caller that passes a raw config WITHOUT `_parsed_space` — ACL/owner data still never reaches `config_json`; the projection carries no credentials/tokens/secrets and stays valid, decycled JSON. Tests `test_projection_omits_acl_and_owner_pii` + `test_safe_keys_exclude_acl_and_include_full_serialized_space`.
  - **Gates after fixes:** GSO full `python -m pytest` → **3969 passed / 17 skipped / 3 xfailed** (0 failures; +26 over the pre-Phase-4 base of 3943; `test_phase4_config_tracking.py` now 26 tests — skips/xfails unchanged ⇒ 0 net-new failures). `uv run ty check` → **392 diagnostics, 0 new** (per-file diagnostic-count diff vs a stashed clean tree is identical; 0 in the touched files + new test).
- 2026-06-25 — **Phase 5 shipped** (decommission MLflow + Prompt Registry + human-review, D3/D6/D7; branch `polly/phase-5`). Tracking/versioning is now Delta-only end-to-end. **SDK note:** verified against the installed `databricks-sdk==0.102.0` — no SDK symbol was needed (this phase REMOVES MLflow/UC-registry calls); the only API surface touched is the SDK-independent removal of `dbutils.jobs.taskValues`/widget plumbing and Delta DDL. The MLflow APIs removed (`mlflow.initialize_logged_model`, `set_logged_model_alias`, `register_model`, `mlflow.genai.register_prompt`, `mlflow.genai.labeling.*`) were on the now-dead paths.
  - **Item 1 — MLflow config/version logging removed (`optimization/models.py`):** `models.py` reduced from the LoggedModel/UC-registry module to a single Delta-only `promote_best_model` (reads `genie_opt_iterations`, picks `idxmax(overall_accuracy)` over non-rolled-back full/enrichment rows, calls `mark_champion_iteration`, writes `best_iteration`/`best_accuracy`; returns the champion iteration int). Removed `create_genie_model_version`, `link_eval_scores_to_model`, the dead `rollback_to_model`, plus the artifact helpers (`_project_space_config_for_artifact`/`_decycle`/`_log_dict_artifact`/`_safe_serialize`/`_resolve_uc_metadata`/`_initialize_logged_model`/`_finalize_logged_model`) and `import mlflow`. **Per-mutation runs:** removed the `model_creation_kwargs` carrier end-to-end (`evaluation.run_evaluation` param + block, `harness.baseline_run_evaluation` + the lever-loop `_model_kwargs`, `run_baseline._baseline_model_kwargs`, the `RunEvaluationKwargs` TypedDict field). `evaluation.run_evaluation` no longer mints a LoggedModel or calls `link_eval_scores_to_model`. **Rollback semantics unchanged** (D3): `applier.rollback` (in-memory `pre_snapshot`) + `integration.discard` (`config_snapshot`) untouched.
  - **Item 2 — UC Model Registry path removed:** `register_uc_model`, `_register_uc_version`, `_GenieConfigSnapshot`, `_extract_space_dimensions` deleted from `models.py`; the harness finalize `register_uc_model` call + UC-registration print block removed (`promote_best_model` now does Delta-only champion marking). `ENABLE_UC_MODEL_REGISTRATION`, `UC_REGISTERED_MODEL_TEMPLATE`, `DEPLOYMENT_JOB_NAME_TEMPLATE`, and the dead `MODEL_NAME_TEMPLATE` removed from `common/config.py`. **Cross-env deploy (OUT OF SCOPE, future = DAB `genie_space`):** removed the dead `ensure_deployment_job` (`backend/job_launcher.py`, only caller was a commented-out block in the old `register_uc_model`) and deleted the `jobs/run_cross_env_deploy.py` + `jobs/run_deploy_approval.py` notebooks + their `_NOTEBOOK_SOURCES` entries. `run_finalize.py` degrades gracefully (its `uc_registration` getter returns None → empty UC task values). `run_deploy.py` (the DAG's gated-off deploy task) kept; its MLflow-integration MAGIC doc note updated to Delta-only.
  - **Item 3 — Review App labeling session removed:** split `optimization/labeling.py` — removed the MLflow Review App functions (`ensure_labeling_schemas`, `create_review_session`, `ingest_human_feedback`, `sync_corrections_to_dataset`, the trace-population/session helpers) + `import mlflow`; **KEPT the Delta-backed flagging** (`flag_for_human_review`/`resolve_stale_flags`/`get_flagged_questions` → `genie_opt_flagged_questions`, plus the MLflow-free `_extract_question_id` the harness reuses) — this IS the NEEDS_REVIEW surfacing we now rely on. Removed `labeling_run_name` (`common/mlflow_names.py`), the harness `create_review_session` block + `labeling_session` report key, and `preflight_load_human_feedback`'s MLflow ingestion (now a no-op returning `[]`). Scrubbed `labeling_session_url`/`_name`/`_run_id` across `ddl.py` (DDL + additive migrations), `state.update_run_status`, `backend/models_db.GSORunRecord`, the engine backend `runs.py` (iteration-detail + pending-reviews endpoints + the `category="review"` links) and `backend/models.PendingReviewsOut`/`IterationDetailResponse`, the app backend `auto_optimize.py` `_run_to_dict`, the app `frontend/src/types` `GSOPipelineRun` + the `RunDetailView` human-review banner + the `OptimizationNarrative` flag.
  - **Item 4 — Prompt Registry dependency dropped (D6):** removed `register_judge_prompts` (227-line fn) + its `run_evaluation` call, the `STRICT_PROMPT_REGISTRATION` env gate, the preflight write-probe gate `preflight_probe_prompt_registry` + its call, and the `GENIE_SPACE_OPTIMIZER_STRICT_PROMPT_REGISTRATION` env setters in `run_baseline.py`/`run_preflight.py`. Judge prompts stay as `common/config.py` constants (`JUDGE_PROMPTS`/`LEVER_PROMPTS`/`BENCHMARK_PROMPTS`, untouched). `register_instruction_version`/`register_benchmark_prompts`/`register_synthesis_prompt` stay as best-effort, non-blocking surviving tagging. **Boundary:** `common/prompt_registry.py` (the read-probe `check_prompt_registry`) was KEPT — it's the customer-facing UC-permission availability check re-exported by the *app* backend (out of scope), NOT the judge-prompt registration gate; its test `test_prompt_registry_probe.py` is untouched. The optional `mlflow.genai` 'Linked Prompts' tagging was SKIPPED (non-trivial, explicitly non-blocking).
  - **Item 5 — MLflow pointers scrubbed:** removed `genie_opt_runs.best_model_id`/`experiment_name`/`experiment_id`/`labeling_session_*` and `genie_opt_iterations.mlflow_run_id`/`model_id` from the DDL, the matching kwargs from `create_run`/`update_run_status`/`write_iteration`/`wh_create_run` (+ the `mlflow_run_id`/`model_id` columns from the `write_iteration` INSERT), and the `experiment_name` job param (`databricks.yml` param + task pass-through, `submit_optimization`, the `run_preflight.py` widget read, and the now-dead `prev_experiment` reuse in `trigger.py`/`spaces.py`). **Surviving MLflow tracing is intact (D7):** `preflight_setup_experiment` still calls `mlflow.set_experiment`, self-resolving a deterministic path via `_resolve_experiment_path(space_id, domain)` when no name is passed — so removing the param/columns does NOT break strategist/benchmark/eval traces; `experiment_name`/`experiment_id` still flow between job tasks as **taskValues** (not persisted columns). Removed the MLflow experiment/run/UC `ResourceLinks` from the engine backend `runs.py:_build_links` AND the app backend `auto_optimize.py`, plus the `mlflow` category from the app `ResourceLinks.tsx`. The app backend `_ITER_COLS` SELECT no longer requests `mlflow_run_id` (would break on fresh post-Phase-5 tables).
  - **Deliberately deferred / out of scope:** (a) the `genie_opt_iterations.mlflow_run_id` step-detail RESPONSE fields (`mlflowRunId`) and the engine-UI (`src/.../ui/`) labeling/MLflow *display* references are left for the **Phase 6** UI-contract migration — they are TS-safe and render nothing at runtime now that no backend emits the source data; (b) `genie_opt_asi.mlflow_run_id` is a different table on the legacy scored-judge path → **Phase 7**; (c) scorer-module physical deletion → **Phase 7**; (d) cross-env deploy replacement (DAB `genie_space`) → future. **Schema scrub note:** removing columns from the `CREATE TABLE` DDL + migration list affects fresh installs; existing deployed tables keep the orphaned columns (harmless — never read/written). Old tables: `write_iteration`'s explicit-column INSERT and the app `_ITER_COLS` SELECT both list a subset, which is valid against a superset table.
  - **Tests:** new `tests/unit/test_phase5_decommission.py` (24 source/contract guards — removed symbols absent, `models.py`/`labeling.py` carry no `import mlflow`, scrubbed DDL columns, dropped writer/launcher kwargs, no `experiment_name` in `databricks.yml`, cross-env notebooks deleted, judge prompts still config constants). Adjusted: `test_mlflow_names.py` (dropped `labeling_run_name`), `test_run_evaluation_kwargs.py` (dropped `model_creation_kwargs` key), `test_write_iteration_schema.py` (dropped `model_id` kwarg + asserts no `model_id` column), `test_phase4_config_tracking.py` (`promote_best_model` is Delta-only → returns the champion iteration, no model_id early-return), `test_sql_qualification_and_miner.py` (dropped the `create_genie_model_version` stub). Deleted `test_models_circular_reference.py` (tested the removed artifact helpers; the Delta-side decycle lives in `state.py`, covered by `test_phase4_config_tracking.py`).
  - **Gates:** GSO `python -m pytest` → **3979 passed / 17 skipped / 3 xfailed** (0 failures; +10 net over the 3969 base — +24 new guards − ~14 from deleting `test_models_circular_reference.py`/`test_labeling_run_name`; **0 net-new failures**). `uv run ty check` → **391 diagnostics (≤ the 392 pre-existing base ⇒ 0 new)** — one *fewer* than baseline because removing the UC-registry MLflow code dropped a pre-existing diagnostic; the touched files add 0 (transient `unexpected-keyword`/`unresolved-reference` errors from the removed `update_run_status` kwargs were all fixed). Backend `./scripts/test.sh` → **445 passed, 2 failed** — both `test_create_agent.py::TestCreateSpaceIdempotency` (`AttributeError: SimpleNamespace has no attribute 'llm_model'` in the untouched `create_agent.py`); **pre-existing on the base branch** (this is the first phase to run the backend suite — Phases 1–4 were engine-only; the failing path has zero overlap with this diff). Frontend (`frontend/`): `npx tsc -b` clean, `npm run lint` 37 errors (all pre-existing `no-explicit-any`, **0 net-new** — count identical to base), `npm test` (vitest) **44 passed**.
