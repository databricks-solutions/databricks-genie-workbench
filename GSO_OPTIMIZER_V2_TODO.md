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
- [ ] Introduce an `EvalRunner` seam; implement `OfficialBenchmarkRunner` over the SDK eval-run methods (create → poll status → list results → get details).
- [ ] Map results into GSO's existing per-question result rows (accuracy from `num_correct/num_questions`; verdict from `assessment`; reasons from `assessment_reasons`).
- [ ] Wire baseline run and lever-loop 3-gate eval (slice/P0/full) to pass `benchmark_question_ids` subsets.
- [ ] Remove/disable the in-process accuracy scoring path so we never double-run.
- [ ] **Eval-run budget guard (§3.4):** subset-first 3-gate, budget-aware iteration cap against the hard 2-hour wall, and a bounded 30–40-question working set. Eval-runs are sequential (no concurrency flag) — budget = sum of all run wall-clocks.

### Phase 2 — Benchmark-question lifecycle into the live space
- [ ] Ensure GSO's EXPLAIN-validated questions are pushed (additive/merge-only) into `serialized_space.config.benchmarks.questions` before each eval run. Push the **whole 30–40-question set** (NO train/held-out split, D8).
- [ ] **Enforce the 30–40 window at preflight:** `>40` ⇒ recommend a prune set surfaced in the UI (EXPLAIN-invalid first, then near-duplicates); `<30` ⇒ top-up via synthesis. Prune is a recommendation, not silent auto-delete.
- [ ] **Example-SQL leakage guard (§3.6, D8):** add an exclusion filter on every Example-SQL write path so no scored benchmark Q/A (and no *passing*-row Q/A) is seeded as example SQL; unit-test it.
- [ ] Confirm prune-invalid behavior still drops SQL-erroring questions before publish.
- [ ] **Benchmark provenance ledger (§3.5):** record every push / prune / change to Delta (`genie_opt_benchmark_mutations`) so the UI can render the added/removed/changed diff; keep the preflight snapshot as the discard revert anchor.

### Phase 3 — Judge re-architecture (D2)
- [ ] Retire ALL 9 scored judges (`result_correctness`, `arbiter`, `schema_accuracy`, `logical_accuracy`, `semantic_equivalence`, `completeness`, `syntax_validity`, `asset_routing`, `response_quality`).
- [ ] Accuracy/correctness from API verdict; lever routing from `assessment_reasons` via the §3 mapping.
- [ ] **Retain the deterministic SQL-shape RCA (D2 refinement):** keep `rca.py`'s `actual_response` vs `expected_response` structural diff (`_measures`/`_tables`/`_where_text`/`_equality_filters`/`extract_failed_row_sql_expression_candidates`/`_classify_result_correctness_reason`) as the FINE lever sub-router — it is NOT an LLM judge and adds no eval round-trip. Map each official `assessment_reason` → a `RcaKind`, then reuse `_RCA_KIND_TO_LEVERS` (§0.4). **Enumerate the 6 official `LLM_JUDGE_*` reasons GSO does not yet mirror** (11/17 today) and handle all 25.
- [ ] Preserve the asset-routing nugget WITHOUT a scored judge: compute `expected_asset_type` / `actual_asset_type` / `asset_type_mismatch` (via `detect_asset_type`, `genie_client.py:559`) as derived annotations on BAD rows → feed Lever 5 routing / example-SQL guidance. (Repo already maps `asset_routing_error → LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE` at `genie_eval_taxonomy.py:136`.)
- [ ] Rework acceptance logic (`acceptance_policy`, `all_thresholds_met`) to gate on API accuracy delta — no per-judge thresholds remain.

### Phase 4 — Delta-only config/version tracking (D3)
- [ ] Add per-iteration full config to Delta: a `config_json` column on `genie_opt_iterations` (or a new `genie_opt_configs(run_id, iteration, config_json)` table). Delta already holds accuracy/scores, run-start `config_snapshot`, and the full patch+provenance trail; CDF gives versioned history.
- [ ] Champion = best `genie_opt_iterations` row (selection is already Delta-driven); mark it in Delta — no UC model registration.
- [ ] Rollback stays Delta-based (in-memory `pre_snapshot` for rejected iterations; `genie_opt_runs.config_snapshot` for discard).

### Phase 5 — Decommission MLflow + Prompt Registry + human-review (D3, D6, D7)
- [ ] Remove MLflow config/version logging: `create_genie_model_version`, `link_eval_scores_to_model`, per-mutation runs, and the dead `rollback_to_model` (`models.py`).
- [ ] Remove the UC Model Registry path (`register_uc_model`, `_GenieConfigSnapshot`, `ENABLE_UC_MODEL_REGISTRATION`) and the MLflow-based `run_cross_env_deploy`. Cross-env deploy is OUT OF SCOPE this PR; future = official DAB `genie_space` resource (`docs.databricks.com/aws/en/dev-tools/bundles/resources#genie_space`).
- [ ] Remove the Review App labeling session (`labeling_session_url` plumbing in `ddl.py`/router/UI); rely on the official `manual_assessment`/`NEEDS_REVIEW`.
- [ ] Drop the Prompt Registry dependency (D6): stop `register_judge_prompts`/registration gating at preflight/startup; keep prompts as `config.py` constants. Optional: best-effort `mlflow.genai` 'Linked Prompts' tagging only (non-blocking).
- [ ] Scrub now-unused MLflow pointers: `genie_opt_runs.best_model_id`/`experiment_*`/`labeling_session_url`, `genie_opt_iterations.mlflow_run_id`/`model_id`, the `experiment_name` job param, and MLflow `ResourceLinks` in the UI — unless optional tracing is retained.

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
- _Plan fully closed — awaiting green light to dispatch the implementer (`claude_code`) + `codex` cross-review._
