# Changelog

All notable changes to the Genie Space Optimizer are documented here.

---

## [Unreleased]

### Added — Connection Pooling, Temporal-Stale Flagging, Resume State, Both-Empty Handling
- **Connection pool sizing** (`config.py`, `genie_client.py`, `harness.py`): new
  `CONNECTION_POOL_SIZE = 20` constant; `configure_connection_pool()` increases
  urllib3 connection pool on `WorkspaceClient` to prevent "Connection pool is full,
  discarding connection" warnings under concurrent evaluation load. Applied
  automatically at `optimize_genie_space()` startup.
- **`fetch_genie_result_df()` retry logic** (`genie_client.py`): now retries up to
  3 times with linear backoff (2s base delay) when the statement is still
  `PENDING`/`RUNNING` or results are transiently unavailable.
- **Temporal-stale benchmark flagging** (`evaluation.py`): new
  `_flag_stale_temporal_benchmarks()` runs GT SQL via Spark and flags benchmarks
  returning 0 rows as `temporal_stale=True`. Flagged benchmarks are excluded from
  the accuracy denominator in `_compute_arbiter_adjusted_accuracy()`.
- **`both_empty` comparison handling** (`evaluation.py`): new comparison result type
  when both GT and Genie SQL return 0 rows. `both_empty` and
  `genie_result_unavailable` error types now excluded from the accuracy denominator
  alongside GT infrastructure failures and quarantined questions.
- **Resume state restoration** (`harness.py`, `state.py`): `_resume_lever_loop()`
  now restores `reflection_buffer`, `tried_patches`, `tried_root_causes`, and
  `skill_exemplars` from Delta `genie_opt_iterations.reflection_json` column. The
  lever loop resumes with full strategic context instead of starting fresh.
- **`reflection_json` parsing** (`state.py`): `load_all_full_iterations()` now
  auto-parses the `reflection_json` JSON column alongside other JSON columns.

### Fixed
- Filtered `None` values from `skill_exemplars` patch_types in `harness.py` and
  `optimizer.py` to prevent `TypeError` in adaptive strategist prompt formatting.

---

### Changed — Labeling Module Simplification, Console Diagnostics
- **Simplified label schema creation** (`labeling.py`): removed the defensive
  `_create_or_reuse_schema()` helper; schemas are now defined in a data-driven list
  and created with `overwrite=True` in a simple loop, eliminating the complex
  existence-checking and error-signal parsing logic.
- **Console-visible diagnostics** (`labeling.py`, `harness.py`): added `print()`
  messages throughout the labeling pipeline — schema creation failures, session
  creation progress, trace search fallbacks, experiment-not-found, and per-batch
  trace counts are now visible in the job notebook console output.
- **Batched trace population** (`labeling.py`): `_populate_session_traces()` now
  calls `session.add_traces()` in two separate batches (priority traces first,
  then backfill) instead of concatenating into a single DataFrame, improving
  memory efficiency and providing per-batch progress logging.
- **Explicit experiment context** (`labeling.py`): `create_review_session()` now
  calls `mlflow.set_experiment()` before session creation to ensure the correct
  experiment context is active.

---

### Added — Per-Question Failure Persistence, Escalation Framework, Pending Reviews
- **Cross-iteration verdict history** (`harness.py`): `_build_verdict_history()` builds
  per-question arbiter verdict history across all full-scope evaluations.
  `_build_question_persistence_summary()` classifies questions as INTERMITTENT, PERSISTENT,
  or ADDITIVE_LEVERS_EXHAUSTED, and renders a strategist-ready summary.
- **Escalation framework** (`harness.py`, `config.py`): the adaptive strategist can now
  output an `escalation` field per action group: `remove_tvf` (auto-assess TVF removal
  confidence), `gt_repair` (LLM-assisted ground-truth correction for `neither_correct`
  patterns), or `flag_for_review` (human review in labeling session). Per-question
  thresholds: `GENIE_CORRECT_CONFIRMATION_THRESHOLD=2`,
  `NEITHER_CORRECT_REPAIR_THRESHOLD=2`, `NEITHER_CORRECT_QUARANTINE_THRESHOLD=3`.
- **Quarantined questions** (`evaluation.py`): `_compute_arbiter_adjusted_accuracy()` now
  accepts `quarantined_qids` — questions excluded from the accuracy denominator after
  repeated `neither_correct` verdicts and failed GT repair.
- **TVF schema overlap analysis** (`uc_metadata.py`): `describe_tvf_output_columns()` and
  `analyze_tvf_table_overlap()` compare TVF output columns with underlying tables to
  produce a coverage analysis and confidence tier for safe TVF removal.
- **Queued patches table** (`state.py`): `genie_opt_queued_patches` Delta table for
  high-risk patches pending human approval; `write_queued_patch()`, `get_queued_patches()`.
- **Flagged questions** (`labeling.py`): `flag_for_human_review()` writes to
  `genie_opt_flagged_questions` Delta table; `get_flagged_questions()` reads pending items.
  Flagged trace IDs are prioritized in labeling session population.
- **Pending reviews API** (`runs.py`, `models.py`): `GET /pending-reviews/{space_id}`
  returns `PendingReviewsOut` with flagged question count, queued patch count, labeling
  session URL, and top items. `PendingReviewItem` model for flagged questions and queued patches.
- **Pending reviews UI** (`$runId.tsx`, `SpaceCard.tsx`, `api.ts`): run detail page shows
  pending review badge with link to labeling session; space card shows pending review count.
- **Reflection entry per-question tracking** (`harness.py`): `_build_reflection_entry()`
  now tracks `affected_question_ids`, `fixed_questions`, `still_failing`, `new_regressions`.
- **Adaptive strategist prompt enhancements** (`config.py`): new `{{ question_persistence_summary }}`
  context block; escalation guidance for ADDITIVE_LEVERS_EXHAUSTED questions;
  optional `"escalation"` field in action group output schema.
- **Config thresholds** (`config.py`): `PERSISTENCE_MIN_FAILURES=2`,
  `TVF_REMOVAL_MIN_ITERATIONS=2`, `TVF_REMOVAL_BLAME_THRESHOLD=2`.
- **New test suites**: `test_arbiter_corrections.py`, `test_persistence_escalation.py`.

---

### Added — Instruction Slot Budget, Labeling Session URLs, Expected SQL on Traces
- **Instruction slot budget** (`genie_schema.py`, `optimizer.py`, `applier.py`):
  `count_instruction_slots()` counts consumed slots (example SQLs + SQL functions +
  text instructions, max 100). Enforced at 3 levels: proposal validation
  (`_validate_lever5_proposals()` caps `add_example_sql` at remaining budget), post-apply
  guard (`apply_patch_set()` trims excess examples), and structural validation
  (`_strict_validate()` rejects configs exceeding 100 slots)
- **Labeling session URLs** (`labeling.py`, `state.py`, `runs.py`): `session_url`
  extracted from `session.url` and persisted to `labeling_session_url` column on
  `genie_opt_runs`. Surfaced as a "Human Review" link in the run detail UI via
  `_build_links()`. Baseline labeling session also captures and persists URL.
- **Expected SQL on traces** (`evaluation.py`, `harness.py`): `log_expectations_on_traces()`
  logs `expected_sql` as MLflow `Expectation` assessments on every evaluation trace,
  making traces self-contained for labeling reviewers
- **Eval-run-based trace population** (`labeling.py`): `create_review_session()` accepts
  `eval_mlflow_run_ids` and searches traces per eval run (`mlflow.search_traces(run_id=...)`),
  avoiding the 200-trace cap and format mismatches from experiment-wide search. Falls back
  to experiment-wide search when no eval run IDs are available.
- **Trace ID diagnostics** (`evaluation.py`): warns when evaluation produces 0 trace IDs;
  logs count of rows with/without trace IDs for debugging
- **Synonym value list support** (`applier.py`): `proposals_to_patches()` now handles
  synonym values as lists (not just comma-separated strings)
- **Job notebook documentation** (`run_lever_loop.py`): extensive documentation update
  covering adaptive loop architecture, 3-gate pattern, reflection buffer, cluster impact
  scoring, instruction slot budget, and MLflow labeling session
- **ProcessFlow UI**: updated pipeline visualization to reflect adaptive loop steps
- **ResourceLinks**: labeling session link with review category

---

### Changed — Adaptive Lever Loop Architecture
- **Adaptive lever loop** (`harness.py`): replaced batch strategist (analyze once → execute
  all AGs) with an iterative adaptive loop: re-cluster from fresh eval → priority-score →
  adaptive strategist (1 AG) → apply → gate → accept/rollback → reflect → repeat. Each
  iteration sees the latest failure state, not a stale snapshot from baseline.
- **Adaptive strategist** (`optimizer.py`, `config.py`): new `ADAPTIVE_STRATEGIST_PROMPT`
  and `_call_llm_for_adaptive_strategy()` — single LLM call per iteration producing exactly
  one action group. Prompt includes priority ranking, reflection history with DO NOT RETRY
  list, schema context, structured metadata, and join specs.
- **Cluster priority scoring** (`optimizer.py`): `cluster_impact()` scores clusters by
  `question_count × causal_weight × severity × fixability`; `rank_clusters()` sorts and
  annotates clusters with `impact_score` and `rank` for the strategist.
- **Reflection buffer** (`harness.py`): `_build_reflection_entry()` records per-iteration
  outcomes (accepted/rolled-back, score deltas, patches applied, new failures). Buffer is
  passed to the adaptive strategist and persisted in `reflection_json` column on
  `genie_opt_iterations`. `format_reflection_buffer()` renders recent entries in full detail
  with older entries compressed to one line.
- **Diminishing returns detection** (`harness.py`, `config.py`):
  `DIMINISHING_RETURNS_EPSILON = 2.0` and `DIMINISHING_RETURNS_LOOKBACK = 2` — stops
  the loop when the last N accepted iterations each improved by < epsilon percent.
- **Tried-patch filtering** (`harness.py`): `_filter_tried_clusters()` removes clusters
  whose `(failure_type, blame_set)` was already attempted and rolled back, preventing
  the strategist from repeating failed approaches.
- **Removed old fallback retry**: the ad-hoc post-loop fallback is replaced by the
  adaptive loop itself, which naturally retries with fresh strategy each iteration.
- **Holistic fallback on iter 1**: if the adaptive strategist returns 0 AGs on the first
  iteration, falls back to `_generate_holistic_strategy()` for a cold-start action group.
- **Delta schema**: `genie_opt_iterations` gains `reflection_json` column for per-iteration
  reflection persistence.
- **New test suite**: `tests/unit/test_adaptive_loop.py` (346 lines) covering reflection
  entry building, diminishing returns detection, cluster filtering, and priority scoring.
- **Preflight return type**: now `tuple[..., list[dict]]` including `human_corrections`.

---

### Added — Instruction Seeding, Confirmation Eval, Fallback Retry, Labeling Schema Reuse
- **Stage 2.95: Proactive instruction seeding** (`harness.py`, `optimizer.py`, `config.py`):
  `_run_proactive_instruction_seeding()` generates conservative routing instructions for
  Genie Spaces with no/insufficient instructions (< 50 chars). Uses new
  `PROACTIVE_INSTRUCTION_PROMPT` covering asset routing, temporal conventions, null handling,
  and common joins. Writes `proactive_instruction_seeding` patch to Delta.
- **Confirmation eval (double-run)** (`harness.py`): full-eval gate now runs two evaluations
  and averages scores to smooth Genie non-determinism, reducing false regressions
- **Fallback conservative retry** (`harness.py`): when all action groups are rolled back,
  re-generates strategy and attempts a single highest-priority lever in isolation
- **Proactive benchmark example SQL application** (`harness.py`): mined example SQLs are
  now applied proactively via the Genie API before the strategy phase, with 0-row result
  validation to skip empty-result queries
- **Labeling schema create-or-reuse** (`labeling.py`): `_create_or_reuse_schema()` checks
  existence before creating; gracefully handles "referenced by labeling sessions" errors
  instead of failing on overwrite conflicts
- **REST-based preflight access validation** (`preflight.py`): `_validate_core_access()`
  now uses `w.tables.get()` instead of Spark `information_schema` queries, avoiding hidden
  `system` catalog dependency; logs Spark runtime identity for debugging
- **Noise floor raised** (`config.py`): `MAX_NOISE_FLOOR` increased from 3.0 to 5.0
- **Slice gate tolerance floor** (`harness.py`): effective slice tolerance now also accounts
  for single-question weight (`100/num_questions + 0.5`) to prevent false drops on small
  benchmark sets
- **Repeatability reference SQL fallback** (`harness.py`): falls back to benchmark
  `expected_sql` when no reference SQLs available from prior iterations
- **Repeatability eval console output** (`evaluation.py`): per-judge scores printed to logs
- **Delta migration fix** (`state.py`): `ALTER TABLE ADD COLUMN` now strips `DEFAULT`
  clauses and applies them separately via `ALTER COLUMN SET DEFAULT` (Delta Lake compat)
- **Grant SQL formatting** (`settings.py`): copyable grant commands now include catalog-level
  vs schema-level SQL comments for clarity
- **CrossRunChart label dedup**: short date labels deduplicated when multiple runs share a day
- **Lever 5 instruction guidance** (`optimizer.py`): uses `instruction_guidance` from lever
  directives instead of deprecated `global_instruction_rewrite`

---

### Changed — Advisor-Only Auth Model, Permission Filtering, Human Feedback Loop
- **Advisor-only settings** (`settings.py`): removed all GRANT/REVOKE execution;
  the app now reads UC & Genie Space permissions via OBO + SP fallback and provides
  copyable SQL commands and sharing instructions instead of mutating state
- **Models cleanup** (`models.py`): removed `DataAccessGrant`, `DataAccessGrantRequest`,
  `DataAccessOverview`, `DetectedSchema`, `MissingGrantDetail`, `SpaceAccessGrantRequest`;
  `SchemaPermission` now carries `readGrantCommand`/`writeGrantCommand`; `SpacePermissions`
  adds `spGrantInstructions`; `PermissionDashboard` adds `workspaceHost`/`jobUrl`
- **Activity permission filtering** (`activity.py`): dashboard results restricted to
  Genie Spaces where the calling user has CAN_MANAGE or CAN_EDIT
- **REST-based ACL helpers** (`genie_client.py`): `get_space_permissions_rest()`,
  `_check_user_edit_from_rest_acl()`, `_check_sp_manage_from_rest_acl()`,
  `_check_user_manage_from_rest_acl()`, `_check_user_edit_from_perms()`; `user_can_edit_space()`
  accepts `cached_perms` to avoid redundant REST calls
- **Extended OBO scopes** (`databricks.yml`): `files.files`, `catalog.catalogs:read`,
  `catalog.schemas:read`, `catalog.tables:read` added for OBO REST API access
- **Write-access validation** (`preflight.py`): `_validate_write_access()` fail-fast check
  when `apply_mode` is `both` or `uc_artifact` — verifies MODIFY on each target schema
- **Human feedback closed loop** (`harness.py`, `preflight.py`, `labeling.py`):
  preflight returns `human_corrections`; lever loop applies `benchmark_correction` entries
  from prior MLflow labeling sessions; labeling ingestion extracts question text for context
- **Baseline labeling session** (`harness.py`): automatically creates a review session
  for baseline eval failures, linking failure trace IDs to the run for human review
- **Gate `failed_eval_result`** (`harness.py`): slice, P0, and full-eval gates now return
  the failed eval result and regressions in their output for downstream diagnostics
- **Job URL resolution** (`job_launcher.py`): `get_job_url()` resolves the persistent
  optimization job URL for display in the permission dashboard
- **ProcessFlow UI overhaul**: added MLflow feature annotations per pipeline step,
  expanded icon set, removed Collapsible dependency in favor of native expand/collapse
- **Settings UI simplified**: stripped grant/revoke mutation buttons; advisor-only read view
  with copyable grant commands
- **Space detail permission loading**: shows "Checking permissions…" during load

---

### Added — Permission Dashboard, Table Enrichment, Example SQL Mining, Noise Floor
- **Permission dashboard** (`settings.py`, `settings.tsx`): overhauled settings page
  with tabbed layout (Data Access, Space Access); read/write privilege probing;
  `PermissionDashboard`, `SchemaPermission`, `SpacePermissions` models; grant/revoke
  space access endpoints; accordion-based schema permission viewer
- **Table description enrichment** (`optimizer.py`, `config.py`): new
  `TABLE_DESCRIPTION_ENRICHMENT_PROMPT` and `_enrich_table_descriptions()` for tables
  with no/insufficient descriptions (< 10 chars); integrated into description enrichment stage
- **Example SQL mining** (`optimizer.py`, `harness.py`): `_mine_benchmark_example_sqls()`
  extracts proven SQL patterns from high-scoring benchmark questions and proposes them
  as `add_example_sql` patches for Genie Space sample queries
- **Noise floor threshold** (`config.py`, `harness.py`): `MAX_NOISE_FLOOR = 3.0` —
  score improvements below this threshold are treated as noise, preventing
  cosmetic-only patches from being accepted
- **LLM timeout management** (`optimizer.py`): `_ws_with_timeout()` creates workspace
  clients with configurable HTTP timeout (600s default) for long-running LLM calls
- **Stable experiment paths** (`preflight.py`): experiments now live under
  `/Shared/genie-space-optimizer/<space_id>/<domain>` — SP can create without OBO
- **Insufficient description detection** (`optimizer.py`): `_is_description_insufficient()`
  replaces `_is_description_blank()` — columns with < 10 char descriptions are now eligible
  for enrichment
- **ProcessFlow UI component**: new pipeline process flow visualization
- **Settings UI overhaul**: tabs, accordion, alert dialogs for confirmation flows
- Expanded tests: +829 optimizer, +28 applier lines

---

### Added — Proactive Join Discovery, Space Metadata Generation, Scorer Accuracy Fixes
- **Stage 2.85: Proactive join discovery** (`harness.py`, `optimizer.py`): parses JOIN
  clauses from successful baseline eval queries (arbiter `both_correct`/`genie_correct`),
  corroborates with UC foreign key constraints and column type metadata, and codifies
  execution-proven joins as Genie Space join specifications; only proposes Tier 1
  (execution-proven) joins
- **UC foreign key extraction** (`uc_metadata.py`): new `get_foreign_keys_for_tables_rest()`
  fetches FK constraints via `w.tables.get()` for join corroboration; FK candidates
  automatically converted to join spec proposals
- **Proactive space metadata** (`optimizer.py`, `config.py`): `SPACE_DESCRIPTION_PROMPT`
  generates structured descriptions for spaces with no description; `SAMPLE_QUESTIONS_PROMPT`
  generates example questions for spaces with no sample questions
- **Asset routing scorer overhaul** (`asset_routing.py`): uses `detect_asset_type()` for
  robust MV/TVF/TABLE classification; result-match and empty-result-match now treated as
  soft preferences instead of hard failures; supports expected_asset auto-detection from SQL
- **FQN-based column lookup** (`optimizer.py`, `uc_metadata.py`): UC metadata now includes
  `catalog_name`/`schema_name` for FQN-based column resolution; eliminates short-name
  collisions in multi-catalog setups
- **Genie schema join spec validation** (`genie_schema.py`): `ensure_join_spec_fields()`
  for validating join specification structure
- Expanded structured metadata tests (+99 lines)

---

### Added — Strategist Architecture, Transparency UI, Description Enrichment
- **Strategist architecture** (`optimizer.py`, `config.py`): new holistic optimization
  strategist that triages all failures into a unified strategy before lever execution;
  `STRATEGIST_PROMPT`, `STRATEGIST_TRIAGE_PROMPT`, `STRATEGIST_DETAIL_PROMPT` generate
  per-lever action plans; `generate_proposals_from_strategy()` converts strategy to
  targeted proposals; `_truncate_to_budget()` manages prompt token limits
- **Stage 2.75: Proactive description enrichment** (`harness.py`, `optimizer.py`):
  new pipeline stage that LLM-generates structured descriptions for columns with no
  description in both Genie Space and Unity Catalog; `DESCRIPTION_ENRICHMENT_PROMPT`
  in config; runs after UC type enrichment and before the strategist
- **Transparency API endpoints** (`runs.py`, `models.py`): `GET /runs/{id}/iterations`
  (per-iteration scores), `GET /runs/{id}/asi` (ASI failure analysis), `GET
  /runs/{id}/provenance` (end-to-end provenance); new Pydantic models `IterationSummary`,
  `AsiResult`, `AsiSummary`, `ProvenanceRecord`, `ProvenanceSummary`
- **Transparency UI components**: `IterationChart` (score-over-iterations line chart),
  `AsiResultsPanel` (failure analysis breakdown), `ProvenancePanel` (judge → cluster →
  patch provenance viewer), `StageTimeline` (pipeline stage visualization),
  `CrossRunChart` (cross-run score comparison); `transparency-api.ts` with React Query
  hooks; integrated into run detail page
- **Extract-over-generate validation** (`config.py`, `benchmarks.py`): column allowlists
  in all LLM prompts; `format_mlflow_template()` for MLflow `{{ variable }}` syntax;
  `PROMPT_TOKEN_BUDGET` for automatic context truncation
- **Shared scorer context** (`scorers/__init__.py`): `build_scorer_context()` builds
  common context block (comparison summary, empty-data notes, column subset notes,
  temporal notes) shared by all SQL-comparison judges; reduces duplication across scorers
- **Plain-text instruction formatting** (`config.py`): Lever 5 holistic prompt and
  instruction section ordering via `INSTRUCTION_SECTION_ORDER`
- **Job notebook training docs**: all 5 task notebooks expanded with DAG placement,
  failure handling, input/output specifications
- Expanded unit tests for optimizer (+191 lines) and structured metadata (+22 lines)
- Added `recharts` dependency for chart components

---

### Added — Dynamic Counterfactual Fixes in Judge Prompts
- **Structured judge responses** (`config.py`): all LLM judge prompts
  (`schema_accuracy`, `logical_accuracy`, `completeness`, `semantic_equivalence`,
  `arbiter`, `response_quality`) now require structured JSON responses with
  `failure_type`, `blame_set`, `counterfactual_fix`, `wrong_clause`, and `rationale`
  fields instead of simple yes/no answers
- **Dynamic counterfactual extraction** (scorers): `completeness`, `logical_accuracy`,
  `result_correctness`, `schema_accuracy`, and `response_quality` scorers now extract
  LLM-provided `counterfactual_fix` from the structured response; falls back to a
  generated fix using `failure_type` and `blame_set` when the LLM omits it
- **Richer ASI metadata**: judge-provided blame sets and counterfactual fixes flow
  directly into ASI metadata, failure clustering, and provenance — eliminating the need
  for separate rationale-pattern extraction for these fields

---

### Added — MLflow Provenance, Labeling Sessions, Trace Tagging, SCD Filters
- **End-to-end provenance table** (`state.py`): new `genie_opt_provenance` Delta table
  linking every patch back to originating judge verdicts, failure clusters, and gate
  outcomes — columns include `signal_type`, `resolution_method`, `gate_result`,
  `gate_regression` for full traceability
- **MLflow Labeling Sessions** (`labeling.py`): new module with custom labeling schemas
  (`judge_verdict_accuracy`, `corrected_expected_sql`, `patch_approval`,
  `improvement_suggestions`) for human-in-the-loop review; auto-creates review
  sessions after lever loop runs; preflight ingests human feedback from prior sessions
  to correct benchmarks and improve subsequent runs
- **MLflow trace tagging** (`evaluation.py`, `harness.py`): all evaluation traces now
  tagged with `genie.optimization_run_id`, `genie.iteration`, `genie.lever`, and
  `genie.eval_scope` for cross-run traceability; experiment-level tags set during
  preflight (`genie.space_id`, `genie.domain`, `genie.pipeline_version`)
- **MLflow Feedback on traces** (`evaluation.py`): `log_asi_feedback_on_traces()` attaches
  ASI root-cause analysis as Feedback; `log_gate_feedback_on_traces()` attaches gate
  pass/fail verdicts with regression details to evaluation traces
- **Model-level metric logging** (`models.py`): `link_eval_scores_to_model()` now logs
  metrics both on the active MLflow Run and directly to the LoggedModel for UI display
- **SCD filter detection** (`optimizer.py`): new `missing_scd_filter` and
  `wrong_filter_condition` failure types; `_classify_sql_diff` detects missing
  `is_current`/`is_active` filters and WHERE condition differences
- **Join type analysis** (`optimizer.py`): new `wrong_join_type` failure type for
  LEFT vs INNER join mismatches; lever mapping sends to Lever 5 for instruction guidance
- **ASI & provenance Delta writes in harness**: lever loop now writes per-iteration
  ASI results and provenance rows to Delta, linking clusters → proposals → gate outcomes
- **Evaluation dataset upsert** (`evaluation.py`): `create_evaluation_dataset()` now uses
  `merge_records` (upsert by question_id) to preserve version history instead of drop/recreate
- **Preflight experiment tags**: `run_preflight()` sets experiment-level tags for
  pipeline version, catalog, schema, space_id, and domain

### Fixed
- **Scope filter fallback**: `run_evaluation()` falls back to full benchmark set when
  scope filtering returns empty results (prevents empty evaluations)
- **Provenance JSON**: patches now carry `provenance_json` column linking to full
  provenance chain from judge verdicts

---

### Added — Holistic Lever 5, Tiered Arbiter Soft Signals, Join Discovery, Deployment Pipeline

- **Holistic Lever 5 instructions** (`config.py`, `optimizer.py`, `applier.py`, `harness.py`):
  Lever 5 now evaluates the entire Genie Space instruction set holistically rather than
  patching individual instructions. A new `LEVER_5_HOLISTIC_PROMPT` (inspired by the
  [AgentSkills.io](https://agentskills.io/specification) specification) generates a
  single cohesive instruction document considering the space's purpose, all benchmark
  evaluation learnings, and prior lever tweaks. New `rewrite_instruction` patch type
  replaces the full instruction body in one operation.
- **Tiered arbiter soft signals** (`harness.py`, `optimizer.py`, `config.py`):
  Questions with `genie_correct` or `both_correct` arbiter verdicts that still have
  individual judge failures are now extracted as "soft signal" rows. These are clustered
  separately (tagged `signal_type: soft`) and passed to Levers 4 and 5 alongside hard
  failure clusters, providing best-practice guidance without triggering aggressive fixes.
- **Lever 4 always-run join discovery** (`harness.py`, `optimizer.py`):
  Lever 4 (Join Specifications) now always runs its discovery path regardless of
  failure cluster count, detecting implicit joins from successful Genie queries and
  proposing explicit join documentation. `_classify_sql_diff` reordered to check
  missing dimension JOINs and wrong join columns before aggregation checks.
- **Robust QID extraction** (`optimizer.py`, `evaluation.py`, `harness.py`):
  New `_row_qid()` helper robustly extracts `question_id` from eval rows, accounting
  for MLflow's `request` column and nested structures. Fixes ASI data merging that was
  silently failing due to mismatched QID keys.
- **Deployment pipeline hardening** (`databricks.yml`, `Makefile`, `job_launcher.py`, `app.py`):
  - `databricks.yml` build step now removes `.build/.gitignore` (which `apx build`
    generates with a blanket `*` pattern that blocks file sync)
  - New `Makefile` with `make deploy` orchestrating four steps: `clean-wheels` →
    `databricks bundle deploy` (builds wheel + syncs to workspace) →
    `databricks apps deploy` (creates snapshot + restarts app) → `verify`
  - `_cleanup_stale_wheels()` in `job_launcher.py` removes old wheels from the
    workspace dist directory after uploading the new one
  - `_WheelHealthCheck` lifespan dependency in `app.py` logs the resolved wheel
    name and size at app startup for immediate deploy verification
  - Enhanced wheel logging throughout `_ensure_artifacts` (hash, size, path)
- **Enhanced soft signal logging** (`harness.py`): soft signal clusters now print
  full per-cluster detail (judge, root cause, ASI type, blame, questions) matching
  the hard cluster format. Per-question failed judge names shown in the Failure
  Analysis section for debugging. New `_get_failed_judges()` helper.
- **Wheel cache busting** (`job_launcher.py`): workspace wheel path now embeds a
  content hash (`{stem}_{hash[:8]}.whl`) to prevent stale wheel caching

### Fixed — Lever Loop Effectiveness

- **Arbiter filter**: now excludes both `genie_correct` AND `both_correct` verdicts
  from failure clustering (previously only excluded `genie_correct`)
- **`_classify_sql_diff` over-classification**: introduced `format_difference` and
  `extra_columns_only` types mapped to lever 0 (no-op) to prevent benign SQL
  differences from triggering unnecessary patches
- **Deduplication**: `_deduplicate_proposals` now checks `update_description` patch type
- **No-op filtering**: `_filter_no_op_proposals` uses 0.97 n-gram similarity threshold
  to drop cosmetic-only proposals while preserving surgical improvements
- **No-op filtering crash** (`optimizer.py`): `_filter_no_op_proposals` crashed with
  `'list' object has no attribute 'strip'` when Genie metadata `description` field
  was a list instead of a string; now coerces to string safely
- **Instruction ID validation** (`applier.py`): generates a valid 32-char hex ID when
  the default `genie_opt` ID fails validation
- **Tied subset matching** (`evaluation.py`): `LIMIT` queries with tied values that
  produce different but semantically equivalent row orderings now score as `tied_subset`
  instead of `mismatch`
- **`mapped_genie_df` UnboundLocalError** (`evaluation.py`): moved assignment before
  exception-prone code path to ensure it's always defined
- **MLflow experiment permissions** (`spaces.py`): Service Principal is now explicitly
  granted `CAN_MANAGE` on the MLflow experiment via the Permissions API

---

### Added — Temporal Date Resolution, Coverage Gap Benchmarks, Spark Resilience
- **Temporal date resolution** (`evaluation.py`): auto-detects relative time
  references ("this year", "last quarter", "YTD", "last N months/days") in
  benchmark questions and rewrites GT SQL date literals to current-date-relative
  values before scoring — eliminates false failures from stale benchmark dates
- **Benchmark coverage gap generation** (`evaluation.py`, `config.py`): new
  `BENCHMARK_COVERAGE_GAP_PROMPT` and `COVERAGE_GAP_SOFT_CAP_FACTOR` to generate
  targeted benchmarks for uncovered assets (tables, MVs, TVFs with zero questions)
- **Spark session resilience** (`_spark.py`): session factory now detects
  `InvalidAccessKeyId`, `ExpiredToken`, `AccessDenied` credential errors and
  auto-recreates the session; `run_with_retry()` wrapper retries Spark operations
  with session recreation on credential failures
- **Delta table resilience** (`delta_helpers.py`): `read_table` now issues
  `REFRESH TABLE` before reads and retries on `DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`
  to handle tables dropped/recreated by upstream tasks
- **Job launcher improvements** (`job_launcher.py`): picks globally-newest wheel
  across all search directories; caches wheel content hash for upload dedup
- **Arbiter scorer overhaul** (`arbiter.py`): result-match now returns
  `both_correct` (was `skipped`); temporal context injected into LLM prompts;
  slim comparison payloads to stay within MLflow trace size limits
- **Scorer temporal awareness**: all LLM scorers (`completeness`, `logical_accuracy`,
  `result_correctness`, `schema_accuracy`, `semantic_equivalence`) now receive
  temporal rewrite context so judges account for date drift
- Expanded test coverage: +481 lines for evaluation (temporal rewriting, coverage
  gap, benchmark generation), +61 lines for optimizer

---

### Added — Lever 5 Quality, Result Matching, Anti-Hallucination
- **Lever 5 prompt improvements** (`config.py`): routing failures now force
  `example_sql` instruction type instead of falling back to text instructions;
  added structured markdown formatting rules, anti-hallucination guard that
  returns empty proposals when no actionable fix is identified, and explicit
  requirement that all instructions reference specific Genie Space assets
- **Description cleanup** (`optimizer.py`): `_detect_instruction_content_in_description()`
  detects when the Genie Space description field contains LLM-facing routing rules
  or SQL patterns and proposes stripping them to user-facing summary only
- **Cluster deduplication** (`optimizer.py`): improved failure clustering with
  downgrade tracking when LLM returns text_instruction for routing failures
- **Fuzzy result matching** (`evaluation.py`): `normalize_result_df` and
  `result_signature` now round to 4 decimals (was 6) to handle Spark vs REST API
  float precision differences; new subset/superset matching and column alias
  detection for result correctness scoring
- **Scorer enhancements**: `completeness`, `logical_accuracy`, `result_correctness`,
  `schema_accuracy`, and `semantic_equivalence` scorers enhanced with additional
  context and edge-case handling
- Expanded unit tests for optimizer (cluster dedup, description cleanup) and applier

---

## [Previous Unreleased]

### Added — Trigger API, Response Quality Scorer, Schema Validation v2
- **Programmatic trigger API** (`trigger.py`): new `POST /trigger` and
  `GET /trigger/status/{run_id}` endpoints for headless optimization — enables
  CI/CD and scheduled workflows without the UI
- **Response quality scorer** (`response_quality.py`): 9th judge that evaluates
  whether Genie's natural language analysis accurately describes the SQL query
  and answers the user's question; returns `unknown` when NL response is absent
- **Genie schema validation v2** (`genie_schema.py`): dual-mode validation —
  lenient (structural/type checks) and strict (32-char hex IDs, sort order,
  uniqueness, size limits per Databricks API spec); `generate_genie_id()` helper
  for time-ordered UUID generation
- **Robust LLM JSON parsing** (`_extract_json` in `evaluation.py`): handles
  markdown fences, prose-wrapped JSON, trailing data, and nested objects
- **Genie API rate limiting**: `genie_client.py` now retries on `ResourceExhausted`
  (HTTP 429) and `TimeoutError` with exponential backoff; configurable via
  `GENIE_RATE_LIMIT_BASE_DELAY` and `GENIE_RATE_LIMIT_RETRIES`
- UC metadata: additional REST API helpers for tags retrieval
- Expanded unit tests: schema validation (+686 lines), evaluation, scorers, optimizer

### Changed
- **Job definition moved inline**: `resources/genie_optimization_job.yml` removed;
  job is now defined directly in `databricks.yml`
- Scorer assembly bumped from 8 → 9 judges (response quality added between
  completeness and asset routing)
- `applier.py` refactored with improved patch sequencing and validation
- `benchmarks.py` enhanced benchmark generation with better question diversity
- `optimizer.py` expanded failure analysis and proposal generation
- Frontend: space detail page updated for trigger API integration

### Removed
- `resources/genie_optimization_job.yml` (migrated into `databricks.yml`)

---

## 2026-02-23

### Added — UC REST API, Scorer Hardening, Harness Improvements (`74f1eb4`)
- **UC metadata REST API** (`uc_metadata.py`): preflight now fetches columns and routines
  via `WorkspaceClient` REST API by default, falling back to Spark SQL only when needed —
  eliminates `system.information_schema` permission issues
- **Arbiter baseline extraction**: harness extracts `genie_correct` arbiter actions from
  baseline iteration to seed lever loop with known-good signals
- **Entity matching propagation wait**: new `PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS`
  config (default 90s) for longer waits after `build_value_dictionary` patches
- **Evaluation debug mode**: `GENIE_SPACE_OPTIMIZER_EVAL_DEBUG` env var enables verbose
  evaluation logging; assessment extraction now handles both trace-based and top-level
  `assessments` column formats (MLflow genai >=2.x compatibility)
- `fetch_genie_result_df` added to `genie_client.py` for direct result DataFrame retrieval
- Prompt matching diagnostic logging in harness with change summaries
- Tuned convergence: `REGRESSION_THRESHOLD` 2.0 → 10.0, `SLICE_GATE_TOLERANCE` 5.0 → 15.0
- Scorers hardened with explicit `LLM_ENDPOINT` config and structured logging

### Added — Documentation Update (`d770458`)
- Updated `README.md` with settings API endpoints, new modules, and quick links
- Updated `QUICKSTART.md` with actual repo clone URL and data access setup
- Created `CHANGELOG.md` with comprehensive history

### Added — Optimizer & Applier Refinements (`fd42c53`)
- Expanded optimizer with improved lever handling and proposal generation
- Enhanced applier with better rollback and state transitions
- Updated preflight, harness, and evaluation state management
- Expanded unit tests for optimizer, applier, config, and rollback scenarios
- Expanded integration tests for lever loop, resume, and rollback flows
- Added `docs/` directory with Genie Space config reference samples

### Added — Evaluation Engine & Repeatability Scorer (`947afd4`)
- Major evaluation engine expansion with improved judge logic and multi-dimension scoring
- Added `repeatability` scorer to detect variance across repeated evaluation runs
- Updated scorer registry to include repeatability in the scoring pipeline
- Refined settings, runs, spaces, and activity route handlers
- Enhanced optimization harness and preflight with better error handling
- Added `backend/constants.py` and `backend/utils.py` helper modules
- Added `scripts/` directory with browser testing tooling

### Added — Settings Routes & Code Review (`95de94e`)
- New **Settings** backend route (`/settings/data-access`) with GET, POST, DELETE endpoints for managing UC data-access grants
- New **Settings** frontend page (`/settings`) for data access management UI
- Enhanced evaluation engine with improved scoring and error handling
- Updated job launcher, harness, and preflight modules
- Added `CODE_REVIEW.md` documentation

### Added — Optimization Harness Updates (`45cdbe2`)
- Refined harness orchestration and error handling
- Updated optimization models with improved typing
- Enhanced optimizer with additional lever support
- Updated `pyproject.toml` dependencies

### Added — End-to-End Pipeline (`61456fc`)
- Full runs and spaces API routes with job launcher integration
- Expanded evaluation engine with comprehensive scorer improvements
- Preflight checks, harness orchestration, and lever loop job logic
- Enhanced frontend with richer run detail, space detail, and pipeline views
- `E2E_TESTING_GUIDE.md` and `QUICKSTART.md` documentation
- UC permissions grant script (`resources/grant_app_uc_permissions.py`)
- `ResourceLinks` UI component for workspace resource links

### Initial Release (`2c83b87`)
- Full-stack Databricks App scaffolding (React + FastAPI via apx)
- 6 optimization levers: tables & columns, metric views, TVFs, joins, column discovery, instructions
- 7 quality dimensions with 8 LLM judge scorers + arbiter
- Delta-backed state machine with 5 state tables
- Multi-task Databricks Job pipeline (preflight → baseline → lever loop → finalize → deploy)
- React dashboard with space listing, run monitoring, pipeline visualization, and config diff comparison
- Genie Space API client, Unity Catalog metadata introspection, and Delta helpers
- MLflow experiment tracking integration
- Databricks Asset Bundle (`databricks.yml`) for one-command deployment
