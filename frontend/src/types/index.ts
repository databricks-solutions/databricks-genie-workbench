/**
 * TypeScript types matching the Python Pydantic models in backend/models.py
 */

// SQL execution types (used by auto-optimize QuestionDetail)
export interface SqlExecutionColumn {
  name: string
  type_name: string
}

export interface SqlExecutionResult {
  columns: SqlExecutionColumn[]
  data: (string | number | boolean | null)[][]
  row_count: number
  truncated: boolean
  error: string | null
}

// Settings types
export interface AppSettings {
  genie_space_id: string | null
  llm_model: string
  sql_warehouse_id: string | null
  databricks_host: string | null
  workspace_directory: string | null
}

export interface LLMModelInfo {
  name: string
  displayName: string
  isDefault: boolean
  optimizerPromptBudgetChars?: number | null
  contextTier?: "standard" | "long" | null
}

// Space fetch/detail response types
export interface FetchSpaceResponse {
  genie_space_id: string
  space_data: Record<string, unknown>
}

export interface SpaceDetailResponse {
  space: Record<string, unknown>
  scan_result: Omit<ScanResult, "space_id" | "scanned_at"> & { scanned_at?: string } | null
  is_starred: boolean
}

// ===== GenieIQ / Workbench Types =====

export type MaturityLevel = "Trusted" | "Ready to Optimize" | "Not Ready"

export interface CheckDetail {
  label: string
  passed: boolean
  detail?: string | null
  severity?: string | null  // "pass" | "warning" | "fail"
}

export interface ScanResult {
  space_id: string
  score: number
  total: number
  maturity: string
  optimization_accuracy: number | null  // 0.0-1.0, null if never optimized
  checks: CheckDetail[]
  findings: string[]
  next_steps: string[]
  warnings: string[]             // Advisory findings from warning-severity checks
  warning_next_steps: string[]   // Paired with warnings
  scanned_at: string
}

export interface SpaceListItem {
  space_id: string
  display_name: string
  score: number | null
  maturity: string | null
  optimization_accuracy: number | null  // 0.0-1.0, null if never optimized
  is_starred: boolean
  last_scanned: string | null
  space_url: string | null
}

export interface StarToggleRequest {
  starred: boolean
}

export interface AdminDashboardStats {
  total_spaces: number
  scanned_spaces: number
  avg_score: number
  critical_count: number
  maturity_distribution: Record<string, number>
}

export interface LeaderboardEntry {
  space_id: string
  display_name: string
  score: number
  maturity: string
  last_scanned: string | null
}

export interface AlertItem {
  space_id: string
  display_name: string
  score: number
  top_finding: string | null
}

export interface ScoreHistoryPoint {
  score: number
  maturity: string
  optimization_accuracy: number | null
  scanned_at: string
}

export interface OptimizationEvent {
  run_id: string
  status: string
  started_at: string | null
  completed_at: string | null
  best_accuracy: number | null
  convergence_reason: string | null
  triggered_by: string | null
}

export interface SpaceHistory {
  scans: ScoreHistoryPoint[]
  optimization_events: OptimizationEvent[]
}

export interface CurrentUser {
  email: string
  is_admin: boolean
  groups: string[]
  auth_source: string
}

// Benchmark question from Genie Agent JSON
export interface BenchmarkQuestion {
  id: string
  question: string[]
  answer?: {
    format: string
    content: string[]
  }[]
}

// ===== Create Wizard Types =====

export interface UcCatalog {
  name: string
  comment?: string
  is_home?: boolean
}

export interface UcSchema {
  name: string
  catalog_name: string
  comment?: string
}

export interface UcTable {
  name: string
  full_name: string
  catalog_name: string
  schema_name: string
  comment?: string
  table_type?: string
}

export interface ValidateConfigResponse {
  valid: boolean
  errors: string[]
  warnings: string[]
}

export interface CreateWizardSpaceResponse {
  space_id: string
  display_name: string
  space_url: string
}

// ===== Create Agent Chat Types =====

export interface AgentUIElement {
  type: "single_select" | "multi_select" | "config_preview"
  id: string
  label?: string
  options?: { value: string; label: string; description?: string }[]
  config?: Record<string, unknown>
}

export type AgentEventType =
  | "session"
  | "step"
  | "thinking"
  | "tool_call"
  | "tool_result"
  | "message_delta"
  | "message"
  | "created"
  | "updated"
  | "error"
  | "done"

export interface AgentStep {
  step: string
  label: string
  index: number
  total: number
}

export interface AgentThinking {
  message: string
  step: string
  round: number
}

export interface AgentSSEEvent {
  event: AgentEventType
  data: Record<string, unknown>
}

export interface AgentChatMessage {
  id: string
  role: "user" | "assistant" | "tool"
  content: string
  timestamp: number
  ui_elements?: AgentUIElement[] | null
  tool_name?: string
  tool_args?: Record<string, unknown>
  tool_result?: Record<string, unknown>
  is_thinking?: boolean
  is_error?: boolean
  created_space?: { space_id: string; url: string; display_name: string }
  updated_space?: { space_id: string; url: string }
}

// ============================================================================
// Auto-Optimize (GSO) Types
// ============================================================================

// GSO v2 4-task DAG — mirrors backend `_STEP_DEFINITIONS`.
// Baseline evaluation and bounded patch attempts run inside "Optimize".
export const GSO_PIPELINE_STEPS = [
  { stepNumber: 1, name: "Intake & Snapshot" },
  { stepNumber: 2, name: "Benchmark QC & Repair" },
  { stepNumber: 3, name: "Optimize" },
  { stepNumber: 4, name: "Publish & Audit" },
] as const
export const GSO_TOTAL_STEPS = GSO_PIPELINE_STEPS.length // 4
export type GSOPipelineStepName = (typeof GSO_PIPELINE_STEPS)[number]["name"]

// GSO v2 — typed loop terminal reason (arch §5.1 / §7.4), the closed set the
// 03_optimize controller stamps. Mirrors backend `_TYPED_TERMINAL_REASONS`.
// EVAL_BUDGET_EXHAUSTED is emitted by Phase 8's eval-budget cap. Legacy
// free-text reasons / in-progress runs surface as null.
export type GSOTerminalReason =
  | "TARGET_REACHED"
  | "MAX_ATTEMPTS"
  | "NO_NEW_HYPOTHESIS"
  | "EVAL_INVALID"
  | "CONFIG_VALIDATION_FAILED"
  | "LOOP_STATE_INVALID"
  | "EVAL_BUDGET_EXHAUSTED"
  | "INSUFFICIENT_VALID_BENCHMARKS"

// GSO v2 — per-attempt mode. Current optimize attempts use "llm_patch";
// legacy runs may still send "coverage" / "surgical", so wire fields accept
// string fallbacks below. Per-attempt aggregate decision (accept/reject/
// continue) comes from the controller.
export type GSOAttemptMode = "llm_patch"
export type GSODecision = "accept" | "reject" | "continue"

// apply_mode supports three values: "genie_config" (default),
// "uc_artifact" (UC-level changes only), "both" (config + UC).
// The UI currently only exposes "genie_config" and disables "both".
export interface GSOTriggerRequest {
  space_id: string
  apply_mode?: "genie_config" | "uc_artifact" | "both"
  levers?: number[]
  llm_model?: string | null
  // GSO v2 loop knobs (optional; omit ⇒ job defaults 0.90 / 3). target_accuracy
  // is the 0–1 stop-early target; max_attempts bounds the patch/eval loop.
  // The run stops at whichever comes first.
  target_accuracy?: number | null
  max_attempts?: number | null
  workload_warehouse_ids?: string[]
  benchmark_policy: "review_only" | "repair_allowed"
  // Metric view advisor knobs (Prompt 11, MV-D1). Mirror of the `mv_*` fields on
  // `TriggerRequest` in backend/routers/auto_optimize.py. All optional; the run
  // config only sends them when the "Suggest metric views" toggle is on, and
  // `buildOptimizationTriggerRequest` clears every one when it is off.
  // `mv_materialize` is plumbed but has no UI control yet — the materialization
  // path is unbuilt (mv-advisor-gap-report.md:1526); a later prompt adds the
  // control and nothing else here changes.
  enable_metric_view_suggestions?: boolean
  mv_action_mode?: "suggest_only" | "create_and_attach"
  mv_min_confidence?: number | null
  mv_approved_suggestion_ids?: string[]
  mv_consent?: MvConsentPayload | null
  mv_materialize?: boolean
}

export interface GSOTriggerResponse {
  runId: string
  jobRunId: string
  jobUrl: string | null
  status: string
  // Resolved loop knobs the run will use (request value or job default).
  targetAccuracy?: number | null
  maxAttempts?: number | null
  benchmarkPolicy?: "review_only" | "repair_allowed"
}

export interface GSOLeverInfo {
  id: number
  name: string
  description: string
}

export interface GSORunStatus {
  runId: string
  status: string
  spaceId: string
  startedAt: string | null
  completedAt: string | null
  baselineScore: number | null
  // Canonical official full-corpus benchmark accuracy headline. The backend guarantees
  // ``optimizedScore >= baselineScore`` (regressions are clamped to baseline,
  // since regressions don't get posted) and ``optimizedScore`` is null while
  // no full-scope iteration > 0 has been evaluated yet.
  optimizedScore: number | null
  // ``0`` can mean the baseline was retained, optimization is still running,
  // or proactive enrichment / recovery produced the winning config without a
  // later lever iteration. ``N > 0`` is the lever iteration that achieved
  // ``optimizedScore``. ``null`` if there's no baseline at all yet.
  bestIteration: number | null
  convergenceReason: string | null
  // GSO v2 — typed loop terminal reason (null for legacy free-text reasons /
  // in-progress runs); supersedes parsing convergenceReason on the client. The
  // round-tripped loop knobs (0–1 target; surgical max_attempts) echo what the
  // run is using — null until the loop commits an attempt / for legacy runs.
  terminalReason?: GSOTerminalReason | null
  targetAccuracy?: number | null
  maxAttempts?: number | null
  stepsCompleted?: number | null
  totalSteps?: number | null
  currentStepName?: string | null
}

export interface GSORunSummary {
  run_id: string
  space_id: string
  status: string
  started_at: string
  completed_at: string | null
  best_accuracy: number | null
  best_iteration: number | null
  convergence_reason: string | null
  // GSO v2 — typed loop terminal reason derived from convergence_reason.
  terminal_reason?: GSOTerminalReason | null
  triggered_by: string | null
  llm_model?: string | null
  benchmark_policy?: "review_only" | "repair_allowed" | string | null
  benchmark_mutation_count?: number | null
  /** True when the run has a captured config snapshot (baseline) to revert to.
   * Absent on older backends — treat undefined as "unknown / optimistic" so
   * the Revert button still renders (the backend will 409 if truly missing). */
  has_config_snapshot?: boolean
}

// ── Current version (fingerprint match of live config vs known run versions) ─
// Mirrors `CurrentVersionResponse` in backend/models.py — keep in sync.

export type CurrentVersionStatus =
  | "matched"
  | "mixed"
  | "drifted"
  | "history_incomplete"
  | "no_known_versions"
  | "unavailable"
  | "optimization_in_progress"

export interface VersionMatch {
  run_id: string
  target: "baseline" | "champion"
  started_at?: string | null
  best_accuracy?: number | null
}

export interface CurrentVersionResponse {
  status: CurrentVersionStatus
  /** Most recent version matching both config and benchmarks. */
  current?: VersionMatch | null
  /** Equivalent versions matching both config and benchmarks. */
  also_matches?: VersionMatch[]
  config_match?: VersionMatch | null
  config_also_matches?: VersionMatch[]
  benchmark_match?: VersionMatch | null
  benchmark_also_matches?: VersionMatch[]
  drifted_dimensions?: Array<"config" | "benchmarks">
  /** Live space update_time when known — used by the drift banner. */
  live_update_time?: string | null
}

export type GSORevertConfigTarget = "champion" | "baseline"
export type GSORevertBenchmarkTarget = "current" | "champion" | "baseline"

export interface GSORevertBenchmarkDiff {
  currentCount: number
  targetCount: number
  willAdd: number
  willRemove: number
  willChange: number
}

export interface GSORevertOptions {
  runId: string
  spaceId: string
  championAvailable: boolean
  baselineAvailable: boolean
  benchmarkChampionAvailable: boolean
  benchmarkBaselineAvailable: boolean
  benchmarkDiffs: {
    champion: GSORevertBenchmarkDiff
    baseline: GSORevertBenchmarkDiff
  }
}

export interface GSOPipelineStep {
  stepNumber: number
  name: string
  status: string
  durationSeconds: number | null
  summary: string | null
  inputs: Record<string, unknown> | null
  outputs: Record<string, unknown> | null
}

export interface GSOStageEvent {
  stage: string
  status: string
  durationSeconds: number | null
  startedAt: string | null
  completedAt: string | null
  summary: string | null
}

export interface GSOResourceLink {
  label: string
  url: string
  category: string
}

export interface GSOPatch {
  iteration: number
  lever: number | null
  patch_type: string
  target_object: string
  scope: string
  risk_level: string
  status: string
  command: string | null
}

export interface GSOPatchDetail {
  patchType: string
  scope: string
  riskLevel: string
  targetObject: string | null
  rolledBack: boolean
  rollbackReason: string | null
  command: Record<string, unknown> | string | null
  patch: Record<string, unknown> | string | null
  appliedAt: string | null
}

export interface GSOLeverIteration {
  iteration: number
  status: string
  patchCount: number
  patchTypes: string[]
  scoreBefore: number | null
  scoreAfter: number | null
  scoreDelta: number | null
  rollbackReason: string | null
  patches: GSOPatchDetail[]
}

export interface GSOLeverStatus {
  lever: number
  name: string
  status: string
  patchCount: number
  scoreBefore: number | null
  scoreAfter: number | null
  scoreDelta: number | null
  rollbackReason: string | null
  patches: GSOPatchDetail[]
  iterations: GSOLeverIteration[]
}

export interface GSOPipelineRun {
  runId: string
  spaceId: string
  spaceName?: string
  status: string
  startedAt: string
  completedAt: string | null
  initiatedBy?: string
  baselineScore: number | null
  optimizedScore: number | null
  baselineIteration: number | null
  bestIteration: number | null
  steps: GSOPipelineStep[]
  stages: GSOStageEvent[]
  levers: GSOLeverStatus[]
  links: GSOResourceLink[]
  convergenceReason: string | null
  // GSO v2 — typed loop terminal reason + round-tripped loop knobs.
  terminalReason?: GSOTerminalReason | null
  targetAccuracy?: number | null
  maxAttempts?: number | null
}

export interface GSOIterationResult {
  iteration: number
  lever: number | null
  eval_scope: string
  // GSO v2 Phase 6 — headline accuracy stays num_correct / num_questions.
  overall_accuracy: number
  // Bug #2 denominator contract. evaluated_count is the denominator of
  // overall_accuracy; total_questions is the pre-exclusion count kept for
  // back-compat. excluded_count reflects runtime exclusions.
  total_questions: number
  evaluated_count?: number | null
  correct_count: number
  excluded_count?: number | null
  rolled_back?: boolean | null
  // GSO v2 Phase 6 — official assessment counts (replace per-judge scores_json).
  // num_correct / num_questions mirror correct_count / total_questions;
  // num_done is the count actually evaluated; num_needs_review counts rows
  // whose native assessment is NEEDS_REVIEW (null on legacy/pre-Phase-6 rows).
  num_correct?: number
  num_questions?: number
  num_done?: number | null
  num_needs_review?: number | null
  // GSO v2 Phase 6 — replaces the retired per-judge `thresholds_met`. Phase 3
  // collapsed acceptance to a single API-accuracy gate; eval_gate_status is
  // the coarse per-iteration outcome ("passed" | "failed" | "rolled_back").
  api_accuracy_gate_met?: boolean
  eval_gate_status?: "passed" | "failed" | "rolled_back" | string
  // GSO v2 Phase 6 — native Genie benchmark eval-run metadata (null on the
  // legacy in-process path).
  eval_run_id?: string | null
  eval_run_status?: string | null
  reflection_json?: string | Record<string, unknown> | null
  // GSO v2 (item 5 + Attempt Ledger) — the EXPLICIT champion flag + loop-state
  // fields, merged from genie_opt_iterations. The UI reads is_champion as
  // authoritative instead of re-deriving idxmax(accuracy). All optional: legacy
  // runs / pre-migration tables omit them. config_json is the FULL effective
  // Genie Agent config for this iteration (raw JSON string).
  is_champion?: boolean
  config_json?: string | null
  attempt_no?: number | null
  attempt_mode?: GSOAttemptMode | string | null
  decision?: GSODecision | string | null
  decision_reason?: string | null
}

// GSO v2 (arch §7.4) — one row per 03_optimize patch attempt. Backed by
// genie_opt_iterations loop-state columns via /runs/{id}/loop-state.
// accuracy/bestAccuracy are 0–100 (as elsewhere in the app). decisionReason
// carries the rejection/rollback explanation so a higher-accuracy-but-rolled-back
// attempt is explained, not hidden.
export interface GSOAttempt {
  attemptNo: number | null
  attemptMode: GSOAttemptMode | string | null
  iteration: number | null
  evalScope: string | null
  lever: number | null
  accuracy: number | null
  bestAccuracy: number | null
  decision: GSODecision | string | null
  decisionReason: string | null
  rolledBack: boolean
  rollbackReason: string | null
  isChampion: boolean
  currentHypothesis: Record<string, unknown> | string | null
  // Per-attempt ledger fields (B2) — also present as run-level aggregates on
  // GSOLoopState. Optional/nullable; absent on legacy runs.
  bestConfigVersionId?: string | null
  nextHypothesis?: Record<string, unknown> | string | null
  doNotRepeat?: unknown[]
  terminalReason: GSOTerminalReason | null
}

// GSO v2 (arch §7.4) — run-level controller loop-state aggregate. targetAccuracy
// is normalized to the 0–1 request scale (per-attempt accuracies stay 0–100).
export interface GSOLoopState {
  bestAccuracy: number | null
  bestConfigVersionId: string | null
  targetAccuracy: number | null
  maxAttempts: number | null
  surgicalAttemptsUsed: number | null
  terminalReason: GSOTerminalReason | null
  doNotRepeat: unknown[]
  nextHypothesis: Record<string, unknown> | string | null
  attemptCount: number
}

// Response of GET /runs/{id}/loop-state. loopState is null + attempts is empty
// for legacy 6-step runs (no loop-state columns).
export interface GSOLoopStateResponse {
  runId: string
  loopState: GSOLoopState | null
  attempts: GSOAttempt[]
}

// GSO v2 Phase 6 — official per-question assessment. The native Benchmark API
// verdict drives a three-valued display state (GOOD/BAD/NEEDS_REVIEW); a row
// is never collapsed to a plain pass/fail boolean.
export type GSOAssessment = "GOOD" | "BAD" | "NEEDS_REVIEW"

// Lightweight official eval-result row (GSO v2 Phase 6). Replaces the retired
// per-judge ASI shape (judge / value / failure_type / confidence);
// `assessment_reasons` is the `failure_type` successor.
export interface GSOQuestionResult {
  question_id: string
  assessment: GSOAssessment | string | null
  assessment_reasons: string[]
}

// Bug #3 — stable exclusion reason codes mirrored from EXCLUSION_* in
// evaluation.py. The UI must degrade gracefully when it sees a code it
// doesn't recognize (server may add new codes before clients update).
export type GSOExclusionReasonCode =
  | "gt_excluded"
  | "both_empty"
  | "genie_result_unavailable"
  | "quarantined"
  | "temporal_stale"
  | string

export interface GSOQuestionDetail {
  question_id: string
  question: string
  generated_sql: string | null
  expected_sql: string | null
  // GSO v2 Phase 6 — `assessment` is the canonical three-valued state
  // (GOOD/BAD/NEEDS_REVIEW). `passed` is kept as a derived convenience
  // (GOOD=true, BAD=false, NEEDS_REVIEW=null) for the Bug #2 denominator math;
  // UI state must read `assessment` so NEEDS_REVIEW is never mislabeled as a
  // plain fail. `assessment_reasons` replaces the retired `judge_verdicts`.
  passed: boolean | null
  assessment?: GSOAssessment | string | null
  assessment_reasons?: string[]
  match_type: string | null
  excluded?: boolean
  exclusion_reason_code?: GSOExclusionReasonCode | null
  exclusion_reason_detail?: string | null
  genie_sample?: string | null
  gt_sample?: string | null
  genie_columns?: string[]
  gt_columns?: string[]
  genie_rows?: number | null
  gt_rows?: number | null
}

// GSO v2 Phase 6 (§3.5) — benchmark provenance ledger. Each entry records a
// benchmark question GSO added / changed, excluded from one run, or recommended
// for prune. `removed` remains for historical ledger rows only.
// in the user's live Genie Agent, with provenance. Backed by
// genie_opt_benchmark_mutations via /runs/{id}/benchmark-changes.
export type GSOBenchmarkOp = "added" | "excluded" | "removed" | "changed" | "prune_recommended"

export interface GSOBenchmarkQuestionState {
  question?: string | null
  sql?: string | null
}

export interface GSOBenchmarkMutation {
  questionId: string | null
  op: GSOBenchmarkOp | string
  before: GSOBenchmarkQuestionState | null
  after: GSOBenchmarkQuestionState | null
  reason: string | null
  loggedAt: string | null
}

export type GSOBenchmarkQualityCategory =
  | "question_quality"
  | "question_sql_alignment"
  | "sql_validity"
  | "data_validity"
  | "review_system"
  | string

export interface GSOBenchmarkQualityFinding {
  question_id: string
  question: string
  source: string
  category: GSOBenchmarkQualityCategory
  code: string
  severity: "warning" | "error" | string
  confidence: number
  explanation: string
  evidence?: unknown
  before?: GSOBenchmarkQuestionState | null
  proposed_question?: string | null
  proposed_sql?: string | null
}

export interface GSOBenchmarkQualityCounts {
  total: number
  trusted: number
  warnings: number
  excluded: number
  review_not_run: number
}

export interface GSOBenchmarkProposedChange {
  question_id?: string | null
  question?: string | null
  proposed_question?: string | null
  proposed_sql?: string | null
  reason?: string | null
}

// GSO v2 (item 7) — benchmark QC metadata from 01_benchmark_qc_and_repair
// (benchmark_qc artifact): the 30–40 window recommendation, repair-try usage,
// and validity findings. `window` is the raw recommendation payload (status +
// counts). New runs exclude repair-exhausted rows and use
// INSUFFICIENT_VALID_BENCHMARKS only when the remaining valid subset is too
// small. BENCHMARK_UNREPAIRABLE is retained for historical runs.
export interface GSOBenchmarkQC {
  validCount: number | null
  persistedCount: number | null
  repairTriesUsed: number | null
  repairMaxTries: number | null
  repairedIds: string[]
  repairSweeps: unknown
  finalValidity: boolean | null
  window: Record<string, unknown> | null
  windowTargetMin: number | null
  windowTargetMax: number | null
  gtCorrectionCandidates: unknown[]
  terminalReason: string | null
  stillInvalidIds: string[] | null
  repairExhaustedIds?: string[]
  repairExhaustedCount?: number | null
  topUpAttempts?: Array<Record<string, unknown>>
  topUpAttemptsUsed?: number | null
  topUpMaxAttempts?: number | null
  topUpBatchSize?: number | null
  topUpStopReason?: string | null
  topUpRequestedCount?: number | null
  topUpGeneratedCount?: number | null
  topUpAcceptedCount?: number | null
  topUpRejectedCount?: number | null
  topUpDuplicateCount?: number | null
  benchmarkPolicy?: "review_only" | "repair_allowed" | string | null
  benchmarkMutationCount?: number | null
  optimizationEligible?: boolean | null
  minimumValidCount?: number | null
  qualityReviewVersion?: string | null
  qualityReviewStatus?: "complete" | "degraded" | string | null
  semanticReviewCoverage?: number | null
  qualityCounts?: GSOBenchmarkQualityCounts | null
  qualityFindings?: GSOBenchmarkQualityFinding[]
  proposedChanges?: GSOBenchmarkProposedChange[]
}

export interface GSOBenchmarkChanges {
  runId: string
  added: GSOBenchmarkMutation[]
  excluded: GSOBenchmarkMutation[]
  removed: GSOBenchmarkMutation[]
  changed: GSOBenchmarkMutation[]
  pruneRecommended: GSOBenchmarkMutation[]
  items: GSOBenchmarkMutation[]
  counts: {
    added: number
    excluded: number
    removed: number
    changed: number
    pruneRecommended: number
    total: number
  }
  // GSO v2 (item 7) — benchmark QC metadata served alongside the ledger.
  qc?: GSOBenchmarkQC | null
}

// GSO v2 (arch §7.3) — one improvement-trajectory rung in the publish record
// (baseline -> patch attempts). Bounded/structural fields only (§3.6
// firewall): no benchmark Q/A or ground-truth SQL.
export interface GSOPublishTrajectoryEntry {
  iteration: number | null
  attemptNo: number | null
  attemptMode: GSOAttemptMode | string | null
  evalScope: string | null
  accuracy: number | null
  deltaVsBaseline: number | null
  bestAccuracy: number | null
  decision: GSODecision | string | null
  rolledBack: boolean
  isChampion: boolean
}

// GSO v2 (arch §7.3) — the publish_and_audit record. The LLM audit summary +
// structured trajectory + concerns + champion pointer + the published/outcome
// verdict gated on the typed terminal reason. targetAccuracy is 0–1.
export interface GSOPublishRecord {
  runId: string | null
  spaceId: string | null
  finalStatus: string | null
  terminalReason: GSOTerminalReason | null
  published: boolean
  publishOutcome: string | null
  championIteration: number | null
  championAccuracy: number | null
  championConfigVersionId: string | null
  targetAccuracy: number | null
  maxAttempts: number | null
  auditSummary: string | null
  improvementTrajectory: GSOPublishTrajectoryEntry[]
  concerns: string[]
}

// Response of GET /runs/{id}/publish. publishRecord is null before the run
// reaches publish, or for legacy runs predating the artifact.
export interface GSOPublishRecordResponse {
  runId: string
  publishRecord: GSOPublishRecord | null
}

// Bug #3 — pre-evaluation quarantine entry (SQL failed EXPLAIN, missing
// permissions, missing ground truth, etc.). Rendered in the Excluded
// section of the iteration drill-down alongside runtime exclusions.
export interface GSOQuarantinedBenchmark {
  question_id: string
  question: string
  reason_code: GSOExclusionReasonCode
  reason_detail: string
}

export interface GSOSchemaAccessStatus {
  catalog: string
  schema_name: string
  read_granted: boolean
  grant_sql: string | null
}

export interface GSOPermissionCheck {
  sp_display_name: string
  sp_application_id: string
  sp_has_manage: boolean
  schemas: GSOSchemaAccessStatus[]
  can_start: boolean
  errors: string[]
  query_usage_signal?: GSOQueryUsageSignal | null
}

export interface GSOQueryUsageSignal {
  status: "system_table_available" | "warehouse_api_available" | "partially_available" | "unavailable"
  system_table_available: boolean
  warehouse_api_available: boolean
  warehouses: { warehouse_id: string; name: string; accessible: boolean }[]
  inaccessible_warehouses: string[]
  system_grant_sql: string | null
}

// Metric view entitlement probe — POST /api/auto-optimize/mv/probe.
// Mirrors MvProbeResult and friends in backend/models.py; update together.
// Distinct from GSOPermissionCheck above, which probes the service principal:
// this one runs under the signed-in user's OBO token.
export type MvCheckStatus = "GRANTED" | "DENIED" | "UNKNOWN"

export interface MvPrivilegeRow {
  label: string
  privilege: string
  securable: string
  status: MvCheckStatus
  detail: string | null
}

// UNKNOWN is the honest answer on a SQL warehouse, which reports only a DBSQL
// version against a floor stated in DBR. For an optional capability the
// generator withholds the feature; for mv_create_edit it does not block (MV-D13).
// observed_warehouse_id is the compute the row was read on — a row observed
// elsewhere does not carry over.
export interface MvCapabilityRow {
  capability: string
  label: string
  required_dbr: string
  observed_version: string | null
  runtime_kind: "DBR" | "DBSQL" | "UNAVAILABLE"
  observed_warehouse_id: string | null
  status: MvCheckStatus
  optional: boolean
  detail: string | null
}

export interface MvProbeResult {
  probe_id: string
  checked_as: string
  auth_identity: "OBO"
  target: string
  checked_at: string
  results: Record<string, MvCheckStatus>
  privileges: MvPrivilegeRow[]
  capabilities: MvCapabilityRow[]
  verdict: "SUFFICIENT" | "INSUFFICIENT" | "UNKNOWN"
  missing: string[]
  // Copy-ready GRANT text. The app never executes it.
  remediation_sql: string | null
  fallback_mode: "suggest_only"
  materialize_consented: boolean
  consent_recorded: boolean
  errors: string[]
}

// Body for POST /api/auto-optimize/mv/probe. Mirrors MvProbeRequest in
// backend/routers/auto_optimize.py; the backend accepts the wire key `schema`
// (a Pydantic alias for `schema_name`). source_tables are three-part names.
export interface MvProbeRequest {
  catalog: string
  schema: string
  space_id: string
  source_tables: string[]
  materialize_consented?: boolean
}

// ── Metric view proposals / consent (Prompt 11, MV-D1/D23) ──────────────────
// Mirrors MvConsentPayload / MvProposal / MvProposalsResponse /
// MvSpaceProposalsResponse in backend/models.py; update together. The output-
// screen shapes (MvDdlArtifact, MvLiftReport, MvCreatedObject,
// MvCreatedObjectsResponse, decision/drop) are mirrored below now that Prompt 13
// wires the run output/results panels to them.

// The scoped, recorded authorization carried on a create_and_attach run. Built
// from a SUFFICIENT probe: probe_id keys the consent, checked_as/checked_at are
// the audit pair. Re-verified under OBO at trigger time before any write.
export interface MvConsentPayload {
  granted_by: string
  granted_at: string
  probe_id: string
}

// One advisor proposal (a genie_opt_mv_candidates row). JSON columns arrive
// decoded; confidence_score is 0–100; approved_for_rerun gates create_and_attach
// (MV-D1). run_id is presentational only — never a state/fetch key (MV-D23).
// One member measure of a view-grained bundle (MV-D30). A pre-15.3 single-measure
// proposal reads back as a one-element `measures`, so every proposal is a bundle.
export interface MvProposalMeasure {
  display_name: string | null
  expr: string | null
  dedup_fingerprint: string | null
  recurrence: number | null
  provenance_count: number | null
  benchmark_question_ids: string[] | null
}

export interface MvProposal {
  suggestion_id: string
  dedup_fingerprint: string
  target_space_id: string
  run_id: string | null
  candidate_type: string
  confidence_score: number | null
  tier: string | null
  proposed_object: string | null
  measures: MvProposalMeasure[]
  score_components: Record<string, unknown> | null
  evidence: Record<string, unknown> | null
  provenance: Record<string, unknown> | null
  alternatives: unknown[] | null
  conflicts: unknown[] | null
  requested_mode: string | null
  effective_mode: string | null
  decision: string | null
  decided_by: string | null
  decided_at: string | null
  suppressed_until: string | null
  approved_for_rerun: boolean
  created_at: string | null
  updated_at: string | null
}

// GET /runs/{run_id}/mv-proposals — the run's proposals, newest first.
export interface MvProposalsResponse {
  run_id: string
  proposals: MvProposal[]
}

// GET /spaces/{space_id}/mv-proposals — a space's proposals (MV-D23). Same
// MvProposal element type as the run-keyed response so one card renders both.
export interface MvSpaceProposalsResponse {
  space_id: string
  proposals: MvProposal[]
}

// POST /spaces/{space_id}/mv/suggest — an on-demand advice run (MV-D23). The IQ
// Scan surface asks the advisor to score a space now, with no optimization run.
// status is the advisor's COMPLETE | SKIPPED | FAILED; skip_reason distinguishes
// the honest empties (no SQL, no candidates, an already-governed estate) from a
// failure, so the panel renders found vs EMPTY vs denial without inferring
// intent from an empty list. proposals is the SAME MvProposal shape the
// space-scoped and run-keyed lists return.
export interface MvSuggestResponse {
  space_id: string
  run_id: string
  status: string
  skip_reason: string | null
  // Prompt 15.3 governance ladder: disambiguates the two NO_CANDIDATES empties.
  // 0 ⇒ nothing recurring found; > 0 with NO_CANDIDATES ⇒ every recurring
  // measure is already governed (the "you're in good shape" confidence empty).
  measures_found: number | null
  error: string | null
  proposals: MvProposal[]
}

// POST /spaces/{space_id}/mv/register — a bring-your-own view (MV-D24).
// full_name is the three-part UC identifier of a metric view the user created
// themselves. suggestion_id is optional: when the user claims the view
// implements a specific proposal, the backend checks the claim by comparing
// dedup fingerprints rather than trusting it.
export interface MvRegisterRequest {
  full_name: string
  suggestion_id?: string | null
}

// One shape carries both the verified and refused states the panel renders:
// registered is the verdict and reason explains a refusal (not a metric view,
// not visible, failed validation, claim mismatch). On success run_id is the
// sentinel advice run hosting the USER_CREATED ledger row and warnings are
// advisory lints that did not block.
export interface MvRegisterResponse {
  registered: boolean
  full_name: string
  provenance: string
  run_id: string | null
  suggestion_id: string | null
  reason: string | null
  warnings: string[]
}

// ── Metric view output-screen shapes (Prompt 13, MV-D21) ────────────────────
// Mirror MvDdlArtifact / MvLiftReport / MvCreatedObject /
// MvCreatedObjectsResponse / decision + drop req-resp in backend/models.py and
// backend/routers/auto_optimize.py; update together.

// GET /runs/{run_id}/mv-ddl — the rendered DDL artifact plus GRANT remediation.
// yaml_text is the immutable rendered body (MV-D22); ddl is the CREATE VIEW
// wrapper; grant_sql is the copy-ready GRANT SELECT, never auto-applied.
export interface MvDdlArtifact {
  suggestion_id: string | null
  dedup_fingerprint: string | null
  proposed_object: string | null
  join_strategy: string | null
  yaml_text: string | null
  ddl: string | null
  validation: Record<string, unknown> | null
  grant_sql: string | null
}

// Verbatim mirror of the engine's frozen 14-key LiftReport (never reshaped).
// Accuracies/deltas are 0–1 fractions; needs-review is counted separately and
// excluded from both sides of the comparison.
export interface MvLiftReport {
  delta_affected: number
  delta_suite: number
  regressed_question_ids: string[]
  needs_review_count: number
  pre_eval_run_id: string
  post_eval_run_id: string
  question_subset: string[]
  pre_accuracy_affected: number
  post_accuracy_affected: number
  pre_accuracy_suite: number
  post_accuracy_suite: number
  needs_review_question_ids: string[]
  graded_affected_count: number
  graded_suite_count: number
}

// A metric view created under OBO for a run (a genie_opt_mv_created_objects row).
// lift_report is present once the isolated attach eval ran.
export interface MvCreatedObject {
  run_id: string
  suggestion_id: string
  full_name: string
  created_by: string | null
  // MV-D24 create-path discriminator (NULL reads as OBO_CREATED server-side).
  // USER_CREATED = a bring-your-own view the app never drops (frame 8b).
  provenance: "OBO_CREATED" | "USER_CREATED"
  status: "CREATED" | "ATTACHED" | "DETACHED" | "DROPPED"
  attach_patch_id: string | null
  baseline_eval_run_id: string | null
  post_attach_eval_run_id: string | null
  on_regression_action: string | null
  created_at: string | null
  lift_report: MvLiftReport | null
}

// GET /runs/{run_id}/mv-created — the run's created-object ledger. downgrade_reason
// is run-level (why a create_and_attach run was downgraded to suggest_only), and
// is distinct from a per-object DETACHED status (a post-attach regression revert).
export interface MvCreatedObjectsResponse {
  run_id: string
  created: MvCreatedObject[]
  downgrade_reason: string | null
}

// POST /mv/proposals/{suggestion_id}/decision — approve/reject a proposal (MV-D1).
export interface MvProposalDecisionRequest {
  space_id: string
  run_id?: string | null
  decision: "approved" | "rejected"
  suppressed_until?: string | null
}

export interface MvProposalDecisionResponse {
  suggestion_id: string
  decision: "approved" | "rejected"
  approved_for_rerun: boolean
}

// POST /mv/created/{suggestion_id}/drop — explicit OBO drop, DETACHED only (MV-D6).
export interface MvDropRequest {
  run_id: string
  confirm: boolean
}

export interface MvDropResponse {
  suggestion_id: string
  full_name: string
  status: "CREATED" | "ATTACHED" | "DETACHED" | "DROPPED"
  dropped: boolean
}

// ── Semantic model graph (Prompt 12, MV-D23) ────────────────────────────────
// Mirrors MvSemanticGraphNode / MvSemanticGraphEdge / MvSemanticGraph in
// backend/models.py; update together. The base graph is assembled server-side
// from serialized_space + the space-scoped proposals read; the ghosted proposal
// overlay is synthesized client-side from `proposals` (no new payload).

export type MvGovernance = "governed" | "curated" | "ungoverned"

// col: 0 = source/fact tables, 1 = joined dimension tables, 2 = metric views,
// 3 = measure concepts. governance/origin are measure-concept only; proposed is
// set on the client-synthesized ghost MV overlay node. coverage (Prompt 12b
// SQL-coverage lens) is the curated-SQL touch count — 0 is a cold spot, not an
// error; benchmark_question_ids is the evidence-lens question overlay. Both are
// ADDITIVE (optional): a lens-free response omits them and renders unchanged.
export interface SemanticGraphNode {
  id: string
  kind: "table" | "metric_view" | "measure"
  label: string
  col: number
  row: number
  governance?: MvGovernance | null
  origin?: string | null
  proposed?: boolean
  coverage?: number | null
  benchmark_question_ids?: string[] | null
}

// join edges carry the decoded ON predicate, relationship, and SCD2 flag;
// membership ties a measure to its MV; replaces is the client overlay's dashed
// "tables freed" edge. weight (Prompt 12b) is the curated-SQL traversal count on
// join edges — ADDITIVE, absent on a lens-free response.
export interface SemanticGraphEdge {
  from: string
  to: string
  kind: "join" | "membership" | "replaces"
  on?: string | null
  relationship?: string | null
  scd2?: boolean
  weight?: number | null
}

// GET /spaces/{space_id}/semantic-graph — the space's semantic model.
// coverage_status/coverage_reason (Prompt 12b) report the SQL-coverage lens in
// the MV-D15 vocabulary (COMPUTED | EMPTY | UNAVAILABLE) — ADDITIVE, optional.
export interface SemanticGraphResponse {
  space_id: string
  nodes: SemanticGraphNode[]
  edges: SemanticGraphEdge[]
  proposals: MvProposal[]
  coverage_status?: string | null
  coverage_reason?: string | null
}
