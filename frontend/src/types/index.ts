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
  | "LOOP_STATE_INVALID"
  | "EVAL_BUDGET_EXHAUSTED"

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
}

export interface GSOTriggerResponse {
  runId: string
  jobRunId: string
  jobUrl: string | null
  status: string
  // Resolved loop knobs the run will use (request value or job default).
  targetAccuracy?: number | null
  maxAttempts?: number | null
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
  /** True when the run has a captured config snapshot (baseline) to revert to.
   * Absent on older backends — treat undefined as "unknown / optimistic" so
   * the Revert button still renders (the backend will 409 if truly missing). */
  has_config_snapshot?: boolean
}

// ── Current version (fingerprint match of live config vs known run versions) ─
// Mirrors `CurrentVersionResponse` in backend/models.py — keep in sync.

export type CurrentVersionStatus =
  | "matched"
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
  /** Most recent matching version when status === "matched". */
  current?: VersionMatch | null
  /** Byte-identical equivalent versions (e.g. run 2's baseline = run 1's champion). */
  also_matches?: VersionMatch[]
  /** Live space update_time when known — used by the drift banner. */
  live_update_time?: string | null
}

export interface GSOPipelineStep {
  stepNumber: number
  name: string
  status: string
  durationSeconds: number | null
  summary: string | null
  inputs: Record<string, any> | null
  outputs: Record<string, any> | null
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
  command: Record<string, any> | string | null
  patch: Record<string, any> | string | null
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
  reflection_json?: string | Record<string, any> | null
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
// benchmark question GSO added / removed / changed (or recommended for prune)
// in the user's live Genie Agent, with provenance. Backed by
// genie_opt_benchmark_mutations via /runs/{id}/benchmark-changes.
export type GSOBenchmarkOp = "added" | "removed" | "changed" | "prune_recommended"

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
// counts). terminalReason is BENCHMARK_UNREPAIRABLE only when the bounded
// repair loop gave up. Null on legacy runs.
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
  removed: GSOBenchmarkMutation[]
  changed: GSOBenchmarkMutation[]
  pruneRecommended: GSOBenchmarkMutation[]
  items: GSOBenchmarkMutation[]
  counts: {
    added: number
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
