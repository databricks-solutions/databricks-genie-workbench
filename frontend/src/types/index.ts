/**
 * TypeScript types matching the Python Pydantic models in backend/models.py
 */

export interface GenieQueryResponse {
  sql: string | null
  status: string
  error: string | null
  conversation_id: string
  message_id: string
}

// SQL execution types
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

// Optimization types
export interface OptimizationSuggestion {
  field_path: string
  current_value: unknown
  suggested_value: unknown
  rationale: string
  checklist_reference: string | null
  priority: "high" | "medium" | "low"
  category: string
}

export interface ComparisonDiscrepancy {
  type: string // "column_mismatch", "extra_rows", "missing_rows", "value_diff", "error"
  detail: string
}

export interface ComparisonResult {
  match_type: string // "exact", "value_match", "partial", "row_count_only", "mismatch"
  confidence: number // 0.0 - 1.0
  auto_label: boolean // suggested label
  discrepancies: ComparisonDiscrepancy[]
  summary: string // human-readable explanation
}

export interface LabelingFeedbackItem {
  question_text: string
  is_correct: boolean | null
  feedback_text: string | null
  auto_label?: boolean | null
  user_overrode_auto_label?: boolean
  auto_comparison_summary?: string | null
}

export interface FailureDiagnosis {
  question: string
  failure_types: string[]
  explanation: string
}

export interface OptimizationResponse {
  suggestions: OptimizationSuggestion[]
  summary: string
  trace_id: string
  diagnosis: FailureDiagnosis[]
}

export interface ConfigMergeResponse {
  merged_config: Record<string, unknown>
  summary: string
  trace_id: string
}

// Genie Space creation types
export interface GenieCreateRequest {
  display_name: string
  merged_config: Record<string, unknown>
  parent_path?: string
}

export interface GenieCreateResponse {
  genie_space_id: string
  display_name: string
  space_url: string
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

// App state types
export type OptimizeView = "benchmarks" | "labeling" | "feedback" | "optimization" | "preview"

// ===== GenieIQ / Workbench Types =====

export type MaturityLevel = "Trusted" | "Ready to Optimize" | "Not Ready"

export interface CheckDetail {
  label: string
  passed: boolean
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

export interface FixPatch {
  field_path: string
  old_value: unknown
  new_value: unknown
  rationale: string
}

export interface FixAgentEvent {
  status: "thinking" | "patch" | "applying" | "complete" | "error"
  message?: string
  field_path?: string
  old_value?: unknown
  new_value?: unknown
  rationale?: string
  patches_applied?: number
  summary?: string
  diff?: {
    patches: FixPatch[]
    original_config?: Record<string, unknown>
    updated_config?: Record<string, unknown>
  }
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
  scanned_at: string
}

export interface CurrentUser {
  email: string
  is_admin: boolean
  groups: string[]
  auth_source: string
}

// Benchmark question from Genie Space JSON
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

// apply_mode supports three values: "genie_config" (default),
// "uc_artifact" (UC-level changes only), "both" (config + UC).
// The UI currently only exposes "genie_config" and disables "both".
export interface GSOTriggerRequest {
  space_id: string
  apply_mode?: "genie_config" | "uc_artifact" | "both"
  levers?: number[]
  deploy_target?: string
}

export interface GSOTriggerResponse {
  runId: string
  jobRunId: string
  jobUrl: string | null
  status: string
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
  optimizedScore: number | null
  convergenceReason: string | null
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
  triggered_by: string | null
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
  judgeScores: Record<string, number | null>
  mlflowRunId: string | null
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
  deploymentStatus: string | null
}

export interface GSOIterationResult {
  iteration: number
  lever: number | null
  eval_scope: string
  overall_accuracy: number
  total_questions: number
  correct_count: number
  scores_json: string | Record<string, number>
  thresholds_met: boolean
  reflection_json?: string | Record<string, any> | null
}

export interface GSOSuggestion {
  suggestionId: string
  runId: string
  spaceId: string
  iteration: number | null
  suggestionType: string
  title: string
  rationale: string | null
  definition: string | null
  affectedQuestions: string[]
  estimatedImpact: string | null
  status: string
}

export interface GSOQuestionResult {
  question_id: string
  judge: string
  value: string
  failure_type: string | null
  confidence: number | null
}

export interface GSOQuestionDetail {
  question_id: string
  question: string
  generated_sql: string | null
  expected_sql: string | null
  passed: boolean | null
  match_type: string | null
  judge_verdicts?: Record<string, string>
  excluded?: boolean
  genie_sample?: string | null
  gt_sample?: string | null
  genie_columns?: string[]
  gt_columns?: string[]
  genie_rows?: number | null
  gt_rows?: number | null
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
}
