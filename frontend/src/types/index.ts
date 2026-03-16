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

export type MaturityLevel = "Trusted" | "Calibrated" | "Configured" | "Connected"

export interface ScoreBreakdown {
  connected: number    // 0-20
  configured: number   // 0-20
  calibrated: number   // 0-20
  trusted: number      // 0-20
  optimized: number    // 0-20
}

export interface CheckDetail {
  label: string
  points: number
  max_points: number
  passed: boolean
}

export interface ScanResult {
  space_id: string
  score: number
  maturity: string
  breakdown: ScoreBreakdown
  checks: Record<string, CheckDetail[]>
  findings: string[]
  next_steps: string[]
  scanned_at: string
}

export interface SpaceListItem {
  space_id: string
  display_name: string
  score: number | null
  maturity: string | null
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
}
