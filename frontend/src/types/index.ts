/**
 * TypeScript types matching the Python Pydantic models in agent_server/models.py
 */

// Qualitative assessment category (replaces numeric scores)
export type AssessmentCategory =
  | "good_to_go"
  | "quick_wins"
  | "foundation_needed"

export interface CompensatingStrength {
  covering_section: string
  covered_section: string
  explanation: string
}

export interface SynthesisResult {
  assessment: AssessmentCategory
  assessment_rationale: string
  compensating_strengths: CompensatingStrength[]
  celebration_points: string[]
  top_quick_wins: string[]
}

export interface ChecklistItem {
  id: string
  description: string
  passed: boolean
  details: string | null
}

export interface Finding {
  category: "best_practice" | "warning" | "suggestion"
  severity: "high" | "medium" | "low"
  description: string
  recommendation: string
  reference: string
}

export interface SectionAnalysis {
  section_name: string
  checklist: ChecklistItem[]
  findings: Finding[]
  score: number
  summary: string
}

export interface AgentOutput {
  genie_space_id: string
  analyses: SectionAnalysis[]
  synthesis: SynthesisResult | null
  overall_score: number
  trace_id: string
}

// API request/response types
export interface SectionInfo {
  name: string
  data: Record<string, unknown> | unknown[] | null
  has_data: boolean
}

export interface FetchSpaceResponse {
  genie_space_id: string
  space_data: Record<string, unknown>
  sections: SectionInfo[]
}

export interface AnalyzeSectionRequest {
  section_name: string
  section_data: Record<string, unknown> | unknown[] | null
  full_space: Record<string, unknown>
}

export interface StreamProgress {
  status: "fetching" | "analyzing" | "synthesizing" | "complete" | "result"
  message?: string
  section?: string
  current?: number
  total?: number
  data?: AgentOutput
}

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

export interface LabelingFeedbackItem {
  question_text: string
  is_correct: boolean | null
  feedback_text: string | null
}

export interface OptimizationResponse {
  suggestions: OptimizationSuggestion[]
  summary: string
  trace_id: string
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

// App state types
export type AppMode = "analyze" | "optimize"
export type Phase = "input" | "ingest" | "analysis" | "summary"
export type OptimizeView = "benchmarks" | "labeling" | "feedback" | "optimization" | "preview"

// ===== GenieIQ / Workbench Types =====

export type MaturityLevel = "Optimized" | "Proficient" | "Developing" | "Basic" | "Nascent"

export interface ScoreBreakdown {
  foundation: number    // 0-30
  data_setup: number    // 0-25
  sql_assets: number    // 0-25
  optimization: number  // 0-20
}

export interface ScanResult {
  space_id: string
  score: number
  maturity: MaturityLevel
  breakdown: ScoreBreakdown
  findings: string[]
  next_steps: string[]
  scanned_at: string
}

export interface SpaceListItem {
  space_id: string
  display_name: string
  score: number | null
  maturity: MaturityLevel | null
  is_starred: boolean
  last_scanned: string | null
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
  maturity: MaturityLevel
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
  maturity: MaturityLevel
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

export interface AppState {
  mode: AppMode | null
  phase: Phase
  optimizeView: OptimizeView | null
  genieSpaceId: string
  spaceData: Record<string, unknown> | null
  sections: SectionInfo[]
  currentSectionIndex: number
  sectionAnalyses: SectionAnalysis[]
  allSectionsAnalyzed: boolean
  showChecklist: boolean
  showSettings: boolean
  isLoading: boolean
  error: string | null
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
  created_space?: { space_id: string; url: string; display_name: string }
}
