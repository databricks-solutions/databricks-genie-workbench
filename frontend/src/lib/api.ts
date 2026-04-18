/**
 * API client for communicating with the Genie Workbench backend.
 */

import type {
  AppSettings,
  FetchSpaceResponse,
  SpaceDetailResponse,
  SpaceListItem,
  ScanResult,
  SpaceHistory,
  AdminDashboardStats,
  LeaderboardEntry,
  AlertItem,
  CurrentUser,
  FixAgentEvent,
  UcCatalog,
  UcSchema,
  UcTable,
  ValidateConfigResponse,
  CreateWizardSpaceResponse,
  GSOTriggerRequest,
  GSOTriggerResponse,
  GSOLeverInfo,
  GSORunStatus,
  GSORunSummary,
  GSOPipelineRun,
  GSOIterationResult,
  GSOQuestionResult,
  GSOQuestionDetail,
  GSOPermissionCheck,
  GSOPatch,
} from "@/types"

const API_BASE = "/api"

// Request timeout values (in milliseconds)
const DEFAULT_TIMEOUT = 30_000 // 30 seconds for most requests
const LONG_TIMEOUT = 300_000 // 5 minutes for LLM operations (optimization can be slow)

class ApiError extends Error {
  status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = "ApiError"
    this.status = status
  }
}

/**
 * Fetch with timeout support.
 */
async function fetchWithTimeout<T>(
  url: string,
  options: RequestInit = {},
  timeout: number = DEFAULT_TIMEOUT
): Promise<T> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    })

    if (!response.ok) {
      const error = await response
        .json()
        .catch(() => ({ detail: response.statusText }))
      throw new ApiError(error.detail || "An error occurred", response.status)
    }

    return response.json()
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new ApiError("Request timed out. Please try again.", 408)
    }
    throw error
  } finally {
    clearTimeout(timeoutId)
  }
}

/**
 * Fetch a Genie Space by ID.
 */
export async function fetchSpace(
  genieSpaceId: string
): Promise<FetchSpaceResponse> {
  return fetchWithTimeout<FetchSpaceResponse>(
    `${API_BASE}/space/fetch`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ genie_space_id: genieSpaceId }),
    },
    DEFAULT_TIMEOUT
  )
}

/**
 * Parse pasted Genie Space JSON.
 */
export async function parseSpaceJson(
  jsonContent: string
): Promise<FetchSpaceResponse> {
  return fetchWithTimeout<FetchSpaceResponse>(
    `${API_BASE}/space/parse`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ json_content: jsonContent }),
    },
    DEFAULT_TIMEOUT
  )
}

/**
 * Get application settings.
 */
export async function getSettings(): Promise<AppSettings> {
  return fetchWithTimeout<AppSettings>(`${API_BASE}/settings`, {}, DEFAULT_TIMEOUT)
}

// ===== GenieIQ / Workbench API =====

export async function listSpaces(params?: {
  search?: string
  starred_only?: boolean
  min_score?: number
  max_score?: number
}): Promise<SpaceListItem[]> {
  const query = new URLSearchParams()
  if (params?.search) query.set("search", params.search)
  if (params?.starred_only) query.set("starred_only", "true")
  if (params?.min_score !== undefined) query.set("min_score", String(params.min_score))
  if (params?.max_score !== undefined) query.set("max_score", String(params.max_score))
  const url = `${API_BASE}/spaces${query.toString() ? "?" + query.toString() : ""}`
  return fetchWithTimeout<SpaceListItem[]>(url, {}, LONG_TIMEOUT)
}

export async function getSpaceDetail(spaceId: string): Promise<SpaceDetailResponse> {
  return fetchWithTimeout<SpaceDetailResponse>(
    `${API_BASE}/spaces/${spaceId}`,
    {},
    DEFAULT_TIMEOUT
  )
}

export async function scanSpace(spaceId: string): Promise<ScanResult> {
  return fetchWithTimeout<ScanResult>(
    `${API_BASE}/spaces/${spaceId}/scan`,
    { method: "POST", headers: { "Content-Type": "application/json" } },
    LONG_TIMEOUT
  )
}

export async function getSpaceHistory(spaceId: string, days = 30): Promise<SpaceHistory> {
  return fetchWithTimeout<SpaceHistory>(
    `${API_BASE}/spaces/${spaceId}/history?days=${days}`,
    {},
    DEFAULT_TIMEOUT
  )
}

export async function toggleStar(spaceId: string, starred: boolean): Promise<void> {
  await fetchWithTimeout(
    `${API_BASE}/spaces/${spaceId}/star`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ starred }),
    },
    DEFAULT_TIMEOUT
  )
}

export async function getAdminDashboard(): Promise<AdminDashboardStats> {
  return fetchWithTimeout<AdminDashboardStats>(`${API_BASE}/admin/dashboard`, {}, DEFAULT_TIMEOUT)
}

export async function getLeaderboard(): Promise<{ top: LeaderboardEntry[]; bottom: LeaderboardEntry[] }> {
  return fetchWithTimeout(`${API_BASE}/admin/leaderboard`, {}, DEFAULT_TIMEOUT)
}

export async function getAlerts(): Promise<AlertItem[]> {
  return fetchWithTimeout<AlertItem[]>(`${API_BASE}/admin/alerts`, {}, DEFAULT_TIMEOUT)
}

export async function getCurrentUser(): Promise<CurrentUser> {
  return fetchWithTimeout<CurrentUser>(`${API_BASE}/auth/me`, {}, DEFAULT_TIMEOUT)
}

export function streamFixAgent(
  spaceId: string,
  findings: string[],
  spaceConfig: Record<string, unknown>,
  onEvent: (event: FixAgentEvent) => void,
  onError: (error: Error) => void
): () => void {
  const abortController = new AbortController()

  fetch(`${API_BASE}/spaces/${spaceId}/fix`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ space_id: spaceId, findings, space_config: spaceConfig }),
    signal: abortController.signal,
  })
    .then(async (response) => {
      if (!response.ok) throw new ApiError("Fix agent request failed", response.status)
      const reader = response.body?.getReader()
      if (!reader) throw new Error("No response body")
      const decoder = new TextDecoder()
      let buffer = ""
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split("\n\n")
        buffer = lines.pop() || ""
        for (const line of lines) {
          if (line.startsWith("data: ")) {
            try {
              const event = JSON.parse(line.slice(6)) as FixAgentEvent
              onEvent(event)
            } catch { /* ignore */ }
          }
        }
      }
    })
    .catch((error) => {
      if (error.name !== "AbortError") onError(error)
    })

  return () => abortController.abort()
}

// ── Create Wizard ────────────────────────────────────────────────────────────

export async function discoverCatalogs(): Promise<{ catalogs: UcCatalog[] }> {
  return fetchWithTimeout<{ catalogs: UcCatalog[] }>(`${API_BASE}/create/discover/catalogs`)
}

export async function discoverSchemas(catalog: string): Promise<{ schemas: UcSchema[] }> {
  return fetchWithTimeout<{ schemas: UcSchema[] }>(
    `${API_BASE}/create/discover/schemas?catalog=${encodeURIComponent(catalog)}`
  )
}

export async function discoverTables(catalog: string, schema: string): Promise<{ tables: UcTable[] }> {
  return fetchWithTimeout<{ tables: UcTable[] }>(
    `${API_BASE}/create/discover/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`
  )
}

export interface SearchTablesResult {
  tables: { full_name: string; name: string; comment: string }[]
  search_results: { full_name: string; comment: string; table_type: string; total_columns: number; matching_columns: string[]; matched_keywords: string[] }[]
  search_terms_used: string[]
  catalogs_searched: string[]
  total_matches: number
  error?: string
}

export async function searchTables(keywords: string[], catalogs?: string[]): Promise<SearchTablesResult> {
  const params = new URLSearchParams({ keywords: keywords.join(",") })
  if (catalogs?.length) params.set("catalogs", catalogs.join(","))
  return fetchWithTimeout<SearchTablesResult>(`${API_BASE}/create/discover/search?${params}`)
}

export async function validateSpaceConfig(serialized_space: Record<string, unknown>): Promise<ValidateConfigResponse> {
  return fetchWithTimeout<ValidateConfigResponse>(`${API_BASE}/create/validate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ serialized_space }),
  })
}

export async function createWizardSpace(payload: {
  display_name: string
  serialized_space: Record<string, unknown>
  parent_path?: string
}): Promise<CreateWizardSpaceResponse> {
  return fetchWithTimeout<CreateWizardSpaceResponse>(
    `${API_BASE}/create`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    LONG_TIMEOUT // Space creation via Databricks API can take time
  )
}

// ── Create Agent Chat ────────────────────────────────────────────────────────

export interface AgentChatCallbacks {
  onSession: (sessionId: string) => void
  onStep: (step: string, label: string, index: number, total: number) => void
  onThinking: (message: string, step: string, round: number) => void
  onToolCall: (tool: string, args: Record<string, unknown>) => void
  onToolResult: (tool: string, result: Record<string, unknown>) => void
  onMessageDelta: (token: string) => void
  onMessage: (content: string, uiElements?: Record<string, unknown>[] | null) => void
  onCreated: (spaceId: string, url: string, displayName: string) => void
  onUpdated: (spaceId: string, url: string) => void
  onError: (message: string) => void
  onDone: (needsContinuation?: boolean | "connection_lost") => void
}

export function streamAgentChat(
  message: string,
  sessionId: string | null,
  selections: Record<string, unknown> | null,
  callbacks: AgentChatCallbacks,
  spaceId?: string | null,
): () => void {
  const abortController = new AbortController()

  const body: Record<string, unknown> = {
    message,
    session_id: sessionId,
    selections,
  }
  if (spaceId) body.space_id = spaceId

  fetch(`${API_BASE}/create/agent/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: abortController.signal,
  })
    .then(async (response) => {
      if (!response.ok) throw new ApiError("Agent chat request failed", response.status)
      const reader = response.body?.getReader()
      if (!reader) throw new Error("No response body")
      const decoder = new TextDecoder()
      let buffer = ""

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const chunks = buffer.split("\n\n")
        buffer = chunks.pop() || ""

        for (const chunk of chunks) {
          const lines = chunk.split("\n")
          let eventType = ""
          let dataStr = ""
          for (const line of lines) {
            if (line.startsWith("event: ")) eventType = line.slice(7)
            else if (line.startsWith("data: ")) dataStr = line.slice(6)
          }
          if (!eventType || !dataStr) continue
          try {
            const data = JSON.parse(dataStr)
            switch (eventType) {
              case "session": callbacks.onSession(data.session_id); break
              case "step": callbacks.onStep(data.step, data.label, data.index, data.total); break
              case "thinking": callbacks.onThinking(data.message, data.step, data.round); break
              case "tool_call": callbacks.onToolCall(data.tool, data.args); break
              case "tool_result": callbacks.onToolResult(data.tool, data.result); break
              case "message_delta": callbacks.onMessageDelta(data.content); break
              case "message": callbacks.onMessage(data.content, data.ui_elements); break
              case "created": callbacks.onCreated(data.space_id, data.url, data.display_name); break
              case "updated": callbacks.onUpdated(data.space_id, data.url); break
              case "error": callbacks.onError(data.message); break
              case "done": callbacks.onDone(data.needs_continuation === true); break
            }
          } catch { /* ignore parse errors */ }
        }
      }
    })
    .catch((error) => {
      if (error.name !== "AbortError") {
        // Network error or proxy disconnect — signal as a connection drop so
        // the UI can auto-reconnect (distinct from backend error events).
        callbacks.onDone("connection_lost")
      }
    })

  return () => abortController.abort()
}

// ===== Auto-Optimize (GSO) API =====

export async function getAutoOptimizeHealth(): Promise<{ configured: boolean; issues: string[] }> {
  return fetchWithTimeout<{ configured: boolean; issues: string[] }>(`${API_BASE}/auto-optimize/health`)
}

export async function getAutoOptimizePermissions(spaceId: string): Promise<GSOPermissionCheck> {
  return fetchWithTimeout<GSOPermissionCheck>(`${API_BASE}/auto-optimize/permissions/${spaceId}`)
}

export async function triggerAutoOptimize(request: GSOTriggerRequest): Promise<GSOTriggerResponse> {
  return fetchWithTimeout<GSOTriggerResponse>(
    `${API_BASE}/auto-optimize/trigger`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT
  )
}

export async function deploySpace(
  spaceId: string,
  config: { target_workspace_url: string; target_space_id?: string; catalog_map?: Record<string, string> }
): Promise<{ status: string; targetSpaceId: string; targetUrl: string }> {
  return fetchWithTimeout<{ status: string; targetSpaceId: string; targetUrl: string }>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/deploy`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(config),
    },
    LONG_TIMEOUT
  )
}

export async function getAutoOptimizeRun(runId: string): Promise<GSOPipelineRun> {
  return fetchWithTimeout<GSOPipelineRun>(`${API_BASE}/auto-optimize/runs/${runId}`)
}

export async function getAutoOptimizeStatus(runId: string): Promise<GSORunStatus> {
  return fetchWithTimeout<GSORunStatus>(`${API_BASE}/auto-optimize/runs/${runId}/status`)
}

export async function getAutoOptimizeLevers(): Promise<GSOLeverInfo[]> {
  return fetchWithTimeout<GSOLeverInfo[]>(`${API_BASE}/auto-optimize/levers`)
}

export async function applyAutoOptimize(runId: string): Promise<{ status: string; runId: string; message: string }> {
  return fetchWithTimeout<{ status: string; runId: string; message: string }>(
    `${API_BASE}/auto-optimize/runs/${runId}/apply`,
    { method: "POST", headers: { "Content-Type": "application/json" } },
    LONG_TIMEOUT
  )
}

export async function discardAutoOptimize(runId: string): Promise<{ status: string; runId: string; message: string }> {
  return fetchWithTimeout<{ status: string; runId: string; message: string }>(
    `${API_BASE}/auto-optimize/runs/${runId}/discard`,
    { method: "POST", headers: { "Content-Type": "application/json" } },
    DEFAULT_TIMEOUT
  )
}

export async function getActiveRunForSpace(
  spaceId: string
): Promise<{ hasActiveRun: boolean; activeRunId: string | null; activeRunStatus: string | null }> {
  return fetchWithTimeout<{ hasActiveRun: boolean; activeRunId: string | null; activeRunStatus: string | null }>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/active-run`
  )
}

export async function getAutoOptimizeRunsForSpace(spaceId: string): Promise<GSORunSummary[]> {
  return fetchWithTimeout<GSORunSummary[]>(`${API_BASE}/auto-optimize/spaces/${spaceId}/runs`)
}

export async function getAutoOptimizeIterations(runId: string): Promise<GSOIterationResult[]> {
  return fetchWithTimeout<GSOIterationResult[]>(`${API_BASE}/auto-optimize/runs/${runId}/iterations`)
}

export async function getAutoOptimizeAsiResults(runId: string, iteration: number): Promise<GSOQuestionResult[]> {
  return fetchWithTimeout<GSOQuestionResult[]>(
    `${API_BASE}/auto-optimize/runs/${runId}/asi-results?iteration=${iteration}`
  )
}

export async function getAutoOptimizeQuestionResults(runId: string, iteration: number): Promise<GSOQuestionDetail[]> {
  try {
    return await fetchWithTimeout<GSOQuestionDetail[]>(
      `${API_BASE}/auto-optimize/runs/${runId}/question-results?iteration=${iteration}`
    )
  } catch {
    return []
  }
}

export async function getAutoOptimizePatches(runId: string): Promise<GSOPatch[]> {
  try {
    return await fetchWithTimeout<GSOPatch[]>(
      `${API_BASE}/auto-optimize/runs/${runId}/patches`
    )
  } catch {
    return []
  }
}

export async function getAutoOptimizeSuggestions(runId: string): Promise<import("@/types").GSOSuggestion[]> {
  try {
    return await fetchWithTimeout<import("@/types").GSOSuggestion[]>(
      `${API_BASE}/auto-optimize/runs/${runId}/suggestions`
    )
  } catch {
    return []
  }
}

export { ApiError }
