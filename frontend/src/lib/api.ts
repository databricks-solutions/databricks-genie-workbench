/**
 * API client for communicating with the Genie Space Analyzer backend.
 */

import type {
  FetchSpaceResponse,
  SectionAnalysis,
  SectionInfo,
  AnalyzeSectionRequest,
  StreamProgress,
  GenieQueryResponse,
  SqlExecutionResult,
  AppSettings,
  LabelingFeedbackItem,
  OptimizationResponse,
  OptimizationSuggestion,
  ConfigMergeResponse,
  GenieCreateRequest,
  GenieCreateResponse,
  SynthesisResult,
  SpaceListItem,
  ScanResult,
  ScoreHistoryPoint,
  AdminDashboardStats,
  LeaderboardEntry,
  AlertItem,
  CurrentUser,
  FixAgentEvent,
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

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }))
    throw new ApiError(error.detail || "An error occurred", response.status)
  }
  return response.json()
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
 * Analyze a single section.
 */
export async function analyzeSection(
  request: AnalyzeSectionRequest
): Promise<SectionAnalysis> {
  return fetchWithTimeout<SectionAnalysis>(
    `${API_BASE}/analyze/section`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT // LLM operation
  )
}

/**
 * Response from analyzing all sections with cross-sectional synthesis.
 */
export interface AnalyzeAllResponse {
  analyses: SectionAnalysis[]
  synthesis: SynthesisResult | null
  is_full_analysis: boolean
}

/**
 * Analyze all sections with cross-sectional synthesis.
 * Returns style detection, section analyses, and synthesis (for full analysis only).
 */
export async function analyzeAllSections(
  sections: SectionInfo[],
  fullSpace: Record<string, unknown>,
  onProgress?: (completed: number, total: number) => void
): Promise<AnalyzeAllResponse> {
  // Simulate progress updates during the request
  const total = sections.length
  let progressInterval: ReturnType<typeof setInterval> | null = null

  if (onProgress) {
    let current = 0
    progressInterval = setInterval(() => {
      if (current < total - 1) {
        current++
        onProgress(current, total)
      }
    }, 2000) // Update every 2 seconds
  }

  try {
    const result = await fetchWithTimeout<AnalyzeAllResponse>(
      `${API_BASE}/analyze/all`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sections: sections.map((s) => ({ name: s.name, data: s.data })),
          full_space: fullSpace,
        }),
      },
      LONG_TIMEOUT // LLM operation
    )

    // Final progress update
    onProgress?.(total, total)
    return result
  } finally {
    if (progressInterval) {
      clearInterval(progressInterval)
    }
  }
}

/**
 * Stream analysis progress using Server-Sent Events.
 */
export function streamAnalysis(
  genieSpaceId: string,
  onProgress: (progress: StreamProgress) => void,
  onComplete: (result: StreamProgress) => void,
  onError: (error: Error) => void
): () => void {
  const abortController = new AbortController()

  fetch(`${API_BASE}/analyze/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ genie_space_id: genieSpaceId }),
    signal: abortController.signal,
  })
    .then(async (response) => {
      if (!response.ok) {
        throw new ApiError("Stream request failed", response.status)
      }

      const reader = response.body?.getReader()
      if (!reader) {
        throw new Error("No response body")
      }

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
              const data = JSON.parse(line.slice(6)) as StreamProgress
              if (data.status === "result") {
                onComplete(data)
              } else {
                onProgress(data)
              }
            } catch {
              // Ignore parse errors
            }
          }
        }
      }
    })
    .catch((error) => {
      if (error.name !== "AbortError") {
        onError(error)
      }
    })

  return () => abortController.abort()
}

/**
 * Get the checklist documentation.
 */
export async function getChecklist(): Promise<string> {
  const response = await fetch(`${API_BASE}/checklist`)
  const data = await handleResponse<{ content: string }>(response)
  return data.content
}

/**
 * Get the list of all section names.
 */
export async function getSections(): Promise<string[]> {
  const response = await fetch(`${API_BASE}/sections`)
  const data = await handleResponse<{ sections: string[] }>(response)
  return data.sections
}

/**
 * Query Genie to generate SQL for a natural language question.
 */
export async function queryGenie(
  genieSpaceId: string,
  question: string
): Promise<GenieQueryResponse> {
  return fetchWithTimeout<GenieQueryResponse>(
    `${API_BASE}/genie/query`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        genie_space_id: genieSpaceId,
        question: question,
      }),
    },
    LONG_TIMEOUT // Genie can take time to respond
  )
}

/**
 * Execute SQL on a Databricks SQL Warehouse.
 */
export async function executeSql(
  sql: string,
  warehouseId?: string
): Promise<SqlExecutionResult> {
  return fetchWithTimeout<SqlExecutionResult>(
    `${API_BASE}/sql/execute`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sql: sql,
        warehouse_id: warehouseId,
      }),
    },
    LONG_TIMEOUT // SQL execution can be slow
  )
}

/**
 * Get application settings.
 */
export async function getSettings(): Promise<AppSettings> {
  return fetchWithTimeout<AppSettings>(`${API_BASE}/settings`, {}, DEFAULT_TIMEOUT)
}

/**
 * Progress event from streaming optimization.
 */
export interface OptimizationStreamProgress {
  status: "processing" | "complete" | "error"
  message?: string
  elapsed_seconds?: number
  data?: OptimizationResponse
}

/**
 * Stream optimization progress using Server-Sent Events.
 * Sends heartbeats to keep the connection alive during long LLM calls.
 */
export function streamOptimizations(
  genieSpaceId: string,
  spaceData: Record<string, unknown>,
  labelingFeedback: LabelingFeedbackItem[],
  onProgress: (progress: OptimizationStreamProgress) => void,
  onComplete: (result: OptimizationResponse) => void,
  onError: (error: Error) => void
): () => void {
  const abortController = new AbortController()

  fetch(`${API_BASE}/optimize`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      genie_space_id: genieSpaceId,
      space_data: spaceData,
      labeling_feedback: labelingFeedback,
    }),
    signal: abortController.signal,
  })
    .then(async (response) => {
      if (!response.ok) {
        throw new ApiError("Stream request failed", response.status)
      }

      const reader = response.body?.getReader()
      if (!reader) {
        throw new Error("No response body")
      }

      const decoder = new TextDecoder()
      let buffer = ""

      const processLine = (line: string) => {
        if (line.startsWith("data: ")) {
          try {
            const data = JSON.parse(line.slice(6)) as OptimizationStreamProgress
            if (data.status === "complete" && data.data) {
              onComplete(data.data)
            } else if (data.status === "error") {
              onError(new Error(data.message || "Optimization failed"))
            } else {
              onProgress(data)
            }
          } catch {
            // Ignore parse errors
          }
        }
      }

      while (true) {
        const { done, value } = await reader.read()
        if (done) {
          // Process any remaining data in the buffer
          if (buffer.trim()) {
            processLine(buffer.trim())
          }
          break
        }

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split("\n\n")
        buffer = lines.pop() || ""

        for (const line of lines) {
          processLine(line)
        }
      }
    })
    .catch((error) => {
      if (error.name !== "AbortError") {
        onError(error)
      }
    })

  return () => abortController.abort()
}

/**
 * Merge config with selected suggestions.
 * This is a fast programmatic operation (no LLM involved).
 */
export async function mergeConfig(
  spaceData: Record<string, unknown>,
  suggestions: OptimizationSuggestion[]
): Promise<ConfigMergeResponse> {
  return fetchWithTimeout<ConfigMergeResponse>(
    `${API_BASE}/config/merge`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        space_data: spaceData,
        suggestions: suggestions,
      }),
    },
    DEFAULT_TIMEOUT
  )
}

/**
 * Create a new Genie Space with the merged configuration.
 */
export async function createGenieSpace(
  request: GenieCreateRequest
): Promise<GenieCreateResponse> {
  return fetchWithTimeout<GenieCreateResponse>(
    `${API_BASE}/genie/create`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT // API call to Databricks
  )
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

export async function scanSpace(spaceId: string): Promise<ScanResult> {
  return fetchWithTimeout<ScanResult>(
    `${API_BASE}/spaces/${spaceId}/scan`,
    { method: "POST", headers: { "Content-Type": "application/json" } },
    LONG_TIMEOUT
  )
}

export async function getSpaceHistory(spaceId: string, days = 30): Promise<ScoreHistoryPoint[]> {
  return fetchWithTimeout<ScoreHistoryPoint[]>(
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

export { ApiError }

