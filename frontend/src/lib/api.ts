/**
 * API client for communicating with the Genie Workbench backend.
 */

import type {
  AppSettings,
  LLMModelInfo,
  FetchSpaceResponse,
  SpaceDetailResponse,
  SpaceListItem,
  ScanResult,
  SpaceHistory,
  AdminDashboardStats,
  LeaderboardEntry,
  AlertItem,
  CurrentUser,
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
  GSOBenchmarkChanges,
  GSOLoopStateResponse,
  GSOPublishRecordResponse,
  CurrentVersionResponse,
  GSORevertOptions,
  MvCreateAtApprovalRequest,
  MvCreateAtApprovalResponse,
  MvProbeRequest,
  MvProbeResult,
  MvSpaceProposalsResponse,
  MvProposalsResponse,
  MvDdlArtifact,
  MvCreatedObjectsResponse,
  MvProposalDecisionRequest,
  MvProposalDecisionResponse,
  MvDropRequest,
  MvDropResponse,
  MvSuggestResponse,
  MvRegisterRequest,
  MvRegisterResponse,
  SemanticGraphResponse,
  JoinCandidate,
  JoinCandidatesResponse,
  JoinAdviceResponse,
} from "@/types"

const API_BASE = "/api"

// Request timeout values (in milliseconds)
const DEFAULT_TIMEOUT = 30_000 // 30 seconds for most requests
const LONG_TIMEOUT = 300_000 // 5 minutes for LLM operations (optimization can be slow)
// Warehouse statements can block up to their 50s server-side wait_timeout.
// Current-version's worst path is two sequential statements (the concurrent
// runs/champions pair, then a status re-read after zombie reconciliation) plus
// Jobs-API calls — 120s covers it where the 30s default would silently lose
// the badge on a cold warehouse.
const CURRENT_VERSION_TIMEOUT = 120_000

class ApiError extends Error {
  status: number
  /**
   * Structured error payload from the backend, when the router raises
   * `HTTPException(detail={...})`. Callers that care about fields like
   * `reason_code`, `error_code`, or `actionable_by` should read from here
   * rather than parsing `message`. `null` when the response didn't carry one,
   * or when `detail` was already a string.
   */
  detail: Record<string, unknown> | null

  constructor(
    message: string,
    status: number,
    detail: Record<string, unknown> | null = null,
  ) {
    super(message)
    this.name = "ApiError"
    this.status = status
    this.detail = detail
  }
}

/**
 * Pull a human-readable string out of a FastAPI `detail` payload.
 *
 * FastAPI routers can raise `HTTPException(detail=...)` with either:
 *   - a string       → use directly
 *   - an object      → prefer a well-known message field, else JSON stringify
 *     so we never surface the JavaScript default `"[object Object]"` from
 *     `new Error(obj)` coercion.
 *   - a list of pydantic validation errors (422) → join their `msg` fields.
 *
 * Keep this pure / sync; it runs inside an error path and must not throw.
 */
export function extractDetailMessage(detail: unknown, fallback: string): string {
  if (typeof detail === "string" && detail.length > 0) return detail
  if (Array.isArray(detail)) {
    const msgs = detail
      .map((d) => (d && typeof d === "object" && "msg" in d ? String((d as { msg: unknown }).msg) : ""))
      .filter(Boolean)
    if (msgs.length > 0) return msgs.join("; ")
  }
  if (detail && typeof detail === "object") {
    const obj = detail as Record<string, unknown>
    for (const key of ["error", "user_message", "message", "detail"] as const) {
      const v = obj[key]
      if (typeof v === "string" && v.length > 0) return v
    }
    try {
      return JSON.stringify(detail)
    } catch {
      return fallback
    }
  }
  return fallback
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
      const message = extractDetailMessage(error.detail, "An error occurred")
      const structured =
        error.detail && typeof error.detail === "object" && !Array.isArray(error.detail)
          ? (error.detail as Record<string, unknown>)
          : null
      throw new ApiError(message, response.status, structured)
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
 * Fetch a Genie Agent by ID.
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
 * Parse pasted Genie Agent JSON.
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

export async function getModels(): Promise<LLMModelInfo[]> {
  return fetchWithTimeout<LLMModelInfo[]>(`${API_BASE}/models`, {}, DEFAULT_TIMEOUT)
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

// ── Create Wizard ────────────────────────────────────────────────────────────

export async function fetchCreatePreflight(): Promise<{ warehouses_available: boolean; obo_enabled: boolean; app_name: string }> {
  return fetchWithTimeout(`${API_BASE}/create/preflight`)
}

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
  model?: string | null,
): () => void {
  const abortController = new AbortController()

  const body: Record<string, unknown> = {
    message,
    session_id: sessionId,
    selections,
  }
  if (spaceId) body.space_id = spaceId
  if (model) body.model = model

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

export async function getAutoOptimizePermissions(
  spaceId: string,
): Promise<GSOPermissionCheck> {
  return fetchWithTimeout<GSOPermissionCheck>(
    `${API_BASE}/auto-optimize/permissions/${spaceId}`,
  )
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

// Probe the signed-in user's entitlement to create a metric view in a schema
// (OBO). Fired when the run-config MV section expands in the re-run state.
export async function probeMvEntitlement(request: MvProbeRequest): Promise<MvProbeResult> {
  return fetchWithTimeout<MvProbeResult>(
    `${API_BASE}/auto-optimize/mv/probe`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT,
  )
}

// Space-scoped proposals for the run-config re-run gate (MV-D23). Passing
// approvedForRerun=true returns only proposals cleared for a create-and-attach
// re-run; it never borrows a prior run_id to answer this per-space question.
export async function fetchSpaceMvProposals(
  spaceId: string,
  approvedForRerun?: boolean,
): Promise<MvSpaceProposalsResponse> {
  const query =
    approvedForRerun === undefined ? "" : `?approved_for_rerun=${approvedForRerun}`
  return fetchWithTimeout<MvSpaceProposalsResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/mv-proposals${query}`,
  )
}

// POST /spaces/{space_id}/mv/suggest — an on-demand advice run (MV-D23). The IQ
// Scan surface asks the advisor to score a space right now, with no optimization
// run; the backend writes a born-terminal sentinel advice run and returns its
// outcome plus the persisted proposals. Runs corpus scan + scoring + the S-signal
// embedding synchronously (seconds), so it takes the long timeout — the embedding
// path has its own hard cap server-side that degrades S rather than hanging.
export async function suggestSpaceMv(spaceId: string): Promise<MvSuggestResponse> {
  return fetchWithTimeout<MvSuggestResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/mv/suggest`,
    { method: "POST", headers: { "Content-Type": "application/json" } },
    LONG_TIMEOUT,
  )
}

// POST /spaces/{space_id}/mv/suggest/stream — staged-progress twin of
// suggestSpaceMv (MV-D31). Emits a `stage` event on ENTRY to each of the four
// honest phases (reading → scanning → scoring → rendering) so the surface shows
// what it is doing *now* over a multi-minute scan, then a final `result` event
// carrying the same MvSuggestResponse. Consumed with fetch + ReadableStream
// (not EventSource), buffer-split on \n\n, exactly like streamAgentChat.
// Returns an abort function so the caller can cancel an in-flight scan.
export interface MvSuggestStreamCallbacks {
  onStage: (stage: string) => void
  onResult: (result: MvSuggestResponse) => void
  onError: (message: string) => void
}

export function streamSpaceMvSuggest(
  spaceId: string,
  callbacks: MvSuggestStreamCallbacks,
): () => void {
  const abortController = new AbortController()

  fetch(`${API_BASE}/auto-optimize/spaces/${spaceId}/mv/suggest/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    signal: abortController.signal,
  })
    .then(async (response) => {
      if (!response.ok) {
        throw new ApiError("Metric-view scan request failed", response.status)
      }
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
          let eventType = ""
          let dataStr = ""
          for (const line of chunk.split("\n")) {
            if (line.startsWith("event: ")) eventType = line.slice(7)
            else if (line.startsWith("data: ")) dataStr = line.slice(6)
          }
          if (!eventType || !dataStr) continue
          try {
            const data = JSON.parse(dataStr)
            switch (eventType) {
              case "stage": callbacks.onStage(data.stage); break
              case "result": callbacks.onResult(data as MvSuggestResponse); break
              case "error": callbacks.onError(data.detail || "Metric-view scan failed."); break
            }
          } catch { /* ignore parse errors (keepalive comments, partial frames) */ }
        }
      }
    })
    .catch((error) => {
      if (error.name !== "AbortError") {
        callbacks.onError(
          error instanceof Error ? error.message : "Could not run the metric-view scan.",
        )
      }
    })

  return () => abortController.abort()
}

// POST /spaces/{space_id}/mv/register — a bring-your-own view (MV-D24). A user
// who created the suggested view themselves reports its identifier; the backend
// verifies it under OBO and, on success, records a USER_CREATED ledger row so
// attach-and-lift picks it up. Verified and refused are both normal 200s the
// panel renders from one shape (registered + reason).
export async function registerSpaceMv(
  spaceId: string,
  request: MvRegisterRequest,
): Promise<MvRegisterResponse> {
  return fetchWithTimeout<MvRegisterResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/mv/register`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT,
  )
}

// POST /spaces/{space_id}/mv/create — create-at-approval (MV-D34). The user
// accepted a suggestion where they meet it; a fresh probe already recorded
// consent. Creates the one proposal now under OBO and returns one of: created,
// degraded (probe fell below SUFFICIENT → approve-for-later), or a create-time
// failure with a reason. Attach + lift stay the next run's job.
export async function createMvAtApproval(
  spaceId: string,
  request: MvCreateAtApprovalRequest,
): Promise<MvCreateAtApprovalResponse> {
  return fetchWithTimeout<MvCreateAtApprovalResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/mv/create`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT,
  )
}

// Space-scoped semantic model graph for the Model tab (Prompt 12, MV-D23). The
// server assembles nodes/edges live from serialized_space (the same OBO-tolerant
// read /space/fetch uses) plus the space-scoped proposals read; the ghosted
// overlay is synthesized client-side from `proposals`.
export async function fetchSemanticGraph(
  spaceId: string,
): Promise<SemanticGraphResponse> {
  return fetchWithTimeout<SemanticGraphResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/semantic-graph`,
  )
}

// ── Join Advisor (Semantic Blueprint §7): candidates + advice ───────────────
// Candidates are data-grounded suggestions; seeding persists them as ADVICE the
// next Auto-Optimize run validates and adds itself. Nothing here edits the
// Genie Agent config — the Workbench makes no ad-hoc serialized_space edits.

export async function fetchJoinCandidates(
  spaceId: string,
): Promise<JoinCandidatesResponse> {
  return fetchWithTimeout<JoinCandidatesResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/join-candidates`,
  )
}

export async function fetchJoinAdvice(
  spaceId: string,
): Promise<JoinAdviceResponse> {
  return fetchWithTimeout<JoinAdviceResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/join-advice`,
  )
}

export async function saveJoinAdvice(
  spaceId: string,
  seeds: JoinCandidate[],
): Promise<JoinAdviceResponse> {
  return fetchWithTimeout<JoinAdviceResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/join-advice`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ seeds }),
    },
  )
}

// ── Metric view output-screen reads/actions (Prompt 13, MV-D21/D23) ─────────
// The run-detail CONTAINER fetches these by run_id — it lives on a run screen —
// and passes the results to presentational panels as props; nothing downstream
// keys state, cache, or identity on run_id (MV-D23).

// GET /runs/{run_id}/mv-proposals — the run's advisor proposals (suggest-only).
export async function getRunMvProposals(runId: string): Promise<MvProposalsResponse> {
  return fetchWithTimeout<MvProposalsResponse>(
    `${API_BASE}/auto-optimize/runs/${runId}/mv-proposals`,
  )
}

// GET /runs/{run_id}/mv-ddl — the rendered DDL artifact + copy-ready GRANT.
// suggestionId pins one candidate on the advice-run fallback path (Prompt 15.1);
// omitted, the route serves the artifact (in-job) or the best candidate (advice).
export async function getMvDdl(
  runId: string,
  suggestionId?: string,
): Promise<MvDdlArtifact> {
  const qs = suggestionId ? `?suggestion_id=${encodeURIComponent(suggestionId)}` : ''
  return fetchWithTimeout<MvDdlArtifact>(
    `${API_BASE}/auto-optimize/runs/${runId}/mv-ddl${qs}`,
  )
}

// GET /runs/{run_id}/mv-created — the create-and-attach ledger + downgrade_reason.
export async function getMvCreatedObjects(runId: string): Promise<MvCreatedObjectsResponse> {
  return fetchWithTimeout<MvCreatedObjectsResponse>(
    `${API_BASE}/auto-optimize/runs/${runId}/mv-created`,
  )
}

// POST /mv/proposals/{suggestion_id}/decision — approve/reject (MV-D1).
export async function decideMvProposal(
  suggestionId: string,
  request: MvProposalDecisionRequest,
): Promise<MvProposalDecisionResponse> {
  return fetchWithTimeout<MvProposalDecisionResponse>(
    `${API_BASE}/auto-optimize/mv/proposals/${suggestionId}/decision`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT,
  )
}

// POST /mv/created/{suggestion_id}/drop — explicit OBO drop, DETACHED only (MV-D6).
export async function dropMvCreated(
  suggestionId: string,
  request: MvDropRequest,
): Promise<MvDropResponse> {
  return fetchWithTimeout<MvDropResponse>(
    `${API_BASE}/auto-optimize/mv/created/${suggestionId}/drop`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    },
    LONG_TIMEOUT,
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

export async function revertAutoOptimizeRun(
  runId: string,
  options: {
    configTarget: "champion" | "baseline"
    benchmarkTarget: "current" | "champion" | "baseline"
  } = { configTarget: "champion", benchmarkTarget: "current" },
): Promise<{ status: string; runId: string; message: string }> {
  const query = new URLSearchParams({
    config_target: options.configTarget,
    benchmark_target: options.benchmarkTarget,
  })
  return fetchWithTimeout<{ status: string; runId: string; message: string }>(
    `${API_BASE}/auto-optimize/runs/${runId}/revert?${query.toString()}`,
    { method: "POST", headers: { "Content-Type": "application/json" } },
    DEFAULT_TIMEOUT
  )
}

export async function getAutoOptimizeRevertOptions(
  runId: string,
): Promise<GSORevertOptions> {
  return fetchWithTimeout<GSORevertOptions>(
    `${API_BASE}/auto-optimize/runs/${runId}/revert-options`,
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

export async function removeAutoOptimizeRunFromHistory(
  runId: string,
): Promise<{ status: string; runId: string; spaceId: string; message: string }> {
  return fetchWithTimeout<{ status: string; runId: string; spaceId: string; message: string }>(
    `${API_BASE}/auto-optimize/runs/${runId}/history-entry`,
    { method: "DELETE" },
    DEFAULT_TIMEOUT,
  )
}

/**
 * Which known optimization versions the live agent's config and benchmarks
 * currently match. Fail-open on the backend: "unavailable" /
 * "no_known_versions" mean "show nothing".
 */
export async function getCurrentVersion(
  spaceId: string,
  refresh = false,
): Promise<CurrentVersionResponse> {
  const query = refresh ? "?refresh=true" : ""
  return fetchWithTimeout<CurrentVersionResponse>(
    `${API_BASE}/auto-optimize/spaces/${spaceId}/current-version${query}`,
    {},
    CURRENT_VERSION_TIMEOUT
  )
}

export async function getAutoOptimizeIterations(runId: string): Promise<GSOIterationResult[]> {
  return fetchWithTimeout<GSOIterationResult[]>(`${API_BASE}/auto-optimize/runs/${runId}/iterations`)
}

// GSO v2 Phase 6 — lightweight official eval-results (assessment +
// assessment_reasons per question). Replaces the retired per-judge ASI rows.
export async function getAutoOptimizeEvalResults(runId: string, iteration: number): Promise<GSOQuestionResult[]> {
  try {
    return await fetchWithTimeout<GSOQuestionResult[]>(
      `${API_BASE}/auto-optimize/runs/${runId}/eval-results?iteration=${iteration}`
    )
  } catch {
    return []
  }
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

// GSO v2 Phase 6 (§3.5) — benchmark provenance ledger (added/removed/changed
// questions GSO made in the live Genie Agent). GSO v2 (item 7): the response
// also carries `qc` (30–40 window status, repair tries, validity findings).
export async function getAutoOptimizeBenchmarkChanges(runId: string): Promise<GSOBenchmarkChanges | null> {
  try {
    return await fetchWithTimeout<GSOBenchmarkChanges>(
      `${API_BASE}/auto-optimize/runs/${runId}/benchmark-changes`
    )
  } catch {
    return null
  }
}

// GSO v2 (arch §7.4) — 03_optimize controller loop-state + per-attempt ledger.
// Returns loopState=null + attempts=[] for legacy 6-step runs.
export async function getAutoOptimizeLoopState(runId: string): Promise<GSOLoopStateResponse | null> {
  try {
    return await fetchWithTimeout<GSOLoopStateResponse>(
      `${API_BASE}/auto-optimize/runs/${runId}/loop-state`
    )
  } catch {
    return null
  }
}

// GSO v2 (arch §7.3) — publish_and_audit record (audit summary + improvement
// trajectory + concerns + champion pointer). publishRecord is null before the
// run reaches publish, or for legacy runs.
export async function getAutoOptimizePublishRecord(runId: string): Promise<GSOPublishRecordResponse | null> {
  try {
    return await fetchWithTimeout<GSOPublishRecordResponse>(
      `${API_BASE}/auto-optimize/runs/${runId}/publish`
    )
  } catch {
    return null
  }
}

export { ApiError }
