// Ontology API client. All endpoints under /api/ontology/* — namespaced like
// the workbench (/api/spaces, /api/admin) and GenieWatch (/api/watch) surfaces.
// Modeled on frontend/src/lib/api.ts (fetch-with-timeout + typed ApiError).

import type {
  DecisionRequest,
  DecisionResponse,
  OntologyDrafts,
  OntologyInventory,
  OntologyPreflight,
  OntologyRefreshStatus,
  OntologySettings,
  OntologyTaxonomy,
  TagLens,
} from "@/ontology/types"

const API_BASE = "/api/ontology"
// SP system-table reads (taxonomy / tags) poll up to ~180s server-side; allow
// 5 min on the client so we never abort before the backend returns.
const DEFAULT_TIMEOUT = 300_000

export class ApiError extends Error {
  status: number
  detail: unknown

  constructor(message: string, status: number, detail: unknown) {
    super(message)
    this.status = status
    this.detail = detail
  }
}

async function fetchJson<T>(
  path: string,
  init: RequestInit = {},
  timeout = DEFAULT_TIMEOUT,
): Promise<T> {
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), timeout)
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      ...init,
      signal: ctrl.signal,
      headers: {
        "Content-Type": "application/json",
        ...(init.headers || {}),
      },
    })
    if (!res.ok) {
      let detail: unknown = null
      try {
        detail = await res.json()
      } catch {
        detail = await res.text()
      }
      throw new ApiError(`${res.status} ${res.statusText}`, res.status, detail)
    }
    if (res.status === 204) return undefined as T
    return (await res.json()) as T
  } finally {
    clearTimeout(timer)
  }
}

export const getPreflight = () => fetchJson<OntologyPreflight>("/preflight")

export const getInventory = () => fetchJson<OntologyInventory>("/inventory")

export const getTaxonomy = () => fetchJson<OntologyTaxonomy>("/taxonomy")

export const getTags = () => fetchJson<TagLens>("/tags")

export const getSettings = () => fetchJson<OntologySettings>("/settings")

export const saveSettings = (settings: OntologySettings) =>
  fetchJson<OntologySettings>("/settings", {
    method: "PUT",
    body: JSON.stringify(settings),
  })

// ── Refresh / freshness (Phase 2) ─────────────────────────────────────────
export const getRefreshStatus = () => fetchJson<OntologyRefreshStatus>("/refresh")

export const triggerRefresh = () =>
  fetchJson<OntologyRefreshStatus>("/refresh", { method: "POST" })

// ── Drafts + decisions (Phase 3d) ──────────────────────────────────────────
export const getDrafts = () => fetchJson<OntologyDrafts>("/drafts")

export const postDecision = (decision: DecisionRequest) =>
  fetchJson<DecisionResponse>("/decision", {
    method: "POST",
    body: JSON.stringify(decision),
  })
