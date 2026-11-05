/**
 * Daily release check, browser side (spec 5.7).
 *
 * The app server never calls the internet. Workspace admins' browsers ask
 * GitHub for the latest release at most once per 24 hours. Nothing about the
 * workspace or the installed version is sent: no referrer, no cookies.
 */
import { isolatedFetch } from "@/lib/isolated-fetch"
import type { UpdateCheckState } from "@/types"

export const CHECK_INTERVAL_MS = 24 * 60 * 60 * 1000
export const GITHUB_TIMEOUT_MS = 5_000
export const STORAGE_KEYS = {
  result: "gwb.updateCheck.result",
  consent: "gwb.updateCheck.consent",
  dismissed: "gwb.updateCheck.dismissed",
} as const

export interface LatestRelease {
  tag: string
  name: string
  html_url: string
  published_at: string
}

export interface CheckResult {
  checked_at: number // epoch ms of the last attempt, success or failure
  ok: boolean
  latest: LatestRelease | null // last successfully fetched release, kept across failures
  last_success_at: number | null
  error?: string
}

export type Consent = "on" | "off"
export type Mode = "on" | "off" | "ask"

export function parseVersion(value: string | null | undefined): number[] | null {
  if (!value) return null
  const trimmed = value.trim().replace(/^v/i, "")
  if (!/^\d+(\.\d+)*$/.test(trimmed)) return null
  return trimmed.split(".").map((part) => Number(part))
}

export function compareVersions(a: number[], b: number[]): -1 | 0 | 1 {
  const len = Math.max(a.length, b.length)
  for (let i = 0; i < len; i++) {
    const diff = (a[i] ?? 0) - (b[i] ?? 0)
    if (diff !== 0) return diff > 0 ? 1 : -1
  }
  return 0
}

/** true when latest is newer than installed; null when either cannot be parsed. */
export function isUpdateAvailable(latestTag: string, installed: string): boolean | null {
  const latest = parseVersion(latestTag)
  const current = parseVersion(installed)
  if (!latest || !current) return null
  return compareVersions(latest, current) > 0
}

export function isCheckDue(result: CheckResult | null, now: number): boolean {
  if (!result) return true
  const age = now - result.checked_at
  return age < 0 || age >= CHECK_INTERVAL_MS // a future timestamp (clock changed) counts as due
}

export async function fetchLatestRelease(
  repo: string,
  fetchImpl: typeof fetch = isolatedFetch, // opaque-origin frame: Origin: null
  timeoutMs: number = GITHUB_TIMEOUT_MS,
): Promise<LatestRelease> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const res = await fetchImpl(`https://api.github.com/repos/${repo}/releases/latest`, {
      method: "GET",
      mode: "cors",
      credentials: "omit",
      referrerPolicy: "no-referrer",
      cache: "no-store",
      headers: { Accept: "application/vnd.github+json" },
      signal: controller.signal,
    })
    if (!res.ok) throw new Error(`GitHub returned ${res.status}`)
    let body: Record<string, unknown> | null
    try {
      body = (await res.json()) as Record<string, unknown> | null
    } catch {
      throw new Error("GitHub returned a non-JSON response")
    }
    const tag = body?.tag_name
    const url = body?.html_url
    if (typeof tag !== "string" || !tag || typeof url !== "string" || !url.startsWith("https://github.com/")) {
      throw new Error("GitHub response had no release")
    }
    const name = typeof body?.name === "string" && body.name ? body.name : tag
    const published = typeof body?.published_at === "string" ? body.published_at : ""
    return { tag, name, html_url: url, published_at: published }
  } finally {
    clearTimeout(timer)
  }
}

export function nextResult(
  prev: CheckResult | null,
  now: number,
  outcome: { latest: LatestRelease } | { error: string },
): CheckResult {
  if ("latest" in outcome) {
    return { checked_at: now, ok: true, latest: outcome.latest, last_success_at: now }
  }
  return {
    checked_at: now,
    ok: false,
    latest: prev?.latest ?? null,
    last_success_at: prev?.last_success_at ?? null,
    error: outcome.error,
  }
}

// ── Storage (localStorage with an in-memory fallback when it is disabled) ──
const memory = new Map<string, string>()

function storageGet(key: string): string | null {
  try {
    return window.localStorage.getItem(key)
  } catch {
    return memory.get(key) ?? null
  }
}

function storageSet(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value)
  } catch {
    memory.set(key, value)
  }
}

export function resetMemoryStorageForTests(): void {
  memory.clear()
}

function isLatestRelease(value: unknown): value is LatestRelease {
  const v = value as Record<string, unknown> | null
  return (
    typeof v === "object" &&
    v !== null &&
    typeof v.tag === "string" &&
    typeof v.name === "string" &&
    typeof v.published_at === "string" &&
    typeof v.html_url === "string" &&
    v.html_url.startsWith("https://github.com/")
  )
}

function isCheckResult(value: unknown): value is CheckResult {
  const v = value as Record<string, unknown> | null
  return (
    typeof v === "object" &&
    v !== null &&
    typeof v.checked_at === "number" &&
    typeof v.ok === "boolean" &&
    (v.last_success_at === null || typeof v.last_success_at === "number") &&
    (v.latest === null || isLatestRelease(v.latest))
  )
}

/** The cached result, or null when missing, corrupt, or an unexpected shape. */
export function loadCheckResult(): CheckResult | null {
  const raw = storageGet(STORAGE_KEYS.result)
  if (!raw) return null
  try {
    const parsed: unknown = JSON.parse(raw)
    return isCheckResult(parsed) ? parsed : null
  } catch {
    return null
  }
}

export function saveCheckResult(result: CheckResult): void {
  storageSet(STORAGE_KEYS.result, JSON.stringify(result))
}

export function loadLocalConsent(): Consent | null {
  const value = storageGet(STORAGE_KEYS.consent)
  return value === "on" || value === "off" ? value : null
}

export function saveLocalConsent(value: Consent): void {
  storageSet(STORAGE_KEYS.consent, value)
}

export function loadDismissedTag(): string | null {
  return storageGet(STORAGE_KEYS.dismissed)
}

export function saveDismissedTag(tag: string): void {
  storageSet(STORAGE_KEYS.dismissed, tag)
}

/** Effective mode: install-time, stored, or errored answers win; then this browser's answer when the server cannot persist one. */
export function effectiveMode(server: UpdateCheckState, localConsent: Consent | null): Mode {
  if (server.locked || server.source === "app" || server.source === "error") return server.mode
  if (!server.persisted && localConsent) return localConsent
  return server.mode
}
