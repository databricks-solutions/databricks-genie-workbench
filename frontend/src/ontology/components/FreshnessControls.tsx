// Freshness chip + "Refresh ontology" button (Phase 2, zero-burden copy).
// The chip reads GET /refresh — "Updated 3 hours ago" when mirror-backed, or
// "Live view" when serving the live fallback. The admin button POSTs /refresh
// then polls until the run settles. No jargon about jobs, Delta, or synced tables.
import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Loader2, RefreshCw, Zap } from "lucide-react"
import { Button } from "@/components/ui/button"
import { getRefreshStatus, triggerRefresh } from "@/ontology/api"
import type { OntologyRefreshStatus } from "@/ontology/types"

function Chip({ status, onOpenSettings }: { status: OntologyRefreshStatus | null; onOpenSettings?: () => void }) {
  if (!status) return null

  // A skipped last-run means the refresh scanned nothing (no catalog allowlist).
  // Surface it distinctly (amber) with a one-click path to fix it, even when a
  // prior snapshot still backs the reads — otherwise the user gets no feedback.
  if (status.state === "skipped") {
    return (
      <span
        className="inline-flex items-center gap-1.5 rounded-full border border-warning/40 bg-warning/5 px-2.5 py-1 text-xs text-warning-foreground"
        title="The last refresh had no catalog scope, so it scanned nothing and kept the previous snapshot"
      >
        <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
        {status.message || "Last refresh scanned nothing — set a catalog allowlist in Settings."}
        {onOpenSettings && (
          <button
            type="button"
            onClick={onOpenSettings}
            className="ml-0.5 rounded font-semibold underline underline-offset-2 hover:opacity-80"
          >
            Set allowlist
          </button>
        )}
      </span>
    )
  }

  const live = status.source === "live"
  const label = status.message || (live ? "Live view" : "Updated recently")
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs ${
        live
          ? "border-default bg-elevated text-muted"
          : "border-success/30 bg-success/10 text-success-foreground"
      }`}
      title={live ? "Reading the estate live" : "Reading the most recent saved snapshot"}
    >
      {live ? <Zap className="h-3.5 w-3.5" /> : <RefreshCw className="h-3.5 w-3.5" />}
      {label}
    </span>
  )
}

export function FreshnessControls({ isAdmin, onOpenSettings }: { isAdmin: boolean; onOpenSettings?: () => void }) {
  const [status, setStatus] = useState<OntologyRefreshStatus | null>(null)
  const [busy, setBusy] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Initial freshness read (non-critical — the panels render regardless).
  useEffect(() => {
    let cancelled = false
    getRefreshStatus()
      .then((s) => { if (!cancelled) setStatus(s) })
      .catch(() => { /* ignore — chip just stays hidden */ })
    return () => {
      cancelled = true
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [])

  const running = busy || status?.state === "queued" || status?.state === "running"

  const startPolling = useCallback(() => {
    if (pollRef.current) clearInterval(pollRef.current)
    pollRef.current = setInterval(async () => {
      try {
        const next = await getRefreshStatus()
        setStatus(next)
        if (next.state !== "running" && next.state !== "queued") {
          if (pollRef.current) clearInterval(pollRef.current)
          setBusy(false)
        }
      } catch {
        /* keep polling silently */
      }
    }, 4000)
  }, [])

  const onRefresh = async () => {
    setBusy(true)
    try {
      setStatus(await triggerRefresh())
      startPolling()
    } catch {
      setBusy(false)
    }
  }

  return (
    <div className="flex items-center gap-2">
      <Chip status={status} onOpenSettings={onOpenSettings} />
      {isAdmin && (
        <Button size="sm" variant="secondary" onClick={onRefresh} disabled={running}>
          {running ? (
            <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
          ) : (
            <RefreshCw className="mr-1 h-3.5 w-3.5" />
          )}
          {running ? "Refreshing…" : "Refresh ontology"}
        </Button>
      )}
    </div>
  )
}
