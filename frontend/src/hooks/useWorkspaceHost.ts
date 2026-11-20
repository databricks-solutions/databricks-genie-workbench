import { useEffect, useState } from "react"
import { getSettings } from "@/lib/api"

// Resolved once per page load and shared by every caller.
let hostPromise: Promise<string | null> | null = null

function loadWorkspaceHost(): Promise<string | null> {
  if (!hostPromise) {
    hostPromise = getSettings()
      .then((s) => s.databricks_host || null)
      .catch(() => {
        hostPromise = null // allow a retry on the next mount
        return null
      })
  }
  return hostPromise
}

/** Databricks workspace host (e.g. https://x.cloud.databricks.com), or null until known. */
export function useWorkspaceHost(): string | null {
  const [host, setHost] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    loadWorkspaceHost().then((h) => { if (!cancelled) setHost(h) })
    return () => { cancelled = true }
  }, [])

  return host
}
