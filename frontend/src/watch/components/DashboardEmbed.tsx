import { useEffect, useRef, useState } from 'react'
import { ExternalLink } from 'lucide-react'
import { DatabricksDashboard } from '@databricks/aibi-client'

import * as api from '@/watch/lib/api'

interface Props {
  dashboardId: string
  height?: number
}

/** Embed a published AI/BI Lakeview dashboard via the app-delegated flow.
 *
 *  The backend mints a short-lived scoped embed token (SP-issued, OAuth
 *  Rich Authorization Requests downscoped to this one dashboard) and
 *  the @databricks/aibi-client SDK uses it to render the dashboard.
 *
 *  This avoids the workspace-session-cookie dependency that the basic
 *  iframe embed had — works for users with third-party cookies disabled
 *  and never shows a Databricks login prompt inside the embed.
 */
export function DashboardEmbed({ dashboardId, height = 720 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const dashboardRef = useRef<DatabricksDashboard | null>(null)
  const [openUrl, setOpenUrl] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!dashboardId || !containerRef.current) return
    const container = containerRef.current
    let cancelled = false

    async function mount() {
      try {
        setLoading(true)
        setError(null)
        const cfg = await api.getDashboardEmbedConfig(dashboardId)
        if (cancelled) return

        setOpenUrl(
          `${cfg.workspace_url.replace(/\/+$/, '')}/sql/dashboardsv3/${cfg.dashboard_id}/published`,
        )

        container.innerHTML = ''
        const dashboard = new DatabricksDashboard({
          instanceUrl: cfg.workspace_url,
          workspaceId: cfg.workspace_id,
          dashboardId: cfg.dashboard_id,
          token: cfg.embed_token,
          container,
          getNewToken: async () => {
            const fresh = await api.getDashboardEmbedConfig(dashboardId)
            return fresh.embed_token
          },
        })
        dashboardRef.current = dashboard
        await dashboard.initialize()
        if (!cancelled) setLoading(false)
      } catch (e) {
        if (cancelled) return
        const msg = e instanceof Error ? e.message : String(e)
        setError(msg)
        setLoading(false)
      }
    }

    mount()
    return () => {
      cancelled = true
      const inst = dashboardRef.current as unknown as { destroy?: () => void } | null
      if (inst && typeof inst.destroy === 'function') inst.destroy()
      dashboardRef.current = null
      container.innerHTML = ''
    }
  }, [dashboardId])

  if (!dashboardId) {
    return (
      <div className="rounded-lg border border-default bg-elevated p-6 text-sm text-muted">
        Set <code className="font-mono">DASHBOARD_COST_ID</code> in <code>app.yaml</code> to enable
        the workspace-wide Cost Explorer dashboard.
      </div>
    )
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between border-b border-default px-4 py-2">
        <p className="text-xs text-muted">
          Dashboard ID <code className="font-mono">{dashboardId}</code>
        </p>
        {openUrl && (
          <a
            href={openUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 rounded border border-default px-3 py-1 text-sm hover:bg-elevated"
          >
            <ExternalLink size={14} /> Open in Databricks
          </a>
        )}
      </div>
      {error ? (
        <div className="p-6 text-center text-sm text-muted">
          Could not load the embedded dashboard: <span className="font-mono">{error}</span>.
          Click <strong>Open in Databricks</strong> above to view it in a new tab.
        </div>
      ) : (
        <div style={{ position: 'relative', width: '100%', height }}>
          {loading && (
            <div className="absolute inset-0 flex items-center justify-center text-sm text-muted">
              Loading dashboard…
            </div>
          )}
          <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
        </div>
      )}
    </div>
  )
}
