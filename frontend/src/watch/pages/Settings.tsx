import { useEffect, useState } from 'react'

import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import * as api from '@/watch/lib/api'
import type { HealthStatus } from '@/watch/types/api'

export function Settings() {
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [savedMessage, setSavedMessage] = useState<string | null>(null)

  useEffect(() => {
    api.getHealth().then(setHealth).catch(() => setHealth(null))
  }, [])

  async function refreshCache() {
    setRefreshing(true); setSavedMessage(null); setError(null)
    try {
      const r = await api.refreshConversationCache()
      setSavedMessage(`Queued ${r.queued} agent sync(s) in the background.`)
    } catch (e) {
      setError(String(e))
    } finally {
      setRefreshing(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Settings</h1>
        <p className="text-sm text-muted">App health, eval mappings, and cache controls.</p>
      </div>

      <Card className="p-4">
        <h2 className="mb-3 text-sm font-medium uppercase text-muted">Health</h2>
        <div className="grid gap-2 text-sm sm:grid-cols-2">
          <Row k="Lakebase">
            <Badge
              className={
                health?.lakebase_available
                  ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
                  : 'bg-amber-500/20 text-amber-400 border-amber-500/30'
              }
            >
              {health?.lakebase_available ? 'connected' : 'in-memory fallback'}
            </Badge>
          </Row>
          <Row k="Warehouse"><span className="font-mono text-xs">{health?.warehouse_id || '—'}</span></Row>
        </div>
      </Card>

      <Card className="p-4">
        <h2 className="mb-3 text-sm font-medium uppercase text-muted">Genie conversation history</h2>
        <p className="mb-3 text-sm text-muted">
          Re-pulls each agent's conversations from the Genie API into the cache that powers the
          “Recent conversations” list on an agent's detail page. Runs in the background across every
          visible agent. Does not affect cost, usage, or feedback metrics — those are read live from
          Databricks system tables.
        </p>
        <Button onClick={refreshCache} disabled={refreshing}>
          {refreshing ? 'Syncing…' : 'Sync conversations'}
        </Button>
      </Card>

      {error && (
        <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{error}</Card>
      )}
      {savedMessage && (
        <Card className="border-emerald-500/30 bg-emerald-500/10 p-3 text-sm text-emerald-400">
          {savedMessage}
        </Card>
      )}
    </div>
  )
}

function Row({ k, children }: { k: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between border-b border-default/50 py-1">
      <span className="text-muted">{k}</span>
      {children}
    </div>
  )
}
