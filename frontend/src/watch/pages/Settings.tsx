import { useEffect, useState } from 'react'

import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import * as api from '@/watch/lib/api'
import type { EvalExperimentMapping, HealthStatus } from '@/watch/types/api'

export function Settings() {
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [spaceId, setSpaceId] = useState('')
  const [experimentId, setExperimentId] = useState('')
  const [mapping, setMapping] = useState<EvalExperimentMapping | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [savedMessage, setSavedMessage] = useState<string | null>(null)

  useEffect(() => {
    api.getHealth().then(setHealth).catch(() => setHealth(null))
  }, [])

  async function loadMapping() {
    setError(null); setMapping(null); setSavedMessage(null)
    if (!spaceId) return
    try {
      const m = await api.getEvalMapping(spaceId)
      if ('experiment_id' in m && m.experiment_id) {
        setMapping(m as EvalExperimentMapping)
        setExperimentId((m as EvalExperimentMapping).experiment_id)
      }
    } catch (e) {
      setError(String(e))
    }
  }

  async function save() {
    setError(null); setSavedMessage(null)
    try {
      const m = await api.setEvalMapping(spaceId, experimentId)
      setMapping(m)
      setSavedMessage('Saved.')
    } catch (e) {
      setError(String(e))
    }
  }

  async function clear() {
    setError(null); setSavedMessage(null)
    try {
      await api.deleteEvalMapping(spaceId)
      setMapping(null)
      setExperimentId('')
      setSavedMessage('Cleared.')
    } catch (e) {
      setError(String(e))
    }
  }

  async function refreshCache() {
    setRefreshing(true); setSavedMessage(null); setError(null)
    try {
      const r = await api.refreshConversationCache()
      setSavedMessage(`Queued ${r.queued} space sync(s) in the background.`)
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
          <Row k="Cost dashboard"><span className="font-mono text-xs">{health?.dashboard_cost_id || '—'}</span></Row>
        </div>
      </Card>

      <Card className="p-4">
        <h2 className="mb-3 text-sm font-medium uppercase text-muted">
          Map a Genie Space → MLflow experiment
        </h2>
        <div className="grid gap-2 sm:grid-cols-3">
          <Input
            value={spaceId}
            onChange={e => setSpaceId(e.target.value)}
            placeholder="Genie Space ID (32-char hex)"
          />
          <Input
            value={experimentId}
            onChange={e => setExperimentId(e.target.value)}
            placeholder="MLflow experiment ID"
          />
          <div className="flex gap-2">
            <Button variant="outline" onClick={loadMapping}>Lookup</Button>
            <Button onClick={save} disabled={!spaceId || !experimentId}>Save</Button>
            <Button variant="ghost" onClick={clear} disabled={!spaceId}>Clear</Button>
          </div>
        </div>
        {mapping && (
          <p className="mt-3 text-xs text-muted">
            Currently mapped: <code className="font-mono">{mapping.experiment_id}</code> by {mapping.created_by}
          </p>
        )}
      </Card>

      <Card className="p-4">
        <h2 className="mb-3 text-sm font-medium uppercase text-muted">Conversation cache</h2>
        <p className="mb-3 text-sm text-muted">
          Refreshes the conversation/message cache for every visible space. Runs in the background.
        </p>
        <Button onClick={refreshCache} disabled={refreshing}>
          {refreshing ? 'Queueing…' : 'Refresh now'}
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
