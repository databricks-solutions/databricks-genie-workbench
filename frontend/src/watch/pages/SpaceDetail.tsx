import { useState } from 'react'
import { ArrowLeft, AlertCircle, ExternalLink, Info, RefreshCw } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import * as api from '@/watch/lib/api'
import type {
  CostPerConversation, CostRollup, HealthStatus,
  ResourceUsage, SpaceSummary, UsageRollup,
} from '@/watch/types/api'
import { formatDate, formatInt, formatMs, formatUsd, formatDay } from '@/watch/lib/format'
import { invalidate, useCachedFetch } from '@/watch/lib/cache'
import { genieSpaceUrl } from '@/watch/lib/genie'
import { Stat } from '@/watch/components/Stat'
import { SimpleBars } from '@/watch/components/SimpleBars'
import { LoadingCard } from '@/watch/components/LoadingCard'
import { CostScopeNote } from '@/watch/components/CostScopeNote'

interface Props {
  spaceId: string
  onBack: () => void
}

export function SpaceDetail({ spaceId, onBack }: Props) {
  const space = useCachedFetch<SpaceSummary>(`space:${spaceId}`, () => api.getSpace(spaceId), [spaceId])
  const health = useCachedFetch<HealthStatus>('health', () => api.getHealth())
  const [refreshKey, setRefreshKey] = useState(0)
  const [refreshing, setRefreshing] = useState(false)

  function refreshAll() {
    setRefreshing(true)
    // Clear this space's cached tab data, then bump refreshKey (a fetch dep on
    // every tab) so the kept-alive tabs re-fetch against the now-empty cache.
    for (const prefix of ['usage:', 'cost:', 'cost-conv:', 'resources:']) {
      invalidate(`${prefix}${spaceId}`)
    }
    space.reload()
    setRefreshKey(k => k + 1)
    // Tabs re-fetch in the background (each owns its own loading state); spin
    // the button briefly so the click gives immediate feedback, mirroring the
    // Spaces list refresh.
    setTimeout(() => setRefreshing(false), 800)
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <Button variant="ghost" onClick={onBack} className="gap-1">
          <ArrowLeft size={16} /> Back to spaces
        </Button>
        <Button variant="outline" onClick={refreshAll} disabled={refreshing} className="gap-1">
          <RefreshCw className={refreshing ? 'animate-spin' : ''} size={14} /> Refresh
        </Button>
      </div>

      {space.error && (
        <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{space.error}</Card>
      )}

      {space.data && (
        <>
          <div>
            <div className="flex items-baseline gap-3">
              <h1 className="text-2xl font-semibold">{space.data.title || '(untitled)'}</h1>
              <a
                href={genieSpaceUrl(space.data.space_id, health.data?.workspace_host || null)}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1 text-sm text-muted hover:text-fg"
                title="Open Genie Space in Databricks"
              >
                <ExternalLink size={14} /> open in Databricks
              </a>
            </div>
            <p className="font-mono text-xs text-muted">{space.data.space_id}</p>
            {space.data.description && (
              <p className="mt-2 max-w-2xl text-sm text-muted">{space.data.description}</p>
            )}
          </div>

          <Tabs defaultValue="overview" className="space-y-4">
            <TabsList>
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="usage">Usage</TabsTrigger>
              <TabsTrigger value="cost">Cost</TabsTrigger>
              <TabsTrigger value="resources">Resources</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" keepAlive><Overview space={space.data} /></TabsContent>
            <TabsContent value="usage" keepAlive><UsageTab spaceId={spaceId} refreshKey={refreshKey} /></TabsContent>
            <TabsContent value="cost" keepAlive><CostTab spaceId={spaceId} refreshKey={refreshKey} /></TabsContent>
            <TabsContent value="resources" keepAlive><ResourcesTab spaceId={spaceId} refreshKey={refreshKey} /></TabsContent>
          </Tabs>
        </>
      )}
    </div>
  )
}

function Overview({ space }: { space: SpaceSummary }) {
  return (
    <div className="grid gap-4 md:grid-cols-2">
      <Card className="p-4">
        <h2 className="mb-2 text-sm font-medium uppercase text-muted">Owner</h2>
        <p className="font-medium">{space.owner_email || '—'}</p>
      </Card>
      <Card className="p-4">
        <h2 className="mb-2 text-sm font-medium uppercase text-muted">Permissions</h2>
        <div className="space-y-1">
          {space.permissions.length === 0 && <p className="text-sm text-muted">No ACL data.</p>}
          {space.permissions.map((p, i) => (
            <div key={i} className="flex items-center justify-between text-sm">
              <span className="truncate">{p.principal || '?'}</span>
              <Badge>{p.permission_level || '—'}</Badge>
            </div>
          ))}
        </div>
      </Card>
    </div>
  )
}

function UsageTab({ spaceId, refreshKey }: { spaceId: string; refreshKey: number }) {
  const { data, error: err } = useCachedFetch<UsageRollup>(
    `usage:${spaceId}:7`, () => api.getSpaceUsage(spaceId, 7), [spaceId, refreshKey],
  )

  if (err) return <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{err}</Card>
  if (!data) return <LoadingCard />


  return (
    <div className="space-y-4">
      <Card className="border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-400">
        <Info className="mr-1 inline" size={14} />
        Feedback events appear with up to 4 hours of audit-log latency. Query metrics typically lag 5–15 min.
      </Card>

      <div className="grid gap-3 sm:grid-cols-4">
        <Stat label="Queries (30d)" value={formatInt(data.total_queries)} />
        <Stat label="Errors (30d)" value={formatInt(data.total_errors)} />
        <Stat label="Distinct users" value={formatInt(data.distinct_users)} />
        <Stat
          label="Feedback"
          value={
            <span className="space-x-1">
              <span className="text-emerald-400">+{data.feedback.positive}</span>
              <span className="text-muted">/</span>
              <span className="text-red-400">−{data.feedback.negative}</span>
            </span>
          }
        />
      </div>

      <Card className="p-4">
        <h3 className="mb-2 text-sm font-medium uppercase text-muted">Daily queries</h3>
        <SimpleBars
          data={data.time_series.map(p => ({ x: formatDay(p.day), y: p.queries }))}
        />
      </Card>

      <Card className="p-4">
        <h3 className="mb-2 text-sm font-medium uppercase text-muted">Latency p95</h3>
        <SimpleBars
          data={data.time_series.map(p => ({ x: formatDay(p.day), y: p.p95_ms || 0 }))}
          formatY={formatMs as (n: number) => string}
        />
      </Card>

      {data.feedback.sample.length > 0 && (
        <Card className="p-4">
          <h3 className="mb-2 text-sm font-medium uppercase text-muted">Recent feedback</h3>
          <ul className="space-y-2 text-sm">
            {data.feedback.sample.map((f, i) => (
              <li key={i} className="rounded border border-default p-2">
                <div className="flex items-center justify-between">
                  <Badge
                    className={
                      (f.rating || '').toUpperCase() === 'POSITIVE'
                        ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
                        : 'bg-red-500/20 text-red-400 border-red-500/30'
                    }
                  >
                    {f.rating || '?'}
                  </Badge>
                  <span className="text-xs text-muted">
                    {formatDate(f.event_time)} · {f.user_email || '?'}
                  </span>
                </div>
                {f.comment && <p className="mt-1 text-muted">{f.comment}</p>}
              </li>
            ))}
          </ul>
        </Card>
      )}

      {data.conversations.length > 0 && (
        <Card className="p-4">
          <h3 className="mb-2 text-sm font-medium uppercase text-muted">
            Recent conversations <span className="text-xs">(cached)</span>
          </h3>
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase text-muted">
              <tr>
                <th className="py-1">User</th>
                <th>Messages</th>
                <th>Last activity</th>
              </tr>
            </thead>
            <tbody>
              {data.conversations.map(c => (
                <tr key={c.conversation_id} className="border-t border-default/50">
                  <td className="py-1.5">{c.user_email || '—'}</td>
                  <td>{c.message_count}</td>
                  <td className="text-muted">{formatDate(c.last_message_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}
    </div>
  )
}

function CostTab({ spaceId, refreshKey }: { spaceId: string; refreshKey: number }) {
  const { data, error: err } = useCachedFetch<CostRollup>(
    `cost:${spaceId}:7`, () => api.getSpaceCost(spaceId, 7), [spaceId, refreshKey],
  )
  // Per-conversation breakdown is heavier (correlates with audit logs).
  // It's loaded lazily — only when this tab is visible.
  const conversations = useCachedFetch<CostPerConversation[]>(
    `cost-conv:${spaceId}:7`,
    () => api.getCostPerConversation(spaceId, 7, 50),
    [spaceId, refreshKey],
  )

  if (err) return <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{err}</Card>
  if (!data) return <LoadingCard />


  return (
    <div className="space-y-4">
      <CostScopeNote />

      <Card className="border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-400">
        <AlertCircle className="mr-1 inline" size={14} />
        Cost is approximate. Databricks bills warehouses, not queries — we apportion warehouse-day cost
        by this space's share of warehouse query duration. See <code>docs/apportionment-caveat.md</code>.
      </Card>

      <div className="grid gap-3 sm:grid-cols-3">
        <Stat label={`Queries (${data.days}d)`} value={formatInt(data.total_query_count)} />
        <Stat label={`Approx cost total (SQL WH, ${data.days}d)`} value={formatUsd(data.total_approx_usd)} />
        <Stat label={`DBUs (${data.days}d)`} value={data.total_approx_dbus != null ? data.total_approx_dbus.toFixed(2) : '—'} />
      </div>

      <Card className="p-4">
        <h3 className="mb-2 text-sm font-medium uppercase text-muted">Daily approx cost (SQL WH)</h3>
        <SimpleBars
          data={data.time_series.map(p => ({ x: formatDay(p.day), y: p.approx_usd || 0 }))}
          formatY={(v: number) => formatUsd(v)}
        />
      </Card>

      <Card className="p-4">
        <h3 className="mb-2 text-sm font-medium uppercase text-muted">By warehouse</h3>
        <table className="w-full text-sm">
          <thead className="text-left text-xs uppercase text-muted">
            <tr>
              <th className="py-1">Warehouse</th>
              <th>Queries</th>
              <th>Approx cost (SQL WH)</th>
            </tr>
          </thead>
          <tbody>
            {data.by_warehouse.map(w => (
              <tr key={w.warehouse_id} className="border-t border-default/50">
                <td className="py-1.5 font-mono text-xs">{w.warehouse_id || '(unknown)'}</td>
                <td>{formatInt(w.query_count)}</td>
                <td>{formatUsd(w.approx_usd)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>

      <Card className="p-4">
        <h3 className="mb-2 text-sm font-medium uppercase text-muted">By conversation</h3>
        <p className="mb-3 text-xs text-muted">
          Correlated from <code>system.access.audit</code> events
          (<code>service_name='aibiGenie'</code>) within ±10 min of each query.
        </p>
        {conversations.error && (
          <p className="text-xs text-red-400">{conversations.error}</p>
        )}
        {!conversations.data && !conversations.error && (
          <p className="text-xs text-muted">Loading conversations…</p>
        )}
        {conversations.data && conversations.data.length === 0 && (
          <p className="text-xs text-muted">
            No audit events linked to queries in this window.
          </p>
        )}
        {conversations.data && conversations.data.length > 0 && (
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase text-muted">
              <tr>
                <th className="py-1">Conversation</th>
                <th>User</th>
                <th>Queries</th>
                <th>Last query</th>
                <th className="text-right">Approx cost (SQL WH)</th>
              </tr>
            </thead>
            <tbody>
              {conversations.data.map(c => (
                <tr key={c.conversation_id} className="border-t border-default/50">
                  <td className="py-1.5 font-mono text-xs">
                    {c.conversation_id.slice(0, 12)}…
                  </td>
                  <td className="text-muted">{c.user_email || '—'}</td>
                  <td>{formatInt(c.query_count)}</td>
                  <td className="text-muted">{formatDate(c.last_query_at)}</td>
                  <td className="text-right tabular-nums">{formatUsd(c.approx_usd)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
    </div>
  )
}

function ResourcesTab({ spaceId, refreshKey }: { spaceId: string; refreshKey: number }) {
  const { data, error: err } = useCachedFetch<ResourceUsage[]>(
    `resources:${spaceId}:7`, () => api.getSpaceResources(spaceId, 7), [spaceId, refreshKey],
  )

  if (err) return <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{err}</Card>
  if (!data) return <LoadingCard />


  return (
    <Card className="overflow-hidden p-0">
      <table className="w-full text-sm">
        <thead className="border-b border-default bg-elevated text-left text-xs uppercase text-muted">
          <tr>
            <th className="px-4 py-2">Resource</th>
            <th className="px-4 py-2">Kind</th>
            <th className="px-4 py-2">Source</th>
            <th className="px-4 py-2 text-right">Queries (30d)</th>
            <th className="px-4 py-2">Last used</th>
          </tr>
        </thead>
        <tbody>
          {data.map(r => (
            <tr key={r.full_name} className="border-t border-default/50">
              <td className="px-4 py-2 font-mono text-xs">{r.full_name}</td>
              <td className="px-4 py-2"><Badge>{r.kind}</Badge></td>
              <td className="px-4 py-2">
                <Badge
                  className={
                    r.source === 'both'
                      ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
                      : r.source === 'configured'
                        ? 'bg-blue-500/20 text-blue-400 border-blue-500/30'
                        : 'bg-amber-500/20 text-amber-400 border-amber-500/30'
                  }
                >
                  {r.source}
                </Badge>
              </td>
              <td className="px-4 py-2 text-right tabular-nums">{formatInt(r.query_count)}</td>
              <td className="px-4 py-2 text-muted">{formatDate(r.last_used)}</td>
            </tr>
          ))}
          {!data.length && (
            <tr><td colSpan={5} className="p-6 text-center text-muted">No resources detected.</td></tr>
          )}
        </tbody>
      </table>
    </Card>
  )
}




