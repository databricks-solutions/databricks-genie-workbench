import { useMemo, useState } from 'react'
import { Search, RefreshCw, AlertTriangle } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import * as api from '@/watch/lib/api'
import type { HealthStatus, SpaceListItem, SpacePermission } from '@/watch/types/api'
import { formatInt, formatUsd, relativeTime } from '@/watch/lib/format'
import { invalidate, useCachedFetch } from '@/watch/lib/cache'
import { genieSpaceUrl } from '@/watch/lib/genie'
import { buildSpacesCsv, filterSpaces } from '@/watch/lib/spaces'

interface Props {
  onOpenSpace: (spaceId: string) => void
}

type SortKey = 'title' | 'queries_7d' | 'cost_7d_usd' | 'feedback' | 'last_query_at'

export function SpacesList({ onOpenSpace }: Props) {
  const [days, setDays] = useState<number>(7)
  const spaces = useCachedFetch<SpaceListItem[]>(
    `watch:spaces:${days}`,
    () => api.listSpaces(days),
    [days],
  )
  const health = useCachedFetch<HealthStatus>('watch:health', () => api.getHealth())
  const data = spaces.data ?? null
  const error = spaces.error
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('cost_7d_usd')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [refreshing, setRefreshing] = useState(false)

  async function onRefresh() {
    setRefreshing(true)
    try {
      invalidate('watch:spaces')
      invalidate('watch:cost:')
      invalidate('watch:usage:')
      invalidate('watch:resources:')
      await api.refreshSpaces()
      spaces.reload()
    } finally {
      setRefreshing(false)
    }
  }

  const filtered = useMemo(() => {
    if (!data) return []
    const q = search.trim().toLowerCase()
    const list = filterSpaces(data, q)
    const sorted = [...list].sort((a, b) => {
      const dir = sortDir === 'asc' ? 1 : -1
      switch (sortKey) {
        case 'title':
          return ((a.title || '').localeCompare(b.title || '')) * dir
        case 'queries_7d':
          return (a.queries_7d - b.queries_7d) * dir
        case 'cost_7d_usd':
          return (a.cost_7d_usd - b.cost_7d_usd) * dir
        case 'feedback':
          return (
            ((a.feedback_pos_7d - a.feedback_neg_7d) -
              (b.feedback_pos_7d - b.feedback_neg_7d)) *
            dir
          )
        case 'last_query_at':
          return (
            (new Date(a.last_query_at || 0).getTime() -
              new Date(b.last_query_at || 0).getTime()) *
            dir
          )
      }
    })
    return sorted
  }, [data, search, sortKey, sortDir])

  function toggleSort(k: SortKey) {
    if (k === sortKey) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(k)
      setSortDir('desc')
    }
  }

  function exportCsv() {
    if (!data) return
    const csv = buildSpacesCsv(filtered)
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `geniewatch-agents-${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Genie Agents</h1>
          <p className="text-sm text-muted">
            {filtered.length} agent{filtered.length === 1 ? '' : 's'} visible to you · last {days}-day cost & usage
          </p>
        </div>
        <div className="flex gap-2">
          <select
            value={days}
            onChange={e => setDays(Number(e.target.value))}
            className="rounded border border-default bg-elevated px-2 py-1 text-sm"
            title="Time window for queries / cost / feedback"
          >
            <option value={7}>last 7 days</option>
            <option value={30}>last 30 days</option>
            <option value={90}>last 90 days</option>
          </select>
          <Button variant="outline" onClick={() => void onRefresh()} disabled={refreshing}>
            <RefreshCw className={refreshing ? 'animate-spin' : ''} size={16} />
            Refresh
          </Button>
          <Button variant="outline" onClick={exportCsv} disabled={!filtered.length}>
            Export CSV
          </Button>
        </div>
      </div>

      <div className="relative">
        <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-muted" />
        <Input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search by name, manager, or agent ID"
          className="pl-9"
        />
      </div>

      {health.data?.system_tables_accessible === false && (
        <Card className="flex items-start gap-2 border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-400">
          <AlertTriangle size={16} className="mt-0.5 shrink-0" />
          <span>
            Cost, usage, feedback, and lineage come from Databricks <code className="font-mono">system.*</code> tables,
            which the app service principal can't currently read — so those panels will be empty
            (not necessarily "no activity"). A workspace admin must grant the required
            {' '}<code className="font-mono">SELECT</code>s (see <code className="font-mono">scripts/grant_permissions.py</code>).
            Agent and permission data still load.
          </span>
        </Card>
      )}

      {error && (
        <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">
          {error}
        </Card>
      )}

      <Card className="overflow-hidden p-0">
        <table className="w-full text-sm">
          <thead className="border-b border-default bg-elevated text-left text-xs uppercase text-muted">
            <tr>
              <Th onClick={() => toggleSort('title')} active={sortKey === 'title'} dir={sortDir}>Agent</Th>
              <Th>Managers</Th>
              <Th onClick={() => toggleSort('queries_7d')} active={sortKey === 'queries_7d'} dir={sortDir} align="right">Queries ({days}d)</Th>
              <Th onClick={() => toggleSort('cost_7d_usd')} active={sortKey === 'cost_7d_usd'} dir={sortDir} align="right">Cost (SQL WH, {days}d)</Th>
              <Th onClick={() => toggleSort('feedback')} active={sortKey === 'feedback'} dir={sortDir} align="right">Feedback ({days}d)</Th>
              <Th onClick={() => toggleSort('last_query_at')} active={sortKey === 'last_query_at'} dir={sortDir}>Last query</Th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(s => (
              <tr
                key={s.space_id}
                className="cursor-pointer border-b border-default/50 hover:bg-elevated/50"
                onClick={() => onOpenSpace(s.space_id)}
              >
                <td className="px-4 py-3">
                  <div className="font-medium">{s.title || '(untitled)'}</div>
                  <SpaceIdLink
                    spaceId={s.space_id}
                    workspaceHost={health.data?.workspace_host || null}
                  />
                </td>
                <td className="min-w-64 px-4 py-3"><ManagerSummary permissions={s.permissions} /></td>
                <td className="px-4 py-3 text-right tabular-nums">{formatInt(s.queries_7d)}</td>
                <td className="px-4 py-3 text-right tabular-nums">
                  {s.cost_7d_usd > 0 ? formatUsd(s.cost_7d_usd) : '—'}
                </td>
                <td className="px-4 py-3 text-right">
                  {s.feedback_pos_7d || s.feedback_neg_7d ? (
                    <span className="space-x-1">
                      <Badge className="bg-emerald-500/20 text-emerald-400 border-emerald-500/30">
                        +{s.feedback_pos_7d}
                      </Badge>
                      <Badge className="bg-red-500/20 text-red-400 border-red-500/30">
                        −{s.feedback_neg_7d}
                      </Badge>
                    </span>
                  ) : (
                    <span className="text-muted">—</span>
                  )}
                </td>
                <td className="px-4 py-3 text-muted">{relativeTime(s.last_query_at)}</td>
              </tr>
            ))}
            {data && !filtered.length && (
              <tr>
                <td colSpan={6} className="p-8 text-center text-muted">
                  No agents match this filter.
                </td>
              </tr>
            )}
            {!data && (
              <tr>
                <td colSpan={6} className="p-8 text-center text-muted">
                  Loading…
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>
    </div>
  )
}

export function SpaceIdLink({
  spaceId,
  workspaceHost,
}: {
  spaceId: string
  workspaceHost: string | null
}) {
  return (
    <a
      href={genieSpaceUrl(spaceId, workspaceHost)}
      target="_blank"
      rel="noreferrer"
      onClick={event => event.stopPropagation()}
      aria-label={`Open Genie Agent ${spaceId} in Databricks`}
      className="inline-block break-all rounded-sm font-mono text-xs text-accent underline-offset-2 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/40"
    >
      {spaceId}
    </a>
  )
}

export function ManagerSummary({ permissions }: { permissions: SpacePermission[] }) {
  if (!permissions.length) return <span className="text-muted">—</span>

  return (
    <ul
      className="space-y-1"
      aria-label={`${permissions.length} manager${permissions.length === 1 ? '' : 's'}`}
    >
      {permissions.map((permission, index) => (
        <li
          key={`${permission.principal}-${index}`}
          className="flex min-w-0 flex-wrap items-baseline gap-x-1.5 leading-tight"
        >
          <span className="min-w-0 break-words font-medium text-fg">
            {permission.principal || 'Unknown principal'}
          </span>
          <span className="whitespace-nowrap text-xs text-muted">
            {(permission.principal_type || 'principal').replaceAll('_', ' ')}
            {permission.inherited ? ' · inherited' : ''}
          </span>
        </li>
      ))}
    </ul>
  )
}

function Th({
  children, onClick, active, dir, align = 'left',
}: {
  children: React.ReactNode
  onClick?: () => void
  active?: boolean
  dir?: 'asc' | 'desc'
  align?: 'left' | 'right'
}) {
  return (
    <th
      className={`px-4 py-2 ${onClick ? 'cursor-pointer select-none hover:text-fg' : ''} ${
        align === 'right' ? 'text-right' : ''
      }`}
      onClick={onClick}
    >
      {children}
      {active ? <span className="ml-1">{dir === 'asc' ? '▲' : '▼'}</span> : null}
    </th>
  )
}
