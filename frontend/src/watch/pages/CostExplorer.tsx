import { useMemo, useState } from 'react'
import { ExternalLink, RefreshCw } from 'lucide-react'

import { Card } from '@/components/ui/card'
import { Stat } from '@/watch/components/Stat'
import { LineChart } from '@/watch/components/LineChart'
import * as api from '@/watch/lib/api'
import type { CostTopSpender, HealthStatus, WorkspaceOverview } from '@/watch/types/api'
import { formatInt, formatUsd } from '@/watch/lib/format'
import { genieSpaceUrl } from '@/watch/lib/genie'
import { useCachedFetch } from '@/watch/lib/cache'

interface Props {
  onOpenSpace: (spaceId: string) => void
}

type SortKey = 'space_id' | 'workspace_name' | 'query_count' | 'approx_usd'

export function CostExplorer({ onOpenSpace }: Props) {
  const [days, setDays] = useState<number>(7)
  const [sortKey, setSortKey] = useState<SortKey>('approx_usd')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const { data: overview, loading: overviewLoading } = useCachedFetch<WorkspaceOverview>(
    `overview:${days}`,
    () => api.getOverview(days),
    [days],
  )
  const { data: top, loading: topLoading } = useCachedFetch<CostTopSpender[]>(
    `top:${days}:50`,
    () => api.getTopSpenders(days, 50),
    [days],
  )
  const { data: health } = useCachedFetch<HealthStatus>('health', () => api.getHealth())

  const sorted = useMemo(() => {
    if (!top) return null
    const dir = sortDir === 'asc' ? 1 : -1
    return [...top].sort((a, b) => {
      switch (sortKey) {
        case 'space_id':
          return ((a.title || '').localeCompare(b.title || '')) * dir
        case 'workspace_name': {
          const av = a.workspace_name ?? a.workspace_id ?? ''
          const bv = b.workspace_name ?? b.workspace_id ?? ''
          return av.localeCompare(bv) * dir
        }
        case 'query_count':
          return (a.query_count - b.query_count) * dir
        case 'approx_usd':
          return ((a.approx_usd ?? 0) - (b.approx_usd ?? 0)) * dir
      }
    })
  }, [top, sortKey, sortDir])

  function toggleSort(k: SortKey) {
    if (k === sortKey) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(k)
      setSortDir(k === 'space_id' || k === 'workspace_name' ? 'asc' : 'desc')
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Genie Agents Overview</h1>
          <p className="text-sm text-muted">
            Workspace-wide health for Genie Agents — KPIs, trends, and top agents.
          </p>
        </div>
        <div className="flex items-center gap-3">
          {(overviewLoading || topLoading) && (
            <span className="flex items-center gap-1 text-xs text-muted">
              <RefreshCw size={12} className="animate-spin" /> Updating…
            </span>
          )}
          <select
            value={days}
            onChange={e => setDays(Number(e.target.value))}
            className="rounded border border-default bg-elevated px-2 py-1 text-sm"
          >
            <option value={7}>last 7 days</option>
            <option value={30}>last 30 days</option>
            <option value={90}>last 90 days</option>
          </select>
        </div>
      </div>

      <div className={`grid grid-cols-2 gap-3 transition-opacity sm:grid-cols-3 lg:grid-cols-6 ${overviewLoading ? 'opacity-50' : ''}`}>
        <Stat label="Active agents" value={overview ? formatInt(overview.active_spaces) : '—'} />
        <Stat label="Queries" value={overview ? formatInt(overview.total_queries) : '—'} />
        <Stat label="Distinct users" value={overview ? formatInt(overview.distinct_users) : '—'} />
        <Stat label="Approx cost" value={overview ? formatUsd(overview.approx_usd) : '—'} />
        <Stat label="Pos. feedback" value={overview ? formatInt(overview.feedback_pos) : '—'} />
        <Stat label="Neg. feedback" value={overview ? formatInt(overview.feedback_neg) : '—'} />
      </div>

      <Card className={`p-4 transition-opacity ${overviewLoading ? 'opacity-50' : ''}`}>
        <h2 className="mb-3 text-sm font-medium uppercase text-muted">
          Daily query volume — last {days} days
        </h2>
        {overview
          ? <LineChart data={overview.daily.map(d => ({ x: d.day, y: d.queries }))} formatY={formatInt} />
          : <p className="text-sm text-muted">Loading…</p>}
      </Card>

      <Card className={`overflow-hidden p-0 transition-opacity ${topLoading ? 'opacity-50' : ''}`}>
        <h2 className="border-b border-default px-4 py-3 text-sm font-medium uppercase text-muted">
          Top spending agents — drill-down
        </h2>
        <table className="w-full text-sm">
          <thead className="border-b border-default bg-elevated text-left text-xs uppercase text-muted">
            <tr>
              <Th onClick={() => toggleSort('space_id')} active={sortKey === 'space_id'} dir={sortDir}>
                Agent
              </Th>
              <Th onClick={() => toggleSort('workspace_name')} active={sortKey === 'workspace_name'} dir={sortDir}>
                Workspace
              </Th>
              <Th onClick={() => toggleSort('query_count')} active={sortKey === 'query_count'} dir={sortDir} align="right">
                Queries
              </Th>
              <Th onClick={() => toggleSort('approx_usd')} active={sortKey === 'approx_usd'} dir={sortDir} align="right">
                Approx USD
              </Th>
              <th className="px-4 py-2 w-8" />
            </tr>
          </thead>
          <tbody>
            {sorted?.map(s => (
              <tr
                key={s.space_id}
                className="cursor-pointer border-t border-default/50 hover:bg-elevated/50"
              >
                <td className="px-4 py-2" onClick={() => onOpenSpace(s.space_id)}>
                  <div className="font-medium">{s.title || '(untitled)'}</div>
                  <div className="font-mono text-xs text-muted">{s.space_id.slice(0, 12)}…</div>
                </td>
                <td
                  className="px-4 py-2 text-xs"
                  title={s.workspace_id ?? undefined}
                  onClick={() => onOpenSpace(s.space_id)}
                >
                  {s.workspace_name ?? (
                    s.workspace_id
                      ? <span className="font-mono text-muted">{s.workspace_id}</span>
                      : <span className="text-muted">—</span>
                  )}
                </td>
                <td className="px-4 py-2 text-right tabular-nums" onClick={() => onOpenSpace(s.space_id)}>
                  {formatInt(s.query_count)}
                </td>
                <td className="px-4 py-2 text-right tabular-nums" onClick={() => onOpenSpace(s.space_id)}>
                  {formatUsd(s.approx_usd)}
                </td>
                <td className="px-2 py-2 text-right">
                  <a
                    href={genieSpaceUrl(s.space_id, health?.workspace_host ?? null)}
                    target="_blank"
                    rel="noreferrer"
                    onClick={e => e.stopPropagation()}
                    title="Open Genie Agent in Databricks"
                    className="inline-flex items-center text-muted hover:text-fg"
                  >
                    <ExternalLink size={14} />
                  </a>
                </td>
              </tr>
            ))}
            {sorted && !sorted.length && (
              <tr><td colSpan={5} className="p-6 text-center text-muted">No cost data yet.</td></tr>
            )}
            {!sorted && (
              <tr><td colSpan={5} className="p-6 text-center text-muted">Loading…</td></tr>
            )}
          </tbody>
        </table>
      </Card>
    </div>
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
