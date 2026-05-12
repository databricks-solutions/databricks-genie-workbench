import { useMemo, useState } from 'react'
import {
  AlertCircle, ChevronDown, ChevronUp, ExternalLink, ThumbsDown, ThumbsUp,
} from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import { LoadingCard } from '@/watch/components/LoadingCard'
import { SimpleBars } from '@/watch/components/SimpleBars'
import { Stat } from '@/watch/components/Stat'
import { useCachedFetch } from '@/watch/lib/cache'
import { formatDate, formatDay, formatInt } from '@/watch/lib/format'
import { genieSpaceUrl } from '@/watch/lib/genie'
import * as api from '@/watch/lib/api'
import type { FeedbackTabResponse, HealthStatus } from '@/watch/types/api'

interface Props {
  onOpenSpace: (spaceId: string) => void
}

type SortKey =
  | 'negative'
  | 'positive'
  | 'total'
  | 'neg_rate_pct'
  | 'last_feedback_at'
type RatingFilter = 'all' | 'POSITIVE' | 'NEGATIVE'

export function Feedback({ onOpenSpace }: Props) {
  const [days, setDays] = useState<7 | 30 | 90>(7)
  const [ratingFilter, setRatingFilter] = useState<RatingFilter>('all')
  const [spaceFilter, setSpaceFilter] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('negative')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [expandedKey, setExpandedKey] = useState<string | null>(null)

  const { data, error } = useCachedFetch<FeedbackTabResponse>(
    `feedback:${days}`,
    () => api.getFeedback(days),
    [days],
  )
  const { data: health } = useCachedFetch<HealthStatus>('health', () => api.getHealth())

  const sortedRollup = useMemo(() => {
    if (!data) return null
    const dir = sortDir === 'asc' ? 1 : -1
    return [...data.per_space].sort((a, b) => {
      switch (sortKey) {
        case 'negative':
          return (a.negative - b.negative) * dir
        case 'positive':
          return (a.positive - b.positive) * dir
        case 'total':
          return (a.total - b.total) * dir
        case 'neg_rate_pct':
          return (a.neg_rate_pct - b.neg_rate_pct) * dir
        case 'last_feedback_at': {
          const av = a.last_feedback_at ? new Date(a.last_feedback_at).getTime() : 0
          const bv = b.last_feedback_at ? new Date(b.last_feedback_at).getTime() : 0
          return (av - bv) * dir
        }
      }
    })
  }, [data, sortKey, sortDir])

  const filteredEvents = useMemo(() => {
    if (!data) return null
    return data.events.filter(e => {
      if (ratingFilter !== 'all' && (e.rating || '').toUpperCase() !== ratingFilter) return false
      if (spaceFilter && e.space_id !== spaceFilter) return false
      return true
    })
  }, [data, ratingFilter, spaceFilter])

  function toggleSort(k: SortKey) {
    if (k === sortKey) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(k)
      setSortDir('desc')
    }
  }

  if (error) {
    return (
      <Card className="border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">
        {error}
      </Card>
    )
  }
  if (!data) return <LoadingCard />

  const hasFeedback = data.summary.total > 0

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Feedback</h1>
          <p className="text-sm text-muted">
            Workspace-wide thumbs up/down on Genie Space responses.
          </p>
        </div>
        <select
          value={days}
          onChange={e => { setDays(Number(e.target.value) as 7 | 30 | 90); setSpaceFilter(null) }}
          className="rounded border border-default bg-elevated px-2 py-1 text-sm"
        >
          <option value={7}>last 7 days</option>
          <option value={30}>last 30 days</option>
          <option value={90}>last 90 days</option>
        </select>
      </div>

      {!hasFeedback && (
        <Card className="p-6 text-center">
          <p className="font-medium">No feedback recorded in the last {data.days} days.</p>
          <p className="mt-2 text-xs text-muted">
            Feedback appears here after end users react to Genie responses with thumbs up or
            thumbs down. Audit events lag up to 4 hours.
          </p>
        </Card>
      )}

      {hasFeedback && (
        <>
          <Card className="border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-400">
            <AlertCircle className="mr-1 inline" size={14} />
            Feedback events appear with up to 4 hours of audit-log latency.
          </Card>

          <div className="grid gap-3 sm:grid-cols-4">
            <Stat label={`Total (${data.days}d)`} value={formatInt(data.summary.total)} />
            <Stat
              label="Positive"
              value={<span className="text-emerald-400">{formatInt(data.summary.positive)}</span>}
            />
            <Stat
              label="Negative"
              value={<span className="text-red-400">{formatInt(data.summary.negative)}</span>}
            />
            <Stat label="Negative rate" value={`${data.summary.neg_rate_pct.toFixed(1)}%`} />
          </div>

          <div className="grid gap-3 md:grid-cols-2">
            <Card className="p-4">
              <h3 className="mb-2 text-sm font-medium uppercase text-muted">Daily positive</h3>
              <SimpleBars
                data={data.trend.map(p => ({ x: formatDay(p.day), y: p.positive }))}
                colorClass="bg-emerald-500"
              />
            </Card>
            <Card className="p-4">
              <h3 className="mb-2 text-sm font-medium uppercase text-muted">Daily negative</h3>
              <SimpleBars
                data={data.trend.map(p => ({ x: formatDay(p.day), y: p.negative }))}
                colorClass="bg-red-500"
              />
            </Card>
          </div>

          <Card className="overflow-hidden p-0">
            <div className="border-b border-default px-4 py-3">
              <h3 className="text-sm font-medium uppercase text-muted">By space</h3>
            </div>
            <table className="w-full text-sm">
              <thead className="border-b border-default bg-elevated text-left text-xs uppercase text-muted">
                <tr>
                  <th className="px-4 py-2">Space</th>
                  <th className="px-4 py-2">Owner</th>
                  <Th onClick={() => toggleSort('positive')} active={sortKey === 'positive'} dir={sortDir}>Positive</Th>
                  <Th onClick={() => toggleSort('negative')} active={sortKey === 'negative'} dir={sortDir}>Negative</Th>
                  <Th onClick={() => toggleSort('total')} active={sortKey === 'total'} dir={sortDir}>Total</Th>
                  <Th onClick={() => toggleSort('neg_rate_pct')} active={sortKey === 'neg_rate_pct'} dir={sortDir}>Neg %</Th>
                  <Th onClick={() => toggleSort('last_feedback_at')} active={sortKey === 'last_feedback_at'} dir={sortDir}>Last</Th>
                </tr>
              </thead>
              <tbody>
                {sortedRollup?.map(r => (
                  <tr
                    key={r.space_id}
                    tabIndex={0}
                    role="button"
                    className="cursor-pointer border-t border-default/50 hover:bg-elevated/50 focus:bg-elevated/50 focus:outline-none"
                    onClick={() => onOpenSpace(r.space_id)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault()
                        onOpenSpace(r.space_id)
                      }
                    }}
                  >
                    <td className="px-4 py-2">
                      <div className="text-sm font-medium">{r.title || '(untitled)'}</div>
                      <div className="font-mono text-xs text-muted">{r.space_id}</div>
                    </td>
                    <td className="px-4 py-2 text-muted">{r.owner_email || '—'}</td>
                    <td className="px-4 py-2 tabular-nums text-emerald-400">{formatInt(r.positive)}</td>
                    <td className="px-4 py-2 tabular-nums text-red-400">{formatInt(r.negative)}</td>
                    <td className="px-4 py-2 tabular-nums">{formatInt(r.total)}</td>
                    <td className="px-4 py-2 tabular-nums">{r.neg_rate_pct.toFixed(1)}%</td>
                    <td className="px-4 py-2 text-muted">{formatDate(r.last_feedback_at)}</td>
                  </tr>
                ))}
                {sortedRollup && sortedRollup.length === 0 && (
                  <tr>
                    <td colSpan={7} className="p-6 text-center text-muted">
                      No spaces with feedback in this window.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </Card>

          <Card className="p-4">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <h3 className="text-sm font-medium uppercase text-muted">Recent feedback</h3>
              <div className="flex flex-wrap items-center gap-2">
                <RatingButton current={ratingFilter} value="all" onClick={setRatingFilter}>
                  All
                </RatingButton>
                <RatingButton current={ratingFilter} value="POSITIVE" onClick={setRatingFilter}>
                  <ThumbsUp size={12} /> Positive
                </RatingButton>
                <RatingButton current={ratingFilter} value="NEGATIVE" onClick={setRatingFilter}>
                  <ThumbsDown size={12} /> Negative
                </RatingButton>
                <select
                  value={spaceFilter ?? ''}
                  onChange={e => setSpaceFilter(e.target.value || null)}
                  className="rounded border border-default bg-elevated px-2 py-1 text-xs"
                >
                  <option value="">All spaces</option>
                  {data.per_space.map(s => (
                    <option key={s.space_id} value={s.space_id}>
                      {s.title || s.space_id.slice(0, 12)}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <ul className="space-y-2">
              {filteredEvents?.map((e, i) => {
                const key = `${e.space_id ?? ''}|${e.message_id ?? e.event_time}|${i}`
                const expanded = expandedKey === key
                const isPositive = (e.rating || '').toUpperCase() === 'POSITIVE'
                return (
                  <li key={key} className="rounded border border-default">
                    <button
                      type="button"
                      aria-expanded={expanded}
                      className="flex w-full items-start gap-3 p-3 text-left hover:bg-elevated/30"
                      onClick={() => setExpandedKey(expanded ? null : key)}
                    >
                      <Badge
                        className={
                          isPositive
                            ? 'border-emerald-500/30 bg-emerald-500/20 text-emerald-400'
                            : 'border-red-500/30 bg-red-500/20 text-red-400'
                        }
                      >
                        {isPositive ? <ThumbsUp size={12} /> : <ThumbsDown size={12} />}
                      </Badge>
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center justify-between text-xs">
                          <span className="truncate">
                            <span className="font-medium">
                              {e.space_title || e.space_id?.slice(0, 12) || '?'}
                            </span>
                            <span className="ml-2 text-muted">
                              {e.user_email || 'unknown user'}
                            </span>
                          </span>
                          <span className="ml-2 shrink-0 text-muted">{formatDate(e.event_time)}</span>
                        </div>
                        {e.comment && (
                          <p className="mt-1 text-sm text-muted">
                            {expanded
                              ? e.comment
                              : e.comment.slice(0, 120) + (e.comment.length > 120 ? '…' : '')}
                          </p>
                        )}
                      </div>
                      {expanded ? (
                        <ChevronUp size={16} className="text-muted" />
                      ) : (
                        <ChevronDown size={16} className="text-muted" />
                      )}
                    </button>
                    {expanded && (
                      <div className="border-t border-default px-3 py-2 text-xs">
                        <div className="grid gap-1 sm:grid-cols-2">
                          <div>
                            <span className="text-muted">message_id:</span>{' '}
                            <code className="font-mono">{e.message_id || '—'}</code>
                          </div>
                          <div>
                            <span className="text-muted">conversation_id:</span>{' '}
                            <code className="font-mono">{e.conversation_id || '—'}</code>
                          </div>
                        </div>
                        {e.space_id && (
                          <a
                            href={genieSpaceUrl(e.space_id, health?.workspace_host ?? null)}
                            target="_blank"
                            rel="noreferrer"
                            className="mt-2 inline-flex items-center gap-1 text-xs text-accent hover:underline"
                          >
                            <ExternalLink size={12} /> Open in Databricks
                          </a>
                        )}
                      </div>
                    )}
                  </li>
                )
              })}
              {filteredEvents && filteredEvents.length === 0 && (
                <li className="p-6 text-center text-muted">No events match these filters.</li>
              )}
            </ul>
          </Card>
        </>
      )}
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

function RatingButton({
  current,
  value,
  onClick,
  children,
}: {
  current: RatingFilter
  value: RatingFilter
  onClick: (v: RatingFilter) => void
  children: React.ReactNode
}) {
  const active = current === value
  return (
    <button
      type="button"
      onClick={() => onClick(value)}
      className={`flex items-center gap-1 rounded border px-2 py-1 text-xs ${
        active
          ? 'border-accent bg-accent/10 text-accent'
          : 'border-default bg-elevated text-muted hover:text-fg'
      }`}
    >
      {children}
    </button>
  )
}
