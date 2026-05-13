import { useEffect, useMemo, useRef, useState } from 'react'
import {
  AlertCircle, ChevronDown, ChevronUp, ExternalLink, Loader2, ThumbsDown, ThumbsUp, X,
} from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import { LoadingCard } from '@/watch/components/LoadingCard'
import { Stat } from '@/watch/components/Stat'
import { getCached, putCached, useCachedFetch } from '@/watch/lib/cache'
import { formatDate, formatInt } from '@/watch/lib/format'
import { genieSpaceUrl } from '@/watch/lib/genie'
import * as api from '@/watch/lib/api'
import type {
  FeedbackMessageComment,
  FeedbackSpaceRow,
  FeedbackTabResponse,
  HealthStatus,
} from '@/watch/types/api'

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
  const eventsRef = useRef<HTMLDivElement>(null)

  // Set the space filter (event feed narrows). On stacked layouts (below the
  // lg breakpoint where the events section sits below the rollup), smooth-
  // scroll the events into view. On lg+ side-by-side layouts the section is
  // already next to the rollup, so no scroll is needed and any scroll feels
  // like jitter.
  function selectSpace(spaceId: string) {
    setSpaceFilter(spaceId)
    const sideBySide =
      typeof window !== 'undefined' &&
      window.matchMedia('(min-width: 1024px)').matches
    if (sideBySide) return
    setTimeout(() => {
      eventsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 50)
  }

  const { data, error, loading } = useCachedFetch<FeedbackTabResponse>(
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

  // Twin headline charts: top 10 spaces by raw positive count and by raw
  // negative count within the selected time window. Bar length is the count
  // (normalized to the chart's max), so visualizations are volume-agnostic —
  // a low-feedback workspace gets short bars but the structure still reads.
  const topByPositive = useMemo(() => {
    if (!data) return []
    return [...data.per_space]
      .filter(s => s.positive > 0)
      .sort((a, b) => b.positive - a.positive)
      .slice(0, 10)
  }, [data])

  const topByNegative = useMemo(() => {
    if (!data) return []
    return [...data.per_space]
      .filter(s => s.negative > 0)
      .sort((a, b) => b.negative - a.negative)
      .slice(0, 10)
  }, [data])

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
        <div className="flex items-center gap-2">
          {loading && data && (
            <span className="flex items-center gap-1 text-xs text-muted">
              <Loader2 size={12} className="animate-spin" />
              Refreshing…
            </span>
          )}
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
            <CountChartCard
              title="Most positive feedback"
              subtitle={`Top 10 spaces by 👍 count over the last ${data.days} days. Click a row to filter the reviews below.`}
              rows={topByPositive}
              valueFn={s => s.positive}
              barColor="bg-emerald-500"
              labelColor="text-emerald-400"
              activeId={spaceFilter}
              onSelect={selectSpace}
              emptyText="No positive feedback in this window."
            />
            <CountChartCard
              title="Most negative feedback"
              subtitle={`Top 10 spaces by 👎 count over the last ${data.days} days. Click a row to filter the reviews below.`}
              rows={topByNegative}
              valueFn={s => s.negative}
              barColor="bg-red-500"
              labelColor="text-red-400"
              activeId={spaceFilter}
              onSelect={selectSpace}
              emptyText="No negative feedback in this window."
            />
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
          <Card className="overflow-hidden p-0">
            <div className="border-b border-default px-4 py-3">
              <h3 className="text-sm font-medium uppercase text-muted">By space</h3>
              <p className="mt-0.5 text-xs text-muted">
                Click a row to see that space's feedback. Click <ExternalLink size={10} className="inline" /> to open the space details.
              </p>
            </div>
            <table className="w-full text-sm">
              <thead className="border-b border-default bg-elevated text-left text-xs uppercase text-muted">
                <tr>
                  <th className="px-4 py-2">Space</th>
                  <Th onClick={() => toggleSort('positive')} active={sortKey === 'positive'} dir={sortDir}>Positive</Th>
                  <Th onClick={() => toggleSort('negative')} active={sortKey === 'negative'} dir={sortDir}>Negative</Th>
                  <Th onClick={() => toggleSort('neg_rate_pct')} active={sortKey === 'neg_rate_pct'} dir={sortDir}>Neg %</Th>
                  <Th onClick={() => toggleSort('last_feedback_at')} active={sortKey === 'last_feedback_at'} dir={sortDir}>Last</Th>
                  <th className="px-2 py-2" aria-label="Open space details"></th>
                </tr>
              </thead>
              <tbody>
                {sortedRollup?.map(r => {
                  const isActive = spaceFilter === r.space_id
                  return (
                  <tr
                    key={r.space_id}
                    tabIndex={0}
                    role="button"
                    aria-label={`Show feedback events for ${r.title || r.space_id}`}
                    className={`cursor-pointer border-t border-default/50 focus:outline-none ${
                      isActive
                        ? 'bg-accent/10 border-l-2 border-l-accent'
                        : 'hover:bg-elevated/50 focus:bg-elevated/50'
                    }`}
                    onClick={() => selectSpace(r.space_id)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault()
                        selectSpace(r.space_id)
                      }
                    }}
                  >
                    <td className="max-w-[200px] px-4 py-2">
                      <div className="truncate text-sm font-medium">{r.title || '(untitled)'}</div>
                      <div className="truncate font-mono text-xs text-muted">{r.space_id}</div>
                    </td>
                    <td className="px-4 py-2 tabular-nums text-emerald-400">{formatInt(r.positive)}</td>
                    <td className="px-4 py-2 tabular-nums text-red-400">{formatInt(r.negative)}</td>
                    <td className="px-4 py-2 tabular-nums">{r.neg_rate_pct.toFixed(1)}%</td>
                    <td className="px-4 py-2 text-muted">{formatDate(r.last_feedback_at)}</td>
                    <td className="px-2 py-2 text-right">
                      <button
                        type="button"
                        className="rounded p-1 text-muted hover:bg-elevated hover:text-fg"
                        aria-label={`Open ${r.title || r.space_id} details`}
                        title="Open space details"
                        onClick={(e) => { e.stopPropagation(); onOpenSpace(r.space_id) }}
                      >
                        <ExternalLink size={14} />
                      </button>
                    </td>
                  </tr>
                  )
                })}
                {sortedRollup && sortedRollup.length === 0 && (
                  <tr>
                    <td colSpan={6} className="p-6 text-center text-muted">
                      No spaces with feedback in this window.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </Card>

          <div ref={eventsRef}>
          <Card className="p-4">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-medium uppercase text-muted">Recent feedback</h3>
                {spaceFilter && (
                  <button
                    type="button"
                    onClick={() => setSpaceFilter(null)}
                    className="flex items-center gap-1 rounded border border-accent/40 bg-accent/10 px-2 py-0.5 text-xs text-accent hover:bg-accent/20"
                    aria-label="Clear space filter"
                    title="Clear space filter"
                  >
                    <span className="max-w-[140px] truncate">
                      {data.per_space.find(s => s.space_id === spaceFilter)?.title || spaceFilter.slice(0, 12)}
                    </span>
                    <X size={12} />
                  </button>
                )}
              </div>
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
                        {e.space_id && e.conversation_id && e.message_id && (
                          <EventComments
                            spaceId={e.space_id}
                            conversationId={e.conversation_id}
                            messageId={e.message_id}
                          />
                        )}
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
          </div>
          </div>
        </>
      )}
    </div>
  )
}

// Renders user-typed comments attached to a feedback event's Genie message.
// Fetched lazily via the Genie API when an event card is expanded; results
// are cached at module scope so re-expanding the same event is instant.
function EventComments({
  spaceId, conversationId, messageId,
}: {
  spaceId: string
  conversationId: string
  messageId: string
}) {
  const cacheKey = `feedback-comments:${spaceId}:${conversationId}:${messageId}`
  const cached = getCached<FeedbackMessageComment[]>(cacheKey)
  const [comments, setComments] = useState<FeedbackMessageComment[] | null>(cached ?? null)
  const [loading, setLoading] = useState<boolean>(cached === undefined)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (cached !== undefined) return
    let cancelled = false
    api.getFeedbackComments(spaceId, conversationId, messageId)
      .then(result => {
        if (cancelled) return
        putCached(cacheKey, result)
        setComments(result)
      })
      .catch(e => {
        if (cancelled) return
        setError(e instanceof Error ? e.message : String(e))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cacheKey])

  if (loading) {
    return (
      <p className="mb-2 flex items-center gap-1 text-xs text-muted">
        <Loader2 size={12} className="animate-spin" /> Loading comment…
      </p>
    )
  }
  if (error) {
    return <p className="mb-2 text-xs text-red-400">Failed to load comments: {error}</p>
  }
  if (!comments || comments.length === 0) return null
  return (
    <div className="mb-3 space-y-2">
      {comments.map(c => (
        <blockquote
          key={c.message_comment_id}
          className="rounded border-l-2 border-default bg-elevated/30 p-2 text-sm text-secondary"
        >
          <p className="whitespace-pre-wrap">{c.content}</p>
          <footer className="mt-1 text-xs text-muted">{formatDate(c.created_at)}</footer>
        </blockquote>
      ))}
    </div>
  )
}

// Horizontal-bar chart card used twice on the page (Most positive / Most
// negative). Bar length is value/max within the chart, so each chart is
// internally normalized and reads the same regardless of absolute volume.
function CountChartCard({
  title, subtitle, rows, valueFn, barColor, labelColor,
  activeId, onSelect, emptyText,
}: {
  title: string
  subtitle: string
  rows: FeedbackSpaceRow[]
  valueFn: (s: FeedbackSpaceRow) => number
  barColor: string
  labelColor: string
  activeId: string | null
  onSelect: (spaceId: string) => void
  emptyText: string
}) {
  const max = Math.max(1, ...rows.map(valueFn))
  return (
    <Card className="p-4">
      <div className="mb-3">
        <h3 className="text-sm font-medium uppercase text-muted">{title}</h3>
        <p className="mt-0.5 text-xs text-muted">{subtitle}</p>
      </div>
      <div className="space-y-1.5">
        {rows.length === 0 && (
          <p className="text-xs text-muted">{emptyText}</p>
        )}
        {rows.map(s => {
          const v = valueFn(s)
          const isActive = activeId === s.space_id
          return (
            <button
              type="button"
              key={s.space_id}
              onClick={() => onSelect(s.space_id)}
              aria-label={`Filter to ${s.title || s.space_id}`}
              className={`flex w-full items-center gap-3 rounded px-2 py-1.5 text-left text-xs transition-colors ${
                isActive ? 'bg-accent/10' : 'hover:bg-elevated/50'
              }`}
            >
              <span className="w-44 shrink-0 truncate" title={s.title || s.space_id}>
                {s.title || s.space_id.slice(0, 16)}
              </span>
              <div className="relative h-3 flex-1 rounded bg-elevated">
                <div
                  className={`absolute inset-y-0 left-0 rounded ${barColor}`}
                  style={{ width: `${(v / max) * 100}%` }}
                />
              </div>
              <span className={`w-12 shrink-0 text-right tabular-nums ${labelColor}`}>
                {v.toLocaleString()}
              </span>
            </button>
          )
        })}
      </div>
    </Card>
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
