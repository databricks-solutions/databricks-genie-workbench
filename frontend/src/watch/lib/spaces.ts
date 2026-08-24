import type { SpaceListItem } from '@/watch/types/api'

export function filterSpaces(spaces: SpaceListItem[], query: string): SpaceListItem[] {
  const q = query.trim().toLowerCase()
  if (!q) return spaces
  return spaces.filter(s =>
    (s.title || '').toLowerCase().includes(q) ||
    s.permissions.some(p => (p.principal || '').toLowerCase().includes(q)) ||
    s.space_id.toLowerCase().includes(q),
  )
}

export function buildSpacesCsv(spaces: SpaceListItem[]): string {
  const header = [
    'space_id', 'title', 'managers', 'queries_7d', 'cost_sql_wh_7d_usd',
    'feedback_pos_7d', 'feedback_neg_7d', 'last_query_at',
  ]
  const rows = spaces.map(s => [
    s.space_id, s.title || '', s.permissions.map(p => p.principal).filter(Boolean).join('; '),
    s.queries_7d, s.cost_7d_usd,
    s.feedback_pos_7d, s.feedback_neg_7d, s.last_query_at || '',
  ])
  return [header, ...rows]
    .map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(','))
    .join('\n')
}
