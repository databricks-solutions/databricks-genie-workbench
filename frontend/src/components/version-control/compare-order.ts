import type { VersionSummary } from '@/types/version-control'

// Order two selected version ids chronologically (older first) using the loaded page, so a
// diff reads Before(older) → After(newer). Unknown/equal timestamps keep the given order.
export function chronoPair(ids: string[], items: VersionSummary[]): [string, string] {
  const at = (id: string) => Date.parse(items.find(v => v.version_id === id)?.observed_at ?? '')
  const [a, b] = ids
  return at(a) <= at(b) ? [a, b] : [b, a]
}
