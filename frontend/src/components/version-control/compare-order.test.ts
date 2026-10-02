import { describe, it, expect } from 'vitest'
import { chronoPair } from './compare-order'
import type { VersionSummary } from '@/types/version-control'

const v = (id: string, iso: string) => ({ version_id: id, observed_at: iso } as VersionSummary)

describe('chronoPair', () => {
  it('returns older id first regardless of selection order', () => {
    const items = [v('new', '2026-09-12T10:00:00Z'), v('old', '2026-09-12T08:00:00Z')]
    expect(chronoPair(['new', 'old'], items)).toEqual(['old', 'new'])
    expect(chronoPair(['old', 'new'], items)).toEqual(['old', 'new'])
  })

  it('preserves input order when a timestamp is missing/unparseable', () => {
    const items = [v('a', 'not-a-date'), v('b', '2026-09-12T08:00:00Z')]
    expect(chronoPair(['a', 'b'], items)).toEqual(['a', 'b'])
    expect(chronoPair(['b', 'a'], items)).toEqual(['b', 'a'])
  })

  it('preserves input order when an id is absent from items', () => {
    const items = [v('known', '2026-09-12T08:00:00Z')]
    expect(chronoPair(['known', 'ghost'], items)).toEqual(['known', 'ghost'])
    expect(chronoPair(['ghost', 'known'], items)).toEqual(['ghost', 'known'])
  })

  it('preserves input order when timestamps are equal', () => {
    const items = [v('x', '2026-09-12T09:00:00Z'), v('y', '2026-09-12T09:00:00Z')]
    expect(chronoPair(['x', 'y'], items)).toEqual(['x', 'y'])
    expect(chronoPair(['y', 'x'], items)).toEqual(['y', 'x'])
  })
})
