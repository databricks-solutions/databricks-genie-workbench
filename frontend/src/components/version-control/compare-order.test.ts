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
})
