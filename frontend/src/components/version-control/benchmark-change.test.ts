import { describe, it, expect } from 'vitest'
import { benchmarkChanged } from './benchmark-change'
const mk = (id: string, bm: string, parent: string | null) => ({
  version_id: id, parent_version_id: parent,
  fingerprints: { config: 'c', benchmark: bm, metadata: 'm', canonicalizer_version: 'v' },
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- partial fixture; only the fields under test matter
} as any)

describe('benchmarkChanged', () => {
  const parent = mk('p', 'BM1', null)
  const byId = (id: string) => (id === 'p' ? parent : undefined)
  it('true when the benchmark fingerprint differs from the parent', () => {
    expect(benchmarkChanged(mk('c', 'BM2', 'p'), byId)).toBe(true)
  })
  it('false when unchanged or no parent', () => {
    expect(benchmarkChanged(mk('c', 'BM1', 'p'), byId)).toBe(false)
    expect(benchmarkChanged(mk('c', 'BM2', null), byId)).toBe(false)
  })
})
