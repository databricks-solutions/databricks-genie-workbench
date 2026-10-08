import { describe, expect, it } from 'vitest'
import { canEditVersions, canReadVersions, vcAccessFromSpace } from './access-state'

describe('version control access', () => {
  it('reads Manage as Edit and passes every other state through', () => {
    expect(vcAccessFromSpace('manage')).toBe('edit')
    for (const a of ['checking', 'edit', 'view', 'none', 'unknown'] as const) expect(vcAccessFromSpace(a)).toBe(a)
  })

  it('grants writes only to edit, and reads to edit, view, and unknown', () => {
    for (const access of ['view', 'none', 'unknown', 'checking'] as const) expect(canEditVersions(access)).toBe(false)
    expect(canEditVersions('edit')).toBe(true)
    for (const access of ['none', 'checking'] as const) expect(canReadVersions(access)).toBe(false)
    for (const access of ['edit', 'view', 'unknown'] as const) expect(canReadVersions(access)).toBe(true)
  })
})
