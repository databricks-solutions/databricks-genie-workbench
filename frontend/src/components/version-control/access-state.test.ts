import { describe, expect, it } from 'vitest'
import { ApiError } from '@/lib/api'
import { canEditVersions, canReadVersions, resolveVcAccess, vcAccessFromLevel } from './access-state'

describe('version control access', () => {
  it('maps the level Genie grants onto what the tab may do', () => {
    expect(vcAccessFromLevel('manage')).toBe('edit')
    expect(vcAccessFromLevel('edit')).toBe('edit')
    expect(vcAccessFromLevel('view')).toBe('view')
    expect(vcAccessFromLevel(null)).toBe('none')
    for (const access of ['view', 'none', 'unknown', 'checking'] as const) expect(canEditVersions(access)).toBe(false)
    for (const access of ['none', 'checking'] as const) expect(canReadVersions(access)).toBe(false)
  })

  it('reads the level from the access route', async () => {
    await expect(resolveVcAccess('s', async () => ({ space_id: 's', level: 'view' })))
      .resolves.toEqual({ spaceId: 's', access: 'view', reason: null })
  })

  it('treats a 403 or 404 as no access and keeps the reason Genie gave', async () => {
    const refusal = new ApiError('You need the aclPath entitlement: /sqlanalytics', 403,
      { code: 'space_access_entitlement_missing' })
    await expect(resolveVcAccess('s', async () => { throw refusal }))
      .resolves.toEqual({ spaceId: 's', access: 'none', reason: 'You need the aclPath entitlement: /sqlanalytics' })
    const missing = new ApiError('Genie Agent not found, or you cannot see it.', 404)
    expect((await resolveVcAccess('s', async () => { throw missing })).access).toBe('none')
  })

  it('never grants a write when the question went unanswered', async () => {
    const outage = new ApiError('Could not verify your access to this Genie Agent. Try again shortly.', 503)
    const state = await resolveVcAccess('s', async () => { throw outage })
    expect(state.access).toBe('unknown')
    expect(canEditVersions(state.access)).toBe(false)
  })
})
