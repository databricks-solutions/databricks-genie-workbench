import { describe, expect, it } from 'vitest'
import { ApiError, extractDetailMessage } from '@/lib/api'
import {
  accessFromLevel, canEditSpace, canReadSpace, describeScanError, resolveSpaceAccess, SCAN_NEEDS_EDIT,
} from './space-access'

const refusal = (status: number, code: string, message: string) =>
  new ApiError(message, status, { code, required: 'edit', message, platform_message: '' })

describe('space access', () => {
  it('keeps Manage distinct and reads a null level as none', () => {
    expect(accessFromLevel('manage')).toBe('manage')
    expect(accessFromLevel('edit')).toBe('edit')
    expect(accessFromLevel('view')).toBe('view')
    expect(accessFromLevel(null)).toBe('none')
  })

  it('grants writes only to edit and manage, and reads to everything but none and checking', () => {
    expect(['manage', 'edit'].every(a => canEditSpace(a as never))).toBe(true)
    expect(['view', 'unknown', 'none', 'checking'].some(a => canEditSpace(a as never))).toBe(false)
    expect(['manage', 'edit', 'view', 'unknown'].every(a => canReadSpace(a as never))).toBe(true)
    expect(['none', 'checking'].some(a => canReadSpace(a as never))).toBe(false)
  })

  it('reports the level Genie grants', async () => {
    await expect(resolveSpaceAccess('s', async () => ({ space_id: 's', level: 'manage' })))
      .resolves.toEqual({ spaceId: 's', access: 'manage', reason: null })
  })

  it('reads 403 and 404 as none with Genie reason', async () => {
    const denied = refusal(403, 'space_access_entitlement_missing', 'Your account is missing a workspace entitlement')
    await expect(resolveSpaceAccess('s', async () => { throw denied }))
      .resolves.toEqual({ spaceId: 's', access: 'none', reason: denied.message })
    const missing = refusal(404, 'space_not_found', 'Genie Agent not found, or you cannot see it.')
    expect((await resolveSpaceAccess('s', async () => { throw missing })).access).toBe('none')
  })

  it('reads any other failure as unknown, which grants no write', async () => {
    const outage = refusal(503, 'space_access_unavailable', 'Could not verify your access to this Genie Agent. Try again shortly.')
    const state = await resolveSpaceAccess('s', async () => { throw outage })
    expect(state).toEqual({ spaceId: 's', access: 'unknown', reason: outage.message })
    expect(canEditSpace(state.access)).toBe(false)
  })
})

describe('M7 — the resolver refusal envelope', () => {
  it('reads the message out of a dict detail for 401 and 403', () => {
    expect(extractDetailMessage({ code: 'authentication_required', required: 'view', message: 'Your session could not be verified. Sign in again.', platform_message: '' }, 'x'))
      .toBe('Your session could not be verified. Sign in again.')
    expect(extractDetailMessage({ code: 'space_access_denied', required: 'edit', message: 'You need Can Edit permission on this Genie Agent.', platform_message: 'PERMISSION_DENIED' }, 'x'))
      .toBe('You need Can Edit permission on this Genie Agent.')
  })
})

describe('describeScanError', () => {
  it('says scanning needs Can Edit on a plain denial', () => {
    expect(describeScanError(refusal(403, 'space_access_denied', 'You need Can Edit permission on this Genie Agent.'))).toBe(SCAN_NEEDS_EDIT)
  })
  it('keeps Genie reason for a missing scope or entitlement, which names the fix', () => {
    const scope = refusal(403, 'space_access_scope_missing', "This app's sign-in is missing the Genie permission scope.")
    expect(describeScanError(scope)).toBe(scope.message)
  })
  it('falls back to the error message, then to a generic line', () => {
    expect(describeScanError(new Error('boom'))).toBe('boom')
    expect(describeScanError('nope')).toBe('Scan failed.')
  })
})
