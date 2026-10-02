import { expect, it, vi } from 'vitest'
import { createVersionControlStore, canMutate } from './use-version-control'
import { VersionControlApi } from '@/lib/version-control-api'
import { bindingFixture, versionFixture } from '@/components/version-control/fixtures'

it('late_response_for_previous_binding_cannot_replace_current_heads', async () => {
  let resolveOld!: (response: Response) => void
  const transport = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input)
    if (url.includes('/old/observe')) return new Promise<Response>(resolve => { resolveOld = resolve })
    if (url.endsWith('/observe')) return new Response(JSON.stringify({ status: { ...bindingFixture, binding_id: 'current', heads: { observed: 'current-head', approved: null, deployed: null } }, captured_version: null, busy: false }))
    return new Response(JSON.stringify({ items: [{ ...versionFixture, binding_id: url.includes('/old/') ? 'old' : 'current' }], next_cursor: null }))
  })
  const store = createVersionControlStore(new VersionControlApi(transport))
  const old = store.open('old')
  await store.open('current')
  resolveOld(new Response(JSON.stringify({ status: { ...bindingFixture, binding_id: 'old' }, captured_version: versionFixture, busy: false })))
  await old
  expect(store.getSnapshot().status?.binding_id).toBe('current')
  expect(store.getSnapshot().status?.heads.observed).toBe('current-head')
  expect(store.getSnapshot().history.items[0].binding_id).toBe('current')
  await store.invalidate()
  expect(transport.mock.calls.filter(([url]) => String(url).endsWith('/current/observe'))).toHaveLength(2)
})

import type { VersionControlState } from './use-version-control'
const permissive: VersionControlState = { bindingId: 'demo-binding', captured: true, captureFailure: null, loading: false, busy: false, stale: false, status: bindingFixture, history: { items: [], next_cursor: null }, error: '' }
const blockers: [string, Partial<VersionControlState>][] = [
  ['quarantined', { status: { ...bindingFixture, quarantined: true } }],
  ['unresolved', { status: { ...bindingFixture, unresolved_operation_id: 'op-1' } }],
  ['status stale', { status: { ...bindingFixture, stale: true } }],
  ['state stale', { stale: true }], ['busy', { busy: true }], ['uncaptured', { captured: false }], ['loading', { loading: true }], ['no status', { status: null }],
  ...(['unknown', 'unreachable', 'applied_unverified', 'conflicted'] as const).map(drift => [drift, { status: { ...bindingFixture, drift } }] as [string, Partial<VersionControlState>]),
]
it.each(blockers)('can_mutate_is_false_for_quarantined_unresolved_and_each_blocking_drift_state: %s', (_name, changes) => {
  expect(canMutate(permissive, 'adopt')).toBe(true)
  expect(canMutate({ ...permissive, ...changes }, 'adopt')).toBe(false)
  expect(canMutate(permissive, 'not-allowed')).toBe(false)
})
