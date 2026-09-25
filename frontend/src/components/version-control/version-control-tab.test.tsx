import { expect, it, vi } from 'vitest'
import { createVersionControlStore, canMutate } from '@/hooks/use-version-control'
import { VersionControlApi } from '@/lib/version-control-api'
import { approvalInputsFixture, bindingFixture, versionFixture } from './fixtures'
import { renderToStaticMarkup } from 'react-dom/server'
import { VersionControlView } from './version-control-tab'
import { RestorePanel } from './restore'
import { PromotionPanel } from './promotion'
import { createDemoTransport } from './demo-api'

it('open_waits_for_capture_before_showing_reconcile_choices', async () => {
  let completeCapture!: (response: Response) => void
  const transport = vi.fn(async (url: RequestInfo | URL) => {
    if (String(url).endsWith('/observe')) return new Promise<Response>(resolve => { completeCapture = resolve })
    return new Response(JSON.stringify({ items: [versionFixture], next_cursor: null }))
  })
  const store = createVersionControlStore(new VersionControlApi(transport))
  const opening = store.open('demo-binding', 'open')
  expect(canMutate(store.getSnapshot(), 'adopt')).toBe(false)
  expect(store.getSnapshot().loading).toBe(true)
  completeCapture(new Response(JSON.stringify({ status: bindingFixture, captured_version: versionFixture, busy: false })))
  await opening
  expect(store.getSnapshot().history.items).toEqual([versionFixture])
  expect(canMutate(store.getSnapshot(), 'adopt')).toBe(true)
  expect(store.getSnapshot().status?.heads).toEqual(bindingFixture.heads)
  const returning = store.open('demo-binding', 'return')
  expect(canMutate(store.getSnapshot(), 'adopt')).toBe(false)
  completeCapture(new Response(JSON.stringify({ status: bindingFixture, captured_version: versionFixture, busy: false })))
  await returning
  expect(transport.mock.calls.filter(([url]) => String(url).endsWith('/observe'))).toHaveLength(2)
})

it('version_control_actions_are_keyboard_accessible_and_errors_announced', () => {
  const state = { bindingId: 'demo-binding', loading: false, captured: false, captureFailure: null, busy: false, stale: true, status: bindingFixture, history: { items: [versionFixture], next_cursor: null }, error: 'Evidence unavailable' }
  const html = renderToStaticMarkup(<VersionControlView state={state} />)
  expect(html).toContain('aria-live="polite"')
  expect(html).toContain('role="alert"')
  expect(html).toContain('Evidence unavailable')
  const api = new VersionControlApi(vi.fn())
  const controls = renderToStaticMarkup(<><RestorePanel api={api} bindingId="demo-binding" command={{ version_id: 'version-1', expected_base: versionFixture.fingerprints, binding_revision: 3 }} disabled onComplete={vi.fn()} /><PromotionPanel api={api} bindingId="demo-binding" sourceVersionId="version-1" inputs={approvalInputsFixture} disabled /></>)
  expect(controls).toContain('<label>Restore approval reference<input')
  expect(controls).toContain('type="checkbox"')
  expect(controls).toContain('<label>Target binding<input')
  expect(controls).toContain('<button disabled=""')
  expect(controls).not.toMatch(/tabindex="-1"|<div[^>]*role="button"/)
})

it('fake_api_supports_approval_restore_promotion_and_receipt_without_network', async () => {
  const network = vi.fn(() => { throw new Error('Real network forbidden') })
  vi.stubGlobal('fetch', network)
  try {
    const api = new VersionControlApi(createDemoTransport())
    const page = await api.overview()
    expect(page.next_cursor).not.toBeNull()
    const observation = await api.observe('demo-binding', 'open', 'observe-1')
    expect(observation.status.heads).toEqual(bindingFixture.heads)
    const inputs = { ...approvalInputsFixture, expires_at: new Date(Date.now() + 3600000).toISOString() }
    const request = await api.requestApproval(inputs, 'request-1')
    const approval = await api.vote(request.approval_id, 'approve', 'vote-1')
    expect(approval.valid).toBe(true)
    const restore = await api.restore('demo-binding', { version_id: 'approved-2', expected_base: versionFixture.fingerprints, binding_revision: 3, approval_id: approval.approval_id }, 'restore-1')
    expect((await api.operation(restore.operation_id)).status).toBe('confirmed')
    const release = await api.release({ source_binding: 'demo-binding', source_version_id: 'external-3', target_binding: 'demo-target', mapping_digest: 'mapping-1', policy: 'production' }, 'release-1')
    expect(await api.receipt(release.release_id)).toHaveProperty('status', 'requested')
    const preflight = await api.validate(release.release_id, 'validate-1')
    expect((await api.operation(preflight.operation_id)).status).toBe('confirmed')
    const promotionApproval = await api.requestApproval({ ...inputs, operation_type: 'promotion', target_binding: { ...inputs.target_binding, binding_id: 'demo-target' }, mapping_digest: 'mapping-1' }, 'promotion-request')
    await api.vote(promotionApproval.approval_id, 'approve', 'promotion-vote')
    await api.promote(release.release_id, { approval_id: promotionApproval.approval_id }, 'promote-1')
    expect(await api.receipt(release.release_id)).toHaveProperty('post_version_id')
    await expect(api.restore('demo-quarantined', { version_id: 'approved-2', expected_base: versionFixture.fingerprints, binding_revision: 3, approval_id: approval.approval_id }, 'blocked')).rejects.toMatchObject({ status: 423 })
    expect(network).not.toHaveBeenCalled()
  } finally { vi.unstubAllGlobals() }
})

it('capture_failure_shows_stale_history_and_disables_mutating_actions', async () => {
  const transport = vi.fn(async (url: RequestInfo | URL) => String(url).endsWith('/observe')
    ? new Response(JSON.stringify({ code: 'PERSISTENCE_UNAVAILABLE', message: 'Snapshot persistence failed', stale: true, retryable: false }), { status: 503 })
    : new Response(JSON.stringify({ items: [versionFixture], next_cursor: null })))
  const store = createVersionControlStore(new VersionControlApi(transport))
  await store.open('demo-binding')
  const state = store.getSnapshot()
  expect(state.stale).toBe(true)
  expect(state.history.items).toEqual([versionFixture])
  for (const action of bindingFixture.allowed_actions) expect(canMutate(state, action)).toBe(false)
  const html = renderToStaticMarkup(<VersionControlView state={state} />)
  expect(html).toContain('Stale history')
  expect(html).toContain('Snapshot persistence failed')
  expect(html).toContain('external-3')
  expect(html).not.toContain('Request adoption approval')
})


it('production_modules_do_not_import_test_fixtures', () => {
  for (const file of readdirSync(new URL('.', import.meta.url)).filter(file => /\.tsx?$/.test(file) && !file.includes('.test.') && !['demo-api.ts', 'fixtures.ts'].includes(file))) {
    expect(readFileSync(new URL(file, import.meta.url), 'utf8'), file).not.toMatch(/from ['"]\.\/fixtures['"]/)
  }
})
import { readFileSync, readdirSync } from 'node:fs'

import m02Errors from '../../../../backend/tests/fixtures/vc_contracts/api_error.json'
it('open_distinguishes_409_conflict_423_unresolved_and_503_stale_surfaces', async () => {
  const markups: string[] = []
  for (const [httpStatus, kind] of [[409, 'conflict'], [423, 'unresolved'], [503, 'unavailable']] as const) {
    const payload = m02Errors.examples[0]
    const transport = vi.fn<typeof fetch>(async url => String(url).endsWith('/observe')
      ? new Response(JSON.stringify(payload), { status: httpStatus })
      : new Response(JSON.stringify({ items: [versionFixture], next_cursor: null })))
    const api = new VersionControlApi(transport)
    const store = createVersionControlStore(api)
    await store.open('demo-binding')
    const state = store.getSnapshot()
    for (const action of bindingFixture.allowed_actions) expect(canMutate(state, action)).toBe(false)
    const html = renderToStaticMarkup(<VersionControlView state={state} api={api} onCompare={vi.fn()} onRefresh={vi.fn()} />)
    markups.push(html)
    if (httpStatus === 409) {
      expect(html).toContain('Preserved conflict'); expect(html).toContain('Compare preserved history')
      expect(html).not.toContain('Stale history')
    } else if (httpStatus === 423) {
      expect(html).toContain('Quarantined / unresolved'); expect(html).toContain(payload.operation_id)
      expect(html).not.toContain('Stale history'); expect(html).not.toMatch(/<button[^>]*>[^<]*[Rr]etry/)
    } else {
      expect(html).toContain('Stale history'); expect(html).toContain('Refresh captured history')
      expect(state.history.items).toEqual([versionFixture]); expect(html).toContain('external-3')
    }
    expect(state.captureFailure).toMatchObject({ kind, operationId: payload.operation_id })
  }
  expect(new Set(markups).size).toBe(3)
})
