// @vitest-environment jsdom
import { expect, it, vi } from 'vitest'
import { OperationStatusView, ReconcilePanel } from './reconcile'
import { reconcileAction } from './reconcile-actions'
import { renderToStaticMarkup } from 'react-dom/server'
import { VersionControlApi } from '@/lib/version-control-api'
import { approvalFixture, approvalInputsFixture, fingerprints, bindingFixture } from './fixtures'

it('adopt_requests_approval_and_reapply_does_not_reuse_invalid_approval', async () => {
  const transport = vi.fn(async () => new Response(JSON.stringify(approvalFixture)))
  const api = new VersionControlApi(transport)
  const reviewed = { binding_revision: 3, expected_base: fingerprints, approval_id: approvalFixture.approval_id }
  await reconcileAction(api, 'demo-binding', 'adopt', reviewed, approvalInputsFixture, approvalFixture, 'adopt-intent')
  expect(transport.mock.calls[0][0]).toBe('/api/version-control/approvals')
  const body = JSON.parse(String(transport.mock.calls[0][1]?.body))
  expect(body.expected_base_fingerprints).toEqual(fingerprints)
  expect(body).not.toHaveProperty('requester_id')
  await expect(reconcileAction(api, 'demo-binding', 'reapply', reviewed, approvalInputsFixture, approvalFixture, 'reapply-intent')).rejects.toThrow('fresh approval')
  expect(transport).toHaveBeenCalledTimes(1)
})

it('quarantine_and_partial_outcome_never_offer_blind_retry', async () => {
  for (const status of ['quarantined', 'applied_partial', 'applied_unverified', 'conflicted'] as const) {
    const html = renderToStaticMarkup(<OperationStatusView operation={{ operation_id: 'unresolved-1', status, job_run_id: null, checkpoints: ['Description write incomplete'], audit: ['Operator must inspect captured evidence'], receipt_references: [] }} onVerify={vi.fn()} />)
    expect(html).toContain(status)
    expect(html).toContain('Description write incomplete')
    expect(html).toContain('Operator must inspect captured evidence')
    expect(html).toContain('Verify operation status')
    // A just-returned partial result must lock the real controls even before a refresh returns.
    const api = new VersionControlApi(vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ operation_id: 'unresolved-1', status, job_run_id: null, checkpoints: [], audit: [], receipt_references: [] }))))
    const host = document.createElement('div'); const root = createRoot(host)
    try {
      await act(async () => root.render(<ReconcilePanel api={api} state={permissiveState} inputs={approvalInputsFixture} approval={null} onApproval={vi.fn()} onComplete={vi.fn()} onCompare={vi.fn()} />))
      const buttons = () => [...host.querySelectorAll('button')].filter(button => /adoption|Adopt captured|Reapply|Acknowledge/.test(button.textContent ?? ''))
      expect(buttons().filter(button => !button.disabled)).toHaveLength(2)
      await act(async () => buttons().find(button => button.textContent?.includes('Acknowledge'))!.click())
      expect(buttons()).toHaveLength(3)
      expect(buttons().every(button => button.disabled)).toBe(true)
    } finally { await act(async () => root.unmount()) }
    expect(html).toContain('No mutation replay')
  }
})


it('reconcile_result_renders_operation_evidence_not_a_bare_message', async () => {
  const operation = { operation_id: 'partial-1', status: 'applied_partial', job_run_id: null, checkpoints: ['Description stopped'], audit: ['External state preserved'], receipt_references: [] }
  const transport = vi.fn(async () => new Response(JSON.stringify(operation)))
  const api = new VersionControlApi(transport)
  const state = { bindingId: 'demo-binding', loading: false, captured: true, captureFailure: null, busy: false, stale: false, status: bindingFixture, history: { items: [], next_cursor: null }, error: '' }
  const host = document.createElement('div')
  const root = createRoot(host)
  try {
    await act(async () => root.render(<ReconcilePanel api={api} state={state} inputs={approvalInputsFixture} approval={null} onApproval={vi.fn()} onComplete={vi.fn()} onCompare={vi.fn()} />))
    await act(async () => [...host.querySelectorAll('button')].find(button => button.textContent?.includes('Acknowledge'))!.click())
    expect(host.textContent).toContain('applied_partial')
    expect(host.textContent).toContain('Description stopped')
    expect(host.textContent).toContain('External state preserved')
    expect(transport.mock.calls[1][0]).toBe('/api/version-control/operations/partial-1')
  } finally { await act(async () => root.unmount()) }
})

import { act } from 'react'
import { createRoot } from 'react-dom/client'
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })

it('adopt_with_fresh_bound_approval_posts_reconcile_action_adopt', async () => {
  const operation = { operation_id: 'adopt-1', status: 'confirmed', job_run_id: null, checkpoints: [], audit: [], receipt_references: [] }
  const transport = vi.fn(async () => new Response(JSON.stringify(operation)))
  const api = new VersionControlApi(transport)
  const approval = { ...approvalFixture, valid: true, inputs: { ...approvalInputsFixture, expires_at: new Date(Date.now() + 60000).toISOString() } }
  const reviewed = { binding_revision: 3, expected_base: fingerprints, approval_id: approval.approval_id }
  await reconcileAction(api, 'demo-binding', 'adopt', reviewed, approvalInputsFixture, approval, 'fresh')
  expect(transport.mock.calls[0][0]).toBe('/api/version-control/bindings/demo-binding/reconcile')
  expect(JSON.parse(String(transport.mock.calls[0][1]?.body))).toMatchObject({ ...reviewed, action: 'adopt' })
  for (const inputs of [{ ...approval.inputs, expires_at: '2000-01-01T00:00:00Z' }, { ...approval.inputs, expected_base_fingerprints: { ...fingerprints, config: 'changed' } }]) {
    const staleTransport = vi.fn(async () => new Response(JSON.stringify(approvalFixture)))
    await reconcileAction(new VersionControlApi(staleTransport), 'demo-binding', 'adopt', reviewed, approvalInputsFixture, { ...approval, inputs }, 'stale')
    expect(staleTransport).toHaveBeenCalledTimes(1)
    expect(staleTransport.mock.calls[0][0]).toBe('/api/version-control/approvals')
  }
})

import { RestorePanel } from './restore'
import { PromotionPanel } from './promotion'
import { canMutate } from '@/hooks/use-version-control'
import type { VersionControlState } from '@/hooks/use-version-control'
const permissiveState: VersionControlState = { bindingId: 'demo-binding', loading: false, captured: true, captureFailure: null, busy: false, stale: false, status: bindingFixture, history: { items: [], next_cursor: null }, error: '' }
it('reconcile_and_restore_controls_render_disabled_for_quarantined_and_partial_bindings', () => {
  const api = new VersionControlApi(vi.fn())
  for (const changes of [{ quarantined: true }, { unresolved_operation_id: 'op-1' }]) {
    const state = { ...permissiveState, status: { ...bindingFixture, ...changes } }
    const html = renderToStaticMarkup(<>
      <ReconcilePanel api={api} state={state} inputs={approvalInputsFixture} approval={null} onApproval={vi.fn()} onComplete={vi.fn()} onCompare={vi.fn()} />
      <RestorePanel api={api} bindingId="demo-binding" command={{ binding_revision: 3, expected_base: fingerprints, version_id: 'version' }} disabled={!canMutate(state, 'restore')} onComplete={vi.fn()} />
      <PromotionPanel api={api} bindingId="demo-binding" sourceVersionId="version" inputs={approvalInputsFixture} disabled={!canMutate(state, 'promote')} />
    </>)
    const host = document.createElement('div'); host.innerHTML = html
    // Exclude only the read-only compare control; new mutation buttons are included automatically.
    const mutations = [...host.querySelectorAll('button')].filter(button => button.textContent !== 'Compare approved with observed')
    expect(mutations).toHaveLength(6)
    expect(mutations.filter(button => button.disabled)).toHaveLength(mutations.length)
  }
})
