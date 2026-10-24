// @vitest-environment jsdom
import { expect, it, vi } from 'vitest'
import { ReceiptView } from './promotion'
import { createPromotionFlow, pollReceipt } from './promotion-actions'
import { VersionControlApi } from '@/lib/version-control-api'
import { approvalFixture, receiptFixture } from './fixtures'
import { renderToStaticMarkup } from 'react-dom/server'

it('promotion_requires_explicit_target_mapping_preflight_and_approval', async () => {
  const transport = vi.fn(async (url: RequestInfo | URL) => new Response(JSON.stringify(String(url).endsWith('/releases')
    ? { release_id: 'release-1', operation_id: 'package-1', status: 'requested', job_run_id: null }
    : { operation_id: 'operation-1', status: String(url).includes('/operations/') ? 'confirmed' : 'requested', job_run_id: null, checkpoints: [], audit: [], receipt_references: [], evidence_digest: 'preflight-from-server', approval_inputs: { ...approvalFixture.inputs, target_binding: { ...approvalFixture.inputs.target_binding, binding_id: 'target' } } })))
  const flow = createPromotionFlow(new VersionControlApi(transport))
  await expect(flow.validate('no-target')).rejects.toThrow('explicit target')
  const selection = { source_binding: 'source', source_version_id: 'version-1', target_binding: 'target', mapping_digest: 'mapping-1', policy: 'production' }
  flow.configure(selection)
  await expect(flow.promote(approvalFixture, 'premature')).rejects.toThrow('preflight')
  await flow.validate('validate-intent')
  expect(transport.mock.calls.map(([url]) => url)).toEqual(['/api/version-control/releases', '/api/version-control/releases/release-1/validate', '/api/version-control/operations/operation-1'])
  expect(JSON.parse(String(transport.mock.calls[0][1]?.body))).toEqual(selection)
  await expect(flow.promote(approvalFixture, 'invalid-approval')).rejects.toThrow('approval')
  const valid = { ...approvalFixture, valid: true, inputs: { ...approvalFixture.inputs, target_binding: { ...approvalFixture.inputs.target_binding, binding_id: 'target' }, preflight_evidence_digest: 'preflight-from-server', approval_inputs: { ...approvalFixture.inputs, target_binding: { ...approvalFixture.inputs.target_binding, binding_id: 'target' } }, source_version_id: 'version-1', mapping_digest: 'mapping-1', expires_at: new Date(Date.now() + 3600000).toISOString() } }
  await expect(flow.promote({ ...valid, inputs: { ...valid.inputs, target_binding: { ...valid.inputs.target_binding, binding_revision: valid.inputs.target_binding.binding_revision + 1 } } }, 'wrong-revision')).rejects.toThrow('approval')
  await flow.promote(valid, 'promote-intent')
  expect(transport.mock.calls[3][0]).toBe('/api/version-control/releases/release-1/promote')
  expect(JSON.parse(String(transport.mock.calls[3][1]?.body))).toEqual({ approval_id: valid.approval_id })
  flow.configure({ ...selection, mapping_digest: 'changed-mapping' })
  await expect(flow.promote(valid, 'stale-preflight')).rejects.toThrow('preflight')
})

it('receipt_shows_intended_rendered_observed_and_compensation_status', async () => {
  const html = renderToStaticMarkup(<ReceiptView receipt={receiptFixture} />)
  for (const text of ['Intended', 'Rendered', 'Observed', receiptFixture.intended_fingerprints.config, receiptFixture.rendered_fingerprints.config, receiptFixture.observed_fingerprints.config, 'applied_partial', receiptFixture.pre_version_id, receiptFixture.executor_id, receiptFixture.package_digest]) expect(html).toContain(text)
  expect(html).not.toContain('Deployment confirmed')
  const transport = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({ operation_id: 'pending-1', status: 'requested', job_run_id: null }), { status: 202 })).mockResolvedValueOnce(new Response(JSON.stringify(receiptFixture)))
  expect(await pollReceipt(new VersionControlApi(transport), 'release/1', async () => {})).toEqual(receiptFixture)
  expect(transport.mock.calls.map(([url]) => url)).toEqual(['/api/version-control/releases/release%2F1/receipt', '/api/version-control/releases/release%2F1/receipt'])
  expect(transport.mock.calls.every(([, init]) => !init.method)).toBe(true)
})


it('promotion_approval_binds_the_preflight_evidence_digest_returned_by_validate', async () => {
  const projection = { ...approvalFixture.inputs, target_binding: { ...approvalFixture.inputs.target_binding, binding_id: 'target' }, raw_source_digest: 'server-raw', rendered_target_digest: 'server-rendered', validation_policy_digest: 'server-policy' }
  const transport = vi.fn(async (url: RequestInfo | URL) => new Response(JSON.stringify(String(url).endsWith('/releases')
    ? { release_id: 'release-evidence', operation_id: 'package', status: 'requested', job_run_id: null }
    : String(url).includes('/approvals') ? approvalFixture
    : { operation_id: 'preflight', status: 'confirmed', job_run_id: null, checkpoints: [], audit: [], receipt_references: [], evidence_digest: 'validated-distinctive-digest', approval_inputs: projection })))
  const host = document.createElement('div')
  const root = createRoot(host)
  const input = async (label: string, value: string) => {
    const element = [...host.querySelectorAll('label')].find(node => node.textContent?.startsWith(label))!.querySelector('input')!
    await act(async () => { Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(element, value); element.dispatchEvent(new Event('input', { bubbles: true })) })
  }
  try {
    await act(async () => root.render(<PromotionPanel api={new VersionControlApi(transport)} bindingId="source" sourceVersionId="version-1" inputs={approvalFixture.inputs} disabled={false} />))
    await input('Target binding', 'target'); await input('Immutable mapping', 'mapping-1'); await input('Validation policy', 'production')
    await act(async () => [...host.querySelectorAll('button')].find(button => button.textContent?.startsWith('Create release'))!.click())
    await act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === 'Request fresh approval')!.click())
    const posts = transport.mock.calls.filter(([url]) => String(url).endsWith('/approvals'))
    expect(posts).toHaveLength(1)
    expect(JSON.parse(String(posts[0][1]?.body)).preflight_evidence_digest).toBe('validated-distinctive-digest')
    await input('Immutable mapping', 'changed')
    expect([...host.querySelectorAll('button')].some(button => button.textContent === 'Request fresh approval')).toBe(false)
  } finally { await act(async () => root.unmount()) }
})
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { PromotionPanel } from './promotion'
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
