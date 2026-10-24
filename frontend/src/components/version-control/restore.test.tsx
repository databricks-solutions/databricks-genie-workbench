import { expect, it, vi } from 'vitest'
import { submitRestore } from './restore-actions'
import { VersionControlApi } from '@/lib/version-control-api'
import { fingerprints } from './fixtures'

it('restore_and_forward_submit_reviewed_base_and_poll_operation', async () => {
  const transport = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => new Response(JSON.stringify({ operation_id: 'restore-1', status: init?.method === 'POST' ? 'requested' : 'confirmed', job_run_id: 'job-1', checkpoints: [], audit: [], receipt_references: [] }), { status: init?.method === 'POST' ? 202 : 200 }))
  const api = new VersionControlApi(transport)
  const command = { version_id: 'historical-1', binding_revision: 3, expected_base: fingerprints, approval_id: 'fresh-approval' }
  const result = await submitRestore(api, 'binding/1', command, true, 'restore-intent')
  expect(result.status).toBe('confirmed')
  expect(transport.mock.calls.map(([url]) => url)).toEqual(['/api/version-control/bindings/binding%2F1/restore', '/api/version-control/operations/restore-1'])
  expect(JSON.parse(String(transport.mock.calls[0][1]?.body))).toEqual(command)
  expect(transport.mock.calls[0][1]?.headers).toHaveProperty('Idempotency-Key', 'restore-intent')
  await submitRestore(api, 'binding/1', { ...command, version_id: 'newer-4' }, true, 'forward-intent')
  expect(JSON.parse(String(transport.mock.calls[2][1]?.body)).version_id).toBe('newer-4')
  await expect(submitRestore(api, 'binding/1', command, false, 'no-confirm')).rejects.toThrow('Confirm')
  expect(transport).toHaveBeenCalledTimes(4)
})
