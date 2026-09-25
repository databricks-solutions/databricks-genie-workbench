import { renderToStaticMarkup } from 'react-dom/server'
import { expect, it, vi } from 'vitest'
import { ApprovalDetails } from './approvals'
import { voteApproval } from './approvals-actions'
import { approvalFixture } from './fixtures'
import { VersionControlApi } from '@/lib/version-control-api'

it('approval_ui_shows_bound_digests_distinct_approvers_and_expiry', async () => {
  const vote = { actor_id: 'human-1', decision: 'approve' as const, recorded_at: '2026-09-07T01:00:00Z', target_group_eligible: true }
  const record = { ...approvalFixture, votes: [vote, vote, { ...vote, actor_id: approvalFixture.inputs.requester_id! }] }
  const html = renderToStaticMarkup(<ApprovalDetails record={record} />)
  for (const text of [approvalFixture.inputs.raw_source_digest, approvalFixture.inputs.rendered_target_digest, approvalFixture.inputs.validation_policy_digest, approvalFixture.inputs.benchmark_policy_digest, approvalFixture.inputs.preflight_evidence_digest, '1 distinct', approvalFixture.inputs.expires_at, 'Base changed after capture', 'neither requester nor deployer', 'Server authorization']) expect(html).toContain(text)
  const transport = vi.fn(async () => new Response(JSON.stringify(record)))
  await voteApproval(new VersionControlApi(transport), record, 'approve', 'vote-intent')
  expect(transport.mock.calls[0][0]).toBe('/api/version-control/approvals/demo-approval/votes')
  expect(JSON.parse(String(transport.mock.calls[0][1]?.body))).toEqual({ decision: 'approve' })
  await expect(voteApproval(new VersionControlApi(transport), { ...record, allowed_actions: [] }, 'approve', 'denied')).rejects.toThrow('not allowed')
  expect(transport).toHaveBeenCalledTimes(1)
})

it('approval_request_body_carries_server_derived_preflight_and_no_client_requester_identity', async () => {
  const inputs = { ...approvalFixture.inputs, raw_source_digest: 'server-raw', rendered_target_digest: 'server-rendered', validation_policy_digest: 'server-policy', preflight_evidence_digest: 'server-preflight' }
  const transport = vi.fn(async () => new Response(JSON.stringify(approvalFixture)))
  await new VersionControlApi(transport).requestApproval(inputs, 'derived-request')
  const body = JSON.parse(String(transport.mock.calls[0][1]?.body))
  expect(body).not.toHaveProperty('requester_id')
  expect(body).not.toHaveProperty('operation_id')
  expect(body.preflight_evidence_digest).toBe('server-preflight')
  for (const fabricated of ['preflight-1', 'validation-1', 'raw-3', 'rendered-3']) expect(Object.values(body)).not.toContain(fabricated)
})
