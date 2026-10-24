import { useEffect, useState } from 'react'
import type { VersionControlApi } from '@/lib/version-control-api'
import { createMutationIntent } from '@/lib/version-control-api'
import type { ApprovalRequestInputs, ApprovalRecord } from '@/types/version-control'
import { voteApproval } from './approvals-actions'

export function ApprovalDetails({ record }: { record: ApprovalRecord }) {
  const approvers = new Set(record.votes.filter(vote => vote.decision === 'approve' && vote.actor_id !== record.inputs.requester_id).map(vote => vote.actor_id))
  return <section aria-label="Approval evidence">
    <h3>Approval {record.approval_id}</h3>
    <p>Server authorization: {record.valid ? 'Valid approval' : 'Not approved / invalidated'}</p>
    <p>{approvers.size} distinct non-requester approvers recorded. Production requires two distinct humans, neither requester nor deployer; at least one target-group eligible. Server revalidates eligibility at execution.</p>
    <p>Expires: <time>{record.inputs.expires_at}</time> · Maximum production lifetime: 24 hours.</p>
    <p>Approval digest: {record.approval_digest ?? 'Not yet bound'}</p>
    <ul>{record.invalidation_reasons.map(reason => <li key={reason}>{reason}</li>)}</ul>
    <h4>Bound inputs and policy digests</h4><pre>{JSON.stringify(record.inputs, null, 2)}</pre>
    <h4>Vote audit</h4><ul>{record.votes.map((vote, index) => <li key={index}>{vote.actor_id}: {vote.decision} · {vote.recorded_at} · Target-group eligible: {vote.target_group_eligible ? 'yes' : 'no'}</li>)}</ul>
  </section>
}
export function ApprovalsPanel({ api, approvalId, inputs, disabled, onApproval, onRecord }: {
  api: VersionControlApi; approvalId: string; inputs: ApprovalRequestInputs | null; disabled: boolean
  onApproval: (approvalId: string) => void; onRecord: (record: ApprovalRecord) => void
}) {
  const [record, setRecord] = useState<ApprovalRecord | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [intent] = useState(createMutationIntent)
  useEffect(() => {
    let active = true
    setRecord(null)
    if (!approvalId) return
    setBusy(true)
    api.approval(approvalId).then(result => { if (active) { setRecord(result); onRecord(result) } })
      .catch(error => { if (active) setError(String(error)) })
      .finally(() => { if (active) setBusy(false) })
    return () => { active = false }
  }, [api, approvalId, onRecord])
  const request = async () => {
    if (busy || disabled || !inputs) return
    setBusy(true)
    try { const result = await api.requestApproval(inputs, intent.key({ action: 'request', inputs })); onApproval(result.approval_id) }
    catch (error) { setError(String(error)) }
    finally { setBusy(false) }
  }
  const vote = async (decision: 'approve' | 'reject') => {
    if (!record || busy || disabled) return
    setBusy(true)
    try { const result = await voteApproval(api, record, decision, intent.key({ approvalId: record.approval_id, decision })); setRecord(result); onRecord(result) }
    catch (error) { setError(String(error)) }
    finally { setBusy(false) }
  }
  return <section aria-label="Approvals and audit">
    <h3>Approvals and audit</h3>
    {!inputs && <p role="status">Server approval evidence unavailable; request blocked.</p>}
    {!record && <p>No approval loaded. Request a new approval bound to the reviewed inputs.</p>}
    <button disabled={disabled || busy || !inputs || Boolean(error)} onClick={() => void request()}>Request fresh approval</button>
    {record && <ApprovalDetails record={record} />}
    <button disabled={disabled || busy || !record?.allowed_actions.includes('vote') || Boolean(error)} onClick={() => void vote('approve')}>Approve as authenticated user</button>
    <button disabled={disabled || busy || !record?.allowed_actions.includes('vote') || Boolean(error)} onClick={() => void vote('reject')}>Reject as authenticated user</button>
    {busy && <p role="status">Loading approval evidence…</p>}
    {error && <p role="alert">{error} Approval remains unverified.</p>}
  </section>
}
