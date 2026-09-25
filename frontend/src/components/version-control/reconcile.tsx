import { useState } from 'react'
import type { VersionControlApi } from '@/lib/version-control-api'
import { createMutationIntent } from '@/lib/version-control-api'
import type { ApprovalInputs, ApprovalRecord, Operation } from '@/types/version-control'
import { canMutate } from '@/hooks/use-version-control'
import type { VersionControlState } from '@/hooks/use-version-control'
import { hasFreshApproval, reconcileAction } from './reconcile-actions'
export function OperationStatusView({ operation, onVerify }: { operation: Operation; onVerify: () => void }) {
  return <section aria-label="Operation verification and audit">
    <h3>Operation {operation.operation_id}</h3>
    <p role="status">{operation.status}</p>
    <p>No mutation replay. Partial, unverified, conflicted or quarantined outcomes require operator evidence and authorized recovery.</p>
    <h4>Checkpoints</h4><ul>{operation.checkpoints.map((checkpoint, index) => <li key={index}>{checkpoint}</li>)}</ul>
    <h4>Audit evidence</h4><ul>{operation.audit.map((fact, index) => <li key={index}>{fact}</li>)}</ul>
    <h4>Receipt references</h4><ul>{operation.receipt_references.map(reference => <li key={reference}>{reference}</li>)}</ul>
    <button onClick={onVerify}>Verify operation status</button>
  </section>
}

export function OperationDrillThrough({ api, operationId }: { api?: VersionControlApi; operationId: string }) {
  const [operation, setOperation] = useState<Operation | null>(null)
  const [error, setError] = useState('')
  const verify = async () => {
    if (!api) return
    try { setOperation(await api.operation(operationId)); setError('') }
    catch (error) { setError(String(error)) }
  }
  return <div>
    <button disabled={!api} onClick={() => void verify()}>Inspect unresolved operation {operationId}</button>
    {operation && <OperationStatusView operation={operation} onVerify={() => void verify()} />}
    {error && <p role="alert">Operation evidence unavailable: {error}. No mutation replay.</p>}
  </div>
}

export function ReconcilePanel({ api, state, inputs, approval, onApproval, onComplete, onCompare }: {
  api: VersionControlApi; state: VersionControlState; inputs: ApprovalInputs; approval: ApprovalRecord | null
  onApproval: (approvalId: string) => void; onComplete: () => void; onCompare: () => void
}) {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const [operation, setOperation] = useState<Operation | null>(null)
  const [error, setError] = useState('')
  const [intent] = useState(createMutationIntent)
  const action = async (action: 'adopt' | 'reapply' | 'acknowledge') => {
    if (!canMutate(state, action) || busy || operation || !state.status) return
    setBusy(true)
    try {
      const reviewed = { binding_revision: state.status.binding_revision, expected_base: inputs.expected_base_fingerprints, approval_id: approval?.approval_id ?? '' }
      const result = await reconcileAction(api, state.bindingId, action, reviewed, inputs, approval, intent.key({ bindingId: state.bindingId, action, reviewed, inputs }))
      if ('approval_id' in result) { onApproval(result.approval_id); setMessage(`Approval requested: ${result.approval_id}. Not yet approved.`) }
      else { setOperation(result); setMessage(`Operation ${result.operation_id}: ${result.status}. Acknowledgement does not align heads.`); onComplete() }
    } catch (error) { setError(String(error)) }
    finally { setBusy(false) }
  }
  return <section aria-label="Reconcile captured changes">
    <h3>Reconcile captured external changes</h3>
    <p>Capture is evidence, not approval. Reapply requires approval for the current base.</p>
    <button onClick={onCompare} disabled={!state.status?.heads.approved || !state.status.heads.observed}>Compare approved with observed</button>
    {(['adopt', 'reapply', 'acknowledge'] as const).map(kind => <button key={kind} disabled={busy || Boolean(operation) || Boolean(error) || !canMutate(state, kind) || (kind === 'reapply' && !approval?.valid)} onClick={() => void action(kind)}>{kind === 'adopt' ? hasFreshApproval(state.bindingId, { binding_revision: state.status?.binding_revision ?? -1, expected_base: inputs.expected_base_fingerprints, approval_id: approval?.approval_id ?? '' }, approval) ? 'Adopt captured state with approval' : 'Request adoption approval' : kind === 'reapply' ? 'Reapply approved state' : 'Acknowledge scoped divergence'}</button>)}
    {busy && <p role="status">Reconciliation pending…</p>}
    {message && <p role="status">{message}</p>}
    {operation && <OperationStatusView operation={operation} onVerify={() => { void api.operation(operation.operation_id).then(setOperation).catch(error => setError(String(error))) }} />}
    {error && <p role="alert">{error} No automatic mutation retry.</p>}
  </section>
}
