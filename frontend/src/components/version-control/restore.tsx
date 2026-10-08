import { useState } from 'react'
import { createMutationIntent } from '@/lib/version-control-api'
import type { VersionControlApi } from '@/lib/version-control-api'
import type { Operation, RestoreCommand } from '@/types/version-control'
import { OperationStatusView } from './reconcile'
import { submitRestore } from './restore-actions'

export function RestorePanel({ api, bindingId, command, disabled, onComplete }: { api: VersionControlApi; bindingId: string; command: Omit<RestoreCommand, 'approval_id'>; disabled: boolean; onComplete: () => void }) {
  const [approvalId, setApprovalId] = useState('')
  const [confirmed, setConfirmed] = useState(false)
  const [busy, setBusy] = useState(false)
  const [operation, setOperation] = useState<Operation | null>(null)
  const [error, setError] = useState('')
  const [intent] = useState(createMutationIntent)
  const submit = async () => {
    if (disabled || busy) return
    setBusy(true)
    setError('')
    const body = { ...command, approval_id: approvalId }
    try { setOperation(await submitRestore(api, bindingId, body, confirmed, intent.key({ bindingId, body }), setOperation)); onComplete() }
    catch (error) { setError(String(error)) }
    finally { setBusy(false) }
  }
  return <section aria-label="Restore or roll forward">
    <h3>Restore / roll forward</h3>
    <p>Use historical content as a new reviewed write; history is never rewound.</p>
    <p>Version {command.version_id} · Reviewed revision {command.binding_revision}</p>
    <pre>{JSON.stringify(command.expected_base, null, 2)}</pre>
    <label>Restore approval reference<input value={approvalId} onChange={event => setApprovalId(event.target.value)} /></label>
    <label><input type="checkbox" checked={confirmed} onChange={event => setConfirmed(event.target.checked)} />Confirm reviewed version and base</label>
    <button disabled={disabled || busy || !confirmed || !approvalId.trim() || Boolean(error) || Boolean(operation)} onClick={() => void submit()}>Submit restore / roll forward</button>
    {busy && <p role="status">Operation pending — polling status, not replaying the command.</p>}
    {operation && <OperationStatusView operation={operation} onVerify={() => { void api.operation(operation.operation_id).then(setOperation).catch(error => setError(String(error))) }} />}
    {error && <p role="alert">{error} Command outcome unresolved; do not blindly retry.</p>}
  </section>
}
