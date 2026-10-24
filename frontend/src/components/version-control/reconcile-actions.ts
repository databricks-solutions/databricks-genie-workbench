import type { VersionControlApi } from '@/lib/version-control-api'
import type { ApprovalInputs, ApprovalRecord, ReviewedCommand } from '@/types/version-control'
import { pollOperation } from './restore-actions'

// Non-component helpers live in a sibling module so the .tsx files export only
// components (react-refresh/only-export-components).

export function hasFreshApproval(bindingId: string, reviewed: ReviewedCommand, approval: ApprovalRecord | null) {
  return Boolean(approval?.valid && approval.approval_id === reviewed.approval_id && approval.inputs.target_binding.binding_id === bindingId && approval.inputs.target_binding.binding_revision === reviewed.binding_revision && Date.parse(approval.inputs.expires_at) > Date.now() && JSON.stringify(approval.inputs.expected_base_fingerprints) === JSON.stringify(reviewed.expected_base))
}

export async function reconcileAction(api: VersionControlApi, bindingId: string, action: 'adopt' | 'reapply' | 'acknowledge', reviewed: ReviewedCommand, inputs: ApprovalInputs, approval: ApprovalRecord | null, key: string) {
  const fresh = hasFreshApproval(bindingId, reviewed, approval)
  if (action === 'adopt' && !fresh) return api.requestApproval(inputs, key)
  if (action === 'reapply' && !fresh) throw new Error('Reapply requires a fresh approval for the newly captured base.')
  const handle = await api.reconcile(bindingId, { ...reviewed, action, policy_inputs: { validation_policy_digest: inputs.validation_policy_digest, benchmark_policy_digest: inputs.benchmark_policy_digest } }, key)
  return pollOperation(api, handle.operation_id)
}
