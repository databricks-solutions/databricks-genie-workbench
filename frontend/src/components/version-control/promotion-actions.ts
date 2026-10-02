import type { VersionControlApi } from '@/lib/version-control-api'
import type { ApprovalRecord, BindingRef, DeploymentReceipt, OperationHandle, ReleaseCommand } from '@/types/version-control'
import { pollOperation } from './restore-actions'

// Non-component helpers live in a sibling module so the .tsx files export only
// components (react-refresh/only-export-components).

export async function pollReceipt(api: VersionControlApi, releaseId: string, wait = () => new Promise<void>(resolve => setTimeout(resolve, 1000)), signal?: AbortSignal): Promise<DeploymentReceipt | OperationHandle> {
  for (let attempt = 0; attempt < 30; attempt += 1) {
    signal?.throwIfAborted()
    const receipt = await api.receipt(releaseId)
    if ('release_id' in receipt || attempt === 29) return receipt
    await wait()
  }
  throw new Error('Receipt polling exhausted')
}

export function createPromotionFlow(api: VersionControlApi) {
  let selection: ReleaseCommand | null = null
  let releaseId = ''
  let preflight = false
  let targetBinding: BindingRef | null = null
  let evidenceDigest: string | null = null
  let revision = 0
  return {
    configure(next: ReleaseCommand) {
      if (JSON.stringify(next) !== JSON.stringify(selection)) { selection = next; releaseId = ''; preflight = false; evidenceDigest = null; targetBinding = null; revision += 1 }
    },
    async validate(key: string) {
      if (!selection?.target_binding.trim() || !selection.mapping_digest.trim() || !selection.source_version_id || !selection.policy.trim() || selection.source_binding === selection.target_binding) throw new Error('Select an explicit target, immutable mapping, source version and policy.')
      const started = revision
      const release = await api.release(selection, `${key}:release`)
      const handle = await api.validate(release.release_id, `${key}:validate`)
      const operation = await pollOperation(api, handle.operation_id)
      if (revision !== started) throw new Error('Selection changed; preflight is invalid.')
      releaseId = release.release_id
      evidenceDigest = operation.evidence_digest ?? null
      preflight = operation.status === 'confirmed' && Boolean(evidenceDigest)
      if (!preflight) throw new Error(`Preflight incomplete: ${operation.status}`)
      const inputs = operation.approval_inputs
      targetBinding = inputs?.target_binding ?? null
      return { releaseId, evidenceDigest, inputs: inputs ? { ...inputs, preflight_evidence_digest: evidenceDigest! } : null }
    },
    async promote(approval: ApprovalRecord | null, key: string) {
      if (!preflight || !releaseId) throw new Error('A confirmed preflight is required.')
      if (!approval?.valid || !approval.approval_digest || approval.inputs.preflight_evidence_digest !== evidenceDigest || !targetBinding || targetBinding.binding_id !== selection?.target_binding || approval.inputs.target_binding.binding_id !== targetBinding.binding_id || approval.inputs.target_binding.binding_revision !== targetBinding.binding_revision || approval.inputs.source_version_id !== selection.source_version_id || approval.inputs.mapping_digest !== selection.mapping_digest || Date.parse(approval.inputs.expires_at) <= Date.now()) throw new Error('Fresh target-local approval matching the selected inputs is required.')
      const handle = await api.promote(releaseId, { approval_id: approval.approval_id }, key)
      return pollOperation(api, handle.operation_id)
    },
  }
}
