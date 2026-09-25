// Thin M02 contract views. Only demo routing/lineage and scenario overlays live here.
// Real UI modules must never import this fake-server/test adapter.
import type { ApprovalInputs, ApprovalRecord, BindingStatus, DeploymentReceipt, Fingerprints, VersionSummary } from '@/types/version-control'
import binding from '../../../../backend/tests/fixtures/vc_contracts/binding_status.json'
import version from '../../../../backend/tests/fixtures/vc_contracts/version_summary.json'
import approval from '../../../../backend/tests/fixtures/vc_contracts/approval_inputs.json'
import receipt from '../../../../backend/tests/fixtures/vc_contracts/deployment_receipt.json'
import fingerprint from '../../../../backend/tests/fixtures/vc_contracts/fingerprints.json'

export const fingerprints: Fingerprints = fingerprint.examples[0]
export const bindingFixture: BindingStatus = {
  ...binding.examples[3], drift: 'diverged', binding_id: 'demo-binding', binding_revision: 3,
  heads: { observed: 'external-3', approved: 'approved-2', deployed: 'deployed-1' }, stale: false,
  allowed_actions: ['restore', 'adopt', 'reapply', 'acknowledge', 'promote'], reasons: ['External changes captured; approval required'],
}
export const versionFixture: VersionSummary = {
  ...version.examples[1], origin: 'external', version_id: 'external-3', binding_id: 'demo-binding', parent_version_id: 'approved-2',
}
export const approvalInputsFixture: ApprovalInputs = {
  ...approval.examples[0], operation_type: 'adopt', source_version_id: versionFixture.version_id,
  target_binding: { ...approval.examples[0].target_binding, binding_id: bindingFixture.binding_id, binding_revision: bindingFixture.binding_revision },
}
export const approvalFixture: ApprovalRecord = {
  approval_id: 'demo-approval', inputs: approvalInputsFixture, approval_digest: 'approval-digest-1', valid: false,
  invalidation_reasons: ['Base changed after capture'], allowed_actions: ['vote'], votes: [],
}
export const receiptFixture: DeploymentReceipt = {
  ...receipt.examples[0], coordination_backend: 'delta', status: 'applied_partial',
  target_binding: { ...receipt.examples[0].target_binding, binding_id: 'demo-target' },
  rendered_fingerprints: { ...fingerprints, config: fingerprints.benchmark },
  observed_fingerprints: { ...fingerprints, config: fingerprints.metadata },
}
