export type Comparison = 'equal' | 'different' | 'unknown'
export type DriftState = 'clean' | 'external_ahead' | 'desired_ahead' | 'diverged' | 'unknown' | 'unreachable' | 'applied_unverified' | 'conflicted'
export type OperationStatus = 'requested' | 'preimage_captured' | 'apply_attempted' | 'applied_unverified' | 'applied_partial' | 'confirmed' | 'conflicted' | 'quarantined' | 'failed' | 'compensation_attempted' | 'compensated' | 'noop'
export type Origin = 'workbench' | 'external' | 'optimizer' | 'restore' | 'promotion' | 'unknown' | 'create'
export interface BindingRef { binding_id: string; binding_revision: number; space_key: string; workspace_id: string; space_id: string | null; environment: string }
export interface Heads { observed: string | null; approved: string | null; deployed: string | null }
export interface Fingerprints { config: string; benchmark: string; metadata: string; canonicalizer_version: string }
export interface BindingStatus {
  binding_id: string; binding_revision: number; heads: Heads; drift: DriftState
  quarantined: boolean; unresolved_operation_id: string | null
  observed_at: string | null; projection_as_of: string; stale: boolean
  allowed_actions: string[]; reasons: string[]; approval_inputs?: ApprovalRequestInputs
}
export interface VersionSummary {
  version_id: string; binding_id: string; observed_at: string; origin: Origin; observed_by: string
  parent_version_id: string | null; restored_from_version_id: string | null
  fingerprints: Fingerprints; optimizer_run_id: string | null; champion_id: string | null
}
export interface VersionTag { label: string; note: string | null; author?: string | null }
export type VersionTagMap = Record<string, VersionTag>
export interface Page<Item> { items: Item[]; next_cursor: string | null }
export type VersionPage = Page<VersionSummary>
export type OverviewPage = Page<BindingStatus>
export interface VersionDetail extends VersionSummary { snapshot?: unknown }
export interface OperationHandle { operation_id: string; status: OperationStatus; job_run_id: string | null }
export interface Operation extends OperationHandle { checkpoints: string[]; audit: string[]; receipt_references: string[]; evidence_digest?: string; approval_inputs?: ApprovalRequestInputs }
export interface DiffItem {
  category: 'sources' | 'columns' | 'instructions' | 'joins' | 'filters' | 'parameters' | 'sql' | 'benchmarks' | 'questions' | 'metadata' | 'bindings'
  path: string; change: 'added' | 'removed' | 'modified'; before: unknown; after: unknown; review_required: boolean
}
export interface SemanticDiff { items: DiffItem[]; comparison: Comparison }
export interface ObservationResult { status: BindingStatus; captured_version: VersionSummary | null; busy: boolean }
export interface ApiError { code: string; message: string; operation_id?: string; retryable: boolean; stale: boolean; details?: Record<string, unknown> }
export interface ReviewedCommand { binding_revision: number; expected_base: Fingerprints; approval_id: string }
export interface RestoreCommand extends ReviewedCommand { version_id: string }
// Simple in-workspace restore (CUJ-1 §4.5): restore a stored version onto the live space
// as the OBO user, guarded by the version the client believes is current.
export interface SpaceRestoreRequest { version_id: string; expected_current_version_id: string }
// Deployment flag surface (CUJ-1 §5) so the UI can render disabled-with-explainer states.
export interface VcConfig { history_enabled: boolean; writes_enabled: boolean; restore_enabled: boolean }
export interface ReconcileCommand extends ReviewedCommand { action: 'adopt' | 'reapply' | 'acknowledge'; policy_inputs: Record<string, unknown> }
export interface ApprovalInputs {
  schema_version: string; operation_id?: string; operation_type: string; source_version_id: string
  raw_source_digest: string; source_fingerprints: Fingerprints; artifact_digest: string | null; mapping_digest: string | null
  rendered_target_digest: string; transformer_version: string | null; canonicalizer_version: string
  target_binding: BindingRef; expected_base_fingerprints: Fingerprints; permission_policy_digest: string | null
  validation_policy_digest: string; benchmark_policy_digest: string; preflight_evidence_digest: string
  thresholds: Record<string, number>; requester_id?: string; recovery_policy: { automatic_clear: boolean; residual_risk_acknowledged: boolean }; expires_at: string
}
export type ApprovalRequestInputs = Omit<ApprovalInputs, 'requester_id' | 'operation_id'>
export interface ApprovalRequest { approval_id: string; inputs: ApprovalInputs }
export interface ApprovalRecord extends ApprovalRequest {
  approval_digest: string | null; valid: boolean; invalidation_reasons: string[]; allowed_actions: string[]
  votes: { actor_id: string; decision: 'approve' | 'reject'; recorded_at: string; target_group_eligible: boolean }[]
}
export interface PackageManifest {
  schema_version: string; space_key: string; source_version_id: string; raw_source_digest: string
  source_fingerprints: Fingerprints; artifact_digest: string; mapping_digest: string; transformer_version: string
  required_sources: string[]; validation_policy_digest: string; benchmark_policy_digest: string
  ownership: Record<string, string>; file_digests: Record<string, string>; package_digest: string
}
export interface DeploymentReceipt {
  schema_version: string; release_id: string; operation_id: string; target_binding: BindingRef; attempt_id: string
  generation: number; coordination_backend: 'delta'; approval_id: string; approval_digest: string
  source_version_id: string; package_digest: string; mapping_digest: string
  intended_fingerprints: Fingerprints; rendered_fingerprints: Fingerprints; observed_fingerprints: Fingerprints
  pre_version_id: string; post_version_id: string | null; transformer_version: string; canonicalizer_version: string
  executor_id: string; job_run_id: string; validation_evidence_digest: string; benchmark_evidence_digest: string
  status: OperationStatus; compensation_operation_id: string | null; recorded_at: string
}
export interface ReleaseCommand { source_binding: string; source_version_id: string; target_binding: string; mapping_digest: string; policy: string }
export interface ReleaseHandle extends OperationHandle { release_id: string }
