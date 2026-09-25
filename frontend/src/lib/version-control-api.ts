import type { ApiError, ApprovalRequestInputs, ApprovalRecord, ApprovalRequest, BindingStatus, DeploymentReceipt, ObservationResult, Operation, OperationHandle, OverviewPage, ReconcileCommand, ReleaseCommand, ReleaseHandle, RestoreCommand, SemanticDiff, SpaceRestoreRequest, VcConfig, VersionDetail, VersionPage, VersionTag } from '@/types/version-control'

export class VersionControlError extends Error implements ApiError {
  code: string
  details?: Record<string, unknown>
  operation_id?: string
  retryable: boolean
  stale: boolean
  status: number
  constructor(status: number, error: ApiError) {
    super(error.message)
    this.status = status
    this.code = error.code
    this.details = error.details
    this.operation_id = error.operation_id
    this.retryable = error.retryable
    this.stale = error.stale
  }
}

const bindingPath = (bindingId: string) => `/bindings/${encodeURIComponent(bindingId)}`
export function createMutationIntent() {
  const keys = new Map<string, string>()
  return { key: (body: unknown) => {
    const serialized = JSON.stringify(body)
    if (!keys.has(serialized)) keys.set(serialized, crypto.randomUUID())
    return keys.get(serialized)!
  } }
}
const pageQuery = (cursor?: string, limit = 25) => {
  const query = new URLSearchParams({ limit: String(Math.min(100, Math.max(1, limit))) })
  if (cursor) query.set('cursor', cursor)
  return query.toString()
}

export class VersionControlApi {
  private transport: typeof fetch
  private commands = new Map<string, { intent: string; result: Promise<unknown> }>()
  private intents = new Map<string, Promise<unknown>>()
  constructor(transport: typeof fetch) { this.transport = transport }
  private async request<Result>(path: string, init: RequestInit): Promise<Result> {
    const response = await this.transport(`/api/version-control${path}`, init)
    const payload = await response.json()
    if (!response.ok) throw new VersionControlError(response.status, payload)
    return payload as Result
  }
  private get<Result>(path: string, signal?: AbortSignal) { return this.request<Result>(path, { signal }) }
  private post<Result>(path: string, body: unknown, key: string, dedupeIntent = true) {
    const intent = JSON.stringify({ path, body })
    const prior = this.commands.get(key)
    if (prior) {
      if (prior.intent !== intent) return Promise.reject(new VersionControlError(409, { code: 'IDEMPOTENCY_MISMATCH', message: 'Key belongs to a different intent; conflict preserved.', stale: true, retryable: false }))
      return prior.result as Promise<Result>
    }
    const result = (dedupeIntent ? this.intents.get(intent) as Promise<Result> | undefined : undefined) ?? this.request<Result>(path, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) })
    this.commands.set(key, { intent, result })
    if (dedupeIntent) this.intents.set(intent, result)
    return result
  }
  // Deployment flag surface (CUJ-1 §5) — lets the UI render disabled-with-explainer states.
  config(signal?: AbortSignal) { return this.get<VcConfig>('/config', signal) }
  // Space-keyed bridge (workbench is space_id-keyed; the ledger is binding-keyed).
  spaceVersions(spaceId: string, cursor?: string, signal?: AbortSignal) { return this.get<VersionPage>(`/spaces/${encodeURIComponent(spaceId)}/versions?${pageQuery(cursor)}`, signal) }
  spaceObserve(spaceId: string, key: string) { return this.post<ObservationResult>(`/spaces/${encodeURIComponent(spaceId)}/observe`, {}, key, false) }
  // Simple in-workspace restore (CUJ-1 §4.5): apply a stored version to the live space.
  spaceRestore(spaceId: string, body: SpaceRestoreRequest, key: string) { return this.post<ObservationResult>(`/spaces/${encodeURIComponent(spaceId)}/restore`, body, key, false) }
  // Version tags (Task 4 endpoints): space-keyed read + per-version set/delete.
  spaceTags(spaceId: string, signal?: AbortSignal) {
    return this.get<Record<string, VersionTag>>(`/spaces/${encodeURIComponent(spaceId)}/tags`, signal)
  }
  setVersionTag(spaceId: string, versionId: string, body: { label: string; note?: string | null }) {
    return this.request<VersionTag & { version_id: string }>(
      `/spaces/${encodeURIComponent(spaceId)}/versions/${encodeURIComponent(versionId)}/tag`,
      { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  }
  deleteVersionTag(spaceId: string, versionId: string) {
    return this.request<{ version_id: string; deleted: boolean }>(
      `/spaces/${encodeURIComponent(spaceId)}/versions/${encodeURIComponent(versionId)}/tag`,
      { method: 'DELETE' })
  }
  overview(cursor?: string, signal?: AbortSignal) { return this.get<OverviewPage>(`/overview?${pageQuery(cursor)}`, signal) }
  status(bindingId: string, signal?: AbortSignal) { return this.get<BindingStatus>(`${bindingPath(bindingId)}/status`, signal) }
  versions(bindingId: string, cursor?: string, signal?: AbortSignal) { return this.get<VersionPage>(`${bindingPath(bindingId)}/versions?${pageQuery(cursor)}`, signal) }
  version(bindingId: string, versionId: string) { return this.get<VersionDetail>(`${bindingPath(bindingId)}/versions/${encodeURIComponent(versionId)}`) }
  diff(bindingId: string, left: string, right: string) { return this.get<SemanticDiff>(`${bindingPath(bindingId)}/diff?${new URLSearchParams({ left, right })}`) }
  observe(bindingId: string, reason: 'open' | 'history' | 'refresh' | 'return', key: string) { return this.post<ObservationResult>(`${bindingPath(bindingId)}/observe`, { reason }, key, false) }
  restore(bindingId: string, body: RestoreCommand, key: string) { return this.post<OperationHandle>(`${bindingPath(bindingId)}/restore`, body, key) }
  reconcile(bindingId: string, body: ReconcileCommand, key: string) { return this.post<OperationHandle>(`${bindingPath(bindingId)}/reconcile`, body, key) }
  requestApproval(inputs: ApprovalRequestInputs, key: string) {
    // Whitelist the request contract: response-only identities never cross this boundary.
    const body: ApprovalRequestInputs = {
      schema_version: inputs.schema_version,
      operation_type: inputs.operation_type,
      source_version_id: inputs.source_version_id,
      raw_source_digest: inputs.raw_source_digest,
      source_fingerprints: inputs.source_fingerprints,
      artifact_digest: inputs.artifact_digest,
      mapping_digest: inputs.mapping_digest,
      rendered_target_digest: inputs.rendered_target_digest,
      transformer_version: inputs.transformer_version,
      canonicalizer_version: inputs.canonicalizer_version,
      target_binding: inputs.target_binding,
      expected_base_fingerprints: inputs.expected_base_fingerprints,
      permission_policy_digest: inputs.permission_policy_digest,
      validation_policy_digest: inputs.validation_policy_digest,
      benchmark_policy_digest: inputs.benchmark_policy_digest,
      preflight_evidence_digest: inputs.preflight_evidence_digest,
      thresholds: inputs.thresholds,
      recovery_policy: inputs.recovery_policy,
      expires_at: inputs.expires_at,
    }
    if (!body.preflight_evidence_digest || !body.raw_source_digest || !body.rendered_target_digest || !body.validation_policy_digest || !body.benchmark_policy_digest) return Promise.reject(new Error('Server approval evidence unavailable; request blocked.'))
    return this.post<ApprovalRequest>('/approvals', body, key)
  }
  vote(approvalId: string, decision: 'approve' | 'reject', key: string) { return this.post<ApprovalRecord>(`/approvals/${encodeURIComponent(approvalId)}/votes`, { decision }, key) }
  approval(approvalId: string) { return this.get<ApprovalRecord>(`/approvals/${encodeURIComponent(approvalId)}`) }
  operation(operationId: string) { return this.get<Operation>(`/operations/${encodeURIComponent(operationId)}`) }
  release(body: ReleaseCommand, key: string) { return this.post<ReleaseHandle>('/releases', body, key) }
  validate(releaseId: string, key: string) { return this.post<OperationHandle>(`/releases/${encodeURIComponent(releaseId)}/validate`, {}, key) }
  promote(releaseId: string, body: { approval_id: string }, key: string) { return this.post<OperationHandle>(`/releases/${encodeURIComponent(releaseId)}/promote`, body, key) }
  receipt(releaseId: string) { return this.get<DeploymentReceipt | OperationHandle>(`/releases/${encodeURIComponent(releaseId)}/receipt`) }
}
