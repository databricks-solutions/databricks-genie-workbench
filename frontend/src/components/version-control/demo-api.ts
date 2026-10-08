import { VersionControlApi } from '@/lib/version-control-api'
import type { ApprovalInputs, ApprovalRecord, BindingStatus, DeploymentReceipt, Operation, OperationStatus, ReleaseCommand, VersionSummary } from '@/types/version-control'
import { approvalFixture, approvalInputsFixture, bindingFixture, receiptFixture, versionFixture } from './fixtures'

const statuses: BindingStatus[] = [
  bindingFixture,
  { ...bindingFixture, binding_id: 'demo-quarantined', quarantined: true, allowed_actions: [], unresolved_operation_id: 'quarantined-operation' },
  { ...bindingFixture, binding_id: 'demo-conflicted', drift: 'conflicted', allowed_actions: [] },
  { ...bindingFixture, binding_id: 'demo-unknown', drift: 'unknown', heads: { observed: null, approved: null, deployed: null }, allowed_actions: [] },
  { ...bindingFixture, binding_id: 'demo-unreachable', drift: 'unreachable', stale: true, allowed_actions: [] },
  { ...bindingFixture, binding_id: 'demo-unverified', drift: 'applied_unverified', unresolved_operation_id: 'unverified-operation', allowed_actions: [] },
  { ...bindingFixture, binding_id: 'demo-partial', unresolved_operation_id: 'partial-operation', allowed_actions: [] },
  { ...bindingFixture, binding_id: 'demo-busy', allowed_actions: [] },
  { ...bindingFixture, binding_id: 'demo-history-unreachable', drift: 'unreachable', stale: true, allowed_actions: [] },
]

export function createDemoTransport(): typeof fetch {
  const approvals = new Map<string, ApprovalRecord>([[approvalFixture.approval_id, structuredClone(approvalFixture)]])
  const operations = new Map<string, Operation>()
  const releases = new Map<string, { selection: ReleaseCommand; validated: boolean; receipt: DeploymentReceipt | null }>()
  const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status })
  const failure = (status: number, message: string) => json({ code: `DEMO_${status}`, message, stale: true, retryable: false }, status)
  const operation = (status: OperationStatus, evidence: string) => {
    const result: Operation = { operation_id: `demo-operation-${operations.size + 1}`, status, job_run_id: 'demo-job', evidence_digest: `demo-evidence-${operations.size + 1}`, checkpoints: ['Preimage persisted', evidence], audit: ['Fixture target executor; no real workspace execution.'], receipt_references: [] }
    operations.set(result.operation_id, result)
    return { ...result, status: 'requested' }
  }
  const approved = (id: string, bindingId: string) => {
    const record = approvals.get(id)
    return record?.valid && record.inputs.target_binding.binding_id === bindingId && Date.parse(record.inputs.expires_at) > Date.now() ? record : null
  }
  return async (input, init) => {
    init?.signal?.throwIfAborted()
    const url = new URL(String(input), 'https://vc-demo.invalid')
    const path = url.pathname.replace('/api/version-control', '')
    if (!url.pathname.startsWith('/api/version-control/')) return failure(404, 'Only the VC/1.0 fixture surface is available.')
    const isCommand = init?.method === 'POST'
    if (isCommand && !new Headers(init.headers).get('Idempotency-Key')) return failure(422, 'Idempotency-Key required.')
    const body: Record<string, unknown> = init?.body ? JSON.parse(String(init.body)) : {}
    const bindingId = decodeURIComponent(path.split('/')[2] ?? '')
    const status = statuses.find(status => status.binding_id === bindingId)
    const history: VersionSummary[] = [
      { ...versionFixture, binding_id: bindingId },
      { ...versionFixture, binding_id: bindingId, version_id: 'approved-2', origin: 'optimizer', optimizer_run_id: 'demo-run-2', champion_id: 'champion-2', parent_version_id: 'deployed-1' },
      { ...versionFixture, binding_id: bindingId, version_id: 'deployed-1', origin: 'workbench', parent_version_id: null },
    ]
    if (path === '/overview' && !isCommand) {
      const offset = Number(url.searchParams.get('cursor') ?? 0)
      const size = Math.min(3, Number(url.searchParams.get('limit') ?? 25))
      return json({ items: statuses.slice(offset, offset + size), next_cursor: offset + size < statuses.length ? String(offset + size) : null })
    }
    if (path.startsWith('/bindings/') && !status) return failure(404, 'Binding is not in this demo.')
    if (path.endsWith('/status')) return json(status)
    if (path.endsWith('/observe') && isCommand) {
      if (status?.stale) return failure(503, 'Snapshot persistence unavailable; retained history is stale.')
      const approval_inputs = { ...approvalInputsFixture, expires_at: new Date(Date.now() + 3600000).toISOString() }
      return json({ status: { ...status, approval_inputs }, captured_version: status?.heads.observed ? history[0] : null, busy: bindingId === 'demo-busy' })
    }
    if (path.endsWith('/versions')) {
      if (bindingId === 'demo-history-unreachable') return failure(503, 'History storage unreachable.')
      const offset = Number(url.searchParams.get('cursor') ?? 0)
      return json({ items: history.slice(offset, offset + 2), next_cursor: offset === 0 ? '2' : null })
    }
    if (path.includes('/versions/')) {
      const version = history.find(version => version.version_id === decodeURIComponent(path.split('/').at(-1) ?? ''))
      return version ? json(version) : failure(404, 'Version unavailable.')
    }
    if (path.endsWith('/diff')) return json({ comparison: status?.stale ? 'unknown' : url.searchParams.get('left') === url.searchParams.get('right') ? 'equal' : 'different', items: status?.stale || url.searchParams.get('left') === url.searchParams.get('right') ? [] : [{ category: 'sql', path: 'example_queries/1', change: 'modified', before: 'SELECT old_column FROM sales', after: 'SELECT new_column FROM sales', review_required: true }] })
    if ((path.endsWith('/restore') || path.endsWith('/reconcile')) && isCommand) {
      if (status?.quarantined || status?.unresolved_operation_id) return failure(423, 'Quarantined / unresolved operation. Inspect evidence; no replay.')
      if (status?.stale) return failure(503, 'Coordination unavailable.')
      if (status?.drift === 'conflicted' || body.binding_revision !== status?.binding_revision || JSON.stringify(body.expected_base) !== JSON.stringify(versionFixture.fingerprints)) return failure(409, 'Reviewed base conflict; preserved external history.')
      if (body.action !== 'acknowledge' && !approved(String(body.approval_id), bindingId)) return failure(409, 'Fresh binding-local approval required.')
      return json(operation('confirmed', body.action === 'acknowledge' ? 'Divergence acknowledged; heads unchanged.' : 'Demo read-back verified; projection remains an illustrative fixture.'), 202)
    }
    if (path === '/approvals' && isCommand) {
      const inputs: ApprovalInputs = JSON.parse(String(init?.body))
      const approvalId = `demo-approval-${approvals.size + 1}`
      const record: ApprovalRecord = { approval_id: approvalId, inputs, valid: false, approval_digest: null, invalidation_reasons: [], allowed_actions: ['vote'], votes: [{ actor_id: 'demo-reviewer-one', decision: 'approve', recorded_at: new Date().toISOString(), target_group_eligible: true }] }
      approvals.set(approvalId, record)
      return json({ approval_id: approvalId, inputs })
    }
    if (path.startsWith('/approvals/')) {
      const approvalId = decodeURIComponent(path.split('/')[2])
      const record = approvals.get(approvalId)
      if (!record) return failure(404, 'Approval unavailable.')
      if (path.endsWith('/votes') && isCommand) {
        if (Object.keys(body).some(key => key !== 'decision') || !['approve', 'reject'].includes(String(body.decision))) return failure(422, 'A vote accepts a decision only; actor comes from authentication.')
        record.votes = [...record.votes.filter(vote => vote.actor_id !== 'demo-reviewer-two'), { actor_id: 'demo-reviewer-two', decision: body.decision as 'approve' | 'reject', recorded_at: new Date().toISOString(), target_group_eligible: false }]
        record.valid = body.decision === 'approve' && Date.parse(record.inputs.expires_at) > Date.now() && record.invalidation_reasons.length === 0
        record.approval_digest = record.valid ? 'demo-bound-approval-digest' : null
      }
      return json(record)
    }
    if (path === '/releases' && isCommand) {
      if (body.target_binding !== 'demo-target' || body.mapping_digest !== 'mapping-1' || body.policy !== 'production') return failure(422, 'Use pre-enrolled demo-target, immutable mapping-1 and production policy.')
      const releaseId = `demo-release-${releases.size + 1}`
      const selection: ReleaseCommand = JSON.parse(String(init?.body))
      releases.set(releaseId, { selection, validated: false, receipt: null })
      return json({ ...operation('confirmed', 'Demo package prepared'), release_id: releaseId }, 202)
    }
    if (path.startsWith('/releases/')) {
      const releaseId = decodeURIComponent(path.split('/')[2])
      const release = releases.get(releaseId)
      if (!release) return failure(404, 'Release unavailable.')
      if (path.endsWith('/validate') && isCommand) {
        release.validated = true
        const handle = operation('confirmed', 'Mapping and preflight evidence validated')
        operations.get(handle.operation_id)!.approval_inputs = { ...approvalInputsFixture, operation_type: 'promotion', target_binding: { ...approvalInputsFixture.target_binding, binding_id: release.selection.target_binding }, source_version_id: release.selection.source_version_id, mapping_digest: release.selection.mapping_digest, preflight_evidence_digest: handle.evidence_digest!, expires_at: new Date(Date.now() + 3600000).toISOString() }
        return json(handle, 202)
      }
      if (path.endsWith('/promote') && isCommand) {
        const approval = approved(String(body.approval_id), release.selection.target_binding)
        if (!release.validated || !approval || approval.inputs.mapping_digest !== release.selection.mapping_digest || approval.inputs.source_version_id !== release.selection.source_version_id) return failure(409, 'Release requires validated mapping and matching target-local approval.')
        const handle = operation('confirmed', 'Rendered target read-back verified')
        release.receipt = { ...receiptFixture, release_id: releaseId, operation_id: handle.operation_id, target_binding: { ...approvalInputsFixture.target_binding, binding_id: release.selection.target_binding }, approval_id: approval.approval_id, source_version_id: release.selection.source_version_id, mapping_digest: release.selection.mapping_digest, status: 'confirmed', observed_fingerprints: receiptFixture.rendered_fingerprints, post_version_id: 'demo-target-after', compensation_operation_id: null }
        return json(handle, 202)
      }
      if (path.endsWith('/receipt')) return release.receipt ? json(release.receipt) : json({ operation_id: releaseId, status: 'requested', job_run_id: null }, 202)
    }
    if (path.startsWith('/operations/')) {
      const operationId = decodeURIComponent(path.split('/')[2])
      const stored = operations.get(operationId)
      if (stored) return json(stored)
      const status: OperationStatus = operationId === 'partial-operation' ? 'applied_partial' : operationId === 'unverified-operation' ? 'applied_unverified' : 'quarantined'
      return json({ operation_id: operationId, status, job_run_id: 'demo-job', checkpoints: ['Description write incomplete or ambiguous create response'], audit: ['Operator must inspect persisted evidence; no blind retry.'], receipt_references: ['demo-partial-receipt'] })
    }
    return failure(404, 'This fixture route is unavailable; no network fallback.')
  }
}
export const demoTransport = createDemoTransport()
export const demoApi = new VersionControlApi(demoTransport)
