import { expect, it, vi } from 'vitest'
import { createMutationIntent, VersionControlApi } from './version-control-api'
import { fingerprints } from '@/components/version-control/fixtures'

it('double_click_or_network_loss_does_not_generate_new_mutation_key', async () => {
  const intent = createMutationIntent()
  const body = { version_id: 'version-1', expected_base: fingerprints, binding_revision: 3, approval_id: 'approval-1' }
  const key = intent.key({ bindingId: 'binding-1', body })
  expect(intent.key({ bindingId: 'binding-1', body })).toBe(key)
  const transport = vi.fn().mockRejectedValue(new Error('Network lost after admission'))
  const api = new VersionControlApi(transport)
  const first = api.restore('binding-1', body, key)
  const second = api.restore('binding-1', body, key)
  await expect(first).rejects.toThrow('Network lost')
  await expect(second).rejects.toThrow('Network lost')
  await expect(api.restore('binding-1', body, key)).rejects.toThrow('Network lost')
  expect(transport).toHaveBeenCalledTimes(1)
  const changed = { ...body, version_id: 'version-2' }
  const newKey = intent.key({ bindingId: 'binding-1', body: changed })
  expect(newKey).not.toBe(key)
  await expect(api.restore('binding-1', changed, key)).rejects.toThrow('different intent')
  await expect(api.restore('binding-1', changed, newKey)).rejects.toThrow('Network lost')
  expect(transport).toHaveBeenCalledTimes(2)
})

it('config_and_space_restore_hit_the_space_keyed_endpoints', async () => {
  const cfgTransport = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ history_enabled: true, writes_enabled: true, restore_enabled: false })))
  const cfg = await new VersionControlApi(cfgTransport).config()
  expect(cfg).toEqual({ history_enabled: true, writes_enabled: true, restore_enabled: false })
  expect(cfgTransport.mock.calls[0][0]).toBe('/api/version-control/config')

  const observation = { status: { binding_id: 'b', binding_revision: 1, heads: { observed: 'v1', approved: null, deployed: null }, drift: 'unknown', quarantined: false, unresolved_operation_id: null, observed_at: null, projection_as_of: '2026-01-01T00:00:00Z', stale: false, allowed_actions: [], reasons: [] }, captured_version: null, busy: false }
  const restoreTransport = vi.fn<typeof fetch>(async () => new Response(JSON.stringify(observation)))
  const result = await new VersionControlApi(restoreTransport).spaceRestore('space 1', { version_id: 'v1', expected_current_version_id: 'v0' }, 'key-1')
  expect(result).toEqual(observation)
  expect(restoreTransport.mock.calls[0][0]).toBe('/api/version-control/spaces/space%201/restore')
  const init = restoreTransport.mock.calls[0][1]
  expect(init?.method).toBe('POST')
  expect(JSON.parse(init?.body as string)).toEqual({ version_id: 'v1', expected_current_version_id: 'v0' })
  expect((init?.headers as Record<string, string>)['Idempotency-Key']).toBe('key-1')
})

it('interleaved_intents_reuse_the_original_key_when_the_first_intent_returns', async () => {
  const intent = createMutationIntent()
  const a = { version_id: 'a', expected_base: fingerprints, binding_revision: 3, approval_id: 'approval-1' }
  const b = { ...a, version_id: 'b' }
  const k1 = intent.key(a); const k2 = intent.key(b)
  expect(intent.key(a)).toBe(k1)
  expect(k2).not.toBe(k1)
  const transport = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ operation_id: 'operation', status: 'requested', job_run_id: null })))
  const api = new VersionControlApi(transport)
  await api.restore('binding-1', a, k1)
  await api.restore('binding-1', b, k2)
  await api.restore('binding-1', a, intent.key(a))
  expect(transport).toHaveBeenCalledTimes(2)
  await api.restore('binding-1', a, 'mismanaged-new-key')
  expect(transport).toHaveBeenCalledTimes(2)
  await expect(api.restore('binding-1', b, 'mismanaged-new-key')).rejects.toThrow('different intent')
})

import type { ApprovalInputs, DeploymentReceipt, DriftState, OperationStatus, Origin, Comparison, DiffItem, PackageManifest } from '@/types/version-control'
import { reconcileAction } from '@/components/version-control/reconcile-actions'
import { approvalInputsFixture } from '@/components/version-control/fixtures'
import enums from '../../../backend/tests/fixtures/vc_contracts/enums.json'
import binding_status from '../../../backend/tests/fixtures/vc_contracts/binding_status.json'
import version_summary from '../../../backend/tests/fixtures/vc_contracts/version_summary.json'
import operation_handle from '../../../backend/tests/fixtures/vc_contracts/operation_handle.json'
import approval_inputs from '../../../backend/tests/fixtures/vc_contracts/approval_inputs.json'
import deployment_receipt from '../../../backend/tests/fixtures/vc_contracts/deployment_receipt.json'
import api_error from '../../../backend/tests/fixtures/vc_contracts/api_error.json'
import diff_item from '../../../backend/tests/fixtures/vc_contracts/diff_item.json'
import package_manifest from '../../../backend/tests/fixtures/vc_contracts/package_manifest.json'
import observation_result from '../../../backend/tests/fixtures/vc_contracts/observation_result.json'

function assertNever(value: never): never { throw new Error(`Unexpected enum: ${value}`) }
function roundTripDriftState(value: DriftState) {
  switch (value) {
    case 'clean': return value
    case 'external_ahead': return value
    case 'desired_ahead': return value
    case 'diverged': return value
    case 'unknown': return value
    case 'unreachable': return value
    case 'applied_unverified': return value
    case 'conflicted': return value
    default: return assertNever(value)
  }
}
const DriftStateValues: Record<DriftState, true> = { clean: true, external_ahead: true, desired_ahead: true, diverged: true, unknown: true, unreachable: true, applied_unverified: true, conflicted: true }
function roundTripOperationStatus(value: OperationStatus) {
  switch (value) {
    case 'requested': return value
    case 'preimage_captured': return value
    case 'apply_attempted': return value
    case 'applied_unverified': return value
    case 'applied_partial': return value
    case 'confirmed': return value
    case 'conflicted': return value
    case 'quarantined': return value
    case 'failed': return value
    case 'compensation_attempted': return value
    case 'compensated': return value
    case 'noop': return value
    default: return assertNever(value)
  }
}
const OperationStatusValues: Record<OperationStatus, true> = { requested: true, preimage_captured: true, apply_attempted: true, applied_unverified: true, applied_partial: true, confirmed: true, conflicted: true, quarantined: true, failed: true, compensation_attempted: true, compensated: true, noop: true }
function roundTripOrigin(value: Origin) {
  switch (value) {
    case 'workbench': return value
    case 'external': return value
    case 'optimizer': return value
    case 'restore': return value
    case 'promotion': return value
    case 'unknown': return value
    case 'create': return value
    default: return assertNever(value)
  }
}
const OriginValues: Record<Origin, true> = { workbench: true, external: true, optimizer: true, restore: true, promotion: true, unknown: true, create: true }
function roundTripComparison(value: Comparison) {
  switch (value) {
    case 'equal': return value
    case 'different': return value
    case 'unknown': return value
    default: return assertNever(value)
  }
}
const ComparisonValues: Record<Comparison, true> = { equal: true, different: true, unknown: true }
function roundTripDiffCategory(value: DiffItem['category']) {
  switch (value) {
    case 'sources': return value
    case 'columns': return value
    case 'instructions': return value
    case 'joins': return value
    case 'filters': return value
    case 'parameters': return value
    case 'sql': return value
    case 'benchmarks': return value
    case 'questions': return value
    case 'metadata': return value
    case 'bindings': return value
    default: return assertNever(value)
  }
}
const DiffCategoryValues: Record<DiffItem['category'], true> = { sources: true, columns: true, instructions: true, joins: true, filters: true, parameters: true, sql: true, benchmarks: true, questions: true, metadata: true, bindings: true }
function roundTripDiffChange(value: DiffItem['change']) {
  switch (value) {
    case 'added': return value
    case 'removed': return value
    case 'modified': return value
    default: return assertNever(value)
  }
}
const DiffChangeValues: Record<DiffItem['change'], true> = { added: true, removed: true, modified: true }

it('every_drift_and_operation_enum_and_full_approval_receipt_payload_parses_from_m02_json_fixtures', async () => {
  expect(Object.keys(DriftStateValues).map(value => roundTripDriftState(value as keyof typeof DriftStateValues))).toEqual(enums.DriftState)
  expect(Object.keys(OperationStatusValues).map(value => roundTripOperationStatus(value as keyof typeof OperationStatusValues))).toEqual(enums.OperationStatus)
  expect(Object.keys(OriginValues).map(value => roundTripOrigin(value as keyof typeof OriginValues))).toEqual(enums.Origin)
  expect(Object.keys(ComparisonValues).map(value => roundTripComparison(value as keyof typeof ComparisonValues))).toEqual(enums.Comparison)
  expect(Object.keys(DiffCategoryValues).map(value => roundTripDiffCategory(value as keyof typeof DiffCategoryValues))).toEqual(enums.DiffCategory)
  expect(Object.keys(DiffChangeValues).map(value => roundTripDiffChange(value as keyof typeof DiffChangeValues))).toEqual(enums.DiffChange)
  const apiFor = (payload: unknown, status = 200) => new VersionControlApi(vi.fn<typeof fetch>(async () => new Response(JSON.stringify(payload), { status })))
  for (const example of binding_status.examples) {
    const parsed = await apiFor(example).status(example.binding_id)
    expect(parsed).toEqual(example)
    expect(roundTripDriftState(parsed.drift)).toBe(example.drift)
    expect(parsed.observed_at).toBeNull()
  }
  for (const example of version_summary.examples) {
    const page = await apiFor({ items: [example], next_cursor: null }).versions(example.binding_id)
    expect(page.items[0]).toEqual(example)
    expect(roundTripOrigin(page.items[0].origin)).toBe(example.origin)
    expect(await apiFor(example).version(example.binding_id, example.version_id)).toEqual(example)
  }
  for (const example of operation_handle.examples) {
    const parsed = await apiFor(example).restore('binding', { version_id: 'version', binding_revision: 1, expected_base: fingerprints, approval_id: 'approval' }, example.status)
    expect(parsed).toEqual(example)
    expect(roundTripOperationStatus(parsed.status)).toBe(example.status)
  }
  const inputs: ApprovalInputs[] = approval_inputs.examples
  const approvalFields = ["schema_version", "operation_id", "operation_type", "source_version_id", "raw_source_digest", "source_fingerprints", "artifact_digest", "mapping_digest", "rendered_target_digest", "transformer_version", "canonicalizer_version", "target_binding", "expected_base_fingerprints", "permission_policy_digest", "validation_policy_digest", "benchmark_policy_digest", "preflight_evidence_digest", "thresholds", "requester_id", "recovery_policy", "expires_at"]
  for (const example of inputs) {
    const parsed = (await apiFor({ approval_id: 'approval', inputs: example }).approval('approval')).inputs
    expect(parsed).toEqual(example)
    expect(Object.keys(parsed).sort()).toEqual([...approvalFields].sort())
    expect(parsed.target_binding.binding_id).toBe(example.target_binding.binding_id)
    expect(parsed.recovery_policy).toEqual({ automatic_clear: false, residual_risk_acknowledged: false })
    for (const field of ['artifact_digest', 'mapping_digest', 'transformer_version', 'permission_policy_digest'] as const) {
      expect(field in parsed).toBe(true)
      expect(parsed[field]).toBe(example[field])
    }
  }
  const receiptFields = ["schema_version", "release_id", "operation_id", "target_binding", "attempt_id", "generation", "coordination_backend", "approval_id", "approval_digest", "source_version_id", "package_digest", "mapping_digest", "intended_fingerprints", "rendered_fingerprints", "observed_fingerprints", "pre_version_id", "post_version_id", "transformer_version", "canonicalizer_version", "executor_id", "job_run_id", "validation_evidence_digest", "benchmark_evidence_digest", "status", "compensation_operation_id", "recorded_at"]
  for (const example of deployment_receipt.examples) {
    const typed: DeploymentReceipt = { ...example, status: roundTripOperationStatus(example.status as OperationStatus), coordination_backend: 'delta' }
    const parsed = await apiFor(example).receipt(example.release_id)
    expect(parsed).toEqual(typed)
    expect(Object.keys(parsed).sort()).toEqual([...receiptFields].sort())
    expect(parsed).toHaveProperty('post_version_id', example.post_version_id)
  }
  for (const example of api_error.examples) {
    await expect(apiFor(example, 423).observe('binding', 'open', 'observe')).rejects.toMatchObject({ code: example.code, details: example.details })
  }
  for (const comparison of enums.Comparison) {
    const parsed = await apiFor({ comparison, items: diff_item.examples }).diff('binding', 'left', 'right')
    expect(roundTripComparison(parsed.comparison)).toBe(comparison)
    expect(parsed.items).toEqual(diff_item.examples)
    for (const item of parsed.items) { roundTripDiffCategory(item.category); roundTripDiffChange(item.change) }
  }
  for (const example of observation_result.examples) expect(await apiFor(example).observe('binding', 'open', 'capture')).toEqual(example)
  const packages: PackageManifest[] = package_manifest.examples
  expect(JSON.parse(JSON.stringify(packages))).toEqual(package_manifest.examples)
  // Exercise the real authorization gate with a separately deserialized BindingRef object.
  const input = { ...inputs[0], expires_at: new Date(Date.now() + 60000).toISOString() }
  const transport = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ operation_id: 'adopt', status: 'confirmed', job_run_id: null, checkpoints: [], audit: [], receipt_references: [] })))
  const reviewed = { binding_revision: input.target_binding.binding_revision, expected_base: input.expected_base_fingerprints, approval_id: 'approval' }
  const approval = { approval_id: 'approval', inputs: input, valid: true, approval_digest: 'digest', invalidation_reasons: [], votes: [], allowed_actions: [] }
  await reconcileAction(new VersionControlApi(transport), input.target_binding.binding_id, 'adopt', reviewed, input, approval, 'adopt')
  expect(transport.mock.calls[0][0]).toBe(`/api/version-control/bindings/${input.target_binding.binding_id}/reconcile`)
  const staleTransport = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ approval_id: 'new-approval', inputs: input })))
  await reconcileAction(new VersionControlApi(staleTransport), input.target_binding.binding_id, 'adopt', { ...reviewed, binding_revision: reviewed.binding_revision + 1 }, input, approval, 'revision-changed')
  expect(staleTransport).toHaveBeenCalledTimes(1)
  expect(staleTransport.mock.calls[0][0]).toBe('/api/version-control/approvals')
  expect(approvalInputsFixture.target_binding).toHaveProperty('binding_revision')
  expect(fingerprints.canonicalizer_version).toBe('vc-c14n/1')
})
