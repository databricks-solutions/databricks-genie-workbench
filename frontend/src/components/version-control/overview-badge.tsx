import type { BindingStatus } from '@/types/version-control'
import type { VersionControlApi } from '@/lib/version-control-api'
import { OperationDrillThrough } from './reconcile'
export function OverviewBadge({ status, api }: { status: BindingStatus; api?: VersionControlApi }) {
  const labels = { clean: 'Policy mismatch', external_ahead: 'External ahead', desired_ahead: 'Desired ahead', diverged: 'Diverged', unknown: 'Unknown', unreachable: 'Unreachable', applied_unverified: 'Applied, unverified', conflicted: 'Conflicted' }
  let label = labels[status.drift]
  if (status.drift === 'clean') {
    label = !status.heads.approved || !status.heads.deployed ? 'Unknown' : status.heads.approved === status.heads.deployed ? 'In sync' : 'Policy mismatch'
  }
  if (status.stale && label === 'In sync') label = 'Policy equality unverified while stale'
  return <div className="rounded-lg border border-border p-3 text-sm">
    <strong>{label}</strong>{status.stale && <span> · Stale</span>}
    {status.quarantined && <span> · Quarantined</span>}
    {status.unresolved_operation_id && <OperationDrillThrough key={status.unresolved_operation_id} api={api} operationId={status.unresolved_operation_id} />}
    <ul aria-label="Drift reasons">{status.reasons.map((reason, index) => <li key={index}>{reason}</li>)}</ul>
    <dl>{Object.entries(status.heads).map(([head, version]) => <div key={head}><dt>{head}</dt><dd title={version ?? undefined}>{version?.slice(0, 8) ?? 'Not recorded'}</dd></div>)}</dl>
    <p>Projection as of <time>{status.projection_as_of}</time></p>
    <p>Observed: {status.observed_at ?? 'Unknown'}</p>
  </div>
}
