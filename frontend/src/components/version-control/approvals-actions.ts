import type { VersionControlApi } from '@/lib/version-control-api'
import type { ApprovalRecord } from '@/types/version-control'

// Non-component helpers live in a sibling module so the .tsx files export only
// components (react-refresh/only-export-components).

export function voteApproval(api: VersionControlApi, record: ApprovalRecord, decision: 'approve' | 'reject', key: string) {
  if (!record.allowed_actions.includes('vote')) return Promise.reject(new Error('Voting is not allowed by the server.'))
  return api.vote(record.approval_id, decision, key)
}
