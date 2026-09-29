import { ApiError, getSpaceAccess } from '@/lib/api'
import type { SpaceAccess, SpaceAccessLevel } from '@/types'

// What the signed-in user may do on one Genie Agent, from the access route's answer.
// 'checking' until it arrives: nothing is shown on an assumed level.
export type SpaceAccessState = 'checking' | 'manage' | 'edit' | 'view' | 'none' | 'unknown'

export interface SpaceAccessAnswer {
  spaceId: string
  access: SpaceAccessState
  reason: string | null
}

export function accessFromLevel(level: SpaceAccessLevel | null): SpaceAccessState {
  return level ?? 'none'
}

export const canEditSpace = (access: SpaceAccessState) => access === 'edit' || access === 'manage'
export const canReadSpace = (access: SpaceAccessState) => canEditSpace(access) || access === 'view' || access === 'unknown'

export async function resolveSpaceAccess(
  spaceId: string,
  fetchAccess: (spaceId: string) => Promise<SpaceAccess> = getSpaceAccess,
): Promise<SpaceAccessAnswer> {
  try {
    const { level } = await fetchAccess(spaceId)
    return { spaceId, access: accessFromLevel(level), reason: null }
  } catch (err) {
    // 403 and 404 are Genie's answer, possibly naming a missing scope or entitlement.
    // Anything else means the question went unanswered, which never grants a write.
    if (err instanceof ApiError && (err.status === 403 || err.status === 404)) {
      return { spaceId, access: 'none', reason: err.message }
    }
    return { spaceId, access: 'unknown', reason: err instanceof Error ? err.message : null }
  }
}

export const SCAN_NEEDS_EDIT = 'Scanning needs Can Edit on this agent'

export function describeScanError(err: unknown): string {
  if (err instanceof ApiError && err.status === 403 && err.detail?.code === 'space_access_denied') return SCAN_NEEDS_EDIT
  if (err instanceof Error && err.message) return err.message
  return 'Scan failed.'
}

export const CHECKING_ACCESS = 'Checking your access…'
export const SPACE_VIEW_NOTICE =
  'You have Can View access: you can see this agent’s score, optimization runs and history. ' +
  'Scanning, optimizing, the semantic model and the agent’s configuration need Can Edit.'
export const SPACE_UNKNOWN_NOTICE =
  'Your access to this agent could not be confirmed, so this page is read-only for now. Reload to try again.'
export const SPACE_NO_ACCESS_TITLE = 'You can’t open this agent'
export const SPACE_NO_ACCESS_FALLBACK = 'You need Can View permission on this Genie Agent.'
export const CONFIG_NEEDS_EDIT = 'Viewing this agent’s configuration needs Can Edit permission.'
export const MODEL_NEEDS_EDIT = 'The semantic model and metric-view suggestions need Can Edit permission on this agent.'
export const SCAN_VIEWER_EMPTY = 'This agent has not been scanned yet. Running an IQ scan needs Can Edit permission.'
export const OPTIMIZE_VIEWER_NOTE =
  'Run details, starting a run, and rolling back, reverting or removing runs need Can Edit permission.'
