import { ApiError, getSpaceAccess } from '@/lib/api'
import type { SpaceAccess, SpaceAccessLevel } from '@/types'

// What the Version Control tab may do, derived only from the access route's answer for
// the current space. 'checking' until it arrives: nothing is shown on an assumed level.
export type VcAccess = 'checking' | 'edit' | 'view' | 'none' | 'unknown'

export interface VcAccessState {
  spaceId: string
  access: VcAccess
  reason: string | null
}

export function vcAccessFromLevel(level: SpaceAccessLevel | null): VcAccess {
  if (level === 'manage' || level === 'edit') return 'edit'
  if (level === 'view') return 'view'
  return 'none'
}

export const canEditVersions = (access: VcAccess) => access === 'edit'
export const canReadVersions = (access: VcAccess) =>
  access === 'edit' || access === 'view' || access === 'unknown'

export async function resolveVcAccess(
  spaceId: string,
  fetchAccess: (spaceId: string) => Promise<SpaceAccess> = getSpaceAccess,
): Promise<VcAccessState> {
  try {
    const { level } = await fetchAccess(spaceId)
    return { spaceId, access: vcAccessFromLevel(level), reason: null }
  } catch (err) {
    // 403 and 404 are Genie's answer, possibly naming a missing scope or entitlement.
    // Anything else means the question went unanswered, which never grants a write.
    if (err instanceof ApiError && (err.status === 403 || err.status === 404)) {
      return { spaceId, access: 'none', reason: err.message }
    }
    return { spaceId, access: 'unknown', reason: err instanceof Error ? err.message : null }
  }
}

export const EDITOR_EXPLAINER =
  'A version is auto-captured whenever you open this tab and after each optimizer run. ' +
  'Edits made directly in Genie are captured the next time you open this tab — there is no background watcher.'
export const VIEWER_EXPLAINER =
  'Versions are captured when someone with Can Edit opens this tab, and after each optimizer run.'
export const VIEW_NOTICE =
  'You have Can View access: you can see when this agent changed. Opening, comparing, capturing, restoring and tagging versions need Can Edit.'
export const UNKNOWN_NOTICE =
  'Your access to this agent could not be confirmed, so version control is read-only for now. Reload the tab to try again.'
export const EDITOR_DETAIL_HINT =
  'Select a version to inspect its configuration and restore it, or tick two versions to compare.'
export const VIEWER_DETAIL_HINT = 'Opening a version’s configuration needs Can Edit permission on this agent.'
export const VIEWER_EMPTY_HINT =
  'No versions have been captured yet. A version is captured when someone with Can Edit opens this tab, or when an optimization runs.'
export const NO_ACCESS_TITLE = 'You can’t see this agent’s version history'
export const NO_ACCESS_FALLBACK = 'You need Can View permission on this Genie Agent.'

export function captureExplainer(access: VcAccess): string {
  return canEditVersions(access) ? EDITOR_EXPLAINER : VIEWER_EXPLAINER
}
