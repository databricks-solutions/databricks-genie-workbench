import type { VersionControlApi } from '@/lib/version-control-api'

// Non-component helpers live in a sibling module so the .tsx files export only
// components (react-refresh/only-export-components).

export function loadOverview(api: VersionControlApi, cursor?: string, signal?: AbortSignal) {
  return api.overview(cursor, signal)
}
