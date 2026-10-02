import type { VersionSummary } from '@/types/version-control'

// True when this version's benchmark fingerprint differs from its parent's — i.e. the
// benchmark question set changed here. No parent (or unknown parent) => not a change.
export function benchmarkChanged(
  version: VersionSummary,
  byId: (id: string) => VersionSummary | undefined,
): boolean {
  if (!version.parent_version_id) return false
  const parent = byId(version.parent_version_id)
  return !!parent && parent.fingerprints.benchmark !== version.fingerprints.benchmark
}
