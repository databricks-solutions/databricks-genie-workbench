/**
 * Access & sharing — pure tier bucketing (MV-D107 Phase 4). Groups the preflight tiers by
 * PURPOSE so the panel reads as "what this does for you", not a capability matrix. Side-effect
 * free + component-free (react-refresh); no API. See PermissionBanner for the rendering.
 */
import type { PermissionTier, TierId } from "@/ontology/types"

// The OBO reads that render the ontology. These are "Reading your estate".
export const READ_TIER_IDS: readonly TierId[] = ["inventory", "signals", "tag_graph"]

export interface TierBuckets {
  /** OBO reads that power the ontology — the "you're set" group. */
  reading: PermissionTier[]
  /** Read tiers that carry an optional SP grant (a shared cache / consumer-safe serving). */
  upgrades: PermissionTier[]
  /** The locked write tier — not used this release (Ontology is read-only). */
  notUsed: PermissionTier[]
  /** The external Context Sources tier (Stage C SourcePanel). */
  external: PermissionTier[]
}

/**
 * Bucket tiers by purpose. A read tier appears in `reading` (its OBO status) and ALSO in
 * `upgrades` when it carries SP grant lines — the two facets are shown separately: current
 * read status vs the optional grant to speed/share it.
 */
export function bucketTiers(tiers: PermissionTier[]): TierBuckets {
  const reading = tiers.filter((t) => READ_TIER_IDS.includes(t.id))
  return {
    reading,
    upgrades: reading.filter((t) => t.grants.length > 0),
    notUsed: tiers.filter((t) => t.id === "membership_write"),
    external: tiers.filter((t) => t.id === "external_enrichment"),
  }
}

/** Whether any REQUIRED read tier is blocked/degraded — the only condition that warns. */
export function readingBlocked(tiers: PermissionTier[]): boolean {
  return tiers
    .filter((t) => READ_TIER_IDS.includes(t.id))
    .some((t) => t.status === "blocked" || t.status === "degraded")
}
