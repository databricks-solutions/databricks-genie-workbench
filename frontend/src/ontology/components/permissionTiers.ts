// Pure helpers for PermissionBanner (kept out of the component file so Fast
// Refresh only sees component exports). MV-D50 identity + copy-button logic.
import type { PermissionTier } from "@/ontology/types"

const IDENTITY_LABEL: Record<PermissionTier["identity"], string> = {
  obo: "OBO",
  sp: "SP",
  batch: "Batch",
}

// The two foundation reads default to OBO (the viewing admin) with the SP as an
// optional upgrade (MV-D50) — so their identity reads "OBO (admin) or SP" rather
// than the backend's frozen enum value. Everything else keeps its plain label.
const DUAL_IDENTITY_TIERS = new Set<PermissionTier["id"]>(["signals", "tag_graph"])

export function identityLabel(tier: PermissionTier): string {
  return DUAL_IDENTITY_TIERS.has(tier.id) ? "OBO (admin) or SP" : IDENTITY_LABEL[tier.identity]
}

// The copy button shows on ANY tier that carries grant lines — including a green
// (ok) tier — so an admin can always copy the optional SP upgrade grants, not only
// when a tier is blocked/degraded.
export function showGrantCopy(tier: PermissionTier): boolean {
  return tier.grants.length > 0
}

export function copyButtonLabel(tier: PermissionTier): string {
  return tier.identity === "sp" ? "Copy GRANT SQL" : "Copy entitlement request"
}

export function grantCopyText(tier: PermissionTier): string {
  return tier.grants.join("\n")
}
