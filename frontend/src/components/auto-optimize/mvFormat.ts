/**
 * Shared formatting helpers for the metric view output panels (Prompt 13).
 *
 * Kept in one place so the suggest-only card and the create-and-attach panel
 * agree on the join-strategy vocabulary and the verbatim honesty copy.
 */

// POV §7.5, verbatim. The suggest-only panel states plainly that nothing was
// created or attached this run, so no lift could be measured.
export const LIFT_NOT_MEASURED =
  "Lift not measured — this metric view was not created or attached during this run."

// The reachable ladder rungs only. `nested` is unreachable today (MV-D14/D15),
// so it is deliberately absent — an unexpected value renders no badge rather
// than inventing a label. Accepts either the backend's `subquery_source` or the
// hyphenated display spelling.
const JOIN_STRATEGY_LABEL: Record<string, string> = {
  denormalized: "Denormalized",
  subquery_source: "Subquery source",
  "subquery-source": "Subquery source",
}

export function joinStrategyLabel(raw: string | null | undefined): string | null {
  if (!raw) return null
  return JOIN_STRATEGY_LABEL[raw] ?? null
}

// Tier drives the confidence badge color; unknown tiers fall back to neutral.
const TIER_VARIANT: Record<string, "high" | "medium" | "low"> = {
  HIGH: "high",
  MEDIUM: "medium",
  LOW: "low",
}

export function tierVariant(
  tier: string | null | undefined,
): "high" | "medium" | "low" | "secondary" {
  if (!tier) return "secondary"
  return TIER_VARIANT[tier.toUpperCase()] ?? "secondary"
}

// A metric_views[] entry identifies its UC object by `identifier`.
export function metricViewIdentifiers(spaceData: unknown): string[] {
  if (!spaceData || typeof spaceData !== "object") return []
  const ds = (spaceData as Record<string, unknown>).data_sources
  if (!ds || typeof ds !== "object") return []
  const views = (ds as Record<string, unknown>).metric_views
  if (!Array.isArray(views)) return []
  return views
    .map((m) =>
      m && typeof m === "object"
        ? String((m as Record<string, unknown>).identifier ?? "")
        : "",
    )
    .filter(Boolean)
}

// Derive a workspace origin from the run's resource links, which point into the
// workspace. Used to build a Catalog Explorer deep link for a created view.
export function workspaceOriginFromLinks(
  links: { url: string }[] | undefined,
): string | null {
  for (const link of links ?? []) {
    try {
      return new URL(link.url).origin
    } catch {
      // Non-absolute URL; keep looking.
    }
  }
  return null
}

// Catalog Explorer deep link for a `catalog.schema.view` full name, or null when
// the origin or name is unavailable (the panel then shows the name without a link).
export function catalogExplorerUrl(
  origin: string | null,
  fullName: string | null | undefined,
): string | null {
  if (!origin || !fullName) return null
  const parts = fullName.split(".")
  if (parts.length !== 3) return null
  const [catalog, schema, view] = parts
  return `${origin}/explore/data/${catalog}/${schema}/${view}`
}
