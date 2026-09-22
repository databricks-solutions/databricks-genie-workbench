/**
 * Ontology Overview — pure, side-effect-free presentation logic (MV-D107 Phase 1).
 *
 * Kept out of the component file so the CTA/KPI/checklist derivations are unit-testable
 * without a DOM harness and so the panel module exports only its component (react-refresh).
 * Nothing here fetches, mutates, or reads the clock beyond formatting a passed-in ISO string.
 * Every input is nullable — the landing renders progressively as head/body reads settle.
 */
import type {
  OntologyDrafts,
  OntologyInventory,
  OntologyPreflight,
  OntologyTaxonomy,
} from "@/ontology/types"

// ── Adaptive primary CTA ────────────────────────────────────────────────────
// One next best action for the landing, resolved from what's loaded. `target` is
// abstract so the caller maps it to a tab switch or the scan routine — the module
// stays free of navigation/side effects.
export type NextActionKind = "scope" | "scan" | "review" | "caught_up"

export interface NextAction {
  kind: NextActionKind
  label: string
  target: "settings" | "scan" | "review" | "map"
}

export function nextAction(input: {
  preflight: OntologyPreflight | null
  drafts: OntologyDrafts | null
}): NextAction {
  const allowlistEmpty = (input.preflight?.catalog_allowlist.length ?? 0) === 0
  if (allowlistEmpty) {
    return { kind: "scope", label: "Choose catalogs to scan", target: "settings" }
  }
  const drafts = input.drafts
  // No snapshot yet (never scanned) — or not loaded (e.g. a read tier is still
  // blocked). Either way the next move that can make progress is a scan.
  if (!drafts || drafts.source === "cold") {
    return { kind: "scan", label: "Scan the estate", target: "scan" }
  }
  const pending = drafts.domains.length + drafts.pages.length
  if (pending > 0) {
    return {
      kind: "review",
      label: `Review ${pending} suggestion${pending === 1 ? "" : "s"}`,
      target: "review",
    }
  }
  return { kind: "caught_up", label: "You're all caught up", target: "map" }
}

// ── KPI strip (capped at 6) ───────────────────────────────────────────────────
export interface Kpi {
  id: string
  label: string
  value: string
}

/** Deterministic UTC date (YYYY-MM-DD) from an ISO string — never a locale-dependent format. */
export function formatAsOf(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toISOString().slice(0, 10)
}

export function buildKpis(input: {
  inventory: OntologyInventory | null
  taxonomy: OntologyTaxonomy | null
  drafts: OntologyDrafts | null
}): Kpi[] {
  const { inventory, taxonomy, drafts } = input
  const kpis: Kpi[] = []
  if (inventory) {
    kpis.push({ id: "metric_views", label: "Metric views", value: String(inventory.metric_view_count) })
    kpis.push({ id: "genie_agents", label: "Genie agents", value: String(inventory.genie_agent_count) })
    kpis.push({ id: "tags", label: "Governed tags", value: String(inventory.governed_tag_count) })
  }
  kpis.push({ id: "domains", label: "Domains", value: String(taxonomy?.domains.length ?? 0) })
  const pending = drafts ? drafts.domains.length + drafts.pages.length : 0
  kpis.push({ id: "pending", label: "Suggestions pending", value: String(pending) })
  kpis.push({ id: "scanned", label: "Last scanned", value: formatAsOf(inventory?.as_of) })
  return kpis.slice(0, 6)
}

/** Hero counts: M = domains, N ≈ domains + sub-domains ("concepts Genie knows"). */
export function heroCounts(taxonomy: OntologyTaxonomy | null): { domains: number; concepts: number } {
  const domains = taxonomy?.domains.length ?? 0
  const subdomains = (taxonomy?.domains ?? []).reduce((n, d) => n + d.subdomains.length, 0)
  return { domains, concepts: domains + subdomains }
}

// ── Getting-started checklist (Scope → Scan → Review → Apply) ─────────────────
export interface ChecklistStep {
  id: "scope" | "scan" | "review" | "apply"
  label: string
  done: boolean
}

export function buildChecklist(input: {
  preflight: OntologyPreflight | null
  drafts: OntologyDrafts | null
  taxonomy: OntologyTaxonomy | null
}): { steps: ChecklistStep[]; complete: boolean } {
  const { preflight, drafts, taxonomy } = input
  const scopeDone = (preflight?.catalog_allowlist.length ?? 0) > 0
  const scanDone = !!drafts && drafts.source !== "cold"
  const pending = drafts ? drafts.domains.length + drafts.pages.length : 0
  const reviewDone = scanDone && pending === 0
  const applyDone = reviewDone && (taxonomy?.domains.length ?? 0) > 0
  const steps: ChecklistStep[] = [
    { id: "scope", label: "Choose catalogs to scan", done: scopeDone },
    { id: "scan", label: "Scan the estate", done: scanDone },
    { id: "review", label: "Review suggestions", done: reviewDone },
    { id: "apply", label: "Apply groupings", done: applyDone },
  ]
  return { steps, complete: steps.every((s) => s.done) }
}

/**
 * Whether a REQUIRED read tier (inventory / signals / tag_graph) is blocked or degraded —
 * the one condition that auto-expands the "Access & sharing" panel in Settings. The optional
 * write + external-enrichment tiers never gate viewing, so they never force it open.
 */
export function readTiersBlocked(preflight: OntologyPreflight | null): boolean {
  if (!preflight) return false
  return preflight.tiers
    .filter((t) => t.id !== "membership_write" && t.id !== "external_enrichment")
    .some((t) => t.status === "blocked" || t.status === "degraded")
}
