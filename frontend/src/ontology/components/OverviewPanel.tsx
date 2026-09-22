// Ontology landing — overview-first (MV-D107 Phase 1). Prop-driven: it never fetches,
// reusing the already-loaded preflight/inventory/taxonomy/drafts. Renders a plain-language
// hero, a neutral read-status line (the amber access matrix is demoted into Settings), a
// KPI strip (≤6), ONE adaptive primary CTA, and a Scope→Scan→Review→Apply checklist that
// hides once complete. No jargon in the primary read (MV-D23).
import { ArrowRight, CheckCircle2, Circle, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import type {
  OntologyDrafts,
  OntologyInventory,
  OntologyPreflight,
  OntologyTaxonomy,
} from "@/ontology/types"
import {
  buildChecklist,
  buildKpis,
  heroCounts,
  nextAction,
  type NextAction,
} from "@/ontology/overviewModel"

export function OverviewPanel({
  preflight,
  inventory,
  taxonomy,
  drafts,
  onPrimary,
  busy = false,
}: {
  preflight: OntologyPreflight | null
  inventory: OntologyInventory | null
  taxonomy: OntologyTaxonomy | null
  drafts: OntologyDrafts | null
  onPrimary: (action: NextAction) => void
  /** True while a scan triggered from the CTA is in flight — disables the button. */
  busy?: boolean
}) {
  const company = preflight?.company_name?.trim() || "your estate"
  const { domains, concepts } = heroCounts(taxonomy)
  const kpis = buildKpis({ inventory, taxonomy, drafts })
  const action = nextAction({ preflight, drafts })
  const { steps, complete } = buildChecklist({ preflight, drafts, taxonomy })
  const catalogCount = preflight?.catalog_allowlist.length ?? 0
  const scanning = busy && action.target === "scan"

  return (
    <section aria-labelledby="ontology-overview-heading" className="space-y-5">
      {/* Hero + neutral status + primary CTA. h3 sits under the page-level "Ontology" h2. */}
      <div className="rounded-xl border border-default bg-elevated px-5 py-5">
        <h3 id="ontology-overview-heading" className="text-lg font-semibold text-primary">
          {concepts > 0
            ? `Genie knows ${concepts} concept${concepts === 1 ? "" : "s"} across ${domains} domain${domains === 1 ? "" : "s"} in ${company}`
            : `Map what Genie knows across ${company}`}
        </h3>
        <p className="mt-1.5 text-xs text-muted">
          Reading as you (admin)
          {inventory ? (
            <>
              {" · "}
              {inventory.metric_view_count} metric views {" · "}
              {inventory.governed_tag_count} tags
            </>
          ) : null}
          {" · "}
          {catalogCount} catalog{catalogCount === 1 ? "" : "s"}
        </p>
        <div className="mt-4">
          <Button onClick={() => onPrimary(action)} disabled={scanning}>
            {scanning ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
            ) : null}
            {scanning ? "Scanning…" : action.label}
            {!scanning && <ArrowRight className="h-4 w-4" aria-hidden="true" />}
          </Button>
        </div>
      </div>

      {/* KPI strip — capped at 6 by the builder */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {kpis.map((k) => (
          <div key={k.id} className="rounded-lg border border-default bg-surface px-3 py-2.5">
            <p className="text-xl font-semibold text-primary">{k.value}</p>
            <p className="mt-0.5 text-xs text-muted">{k.label}</p>
          </div>
        ))}
      </div>

      {/* Getting-started checklist — shown until every step is done */}
      {!complete && (
        <div className="rounded-xl border border-default bg-surface px-4 py-3.5">
          <p className="text-sm font-semibold text-primary">Getting started</p>
          <ol className="mt-2 space-y-1.5">
            {steps.map((s) => (
              <li key={s.id} className="flex items-center gap-2 text-sm">
                {s.done ? (
                  <CheckCircle2 className="h-4 w-4 shrink-0 text-success-foreground" aria-hidden="true" />
                ) : (
                  <Circle className="h-4 w-4 shrink-0 text-muted" aria-hidden="true" />
                )}
                <span className={s.done ? "text-muted line-through" : "text-secondary"}>{s.label}</span>
              </li>
            ))}
          </ol>
        </div>
      )}
    </section>
  )
}
