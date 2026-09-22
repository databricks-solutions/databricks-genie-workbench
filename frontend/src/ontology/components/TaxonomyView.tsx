// Estate → Taxonomy (MV-D107 Phase 2). The Domain → Sub-Domain tree as it exists in governed
// tags, now navigable (search · hide-0-member · sort · collapse-all) and PROVENANT: each row is
// reconciled against the live drafts and badged ✓ Confirmed / △ Better proposal / ○ Declared·
// unpopulated. Read-only; nothing here is written. Pure reconcile/filter/sort live in
// ../estateModel; this component holds only the control state + presentation.
import { useState } from "react"
import {
  Check,
  ChevronDown,
  ChevronRight,
  Circle,
  Database,
  FolderTree,
  Layers,
  Search,
  Sparkles,
  TriangleAlert,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type {
  DomainDraft,
  DomainNode,
  OntologyInventory,
  OntologyTaxonomy,
  TagLens,
} from "@/ontology/types"
import {
  countNonDomainTags,
  defaultHideEmpty,
  estateNeedsReview,
  filterAndSortDomains,
  reconcile,
  type DomainReconcile,
  type EstateSort,
} from "@/ontology/estateModel"

function EstateReadLine({ inventory }: { inventory: OntologyInventory | null }) {
  if (!inventory) return null
  return (
    <p className="text-xs text-muted">
      Read <span className="font-mono text-secondary">{inventory.genie_agent_count}</span> Genie Agents
      (workspace) · <span className="font-mono text-secondary">{inventory.metric_view_count}</span> metric
      views · <span className="font-mono text-secondary">{inventory.governed_tag_count}</span> governed
      tags across <span className="font-mono text-secondary">{inventory.catalogs_scanned.length}</span>{" "}
      catalog(s) (via <span className="font-mono text-secondary">system.information_schema</span>, OBO)
    </p>
  )
}

// The provenance badge for one domain row (reconcile result). "Better proposal" is actionable —
// it deep-links to Review; the others are static status.
function ProvenanceBadge({
  reconcile: r,
  onReview,
}: {
  reconcile: DomainReconcile | undefined
  onReview?: (proposalId?: string) => void
}) {
  if (!r || r.provenance === "none") return null
  if (r.provenance === "confirmed") {
    return (
      <Badge variant="success" className="gap-1">
        <Check className="h-3 w-3" aria-hidden="true" />
        Confirmed
      </Badge>
    )
  }
  if (r.provenance === "better") {
    return (
      <button
        type="button"
        onClick={() => onReview?.(r.proposalId)}
        className="inline-flex items-center gap-1 rounded-full bg-warning/10 px-2.5 py-0.5 text-xs font-semibold text-warning-foreground transition-colors hover:bg-warning/20 dark:bg-warning/20"
      >
        <TriangleAlert className="h-3 w-3" aria-hidden="true" />
        Better proposal
      </button>
    )
  }
  return (
    <Badge variant="secondary" className="gap-1">
      <Circle className="h-3 w-3" aria-hidden="true" />
      Declared · unpopulated
    </Badge>
  )
}

function DomainCard({
  domain,
  reconcile: r,
  collapsed,
  onReview,
}: {
  domain: DomainNode
  reconcile: DomainReconcile | undefined
  collapsed: boolean
  onReview?: (proposalId?: string) => void
}) {
  return (
    <div className="rounded-xl border border-default bg-elevated p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <Layers className="h-4 w-4 text-accent" aria-hidden="true" />
          <span className="font-mono text-sm font-semibold text-primary">{domain.name}</span>
          <span className="text-xs text-muted">Domain</span>
          <ProvenanceBadge reconcile={r} onReview={onReview} />
        </div>
        <span className="text-xs text-muted">{domain.member_count} member(s)</span>
      </div>

      {!collapsed && domain.subdomains.length === 0 && domain.members.length === 0 && (
        <p className="mt-1 text-xs text-muted">No members assigned yet.</p>
      )}

      {!collapsed && domain.members.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {domain.members.map((m) => (
            <span
              key={m.fqn}
              className="inline-flex items-center gap-1 rounded-full border border-default bg-surface px-2.5 py-0.5 font-mono text-xs text-secondary"
            >
              <Database className="h-3 w-3 text-muted" aria-hidden="true" />
              {m.fqn}
            </span>
          ))}
        </div>
      )}

      {!collapsed && domain.subdomains.length > 0 && (
        <div className="mt-3 space-y-2 border-l border-default pl-3">
          {domain.subdomains.map((sub) => (
            <div key={sub.tag_value} className="rounded-lg border border-default bg-sunken p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1.5">
                  <ChevronRight className="h-3.5 w-3.5 text-muted" aria-hidden="true" />
                  <span className="font-mono text-sm font-medium text-primary">{sub.name}</span>
                  <span className="text-xs text-muted">Sub-Domain</span>
                </div>
                <span className="inline-flex items-center gap-1 text-xs text-muted">
                  <Database className="h-3 w-3" aria-hidden="true" />
                  {sub.member_count} member(s)
                </span>
              </div>
              {sub.members.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {sub.members.map((m) => (
                    <span
                      key={m.fqn}
                      className="inline-flex items-center gap-1 rounded-full border border-default bg-elevated px-2.5 py-0.5 font-mono text-xs text-secondary"
                    >
                      <Database className="h-3 w-3 text-muted" aria-hidden="true" />
                      {m.fqn}
                    </span>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export function TaxonomyView({
  taxonomy,
  inventory,
  tags = null,
  drafts = [],
  onReview,
}: {
  taxonomy: OntologyTaxonomy
  inventory: OntologyInventory | null
  /** Governed-tag lens, for the "K other tags aren't domains" explainer count. */
  tags?: TagLens | null
  /** Live domain drafts, for provenance reconcile + the "nothing tagged yet" lead. */
  drafts?: DomainDraft[]
  /** Deep-link to Review (optionally focusing a proposal). */
  onReview?: (proposalId?: string) => void
}) {
  const { domains, ungrouped } = taxonomy
  const ungroupedCount = ungrouped.metric_views.length + ungrouped.genie_agents.length
  const nonDomainTags = countNonDomainTags(tags?.tags)

  const [query, setQuery] = useState("")
  // Smart default: hide-empty ON only when something is actually tagged; otherwise OFF so the
  // declared-but-unpopulated taxonomy is visible on load (never a blank tree, MV-D107 P2 fix).
  const [hideEmpty, setHideEmpty] = useState(() => defaultHideEmpty(domains))
  const [sortBy, setSortBy] = useState<EstateSort>("members")
  const [collapsed, setCollapsed] = useState(false)

  const provenance = reconcile(taxonomy, drafts)
  const lead = estateNeedsReview(taxonomy, drafts)
  const visible = filterAndSortDomains(domains, { query, hideEmpty, sortBy })

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <FolderTree className="h-4 w-4 text-accent" aria-hidden="true" />
        <h3 className="text-sm font-semibold text-primary">Discovered in your estate</h3>
        <span className="text-xs text-muted">
          — Domains &amp; Sub-Domains as they exist in your governed tags. Read-only; nothing is written.
        </span>
      </div>
      <EstateReadLine inventory={inventory} />

      {/* Domain vs governed-tag cardinality: only tags following the Domain/Sub-Domain convention
          are domains, so this list is shorter than the full Tags lens. Explain the gap. */}
      {domains.length > 0 && (
        <p className="text-xs text-muted">
          Showing <span className="font-mono text-secondary">{domains.length}</span> domain
          {domains.length === 1 ? "" : "s"} (governed tags with sub-domains)
          {nonDomainTags > 0 && (
            <>
              {" "}
              · <span className="font-mono text-secondary">{nonDomainTags}</span> other governed tag
              {nonDomainTags === 1 ? " isn’t a domain" : "s aren’t domains"} — see the Tags lens.
            </>
          )}
        </p>
      )}

      {/* Scoped but nothing tagged yet, with proposals waiting — lead straight to Review. */}
      {lead.show && (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-info/30 bg-info/5 px-4 py-3.5">
          <div>
            <p className="text-sm font-semibold text-primary">Nothing tagged yet</p>
            <p className="mt-1 text-xs text-secondary">
              {lead.count} proposal{lead.count === 1 ? "" : "s"} ready to review — approve them to
              start grouping the estate.
            </p>
          </div>
          <Button size="sm" onClick={() => onReview?.()}>
            Review {lead.count} proposal{lead.count === 1 ? "" : "s"}
          </Button>
        </div>
      )}

      {domains.length === 0 && ungroupedCount === 0 ? (
        <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center">
          <p className="text-sm font-medium text-primary">No governed-tag taxonomy in scope yet</p>
          <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
            We didn&rsquo;t find governed tags following the Domain / Domain/Sub-Domain convention in the
            selected catalogs. As your team applies governed tags, the taxonomy will appear here.
          </p>
        </div>
      ) : (
        <>
          {/* Navigation controls: search · hide-empty · sort · collapse-all */}
          {domains.length > 0 && (
            <div className="flex flex-wrap items-center gap-3">
              <label className="relative flex-1 min-w-[12rem]">
                <span className="sr-only">Search domains</span>
                <Search
                  className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted"
                  aria-hidden="true"
                />
                <input
                  type="search"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search domains, sub-domains, assets…"
                  className="w-full rounded-lg border border-default bg-surface py-1.5 pl-8 pr-3 text-sm text-primary placeholder:text-muted focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/40"
                />
              </label>
              <label className="flex items-center gap-1.5 text-xs text-secondary">
                <input
                  type="checkbox"
                  checked={hideEmpty}
                  onChange={(e) => setHideEmpty(e.target.checked)}
                  className="h-3.5 w-3.5 rounded border-default accent-accent"
                />
                Hide empty
              </label>
              <label className="flex items-center gap-1.5 text-xs text-secondary">
                <span className="text-muted">Sort</span>
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value as EstateSort)}
                  className="rounded-lg border border-default bg-surface px-2 py-1 text-xs text-primary focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/40"
                >
                  <option value="members">Members</option>
                  <option value="name">Name</option>
                </select>
              </label>
              <Button variant="secondary" size="sm" onClick={() => setCollapsed((c) => !c)}>
                {collapsed ? (
                  <>
                    <ChevronDown className="h-4 w-4" aria-hidden="true" /> Expand all
                  </>
                ) : (
                  <>
                    <ChevronRight className="h-4 w-4" aria-hidden="true" /> Collapse all
                  </>
                )}
              </Button>
            </div>
          )}

          {visible.length === 0 ? (
            <p className="rounded-xl border border-default bg-elevated px-4 py-6 text-center text-sm text-muted">
              No domains match these filters.
            </p>
          ) : (
            <div className="space-y-3">
              {visible.map((d) => (
                <DomainCard
                  key={d.tag_key}
                  domain={d}
                  reconcile={provenance.get(d.tag_key)}
                  collapsed={collapsed}
                  onReview={onReview}
                />
              ))}
            </div>
          )}
        </>
      )}

      {ungroupedCount > 0 && (
        <div className="rounded-xl border border-default bg-sunken p-4">
          <div className="flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-accent" aria-hidden="true" />
            <p className="text-sm font-semibold text-primary">Ungrouped</p>
            <span className="text-xs text-muted">
              — {ungroupedCount} asset(s) under no domain tag (a coverage signal, not a problem)
            </span>
          </div>
          {ungrouped.metric_views.length > 0 && (
            <div className="mt-2">
              <p className="text-xs font-semibold uppercase tracking-wide text-secondary">Metric views</p>
              <div className="mt-1.5 flex flex-wrap gap-1.5">
                {ungrouped.metric_views.map((m) => (
                  <span
                    key={m.fqn}
                    className="inline-flex items-center gap-1 rounded-full border border-default bg-elevated px-2.5 py-0.5 font-mono text-xs text-secondary"
                  >
                    <Database className="h-3 w-3 text-muted" aria-hidden="true" />
                    {m.fqn}
                  </span>
                ))}
              </div>
            </div>
          )}
          {ungrouped.genie_agents.length > 0 && (
            <div className="mt-2.5">
              <p className="text-xs font-semibold uppercase tracking-wide text-secondary">Genie Agents</p>
              <div className="mt-1.5 flex flex-wrap gap-1.5">
                {ungrouped.genie_agents.map((a) => (
                  <span
                    key={a.fqn}
                    className="inline-flex items-center gap-1 rounded-full border border-default bg-elevated px-2.5 py-0.5 text-xs text-secondary"
                  >
                    <Sparkles className="h-3 w-3 text-accent" aria-hidden="true" />
                    {a.fqn}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
