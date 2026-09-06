// Frame 17.0b — the Domain → Sub-Domain tree as it exists in governed tags,
// plus an ungrouped coverage bucket. Phase 1 renders WHAT EXISTS (read straight
// from tags + assignments) and proposes nothing. Driven by GET /api/ontology/taxonomy.
import { ChevronRight, Database, FolderTree, Layers, Sparkles } from "lucide-react"
import type { DomainNode, OntologyInventory, OntologyTaxonomy } from "@/ontology/types"

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

function DomainCard({ domain }: { domain: DomainNode }) {
  return (
    <div className="rounded-xl border border-default bg-elevated p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <Layers className="h-4 w-4 text-accent" />
          <span className="font-mono text-sm font-semibold text-primary">{domain.name}</span>
          <span className="text-xs text-muted">Domain</span>
        </div>
        <span className="text-xs text-muted">{domain.member_count} member(s)</span>
      </div>

      {domain.subdomains.length === 0 && domain.members.length === 0 && (
        <p className="mt-1 text-xs text-muted">No members assigned yet.</p>
      )}

      {domain.members.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {domain.members.map((m) => (
            <span
              key={m.fqn}
              className="inline-flex items-center gap-1 rounded-full border border-default bg-surface px-2.5 py-0.5 font-mono text-xs text-secondary"
            >
              <Database className="h-3 w-3 text-muted" />
              {m.fqn}
            </span>
          ))}
        </div>
      )}

      {domain.subdomains.length > 0 && (
        <div className="mt-3 space-y-2 border-l border-default pl-3">
          {domain.subdomains.map((sub) => (
            <div key={sub.tag_value} className="rounded-lg border border-default bg-sunken p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1.5">
                  <ChevronRight className="h-3.5 w-3.5 text-muted" />
                  <span className="font-mono text-sm font-medium text-primary">{sub.name}</span>
                  <span className="text-xs text-muted">Sub-Domain</span>
                </div>
                <span className="inline-flex items-center gap-1 text-xs text-muted">
                  <Database className="h-3 w-3" />
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
                      <Database className="h-3 w-3 text-muted" />
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
}: {
  taxonomy: OntologyTaxonomy
  inventory: OntologyInventory | null
}) {
  const { domains, ungrouped } = taxonomy
  const ungroupedCount = ungrouped.metric_views.length + ungrouped.genie_agents.length

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <FolderTree className="h-4 w-4 text-accent" />
        <h3 className="text-sm font-semibold text-primary">Taxonomy</h3>
        <span className="text-xs text-muted">
          — Domains &amp; Sub-Domains as they exist in your governed tags. Read-only; nothing is written.
        </span>
      </div>
      <EstateReadLine inventory={inventory} />

      {domains.length === 0 && ungroupedCount === 0 ? (
        <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center">
          <p className="text-sm font-medium text-primary">No governed-tag taxonomy in scope yet</p>
          <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
            We didn&rsquo;t find governed tags following the Domain / Domain/Sub-Domain convention in the
            selected catalogs. As your team applies governed tags, the taxonomy will appear here.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {domains.map((d) => (
            <DomainCard key={d.tag_key} domain={d} />
          ))}
        </div>
      )}

      {ungroupedCount > 0 && (
        <div className="rounded-xl border border-default bg-sunken p-4">
          <div className="flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-accent" />
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
                    <Database className="h-3 w-3 text-muted" />
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
                    <Sparkles className="h-3 w-3 text-accent" />
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
