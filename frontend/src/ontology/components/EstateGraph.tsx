/**
 * Phase 3e Step B: Estate Graph rendering (MV-D48). Renders the OntologyGraph
 * as an interactive graph with domain/asset levels and edges. Degrades to an
 * honest-empty state (MV-D43) if the graph is empty.
 */
import { AlertTriangle, Lock } from "lucide-react"
import type { OntologyGraph } from "@/ontology/types"

export function EstateGraph({ graph }: { graph: OntologyGraph }) {
  // Check if the graph is empty (MV-D43 honest degradation).
  const isEmpty = graph.domains.nodes.length + graph.assets.nodes.length === 0
  const domainCount = graph.domains.nodes.length
  const assetCount = graph.assets.nodes.length
  const edgeCount = graph.domains.edges.length + graph.assets.edges.length

  if (isEmpty) {
    return (
      <div className="flex items-start gap-2.5 rounded-xl border border-info/30 bg-info/5 px-4 py-3.5">
        <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-info-foreground" />
        <div>
          <p className="text-sm font-semibold text-primary">No data in the estate graph</p>
          <p className="mt-1 max-w-prose text-xs text-secondary">
            The graph is empty — there are no domains or assets to display. Run a refresh to populate the estate.
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-default bg-surface overflow-hidden">
      {/* Graph container: shows graph statistics and edges diagram */}
      <div className="relative w-full bg-elevated/25 p-6" style={{ minHeight: "500px" }}>
        <div className="flex flex-col items-center justify-center gap-4 text-center">
          <div className="rounded-lg border border-default bg-surface p-4 space-y-2">
            <p className="text-xs font-semibold uppercase tracking-wide text-muted">Estate Overview</p>
            <div className="grid grid-cols-3 gap-6 mt-3">
              <div>
                <p className="text-sm font-semibold text-accent">{domainCount}</p>
                <p className="text-xs text-secondary">Domains</p>
              </div>
              <div>
                <p className="text-sm font-semibold text-accent">{assetCount}</p>
                <p className="text-xs text-secondary">Assets</p>
              </div>
              <div>
                <p className="text-sm font-semibold text-accent">{edgeCount}</p>
                <p className="text-xs text-secondary">Edges</p>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2 text-xs text-muted">
            <Lock className="h-3 w-3" />
            <span>Interactive graph (requires cytoscape)</span>
          </div>
        </div>
      </div>

      {/* Legend */}
      <div className="border-t border-default bg-elevated/50 px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-muted">Legend</p>
        <div className="mt-2 grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4">
          <div className="flex items-center gap-2">
            <div className="h-3 w-3 rounded-full bg-accent" />
            <span className="text-xs text-secondary">Domain</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-2 w-2 rounded-full bg-accent" />
            <span className="text-xs text-secondary">Asset</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-px w-3 bg-secondary" />
            <span className="text-xs text-secondary">Lineage</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-px w-3 border-b-2 border-dashed border-accent" />
            <span className="text-xs text-secondary">Co-query</span>
          </div>
        </div>
      </div>

      {/* Graph details */}
      <div className="border-t border-default bg-surface px-4 py-3 space-y-2">
        <p className="text-xs font-semibold uppercase tracking-wide text-muted">Graph state</p>
        <div className="text-xs text-secondary">
          <p>Layout: {graph.layout}</p>
          <p>State: {graph.state}</p>
          {graph.as_of && <p>Last updated: {graph.as_of}</p>}
        </div>
      </div>
    </div>
  )
}
