/* eslint-disable react-refresh/only-export-components */
/**
 * SemanticModelTab — the fourth SpaceDetail tab ("Model", Prompt 12, MV-D23).
 *
 * Renders the CURRENT state of a Genie Agent's semantic model as a deterministic
 * layered SVG (SemanticGraph), with any space-scoped proposals offered as a
 * ghosted, default-off overlay. The base graph must render for a space that has
 * never been optimized. Config is fetched live on tab entry (the server reads
 * serialized_space the same OBO-tolerant way /space/fetch does), never from a run
 * artifact or cache — a refresh affordance re-reads because the config can change
 * under the tab (including edits made outside the workbench).
 *
 * SemanticModelView is split out and pure so every state (loading / error /
 * empty / populated / overlay / node-detail) renders under renderToStaticMarkup
 * in tests without a live fetch.
 */
import { useEffect, useMemo, useState } from "react"
import { AlertTriangle, GitBranch, RefreshCw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { fetchSemanticGraph } from "@/lib/api"
import type { MvGovernance, MvProposal, SemanticGraphEdge, SemanticGraphNode, SemanticGraphResponse } from "@/types"
import { GOVERNANCE, LADDER_ORDER, SemanticGraph, countGovernance } from "./SemanticGraph"

function shortName(identifier: string): string {
  const cleaned = (identifier || "").replace(/`/g, "").trim()
  return cleaned ? cleaned.split(".").pop()! : cleaned
}

// The ghosted proposal overlay is synthesized CLIENT-side from `proposals` (the
// same MvProposal shape the cards use — no new payload): a proposed MV node,
// dashed "replaces" edges to the raw tables it would cover (the tables-freed
// story), and a membership edge from the ungoverned concept it would govern.
export function withOverlay(graph: SemanticGraphResponse): { nodes: SemanticGraphNode[]; edges: SemanticGraphEdge[] } {
  const nodes: SemanticGraphNode[] = [...graph.nodes]
  const edges: SemanticGraphEdge[] = [...graph.edges]
  const tableIds = new Set(graph.nodes.filter((n) => n.kind === "table").map((n) => n.id))
  const mvRows = graph.nodes.filter((n) => n.kind === "metric_view").length
  graph.proposals.forEach((p, i) => {
    if (!p.proposed_object) return
    const ghostId = `proposed:${p.proposed_object}`
    if (nodes.some((n) => n.id === ghostId)) return
    nodes.push({ id: ghostId, kind: "metric_view", label: shortName(p.proposed_object), col: 2, row: mvRows + i, proposed: true })
    const conceptId = `measure:${shortName(p.proposed_object)}`
    if (nodes.some((n) => n.id === conceptId)) edges.push({ from: conceptId, to: ghostId, kind: "membership" })
    const sources = Array.isArray(p.evidence?.source_tables) ? (p.evidence!.source_tables as string[]) : []
    for (const s of sources) if (tableIds.has(s)) edges.push({ from: ghostId, to: s, kind: "replaces" })
  })
  return { nodes, edges }
}

// Prompt 12b SQL-coverage lens status, in the MV-D15 vocabulary. Rendered only
// when the server reported a status — a lens-free (Prompt 12) response leaves it
// undefined and this note is absent, so older behavior is unchanged.
function CoverageNote({ status, reason }: { status?: string | null; reason?: string | null }) {
  if (!status) return null
  if (status === "EMPTY") {
    return (
      <p className="text-xs text-muted">
        No curated SQL in this Agent yet, so query coverage is not measured. Add example queries or let an
        optimization run harvest them.
      </p>
    )
  }
  if (status === "UNAVAILABLE") {
    return (
      <p className="text-xs text-[var(--color-warning)]">
        Query coverage unavailable{reason ? ` — ${reason}` : ""}.
      </p>
    )
  }
  return (
    <p className="text-xs text-muted">
      Query coverage: badges count the curated queries touching each node; a dashed 0 is a cold spot no curated
      SQL exercises.
    </p>
  )
}

function GovernanceLadder({ counts }: { counts: Record<MvGovernance, number> }) {
  const present = LADDER_ORDER.filter((rung) => counts[rung] > 0)
  // Honesty rule (both ways): an empty space has found nothing — no green rung it
  // does not have, and no red rung for a measure it never saw.
  if (present.length === 0) {
    return (
      <p className="text-xs text-muted">
        No measure concepts yet — none are defined in this Agent's config, and none have been suggested.
      </p>
    )
  }
  const badgeVariant: Record<MvGovernance, "success" | "warning" | "danger"> = {
    governed: "success",
    curated: "warning",
    ungoverned: "danger",
  }
  return (
    <div className="flex flex-wrap items-center gap-2" aria-label="Governance ladder">
      {present.map((rung) => {
        const g = GOVERNANCE[rung]
        const Icon = g.Icon
        return (
          <Badge key={rung} variant={badgeVariant[rung]}>
            <Icon className="mr-1 h-3 w-3" />
            {g.label} · {counts[rung]}
          </Badge>
        )
      })}
    </div>
  )
}

export function NodeDetail({ node, proposals }: { node: SemanticGraphNode; proposals: MvProposal[] }) {
  const match = proposals.find((p) => p.proposed_object && shortName(p.proposed_object) === node.label)
  const evidence = match?.evidence ?? null
  const recurrence = evidence && typeof evidence.recurrence_count === "number" ? evidence.recurrence_count : null
  const questionIds = evidence && Array.isArray(evidence.benchmark_question_ids) ? (evidence.benchmark_question_ids as string[]) : []
  const sourceTables = evidence && Array.isArray(evidence.source_tables) ? (evidence.source_tables as string[]) : []
  const conflicts = Array.isArray(match?.conflicts) ? match!.conflicts! : []
  const g = node.kind === "measure" && node.governance ? GOVERNANCE[node.governance] : null

  return (
    <div className="space-y-2 rounded-lg border border-default bg-elevated p-3">
      <div className="flex items-center justify-between">
        <span className="font-mono text-sm font-medium text-primary">{node.label}</span>
        {g && <Badge variant={node.governance === "governed" ? "success" : node.governance === "curated" ? "warning" : "danger"}>{g.label}</Badge>}
        {node.kind === "metric_view" && <Badge variant="secondary"><GitBranch className="mr-1 h-3 w-3" />{node.proposed ? "Proposed" : "Metric view"}</Badge>}
      </div>
      <dl className="space-y-1 text-xs text-muted">
        {node.origin && (
          <div className="flex gap-2"><dt className="text-secondary">origin</dt><dd>{node.origin}</dd></div>
        )}
        {recurrence != null && (
          <div className="flex gap-2"><dt className="text-secondary">recurrence</dt><dd>{recurrence} occurrences</dd></div>
        )}
        {questionIds.length > 0 && (
          <div className="flex gap-2"><dt className="text-secondary">questions</dt><dd className="font-mono">{questionIds.join(", ")}</dd></div>
        )}
        {sourceTables.length > 0 && (
          <div className="flex gap-2"><dt className="text-secondary">source tables</dt><dd className="font-mono">{sourceTables.join(", ")}</dd></div>
        )}
        {conflicts.length > 0 && (
          <div className="flex items-center gap-2 text-[var(--color-warning)]"><AlertTriangle className="h-3 w-3" />{conflicts.length} conflict{conflicts.length > 1 ? "s" : ""} reported</div>
        )}
      </dl>
    </div>
  )
}

export function SemanticModelView({
  graph,
  isLoading,
  error,
  onRefresh,
}: {
  graph: SemanticGraphResponse | null
  isLoading: boolean
  error: string | null
  onRefresh: () => void
}) {
  const [showOverlay, setShowOverlay] = useState(false)
  const [selectedId, setSelectedId] = useState<string | null>(null)

  const hasProposals = (graph?.proposals.length ?? 0) > 0
  const rendered = useMemo(() => {
    if (!graph) return { nodes: [], edges: [] }
    return showOverlay ? withOverlay(graph) : { nodes: graph.nodes, edges: graph.edges }
  }, [graph, showOverlay])

  const ladderCounts = useMemo(() => countGovernance(graph?.nodes ?? []), [graph])
  const selectedNode = rendered.nodes.find((n) => n.id === selectedId) ?? null
  const isEmpty = !!graph && graph.nodes.length === 0

  return (
    <div className="rounded-xl border border-default bg-surface">
      <div className="flex flex-wrap items-center justify-between gap-2 px-4 py-3">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model</h3>
        <div className="flex items-center gap-3">
          {hasProposals && (
            <label className="flex items-center gap-2 text-xs text-secondary">
              <input type="checkbox" checked={showOverlay} onChange={(e) => setShowOverlay(e.target.checked)} className="accent-[var(--color-accent)]" />
              Show proposal overlay
              <span className="text-muted">(default off)</span>
            </label>
          )}
          <button type="button" onClick={onRefresh} disabled={isLoading} className="flex items-center gap-1 text-xs text-muted hover:text-accent transition-colors disabled:opacity-50" title="Reload the semantic model">
            <RefreshCw className={`h-3 w-3 ${isLoading ? "animate-spin" : ""}`} />
            Refresh
          </button>
        </div>
      </div>

      <div className="space-y-3 border-t border-default p-4">
        {isLoading && !graph && (
          <p className="py-8 text-center text-sm text-muted">Loading semantic model…</p>
        )}

        {error && (
          <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-400">
            <p>{error}</p>
            <button type="button" onClick={onRefresh} className="mt-2 text-accent hover:underline">Try again</button>
          </div>
        )}

        {graph && !error && (
          <>
            <GovernanceLadder counts={ladderCounts} />
            {!isEmpty && <CoverageNote status={graph.coverage_status} reason={graph.coverage_reason} />}
            {isEmpty ? (
              <p className="text-xs text-muted">
                This Agent's configuration defines no joins, SQL snippets, or metric views yet — the graph shows the config as
                it is now. Connect it by adding join specs and snippets yourself, or let an optimization run discover and apply
                them. Metric view suggestions don't require a run.
              </p>
            ) : (
              <>
                <SemanticGraph nodes={rendered.nodes} edges={rendered.edges} selectedId={selectedId} onSelectNode={(n) => setSelectedId(n.id)} />
                {selectedNode && <NodeDetail node={selectedNode} proposals={graph.proposals} />}
              </>
            )}
          </>
        )}
      </div>
    </div>
  )
}

export function SemanticModelTab({ spaceId }: { spaceId: string }) {
  const [graph, setGraph] = useState<SemanticGraphResponse | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = () => {
    setIsLoading(true)
    setError(null)
    fetchSemanticGraph(spaceId)
      .then((data) => setGraph(data))
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load the semantic model"))
      .finally(() => setIsLoading(false))
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [spaceId])

  return <SemanticModelView graph={graph} isLoading={isLoading} error={error} onRefresh={load} />
}
