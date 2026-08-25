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
import { useEffect, useMemo, useState, type ReactNode } from "react"
import { AlertTriangle, GitBranch, RefreshCw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { fetchSemanticGraph } from "@/lib/api"
import type { MvGovernance, MvProposal, SemanticGraphEdge, SemanticGraphNode, SemanticGraphResponse } from "@/types"
import { GOVERNANCE, LADDER_ORDER, SemanticGraph, countGovernance, isDisplayableMeasureLabel, relationshipGlyph } from "./SemanticGraph"

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

// Curator inset (Prompt 12e / MV-D33): the interesting facts about the selected
// artifact the graph itself can't carry. For a metric view: the tables in its
// definition (the `uses`-edge members) or, when its YAML could not be read, the
// honest "definition unavailable". For a table: which metric views source it
// (reverse `uses`) — the reuse/impact signal a curator needs before touching it.
export function NodeDetail({
  node,
  proposals,
  nodes = [],
  edges = [],
}: {
  node: SemanticGraphNode
  proposals: MvProposal[]
  nodes?: SemanticGraphNode[]
  edges?: SemanticGraphEdge[]
}) {
  const match = proposals.find((p) => p.proposed_object && shortName(p.proposed_object) === node.label)
  const evidence = match?.evidence ?? null
  const recurrence = evidence && typeof evidence.recurrence_count === "number" ? evidence.recurrence_count : null
  const questionIds = evidence && Array.isArray(evidence.benchmark_question_ids) ? (evidence.benchmark_question_ids as string[]) : []
  const sourceTables = evidence && Array.isArray(evidence.source_tables) ? (evidence.source_tables as string[]) : []
  const conflicts = Array.isArray(match?.conflicts) ? match!.conflicts! : []
  const g = node.kind === "measure" && node.governance ? GOVERNANCE[node.governance] : null

  const labelOf = (id: string) => nodes.find((n) => n.id === id)?.label ?? shortName(id)
  // Member tables of a selected MV (uses-edge targets) and, for a table, the MVs
  // that use it (reverse). Deterministic order (edge order).
  const memberTables = node.kind === "metric_view" ? edges.filter((e) => e.kind === "uses" && e.from === node.id).map((e) => labelOf(e.to)) : []
  const usedByMvs = node.kind === "table" ? edges.filter((e) => e.kind === "uses" && e.to === node.id).map((e) => labelOf(e.from)) : []
  const defUnavailable = node.kind === "metric_view" && !node.proposed && node.definition_available === false

  // 12f: the curator inset the v7 contract frame shows, rather than the flat
  // key/value list that shipped. The panel earns a header (name + kind +
  // governance roll-up) and sectioned columns, because this is the surface a
  // curator reads to decide something — the joins that were DECLARED (with their
  // ON predicates, where the canvas can only afford a glyph), the measures the
  // metric view governs, and the evidence behind a proposed one.
  //
  // What it does NOT show, deliberately: the metric view's filter,
  // materialization, and reuse count. The v7 frame drew them from the MV's YAML;
  // the semantic-graph payload does not carry them, and an inset that prints
  // "unknown" for a governance-relevant field is worse than one that omits it.
  const memberTableIds = new Set(
    node.kind === "metric_view" ? edges.filter((e) => e.kind === "uses" && e.from === node.id).map((e) => e.to) : [],
  )
  // Joins worth showing: for an MV, the ones INSIDE its own source set (its join
  // graph); for a table, every join it participates in.
  const joins = edges.filter((e) => {
    if (e.kind !== "join") return false
    if (node.kind === "metric_view") return memberTableIds.has(e.from) && memberTableIds.has(e.to)
    if (node.kind === "table") return e.from === node.id || e.to === node.id
    return false
  })
  // The measures this metric view governs, with their rungs — the roll-up the
  // canvas chip summarises, itemised.
  const ownMeasures =
    node.kind === "metric_view"
      ? edges
          .filter((e) => e.kind === "membership" && e.to === node.id)
          .map((e) => nodes.find((n) => n.id === e.from))
          .filter((n): n is SemanticGraphNode => n != null)
      : []
  const namedMeasures = ownMeasures.filter((m) => isDisplayableMeasureLabel(m.label))
  const unnamedCount = ownMeasures.length - namedMeasures.length
  const rollup = LADDER_ORDER.map((rung) => ({
    rung,
    count: ownMeasures.filter((m) => m.governance === rung).length,
  })).filter((r) => r.count > 0)

  return (
    <div className="overflow-hidden rounded-lg border border-default bg-elevated">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-default px-3 py-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className="truncate font-mono text-sm font-medium text-primary">{node.label}</span>
          {node.kind === "metric_view" && <Badge variant="secondary"><GitBranch className="mr-1 h-3 w-3" />{node.proposed ? "Proposed" : "Metric view"}</Badge>}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {g && <Badge variant={node.governance === "governed" ? "success" : node.governance === "curated" ? "warning" : "danger"}>{g.label}</Badge>}
          {rollup.map(({ rung, count }) => (
            <span key={rung} className="inline-flex items-center gap-1.5 rounded-full border border-default px-2 py-0.5 text-[11px] text-secondary">
              <span className="h-2 w-2 rounded-full" style={{ background: GOVERNANCE[rung].color }} />
              {count} {GOVERNANCE[rung].label.toLowerCase()}
            </span>
          ))}
        </div>
      </div>

      {defUnavailable && (
        <p className="flex items-start gap-2 border-b border-default bg-[var(--color-warning)]/5 px-3 py-2 text-xs text-[var(--color-warning)]">
          <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
          definition unavailable — its YAML could not be read, so no source or joins are shown
        </p>
      )}
      {conflicts.length > 0 && (
        <p className="flex items-start gap-2 border-b border-default bg-[var(--color-warning)]/5 px-3 py-2 text-xs text-[var(--color-warning)]">
          <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
          {conflicts.length} conflict{conflicts.length > 1 ? "s" : ""} reported — its measures overlap another proposal's
        </p>
      )}

      <div className="grid gap-x-6 gap-y-4 p-3 sm:grid-cols-2">
        {memberTables.length > 0 && (
          <InsetSection title="Source tables">
            <div className="flex flex-wrap gap-1.5">
              {memberTables.map((t) => (
                <span key={t} className="rounded border border-default px-1.5 py-0.5 font-mono text-[11px] text-secondary">{t}</span>
              ))}
            </div>
          </InsetSection>
        )}

        {ownMeasures.length > 0 && (
          <InsetSection title={`Measures (${ownMeasures.length})`}>
            <ul className="space-y-1">
              {namedMeasures.map((m) => (
                <li key={m.id} className="flex items-center gap-2">
                  <span className="h-2 w-2 shrink-0 rounded-full" style={{ background: m.governance ? GOVERNANCE[m.governance].color : "var(--border-color-strong)" }} />
                  <span className="truncate font-mono text-[11px] text-secondary">{m.label}</span>
                </li>
              ))}
              {/* Same truth the canvas tells (12d finding 1): a canonical_expr or
                  sug_ id is not a name, so it is counted, never printed. The
                  inset listing them raw while the canvas summarised them was the
                  two surfaces disagreeing about the same measures. */}
              {unnamedCount > 0 && (
                <li className="text-[11px] italic text-muted">+{unnamedCount} unnamed</li>
              )}
            </ul>
          </InsetSection>
        )}

        {joins.length > 0 && (
          <InsetSection title={`Declared joins (${joins.length})`}>
            <ul className="space-y-1.5">
              {joins.map((e, i) => (
                <li key={i}>
                  <div className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                    <span className="truncate">{labelOf(e.from)}</span>
                    <span className="text-muted">→</span>
                    <span className="truncate">{labelOf(e.to)}</span>
                    {relationshipGlyph(e.relationship) && <span className="rounded bg-[var(--bg-sunken)] px-1 text-[10px] text-muted">{relationshipGlyph(e.relationship)}</span>}
                    {e.scd2 && <span className="rounded bg-[var(--bg-sunken)] px-1 text-[10px] text-muted">SCD2</span>}
                  </div>
                  {e.on && <div className="truncate font-mono text-[10px] text-muted">ON {e.on}</div>}
                </li>
              ))}
            </ul>
          </InsetSection>
        )}

        {usedByMvs.length > 0 && (
          <InsetSection title="Used by">
            <div className="flex flex-wrap gap-1.5">
              {usedByMvs.map((m) => (
                <span key={m} className="rounded border border-default px-1.5 py-0.5 font-mono text-[11px] text-secondary">{m}</span>
              ))}
            </div>
            {usedByMvs.length > 1 && <p className="mt-1 text-[11px] text-muted">shared, changes ripple</p>}
          </InsetSection>
        )}

        {(node.origin || recurrence != null || questionIds.length > 0 || sourceTables.length > 0) && (
          <InsetSection title="Evidence">
            <dl className="space-y-1 text-[11px] text-muted">
              {node.origin && <div className="flex gap-2"><dt className="shrink-0 text-secondary">origin</dt><dd className="truncate">{node.origin}</dd></div>}
              {recurrence != null && <div className="flex gap-2"><dt className="shrink-0 text-secondary">recurrence</dt><dd>{recurrence} occurrences</dd></div>}
              {questionIds.length > 0 && <div className="flex gap-2"><dt className="shrink-0 text-secondary">questions</dt><dd className="truncate font-mono">{questionIds.join(", ")}</dd></div>}
              {sourceTables.length > 0 && <div className="flex gap-2"><dt className="shrink-0 text-secondary">source tables</dt><dd className="truncate font-mono">{sourceTables.join(", ")}</dd></div>}
            </dl>
          </InsetSection>
        )}
      </div>
    </div>
  )
}

function InsetSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted">{title}</p>
      {children}
    </div>
  )
}

export function SemanticModelView({
  graph,
  isLoading,
  error,
  onRefresh,
  initialSelectedId = null,
}: {
  graph: SemanticGraphResponse | null
  isLoading: boolean
  error: string | null
  onRefresh: () => void
  // Seeds the selection. Selection is otherwise internal state, which means a
  // static render (the fidelity-gate export) could never show the selected-state
  // surface — the boundary, the focus dimming, the curator inset — and that is
  // exactly the state the v7 contract frame depicts.
  initialSelectedId?: string | null
}) {
  const [showOverlay, setShowOverlay] = useState(false)
  const [selectedId, setSelectedId] = useState<string | null>(initialSelectedId)

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
                {selectedNode && <NodeDetail node={selectedNode} proposals={graph.proposals} nodes={rendered.nodes} edges={rendered.edges} />}
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
