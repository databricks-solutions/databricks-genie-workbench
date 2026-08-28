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
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react"
import { AlertTriangle, GitBranch, RefreshCw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { fetchJoinAdvice, fetchJoinCandidates, fetchSemanticGraph, saveJoinAdvice } from "@/lib/api"
import type { JoinCandidate, MvGovernance, MvProposal, SemanticGraphEdge, SemanticGraphNode, SemanticGraphResponse } from "@/types"
import { MvIqScanAdvisorySection } from "@/components/auto-optimize/MvIqScanAdvisorySection"
import { GOVERNANCE, LADDER_ORDER, SemanticGraph, countGovernance, isDisplayableMeasureLabel, relationshipGlyph } from "./SemanticGraph"
import { SemanticBlueprint } from "./SemanticBlueprint"

type CanvasMode = "classic" | "blueprint"

function shortName(identifier: string): string {
  const cleaned = (identifier || "").replace(/`/g, "").trim()
  return cleaned ? cleaned.split(".").pop()! : cleaned
}

// The ghosted proposal overlay is synthesized CLIENT-side from `proposals` (the
// same MvProposal shape the cards use — no new payload).
//
// Round-6 (reviewer): the overlay used to add a `membership` edge from the loose
// measure to the ghost MV, which made buildCards MOVE that measure OUT of the
// Space-config box and INTO an off-screen ghost card — so turning the overlay on
// looked like it HID measures and showed no proposals. It now KEEPS every loose
// measure where it is and instead draws a dashed "would govern →" link
// (kind: "governs") from the proposed MV to the measure it would govern. The
// proposed MV renders as a visible ghost card in the metric-view column; the
// caller re-fits so the new card is on-screen. The raw-tables-freed story is no
// longer drawn as canvas spaghetti (dashed "replaces" edges) — it lives in the
// proposal's detail panel instead.
// `proposals` defaults to the graph's own set, but the Model tab passes the
// advisory's live-scanned proposals so freshly-scanned suggestions ghost onto
// the canvas without waiting for a graph refetch (single source of truth).
export function withOverlay(
  graph: SemanticGraphResponse,
  proposals: MvProposal[] = graph.proposals,
): { nodes: SemanticGraphNode[]; edges: SemanticGraphEdge[] } {
  const nodes: SemanticGraphNode[] = [...graph.nodes]
  const edges: SemanticGraphEdge[] = [...graph.edges]
  const mvRows = graph.nodes.filter((n) => n.kind === "metric_view").length
  proposals.forEach((p, i) => {
    if (!p.proposed_object) return
    const ghostId = `proposed:${p.proposed_object}`
    if (nodes.some((n) => n.id === ghostId)) return
    nodes.push({ id: ghostId, kind: "metric_view", label: shortName(p.proposed_object), col: 2, row: mvRows + i, proposed: true })
    // Link (don't move): the loose measure stays in Space config; a "would
    // govern →" edge points from the proposed MV to it. Only drawn when the
    // matching loose concept actually exists.
    const conceptId = `measure:${shortName(p.proposed_object)}`
    if (nodes.some((n) => n.id === conceptId)) edges.push({ from: ghostId, to: conceptId, kind: "governs" })
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
  // curator reads to decide something — the join TREE rooted at the metric
  // view's source (with the ON predicates the canvas can only afford a glyph
  // for), the dimensions grouped by the relation they bind to, the definition's
  // filter and materialization posture, and the evidence behind a proposal.
  //
  // Every field here is read from the payload; nothing is inferred. A view whose
  // YAML did not parse shows the "definition unavailable" banner and none of
  // these sections, because an inset that prints "unknown" for a
  // governance-relevant field is worse than one that omits it.
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

  // 12f — the join TREE, rooted where the YAML says (`mv_source`). Falling back
  // to a topological guess (a `from` that is never a `to`) would root the tree on
  // a detail table whenever the config declares a fact→fact join, which is
  // exactly the shape the v7 frame draws.
  const joinRootId = (() => {
    if (node.kind !== "metric_view") return null
    if (node.mv_source && memberTableIds.has(node.mv_source)) return node.mv_source
    const targets = new Set(joins.map((e) => e.to))
    return joins.map((e) => e.from).find((id) => !targets.has(id)) ?? [...memberTableIds][0] ?? null
  })()

  // Branches at each level: a join TOUCHING the level's table, reported as the
  // other endpoint. Edge direction encodes the declared relationship, not the
  // reading order, so a tree rooted at the source must look both ways. Two
  // levels — enough for the star and the snowflake the model actually supports —
  // and a table is placed once, so a cycle cannot loop.
  const joinTree = (() => {
    if (!joinRootId) return []
    const placed = new Set([joinRootId])
    const branchesOf = (id: string) =>
      joins
        .filter((e) => e.from === id || e.to === id)
        .map((e) => ({ edge: e, other: e.from === id ? e.to : e.from }))
        .filter(({ other }) => !placed.has(other))
    const level1 = branchesOf(joinRootId)
    for (const b of level1) placed.add(b.other)
    return level1.map((b) => ({ ...b, children: branchesOf(b.other) }))
  })()

  // Dimensions grouped by the relation their expression binds to, so "which of
  // these came through a join" is answerable at a glance. Bindings arrive as
  // fully-qualified names; a dimension with no proven binding is grouped last
  // under the honest empty label.
  const dimensionsByBinding = (() => {
    const dims = node.kind === "metric_view" ? (node.dimensions ?? []) : []
    const groups = new Map<string, string[]>()
    for (const d of dims) {
      const key = d.binding ? shortName(d.binding) : "unbound"
      groups.set(key, [...(groups.get(key) ?? []), d.name])
    }
    return [...groups.entries()]
  })()

  // Reuse: how much of this view's source set OTHER metric views also read — the
  // "changing this ripples" signal, counted from the uses edges rather than
  // asserted.
  const sharedMemberCount =
    node.kind === "metric_view"
      ? [...memberTableIds].filter((tid) =>
          edges.some((e) => e.kind === "uses" && e.to === tid && e.from !== node.id),
        ).length
      : 0

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
      {/* 12f: the name collision the canvas cannot draw. A loose measure that
          reuses a governed measure's name under a different expression is two
          answers to one question — the curator's problem, stated up front. */}
      {node.overlaps && (
        <p className="flex items-start gap-2 border-b border-default bg-[var(--color-warning)]/5 px-3 py-2 text-xs text-[var(--color-warning)]">
          <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
          <span>
            <span className="font-mono">{shortName(node.overlaps)}</span> already exposes a measure named{" "}
            <span className="font-mono">{node.label}</span> — two definitions, one name
          </span>
        </p>
      )}

      <div className="grid gap-x-6 gap-y-4 p-3 sm:grid-cols-2">
        {/* Round-5: selecting a measure shows WHAT it computes — its defining
            expression and, when the source carries one, a description — not just
            its governance rung. An ungoverned proposal exposes only a name, so
            expr is absent and this reads honestly rather than printing "unknown".
            The section spans both columns so a long expression has room to wrap. */}
        {node.kind === "measure" && (node.expr || node.description) && (
          <div className="sm:col-span-2">
            <InsetSection title="Definition">
              {node.expr && (
                <div className="mb-1.5">
                  <p className="mb-0.5 text-[10px] text-muted">expression</p>
                  <code className="block whitespace-pre-wrap break-words rounded bg-[var(--bg-sunken)] px-2 py-1.5 font-mono text-[11px] leading-relaxed text-secondary">{node.expr}</code>
                </div>
              )}
              {node.description && <p className="text-[11px] leading-relaxed text-muted">{node.description}</p>}
            </InsetSection>
          </div>
        )}

        {/* The join tree, rooted at the source: the shape of the semantic model
            inside the view, which the canvas flattens into columns. */}
        {node.kind === "metric_view" && joinRootId && (
          <InsetSection title="Join tree">
            <ul className="space-y-1">
              <li>
                <span className="font-mono text-[11px] text-secondary">{labelOf(joinRootId)}</span>
                <span className="ml-1.5 text-[10px] text-muted">source</span>
              </li>
              {joinTree.map(({ edge, other, children }, i) => (
                <li key={i}>
                  <JoinBranch edge={edge} label={labelOf(other)} />
                  {children.length > 0 && (
                    <ul className="pl-4">
                      {children.map((c, j) => (
                        <li key={j}>
                          <JoinBranch edge={c.edge} label={labelOf(c.other)} />
                        </li>
                      ))}
                    </ul>
                  )}
                </li>
              ))}
              {joinTree.length === 0 && (
                <li className="pl-3 text-[11px] italic text-muted">no joins — single-source view</li>
              )}
            </ul>
          </InsetSection>
        )}

        {node.kind !== "metric_view" && memberTables.length > 0 && (
          <InsetSection title="Source tables">
            <div className="flex flex-wrap gap-1.5">
              {memberTables.map((t) => (
                <span key={t} className="rounded border border-default px-1.5 py-0.5 font-mono text-[11px] text-secondary">{t}</span>
              ))}
            </div>
          </InsetSection>
        )}

        {dimensionsByBinding.length > 0 && (
          <InsetSection title={`Dimensions (${(node.dimensions ?? []).length})`}>
            <ul className="space-y-1.5">
              {dimensionsByBinding.map(([binding, names]) => (
                <li key={binding}>
                  <p className="font-mono text-[10px] text-muted">{binding}</p>
                  <div className="flex flex-wrap gap-1">
                    {names.map((n) => (
                      <span key={n} className="rounded border border-default px-1.5 py-0.5 font-mono text-[11px] text-secondary">{n}</span>
                    ))}
                  </div>
                </li>
              ))}
            </ul>
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

        {/* 12f: what the definition applies to every query, and how it is
            served. Read from the MV's YAML (payload), so a view that declares
            neither renders neither — an absent filter must never read as an
            unknown one. */}
        {node.kind === "metric_view" && (node.mv_filter || node.materialization || sharedMemberCount > 0) && (
          <InsetSection title="Definition">
            <dl className="space-y-1 text-[11px] text-muted">
              {node.mv_filter && (
                <div className="flex gap-2">
                  <dt className="shrink-0 text-secondary">filter</dt>
                  <dd className="truncate font-mono" title={node.mv_filter}>{node.mv_filter}</dd>
                </div>
              )}
              {node.materialization && (
                <div className="flex gap-2"><dt className="shrink-0 text-secondary">served</dt><dd>{node.materialization}</dd></div>
              )}
              {sharedMemberCount > 0 && (
                <div className="flex gap-2">
                  <dt className="shrink-0 text-secondary">reuse</dt>
                  <dd>
                    {sharedMemberCount} of {memberTableIds.size} source table{memberTableIds.size > 1 ? "s" : ""} shared with other metric views
                  </dd>
                </div>
              )}
            </dl>
          </InsetSection>
        )}

        {node.kind !== "metric_view" && joins.length > 0 && (
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

// One branch of the inset's join tree: the joined table, its relationship glyph,
// and the ON predicate the canvas has no room for.
function JoinBranch({ edge, label }: { edge: SemanticGraphEdge; label: string }) {
  return (
    <div className="flex items-baseline gap-1.5 pl-3">
      <span className="text-muted">└</span>
      <span className="min-w-0">
        <span className="font-mono text-[11px] text-secondary">{label}</span>
        {relationshipGlyph(edge.relationship) && (
          <span className="ml-1.5 rounded bg-[var(--bg-sunken)] px-1 text-[10px] text-muted">{relationshipGlyph(edge.relationship)}</span>
        )}
        {edge.scd2 && <span className="ml-1 rounded bg-[var(--bg-sunken)] px-1 text-[10px] text-muted">SCD2</span>}
        {edge.on && <span className="block truncate font-mono text-[10px] text-muted">ON {edge.on}</span>}
      </span>
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
  proposalsOverride,
  onSelectionChange,
  selectRequest,
  joinCandidates,
  onSeedJoins,
  seededJoinCount,
  onBlueprintActive,
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
  // Model-tab sync: the advisory's live proposal set drives the ghost overlay
  // instead of the graph's own (single source of truth). Falls back to the
  // graph's proposals when absent (standalone / tests).
  proposalsOverride?: MvProposal[]
  // Model-tab sync: reports the selected node id up so the advisory can
  // highlight/scroll to the matching card.
  onSelectionChange?: (nodeId: string | null) => void
  // Model-tab sync: an imperative "select this node" from the advisory's "View
  // in graph". A bumped nonce re-triggers even for a repeated id.
  selectRequest?: { id: string; nonce: number } | null
  // Join Advisor (§7): data-grounded candidate joins for the Blueprint canvas,
  // and the seed callback that persists the checked set as ADVICE for the next
  // Auto-Optimize run. These are ADVICE to the optimizer, never a Genie Agent
  // config edit — the Workbench makes no ad-hoc serialized_space edits. Absent
  // in the classic canvas / static-render tests, so the view stays pure.
  joinCandidates?: JoinCandidate[]
  onSeedJoins?: (seeds: JoinCandidate[]) => void
  seededJoinCount?: number
  // Fired when the user first switches to the Blueprint canvas, so the container
  // can lazily discover candidates (warehouse probes) instead of on every open.
  onBlueprintActive?: () => void
}) {
  const proposals = useMemo(() => proposalsOverride ?? graph?.proposals ?? [], [proposalsOverride, graph])
  const hasProposals = proposals.length > 0
  // Overlay follows the proposals by default (on when any exist); a manual
  // toggle wins once the user touches the checkbox. Derived at render time so a
  // static render reflects the default without an effect.
  const [manualOverlay, setManualOverlay] = useState<boolean | null>(null)
  const showOverlay = manualOverlay ?? hasProposals
  const [selectedId, setSelectedId] = useState<string | null>(initialSelectedId)
  // v4 canvas swap (docs/design/semantic-graph-v4-blueprint-note.md §5): the new
  // Semantic Blueprint canvas ships beside the classic graph behind this toggle,
  // Classic default until it reaches full parity. Blueprint renders the base
  // graph only (the proposal overlay / Join Advisor is Phase 3), so it stays
  // grounded — arrows require proof (§2).
  const [canvas, setCanvas] = useState<CanvasMode>("classic")

  // Report selection changes upward. The callback is held in a ref so a parent
  // passing a fresh function each render does not re-fire this effect.
  const onSelectionChangeRef = useRef(onSelectionChange)
  useEffect(() => {
    onSelectionChangeRef.current = onSelectionChange
  })
  useEffect(() => {
    onSelectionChangeRef.current?.(selectedId)
  }, [selectedId])

  // Imperative select from "View in graph": select the node and force the
  // overlay on so its ghost card is drawn.
  useEffect(() => {
    if (!selectRequest) return
    setSelectedId(selectRequest.id)
    setManualOverlay(true)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectRequest?.nonce])

  const rendered = useMemo(() => {
    if (!graph) return { nodes: [], edges: [] }
    return showOverlay ? withOverlay(graph, proposals) : { nodes: graph.nodes, edges: graph.edges }
  }, [graph, showOverlay, proposals])

  const ladderCounts = useMemo(() => countGovernance(graph?.nodes ?? []), [graph])
  const selectedNode = rendered.nodes.find((n) => n.id === selectedId) ?? null
  const isEmpty = !!graph && graph.nodes.length === 0

  return (
    <div className="rounded-xl border border-default bg-surface">
      <div className="flex flex-wrap items-center justify-between gap-2 px-4 py-3">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model</h3>
        <div className="flex items-center gap-3">
          <span className="inline-flex overflow-hidden rounded-md border border-default text-xs" role="group" aria-label="Canvas style">
            {(["classic", "blueprint"] as CanvasMode[]).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => {
                  setCanvas(mode)
                  if (mode === "blueprint") onBlueprintActive?.()
                }}
                className={
                  canvas === mode
                    ? "bg-accent px-2.5 py-1 font-medium text-white"
                    : "px-2.5 py-1 text-secondary hover:bg-elevated"
                }
              >
                {mode === "classic" ? "Classic" : "Blueprint"}
              </button>
            ))}
          </span>
          {hasProposals && canvas === "classic" && (
            <label className="flex items-center gap-2 text-xs text-secondary">
              <input type="checkbox" checked={showOverlay} onChange={(e) => setManualOverlay(e.target.checked)} className="accent-[var(--color-accent)]" />
              Show proposal overlay
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
            ) : canvas === "blueprint" ? (
              <SemanticBlueprint
                nodes={graph.nodes}
                edges={graph.edges}
                candidates={joinCandidates}
                onSeed={onSeedJoins}
                initialSeededCount={seededJoinCount}
              />
            ) : (
              <>
                <SemanticGraph
                  // Remount on overlay toggle so the canvas re-fits and frames
                  // the newly-added proposed MV cards (round-6: proposals were
                  // rendering below the fold because the view never re-fit).
                  key={showOverlay ? "graph-overlay" : "graph-base"}
                  nodes={rendered.nodes}
                  edges={rendered.edges}
                  selectedId={selectedId}
                  // Toggle: re-clicking the selection (or a null from an empty
                  // -canvas click) clears it — reference parity (BlueprintCanvas).
                  onSelectNode={(n) => setSelectedId((prev) => (n === null || prev === n.id ? null : n.id))}
                />
                {selectedNode && <NodeDetail node={selectedNode} proposals={proposals} nodes={rendered.nodes} edges={rendered.edges} />}
              </>
            )}
          </>
        )}
      </div>
    </div>
  )
}

export function SemanticModelTab({
  spaceId,
  onReviewCreate,
}: {
  spaceId: string
  // Deep-link "Review in run setup" — opens the optimize tab in
  // create_and_attach mode (forwarded to the advisory, unchanged behavior).
  onReviewCreate?: (proposal: MvProposal | null) => void
}) {
  const [graph, setGraph] = useState<SemanticGraphResponse | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  // The advisory owns the proposal lifecycle (hydrate + scan); it publishes its
  // set here so the graph overlays the SAME proposals. Null until the advisory
  // reports, so the graph's own proposals seed the first paint.
  const [overlayProposals, setOverlayProposals] = useState<MvProposal[] | null>(null)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [selectRequest, setSelectRequest] = useState<{ id: string; nonce: number } | null>(null)
  const nonceRef = useRef(0)

  // Join Advisor (§7). Candidates are discovered lazily (warehouse probes are
  // not free) the first time the Blueprint canvas is opened. `seededSeeds` is
  // the persisted ADVICE set — the checked candidates carried forward as a
  // proposal the next Auto-Optimize run validates and adds itself. Seeding here
  // never edits the Genie Agent config; the Workbench makes no ad-hoc
  // serialized_space edits (that is the product UI's job).
  const [joinCandidates, setJoinCandidates] = useState<JoinCandidate[]>([])
  const [seededSeeds, setSeededSeeds] = useState<JoinCandidate[]>([])
  const joinAdvisorLoadedRef = useRef(false)

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
    // Reset Join Advisor state when the space changes so probes/advice don't
    // leak across spaces; re-discovered on the next Blueprint open.
    joinAdvisorLoadedRef.current = false
    setJoinCandidates([])
    setSeededSeeds([])
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [spaceId])

  // Lazy discovery: candidates + already-persisted advice, once per space, the
  // first time the Blueprint canvas is shown. Best-effort — a failure leaves the
  // inset in its honest-empty state rather than surfacing an error.
  const loadJoinAdvisor = () => {
    if (joinAdvisorLoadedRef.current) return
    joinAdvisorLoadedRef.current = true
    fetchJoinCandidates(spaceId)
      .then((res) => setJoinCandidates(res.candidates ?? []))
      .catch(() => setJoinCandidates([]))
    fetchJoinAdvice(spaceId)
      .then((res) => setSeededSeeds(res.seeds ?? []))
      .catch(() => setSeededSeeds([]))
  }

  // Seeding persists the checked candidates as ADVICE for the optimizer. The
  // POST replaces the space's advice set, so we send the cumulative union
  // (deduped by candidate id) rather than only the newly-checked ones.
  const onSeedJoins = (seeds: JoinCandidate[]) => {
    setSeededSeeds((prev) => {
      const byId = new Map(prev.map((c) => [c.id, c]))
      for (const s of seeds) byId.set(s.id, s)
      const merged = [...byId.values()]
      saveJoinAdvice(spaceId, merged).catch(() => {
        /* best-effort; the inset already reflects the optimistic set */
      })
      return merged
    })
  }

  const proposalsForGraph = useMemo(() => overlayProposals ?? graph?.proposals ?? [], [overlayProposals, graph])

  // A selected proposed node (id `proposed:<full name>`) maps to its
  // suggestion_id via the same shortName rule the overlay/NodeDetail use, so the
  // advisory can highlight and scroll to the matching card.
  const highlightSuggestionId = useMemo(() => {
    if (!selectedNodeId || !selectedNodeId.startsWith("proposed:")) return null
    const label = shortName(selectedNodeId.slice("proposed:".length))
    const match = proposalsForGraph.find(
      (p) => p.proposed_object && shortName(p.proposed_object) === label,
    )
    return match?.suggestion_id ?? null
  }, [selectedNodeId, proposalsForGraph])

  const locateInGraph = (p: MvProposal) => {
    if (!p.proposed_object) return
    nonceRef.current += 1
    setSelectRequest({ id: `proposed:${p.proposed_object}`, nonce: nonceRef.current })
  }

  return (
    <div className="space-y-6">
      <SemanticModelView
        graph={graph}
        isLoading={isLoading}
        error={error}
        onRefresh={load}
        proposalsOverride={proposalsForGraph}
        onSelectionChange={setSelectedNodeId}
        selectRequest={selectRequest}
        joinCandidates={joinCandidates}
        onSeedJoins={onSeedJoins}
        seededJoinCount={seededSeeds.length}
        onBlueprintActive={loadJoinAdvisor}
      />
      <MvIqScanAdvisorySection
        spaceId={spaceId}
        onReviewCreate={onReviewCreate}
        onProposalsChange={setOverlayProposals}
        highlightSuggestionId={highlightSuggestionId}
        onLocateInGraph={locateInGraph}
        onCreated={() => load()}
      />
    </div>
  )
}
