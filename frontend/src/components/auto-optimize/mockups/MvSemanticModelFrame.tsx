/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * The Model tab (frames 9a–9d), added at Prompt 12.0 as the review gate for the
 * fourth SpaceDetail tab that Prompt 12 implements. Frame 6 (the old single
 * "semantic model" static preview) was UPGRADED into these rather than
 * duplicated — it is 9c's ancestor (proposal overlay), and the other three frames
 * are the base graph (9a), the empty state (9b), and the node-detail panel (9d).
 *
 * The live view is a DETERMINISTIC LAYERED SVG, not react-force-graph-2d (see the
 * amendment note on the Prompt 12 body in docs/design/mv-advisor-playbook.md —
 * force layouts are non-deterministic under a diff overlay, canvas is untestable
 * under renderToStaticMarkup, and rich nodes need DOM; ForceGraph2D stays in
 * watch/pages/ResourceGraphView.tsx only). These frames are therefore the
 * rendering approach Prompt 12 upgrades with data, layout, and pan/zoom.
 *
 * Governance ladder = TRAFFIC LIGHT on the theme's semantic tokens
 * (governed=success / curated=warning / ungoverned=danger), the 12.0 correction —
 * NOT the Prompt 10 confidence trio. Every rung carries an icon + label so it
 * never leans on hue alone.
 *
 * ✓ COPY REVIEW — frame 9b (empty state) + the ladder empty text: AUTHORED NEW
 * for this branch and REVIEWED-AND-APPROVED at Prompt 12.0 with the exact wording
 * below. It states the config-scoped fact ("this Agent's configuration defines
 * no joins…"), gives both populators (author it yourself OR let a run discover
 * and apply — add_join_spec/update_join_spec are in the unified-loop allowlist,
 * unified_loop.py:91-92, applied at applier.py:3173), and records that metric
 * view SUGGESTIONS do not require a run (13.5 / POV Delta 9). Do not present a
 * run as the only populator on this surface — see the governance-derivation rule
 * on the Prompt 12 body in docs/design/mv-advisor-playbook.md before editing.
 *
 * Disposal: after Prompt 13.5 wires the last frame, with the rest of the scaffold
 * (not Prompt 13).
 */
import type { ReactElement } from "react"
import { AlertTriangle, GitBranch, ShieldCheck, Wrench } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import {
  joinDetail,
  measureDetail,
  proposalRevenue,
  semanticGraphEmpty,
  semanticGraphPopulated,
  semanticGraphProposalOverlay,
  type MvGovernance,
  type MvGraphEdgeFixture,
  type MvGraphNodeFixture,
  type MvSemanticGraphFixture,
} from "./mvMockData"

// ── Governance ladder — the single source of the traffic-light mapping ───────
const GOVERNANCE: Record<
  MvGovernance,
  { label: string; badge: "success" | "warning" | "danger"; color: string; Icon: typeof ShieldCheck }
> = {
  governed: { label: "Governed", badge: "success", color: "var(--color-success)", Icon: ShieldCheck },
  curated: { label: "Curated", badge: "warning", color: "var(--color-warning)", Icon: Wrench },
  ungoverned: { label: "Ungoverned", badge: "danger", color: "var(--color-danger)", Icon: AlertTriangle },
}

const LADDER_ORDER: MvGovernance[] = ["governed", "curated", "ungoverned"]

// ── Layered SVG layout (deterministic — no force layout, MV-D "no new graph dep") ─
const COL_X = [56, 226, 392, 540]
const COL_W = [120, 120, 120, 148]
const ROW_TOP = 58
const ROW_GAP = 62
const NODE_H = 34

interface Placed {
  node: MvGraphNodeFixture
  x: number
  y: number
  w: number
  cx: number
  cy: number
}

function place(nodes: MvGraphNodeFixture[]): Map<string, Placed> {
  const placed = new Map<string, Placed>()
  for (const node of nodes) {
    const x = COL_X[node.col]
    const w = COL_W[node.col]
    const y = ROW_TOP + node.row * ROW_GAP
    placed.set(node.id, { node, x, y, w, cx: x + w / 2, cy: y + NODE_H / 2 })
  }
  return placed
}

function GraphEdge({ edge, from, to }: { edge: MvGraphEdgeFixture; from: Placed; to: Placed }) {
  const midX = (from.cx + to.cx) / 2
  const midY = (from.cy + to.cy) / 2
  if (edge.kind === "replaces") {
    return (
      <g>
        <line
          x1={from.cx}
          y1={from.cy}
          x2={to.cx}
          y2={to.cy}
          stroke="var(--color-danger)"
          strokeWidth="1.5"
          strokeDasharray="4 3"
        />
        <text x={midX} y={midY - 4} textAnchor="middle" className="fill-[var(--color-danger)]" fontSize="9">
          replaces
        </text>
      </g>
    )
  }
  const stroke = edge.kind === "membership" ? "var(--color-accent)" : "var(--border-color-strong)"
  return (
    <g>
      <line x1={from.cx} y1={from.cy} x2={to.cx} y2={to.cy} stroke={stroke} strokeWidth="1.5" markerEnd="url(#mv-arrow)" />
      {edge.on && (
        <text x={midX} y={midY - 6} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8.5">
          ON {edge.on}
        </text>
      )}
      {edge.kind === "join" && (edge.relationship || edge.scd2) && (
        <text x={midX} y={midY + 6} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8.5">
          {[edge.relationship, edge.scd2 ? "SCD2 (is_current)" : null].filter(Boolean).join(" · ")}
        </text>
      )}
    </g>
  )
}

function GraphNode({ p }: { p: Placed }) {
  const { node, x, y, w } = p
  if (node.kind === "measure" && node.governance) {
    const g = GOVERNANCE[node.governance]
    return (
      <g>
        <rect x={x} y={y} width={w} height={NODE_H} rx="6" fill={g.color} opacity="0.14" stroke={g.color} strokeWidth="1.5" />
        <text x={x + w / 2} y={y + 15} textAnchor="middle" className="fill-[var(--text-primary)]" fontSize="10" fontWeight="600">
          {node.label}
        </text>
        {/* Non-color discriminator: the rung word rides on every chip. */}
        <text x={x + w / 2} y={y + 27} textAnchor="middle" fill={g.color} fontSize="8" fontWeight="600">
          {g.label}
        </text>
      </g>
    )
  }
  if (node.kind === "metric_view") {
    return (
      <g opacity={node.proposed ? 0.85 : 1}>
        <rect
          x={x}
          y={y}
          width={w}
          height={NODE_H}
          rx="8"
          fill="var(--color-accent)"
          opacity={node.proposed ? 0.1 : 0.15}
          stroke="var(--color-accent)"
          strokeWidth="1.5"
          strokeDasharray={node.proposed ? "5 3" : undefined}
        />
        <text x={x + w / 2} y={y + 15} textAnchor="middle" className="fill-[var(--text-primary)]" fontSize="10" fontWeight="600">
          {node.label}
        </text>
        <text x={x + w / 2} y={y + 27} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8">
          {node.proposed ? "proposed metric view" : "metric view"}
        </text>
      </g>
    )
  }
  // table
  return (
    <g>
      <rect x={x} y={y} width={w} height={NODE_H} rx="6" fill="var(--bg-surface)" stroke="var(--border-color-strong)" />
      <text x={x + w / 2} y={y + 21} textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="10">
        {node.label}
      </text>
    </g>
  )
}

function GraphSvg({ graph, label }: { graph: MvSemanticGraphFixture; label: string }) {
  const placed = place(graph.nodes)
  return (
    <svg viewBox="0 0 700 260" className="w-full rounded-lg border border-default bg-sunken" role="img" aria-label={label}>
      <defs>
        <marker id="mv-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" className="fill-[var(--text-muted)]" />
        </marker>
      </defs>
      {/* Column headers make the fact -> dims -> MV -> fields order legible. */}
      {["source / fact", "dimensions", "metric views", "measure concepts"].map((h, i) => (
        <text key={h} x={COL_X[i] + COL_W[i] / 2} y={28} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="9" fontWeight="600">
          {h}
        </text>
      ))}
      {graph.edges.map((edge, i) => {
        const from = placed.get(edge.from)
        const to = placed.get(edge.to)
        if (!from || !to) return null
        return <GraphEdge key={i} edge={edge} from={from} to={to} />
      })}
      {graph.nodes.map((node) => {
        const p = placed.get(node.id)
        return p ? <GraphNode key={node.id} p={p} /> : null
      })}
    </svg>
  )
}

// ── Shared chrome ────────────────────────────────────────────────────────────
const TABS = ["Score", "Model", "Optimize", "History"] as const

function TabStrip() {
  return (
    <div className="flex items-center gap-1 border-b border-default" role="tablist" aria-label="Agent detail tabs">
      {TABS.map((tab) => (
        <span
          key={tab}
          role="tab"
          aria-selected={tab === "Model"}
          className={
            tab === "Model"
              ? "-mb-px border-b-2 border-accent px-3 py-2 text-sm font-medium text-primary"
              : "px-3 py-2 text-sm text-muted"
          }
        >
          {tab}
        </span>
      ))}
    </div>
  )
}

function GovernanceLadder({ counts }: { counts: Record<MvGovernance, number> }) {
  const present = LADDER_ORDER.filter((rung) => counts[rung] > 0)
  // Honesty rule (frame-7b, both ways): an empty space has found nothing —
  // render no colored rung, so the ladder never alarms (red) about a measure it
  // never saw, nor implies governance (green) it does not have.
  if (present.length === 0) {
    return (
      <p className="text-xs text-muted">
        No measure concepts yet — none are defined in this Agent's config, and none have been suggested.
      </p>
    )
  }
  return (
    <div className="flex flex-wrap items-center gap-2" aria-label="Governance ladder">
      {present.map((rung) => {
        const g = GOVERNANCE[rung]
        const Icon = g.Icon
        return (
          <Badge key={rung} variant={g.badge}>
            <Icon className="mr-1 h-3 w-3" />
            {g.label} · {counts[rung]}
          </Badge>
        )
      })}
    </div>
  )
}

function OverlayToggle({ on }: { on: boolean }) {
  return (
    <label className="flex items-center gap-2 text-xs text-secondary">
      <span
        className={`inline-flex h-4 w-7 items-center rounded-full px-0.5 ${on ? "justify-end bg-accent" : "justify-start bg-elevated border border-default"}`}
        role="switch"
        aria-checked={on}
      >
        <span className="h-3 w-3 rounded-full bg-white" />
      </span>
      Show proposal overlay
      <span className="text-muted">(default off)</span>
    </label>
  )
}

function countGovernance(graph: MvSemanticGraphFixture): Record<MvGovernance, number> {
  const counts: Record<MvGovernance, number> = { governed: 0, curated: 0, ungoverned: 0 }
  for (const node of graph.nodes) {
    if (node.kind === "measure" && node.governance) counts[node.governance] += 1
  }
  return counts
}

function ModelTabShell({
  children,
  ladderCounts,
  toggle,
}: {
  children: ReactElement
  ladderCounts: Record<MvGovernance, number>
  toggle?: ReactElement
}) {
  return (
    <div className="rounded-xl border border-default bg-surface">
      <TabStrip />
      <div className="space-y-3 p-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model</h3>
          {toggle}
        </div>
        <GovernanceLadder counts={ladderCounts} />
        {children}
      </div>
    </div>
  )
}

// ── 9a · Model tab, populated ────────────────────────────────────────────────
export function ModelTabPopulatedFrame() {
  return (
    <ModelTabShell ladderCounts={countGovernance(semanticGraphPopulated)} toggle={<OverlayToggle on={false} />}>
      <GraphSvg graph={semanticGraphPopulated} label="Semantic model — populated" />
    </ModelTabShell>
  )
}

// ── 9b · Model tab, never optimized / empty ──────────────────────────────────
// No overlay toggle here: the Prompt 12 body offers the overlay only when the
// space-scoped proposals read returns candidates, and an empty space has none —
// a visible toggle would imply something to overlay. 9a keeps it (off).
export function ModelTabEmptyFrame() {
  return (
    <ModelTabShell ladderCounts={countGovernance(semanticGraphEmpty)}>
      <div className="space-y-3">
        <GraphSvg graph={semanticGraphEmpty} label="Semantic model — never optimized" />
        <p className="text-xs text-muted">
          This Agent's configuration defines no joins, SQL snippets, or metric views yet — the graph shows the config as
          it is now. Connect it by adding join specs and snippets yourself, or let an optimization run discover and apply
          them. Metric view suggestions don't require a run.
        </p>
      </div>
    </ModelTabShell>
  )
}

// ── 9c · Proposal overlay ON (frame 6's descendant) ──────────────────────────
export function ModelTabProposalOverlayFrame() {
  const merged: MvSemanticGraphFixture = {
    nodes: [...semanticGraphPopulated.nodes, ...semanticGraphProposalOverlay.nodes],
    edges: [...semanticGraphPopulated.edges, ...semanticGraphProposalOverlay.edges],
  }
  return (
    // Ladder counts the BASE graph, not `merged`: a proposal isn't governance
    // until it's real (created + attached), so the ghosted overlay MV must not
    // move the governed/curated/ungoverned tallies. Deliberate — do not "fix".
    <ModelTabShell ladderCounts={countGovernance(semanticGraphPopulated)} toggle={<OverlayToggle on={true} />}>
      <GraphSvg graph={merged} label="Semantic model — proposal overlay" />
    </ModelTabShell>
  )
}

// ── 9d · Node detail panel (one measure, one join) ───────────────────────────
export function ModelNodeDetailFrame() {
  const g = GOVERNANCE[measureDetail.governance]
  const MeasureIcon = g.Icon
  return (
    <div className="grid gap-3 sm:grid-cols-2">
      {/* Measure detail */}
      <div className="space-y-2 rounded-lg border border-default bg-elevated p-3">
        <div className="flex items-center justify-between">
          <span className="font-mono text-sm font-medium text-primary">{measureDetail.name}</span>
          <Badge variant={g.badge}>
            <MeasureIcon className="mr-1 h-3 w-3" />
            {g.label}
          </Badge>
        </div>
        <dl className="space-y-1 text-xs text-muted">
          <div className="flex gap-2">
            <dt className="text-secondary">expr</dt>
            <dd className="font-mono">{measureDetail.expr}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-secondary">format</dt>
            <dd>{measureDetail.format}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-secondary">synonyms</dt>
            <dd>{measureDetail.synonyms.join(", ")}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-secondary">recurrence</dt>
            <dd>{proposalRevenue.evidence?.recurrence_count} occurrences</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-secondary">questions</dt>
            <dd className="font-mono">{proposalRevenue.evidence?.benchmark_question_ids.join(", ")}</dd>
          </div>
        </dl>
      </div>

      {/* Join detail */}
      <div className="space-y-2 rounded-lg border border-default bg-elevated p-3">
        <div className="flex items-center justify-between">
          <span className="font-mono text-sm font-medium text-primary">
            {joinDetail.from} → {joinDetail.to}
          </span>
          <Badge variant="secondary">
            <GitBranch className="mr-1 h-3 w-3" />
            {joinDetail.join_strategy === "subquery-source" ? "Subquery source" : "Denormalized"}
          </Badge>
        </div>
        <dl className="space-y-1 text-xs text-muted">
          <div className="flex gap-2">
            <dt className="text-secondary">on</dt>
            <dd className="font-mono">{joinDetail.on}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-secondary">cardinality</dt>
            <dd>{joinDetail.cardinality}</dd>
          </div>
          {joinDetail.scd2 && (
            <div className="flex gap-2">
              <dt className="text-secondary">SCD2</dt>
              <dd>is_current guard on the predicate</dd>
            </div>
          )}
        </dl>
      </div>
    </div>
  )
}
