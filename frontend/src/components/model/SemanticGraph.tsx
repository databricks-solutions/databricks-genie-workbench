/* eslint-disable react-refresh/only-export-components */
/**
 * SemanticGraph — deterministic layered SVG of a Genie Agent's semantic model.
 *
 * Promoted to production from the Prompt 12.0 mockup (MvSemanticModelFrame.tsx),
 * NOT imported from it: that scaffold is disposed at Prompt 13.5, so coupling
 * ship code to it would be backwards. The layout is hand-rolled and
 * deterministic (no force layout, no graph/layout dependency) — two renders of
 * one space produce one picture, which is what a diff overlay requires.
 *
 * Columns, left to right: 0 source/fact tables · 1 joined dimension tables ·
 * 2 metric views · 3 measure concepts. Governance ladder is a TRAFFIC LIGHT on
 * the theme's semantic tokens (governed=success / curated=warning /
 * ungoverned=danger), each rung carrying a non-color label so it never leans on
 * hue alone.
 */
import { Component, useMemo, useRef, useState, type ErrorInfo, type ReactNode } from "react"
import { AlertTriangle, Maximize2, Minus, Plus, RefreshCw, ShieldCheck, Wrench } from "lucide-react"
import type { MvGovernance, SemanticGraphEdge, SemanticGraphNode } from "@/types"

export const GOVERNANCE: Record<
  MvGovernance,
  { label: string; color: string; Icon: typeof ShieldCheck }
> = {
  governed: { label: "Governed", color: "var(--color-success)", Icon: ShieldCheck },
  curated: { label: "Curated", color: "var(--color-warning)", Icon: Wrench },
  ungoverned: { label: "Ungoverned", color: "var(--color-danger)", Icon: AlertTriangle },
}

export const LADDER_ORDER: MvGovernance[] = ["governed", "curated", "ungoverned"]

export function countGovernance(nodes: SemanticGraphNode[]): Record<MvGovernance, number> {
  const counts: Record<MvGovernance, number> = { governed: 0, curated: 0, ungoverned: 0 }
  for (const n of nodes) {
    if (n.kind === "measure" && n.governance) counts[n.governance] += 1
  }
  return counts
}

// ── Deterministic layered layout ─────────────────────────────────────────────
const COL_X = [40, 220, 400, 560]
const COL_W = [140, 140, 140, 150]
const ROW_TOP = 44
const ROW_GAP = 58
const NODE_H = 34
const COL_HEADERS = ["source / fact", "dimensions", "metric views", "measure concepts"]

interface Placed {
  node: SemanticGraphNode
  x: number
  y: number
  w: number
  cx: number
  cy: number
}

function layout(nodes: SemanticGraphNode[]): { placed: Map<string, Placed>; width: number; height: number } {
  const placed = new Map<string, Placed>()
  let maxRow = 0
  for (const node of nodes) {
    const col = Math.max(0, Math.min(COL_X.length - 1, node.col))
    const x = COL_X[col]
    const w = COL_W[col]
    const y = ROW_TOP + node.row * ROW_GAP
    placed.set(node.id, { node, x, y, w, cx: x + w / 2, cy: y + NODE_H / 2 })
    maxRow = Math.max(maxRow, node.row)
  }
  const width = COL_X[COL_X.length - 1] + COL_W[COL_W.length - 1] + 24
  const height = ROW_TOP + (maxRow + 1) * ROW_GAP
  return { placed, width, height }
}

function abbreviate(text: string, max = 28): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text
}

/**
 * Pan delta from a drag anchor to the current pointer. Null-safe by contract:
 * a null anchor (the pointerup-vs-pointermove race that used to crash the tab
 * via `drag.current!.tx`) yields null and the caller no-ops. Pure so the
 * invariant is testable without a DOM.
 */
export function panDelta(
  anchor: { x: number; y: number } | null,
  clientX: number,
  clientY: number,
): { dx: number; dy: number } | null {
  if (!anchor) return null
  return { dx: clientX - anchor.x, dy: clientY - anchor.y }
}

function EdgeView({
  edge,
  from,
  to,
  active,
  onHover,
}: {
  edge: SemanticGraphEdge
  from: Placed
  to: Placed
  active: boolean
  onHover: (on: boolean) => void
}) {
  const midX = (from.cx + to.cx) / 2
  const midY = (from.cy + to.cy) / 2

  if (edge.kind === "replaces") {
    return (
      <g>
        <line x1={from.cx} y1={from.cy} x2={to.cx} y2={to.cy} stroke="var(--color-danger)" strokeWidth="1.5" strokeDasharray="4 3" />
        <text x={midX} y={midY - 4} textAnchor="middle" className="fill-[var(--color-danger)]" fontSize="9">replaces</text>
      </g>
    )
  }
  if (edge.kind === "membership") {
    return <line x1={from.cx} y1={from.cy} x2={to.cx} y2={to.cy} stroke="var(--color-accent)" strokeWidth="1.5" />
  }

  // join — declutter: relationship + SCD2 at rest, full ON predicate on hover.
  // Prompt 12b SQL-coverage lens: a positive edge weight (curated statements that
  // traverse this join) rides the rest label as "×N".
  const weightLabel = typeof edge.weight === "number" && edge.weight > 0 ? `×${edge.weight}` : null
  const restLabel = [edge.relationship, edge.scd2 ? "SCD2" : null, weightLabel].filter(Boolean).join(" · ")
  return (
    <g onMouseEnter={() => onHover(true)} onMouseLeave={() => onHover(false)} style={{ cursor: "default" }}>
      <line x1={from.cx} y1={from.cy} x2={to.cx} y2={to.cy} stroke="var(--border-color-strong)" strokeWidth={active ? 2 : 1.5} markerEnd="url(#mv-arrow)" />
      {active && edge.on ? (
        <text x={midX} y={midY - 6} textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="8.5">
          ON {abbreviate(edge.on, 42)}
        </text>
      ) : (
        restLabel && (
          <text x={midX} y={midY - 6} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8.5">{restLabel}</text>
        )
      )}
      {active && (edge.relationship || edge.scd2) && (
        <text x={midX} y={midY + 8} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8.5">
          {[edge.relationship, edge.scd2 ? "SCD2 (is_current)" : null].filter(Boolean).join(" · ")}
        </text>
      )}
    </g>
  )
}

// Prompt 12b SQL-coverage lens: a small badge on the node's top-right corner
// carrying the curated-SQL touch count. 0 is a legible COLD SPOT (a node no
// curated query exercises), rendered plainly with a dashed ring — the lens's
// point, not an error. Absent (undefined) coverage renders nothing, so a
// lens-free Prompt 12 response is unchanged.
function CoverageBadge({ x, y, w, coverage }: { x: number; y: number; w: number; coverage?: number | null }) {
  if (coverage == null) return null
  const cold = coverage === 0
  const bx = x + w - 12
  const by = y - 6
  return (
    <g aria-label={cold ? "no curated SQL coverage" : `curated SQL coverage ${coverage}`}>
      <title>{cold ? "cold spot — no curated SQL touches this" : `${coverage} curated statement${coverage === 1 ? "" : "s"}`}</title>
      <circle cx={bx} cy={by} r="8" fill={cold ? "var(--bg-surface)" : "var(--color-accent)"} opacity={cold ? 1 : 0.85}
        stroke={cold ? "var(--color-danger)" : "var(--color-accent)"} strokeWidth="1" strokeDasharray={cold ? "2 2" : undefined} />
      <text x={bx} y={by + 3} textAnchor="middle" fontSize="8" fontWeight="700"
        fill={cold ? "var(--color-danger)" : "var(--bg-surface)"}>{coverage}</text>
    </g>
  )
}

function NodeView({ p, selected, onSelect }: { p: Placed; selected: boolean; onSelect: (n: SemanticGraphNode) => void }) {
  const { node, x, y, w } = p
  const stroke = selected ? "var(--color-accent)" : undefined
  const selWidth = selected ? 2 : 1.5

  if (node.kind === "measure" && node.governance) {
    const g = GOVERNANCE[node.governance]
    return (
      <g onClick={() => onSelect(node)} style={{ cursor: "pointer" }}>
        <title>{node.origin ? `${node.label} — ${node.origin}` : node.label}</title>
        <rect x={x} y={y} width={w} height={NODE_H} rx="6" fill={g.color} opacity="0.14" stroke={stroke ?? g.color} strokeWidth={selWidth} />
        <text x={x + w / 2} y={y + 14} textAnchor="middle" className="fill-[var(--text-primary)]" fontSize="10" fontWeight="600">{abbreviate(node.label, 18)}</text>
        <text x={x + w / 2} y={y + 26} textAnchor="middle" fill={g.color} fontSize="8" fontWeight="600">{g.label}</text>
        <CoverageBadge x={x} y={y} w={w} coverage={node.coverage} />
      </g>
    )
  }
  if (node.kind === "metric_view") {
    return (
      <g opacity={node.proposed ? 0.85 : 1} onClick={() => onSelect(node)} style={{ cursor: "pointer" }}>
        <title>{node.proposed ? `${node.label} — proposed metric view` : node.label}</title>
        <rect x={x} y={y} width={w} height={NODE_H} rx="8" fill="var(--color-accent)" opacity={node.proposed ? 0.1 : 0.15} stroke={stroke ?? "var(--color-accent)"} strokeWidth={selWidth} strokeDasharray={node.proposed ? "5 3" : undefined} />
        <text x={x + w / 2} y={y + 14} textAnchor="middle" className="fill-[var(--text-primary)]" fontSize="10" fontWeight="600">{abbreviate(node.label, 18)}</text>
        <text x={x + w / 2} y={y + 26} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8">{node.proposed ? "proposed metric view" : "metric view"}</text>
      </g>
    )
  }
  return (
    <g onClick={() => onSelect(node)} style={{ cursor: "pointer" }}>
      <title>{node.label}</title>
      <rect x={x} y={y} width={w} height={NODE_H} rx="6" fill="var(--bg-surface)" stroke={stroke ?? "var(--border-color-strong)"} strokeWidth={selWidth} strokeDasharray={node.coverage === 0 ? "4 3" : undefined} />
      <text x={x + w / 2} y={y + 21} textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="10">{abbreviate(node.label, 18)}</text>
      <CoverageBadge x={x} y={y} w={w} coverage={node.coverage} />
    </g>
  )
}

interface SemanticGraphProps {
  nodes: SemanticGraphNode[]
  edges: SemanticGraphEdge[]
  selectedId?: string | null
  onSelectNode?: (node: SemanticGraphNode) => void
  label?: string
}

function SemanticGraphInner({ nodes, edges, selectedId, onSelectNode, label = "Semantic model" }: SemanticGraphProps) {
  const { placed, width, height } = useMemo(() => layout(nodes), [nodes])
  const [hoverEdge, setHoverEdge] = useState<number | null>(null)
  const [view, setView] = useState({ scale: 1, tx: 0, ty: 0 })
  const [dragging, setDragging] = useState(false)
  const drag = useRef<{ x: number; y: number; tx: number; ty: number } | null>(null)

  const clampScale = (s: number) => Math.min(2.5, Math.max(0.4, s))
  const zoomBy = (f: number) => setView((v) => ({ ...v, scale: clampScale(v.scale * f) }))
  const reset = () => setView({ scale: 1, tx: 0, ty: 0 })

  const onWheel = (e: React.WheelEvent) => {
    e.preventDefault()
    zoomBy(e.deltaY < 0 ? 1.1 : 0.9)
  }
  const onPointerDown = (e: React.PointerEvent) => {
    drag.current = { x: e.clientX, y: e.clientY, tx: view.tx, ty: view.ty }
    setDragging(true)
    ;(e.target as Element).setPointerCapture?.(e.pointerId)
  }
  const onPointerMove = (e: React.PointerEvent) => {
    // Read the ref ONCE into a local: a pointerup between this guard and the
    // setView closure would otherwise null drag.current and crash the tab
    // (the SemanticGraph.tsx:219 non-null-assertion race). No `!` in handlers.
    const d = drag.current
    const delta = panDelta(d, e.clientX, e.clientY)
    if (!d || !delta) return
    setView((v) => ({ ...v, tx: d.tx + delta.dx, ty: d.ty + delta.dy }))
  }
  const onPointerUp = () => {
    drag.current = null
    setDragging(false)
  }

  const select = (n: SemanticGraphNode) => onSelectNode?.(n)
  const selectedNode = placed.get(selectedId ?? "")?.node

  return (
    <div className="relative overflow-hidden rounded-lg border border-default bg-sunken">
      <div className="absolute right-2 top-2 z-10 flex flex-col gap-1">
        <button type="button" aria-label="Zoom in" onClick={() => zoomBy(1.2)} className="rounded border border-default bg-surface p-1 text-muted hover:text-secondary"><Plus className="h-3.5 w-3.5" /></button>
        <button type="button" aria-label="Zoom out" onClick={() => zoomBy(0.83)} className="rounded border border-default bg-surface p-1 text-muted hover:text-secondary"><Minus className="h-3.5 w-3.5" /></button>
        <button type="button" aria-label="Fit to view" onClick={reset} className="rounded border border-default bg-surface p-1 text-muted hover:text-secondary"><Maximize2 className="h-3.5 w-3.5" /></button>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="w-full touch-none select-none"
        style={{ maxHeight: 480, cursor: dragging ? "grabbing" : "grab" }}
        role="img"
        aria-label={label}
        onWheel={onWheel}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerLeave={onPointerUp}
      >
        <defs>
          <marker id="mv-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" className="fill-[var(--text-muted)]" />
          </marker>
        </defs>
        <g transform={`translate(${view.tx} ${view.ty}) scale(${view.scale})`}>
          {COL_HEADERS.map((h, i) => (
            <text key={h} x={COL_X[i] + COL_W[i] / 2} y={20} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="9" fontWeight="600">{h}</text>
          ))}
          {edges.map((edge, i) => {
            const from = placed.get(edge.from)
            const to = placed.get(edge.to)
            if (!from || !to) return null
            const active = hoverEdge === i || edge.from === selectedId || edge.to === selectedId
            return <EdgeView key={i} edge={edge} from={from} to={to} active={active} onHover={(on) => setHoverEdge(on ? i : null)} />
          })}
          {nodes.map((node) => {
            const p = placed.get(node.id)
            return p ? <NodeView key={node.id} p={p} selected={node.id === selectedId} onSelect={select} /> : null
          })}
        </g>
      </svg>
      {selectedNode && (
        <div className="pointer-events-none absolute bottom-2 left-2 rounded bg-surface/90 px-2 py-1 text-xs text-muted">
          {selectedNode.label}
        </div>
      )}
    </div>
  )
}

// ── Error boundary (12c Part 1) ──────────────────────────────────────────────
// A visualization must never take the page down. Any render/interaction throw
// inside the graph is caught here and replaced with a recoverable card, instead
// of an uncaught error unmounting the whole tab (smoke finding 5).
export class GraphErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  constructor(props: { children: ReactNode }) {
    super(props)
    this.state = { failed: false }
  }

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("SemanticGraph render failed", error, info)
  }

  handleRetry = () => this.setState({ failed: false })

  render() {
    if (this.state.failed) {
      return (
        <div
          role="alert"
          className="flex flex-col items-center justify-center gap-2 rounded-lg border border-default bg-sunken px-4 py-8 text-center"
        >
          <AlertTriangle className="h-5 w-5 text-[var(--color-danger)]" />
          <p className="text-sm text-secondary">The visualization failed to render.</p>
          <button
            type="button"
            onClick={this.handleRetry}
            className="mt-1 inline-flex items-center gap-1.5 rounded border border-default bg-surface px-2.5 py-1 text-xs text-secondary hover:text-primary"
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh to retry
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

export function SemanticGraph(props: SemanticGraphProps) {
  return (
    <GraphErrorBoundary>
      <SemanticGraphInner {...props} />
    </GraphErrorBoundary>
  )
}
