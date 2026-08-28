/**
 * Semantic Blueprint (v4) — production canvas (Phase 1).
 *
 * The world-class rebuild of the semantic-model canvas
 * (docs/design/semantic-graph-v4-blueprint-note.md §5; north star
 * docs/design/mockups/10-blueprint-prototype.html), built beside the classic
 * `SemanticGraph.tsx` and swapped behind a flag in `SemanticModelTab.tsx` until
 * it reaches parity. The SVG canvas is a PURE function of
 * `(model, zoom, selected, layoutMode)` via the `blueprint/` modules
 * (renderToStaticMarkup-testable, §8); the surrounding component only holds the
 * interactive band/layout/selection state. Consumes the live
 * `SemanticGraphResponse` through `fromSemanticGraph` — no new payload (§5.8).
 */
import { useEffect, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent, type ReactNode, type WheelEvent as ReactWheelEvent } from "react"
import type { SemanticGraphEdge, SemanticGraphNode } from "@/types"
import {
  fromSemanticGraph,
  shortName,
  type BlueprintModel,
  type BlueprintMv,
  type BlueprintTable,
} from "./blueprint/model"
import {
  COL_H,
  COL_TOP,
  derivePlacement,
  layoutBoxes,
  measureIndex,
  nodeById,
  nodeWidth,
  rankLabel,
  type BlueprintLayoutMode,
  type BlueprintZoom,
} from "./blueprint/layout"
import { lineagePaths, resolveEdges, routePath } from "./blueprint/routing"
import { cardinalityMarkers } from "./blueprint/cardinality"
import {
  govColor,
  headlineCounts,
  neighbourhood,
  onStr,
  rankInsights,
  unmodeledRegion,
  worstColdSpot,
} from "./blueprint/annotate"
import { isWeak, seedPayload, verdict, type JoinCandidate } from "./blueprint/advisor"

export interface SemanticBlueprintProps {
  nodes: SemanticGraphNode[]
  edges: SemanticGraphEdge[]
  label?: string
  /**
   * Join Advisor candidates (validated-seed model, §7). Data-grounded FK /
   * name-type / warehouse-probe suggestions, `MvProposal`-shaped. Empty by
   * default — candidate generation + containment probes are a warehouse-backed
   * backend concern (deploy-gated); the inset shows the honest-empty state until
   * they arrive.
   */
  candidates?: JoinCandidate[]
  /** Commit the checked candidate set as the Auto-Optimize run seed (§7). */
  onSeed?: (seeds: JoinCandidate[]) => void
  /**
   * Count of joins already seeded as pending advice for this space (read back
   * from persistence on entry), so the inset reflects prior seeds across a
   * tab re-mount rather than resetting to 0.
   */
  initialSeededCount?: number
}

/** Viewport transform (pan + zoom) applied to the whole scene group. */
export interface CanvasView {
  tx: number
  ty: number
  scale: number
}

const IDENTITY_VIEW: CanvasView = { tx: 0, ty: 0, scale: 1 }
const MIN_SCALE = 0.35
const MAX_SCALE = 4

interface CanvasState {
  model: BlueprintModel
  zoom: BlueprintZoom
  selected: string | null
  layoutMode: BlueprintLayoutMode
  onSelect: (id: string | null) => void
  /** Checked Join Advisor candidates → dashed `proposed_join` overlay (§7). */
  overlay?: JoinCandidate[]
  /**
   * Manual per-node position nudges (SVG user units), keyed by node id. Applied
   * on top of the deterministic layout so a user can drag a card to declutter;
   * absent/empty renders the pure layout unchanged (byte-stable, §8).
   */
  offsets?: Record<string, { dx: number; dy: number }>
  /** Report a node's new absolute offset while dragging (identity → no drag). */
  onNodeMove?: (id: string, dx: number, dy: number) => void
  /**
   * Viewport pan/zoom transform. Absent → identity (byte-stable static render,
   * §8); the interactive parent owns it so Reset view can restore it.
   */
  view?: CanvasView
  /** Report a new viewport transform (absent → pan/zoom disabled). */
  onViewChange?: (v: CanvasView) => void
}

// ── Toolbar: zoom bands · layout toggle · Reset view ─────────────────────────
function Seg<T extends string>({
  options,
  value,
  onChange,
}: {
  options: { id: T; label: string }[]
  value: T
  onChange: (v: T) => void
}) {
  return (
    <span className="inline-flex overflow-hidden rounded-md border border-default text-xs">
      {options.map((o) => (
        <button
          key={o.id}
          type="button"
          onClick={() => onChange(o.id)}
          className={
            o.id === value
              ? "bg-accent px-2.5 py-1 font-medium text-white"
              : "px-2.5 py-1 text-secondary hover:bg-elevated"
          }
        >
          {o.label}
        </button>
      ))}
    </span>
  )
}

// ── Health headline (§5.7) ────────────────────────────────────────────────────
function Headline({ model }: { model: BlueprintModel }) {
  const c = headlineCounts(model)
  const pill = (color: string, n: number) => (
    <span className="inline-flex items-center gap-1 rounded-full border border-default px-2 text-[11px] font-semibold">
      <span className="inline-block h-2 w-2 rounded-full" style={{ background: color }} />
      {n}
    </span>
  )
  return (
    <p className="text-[13px] text-secondary" data-headline>
      <b className="text-primary">{c.governed}</b> governed · <b className="text-primary">{c.curated}</b> curated ·{" "}
      <b className="text-primary">{c.ungoverned}</b> ungoverned{" "}
      <span className="inline-flex gap-1.5 align-middle">
        {pill("var(--color-success)", c.governed)}
        {pill("var(--color-warning)", c.curated)}
        {pill("var(--color-danger)", c.ungoverned)}
      </span>{" "}
      — <b className="text-primary">{c.unmodeled}</b> table{c.unmodeled === 1 ? "" : "s"} in no metric view ·{" "}
      <b className="text-primary">{c.cold}</b> cold spot{c.cold === 1 ? "" : "s"}
    </p>
  )
}

// ── SVG canvas — PURE given CanvasState ──────────────────────────────────────
export function BlueprintCanvas({ model, zoom, selected, layoutMode, onSelect, overlay, offsets, onNodeMove, view: viewProp, onViewChange }: CanvasState) {
  const m = model
  const view = viewProp ?? IDENTITY_VIEW
  const svgRef = useRef<SVGSVGElement | null>(null)
  const sceneRef = useRef<SVGGElement | null>(null)
  // Live interaction bookkeeping — refs (not state) so a drag/pan never
  // re-renders per move beyond the offset/view update the parent owns.
  const dragRef = useRef<{ id: string; startX: number; startY: number; baseDx: number; baseDy: number; moved: boolean } | null>(null)
  const panRef = useRef<{ x: number; y: number; tx: number; ty: number } | null>(null)
  const bgPressRef = useRef(false)
  const panMovedRef = useRef(false)
  const suppressClickRef = useRef(false)

  // Client point → scene-group local coords (through the group's CTM, which
  // already folds in the pan/zoom transform), so a node drag tracks the pointer
  // 1:1 at any zoom.
  const clientToScene = (cx: number, cy: number): { x: number; y: number } => {
    const g = sceneRef.current
    const ctm = g?.getScreenCTM?.()
    if (!g || !ctm || typeof DOMPoint === "undefined") return { x: cx, y: cy }
    const p = new DOMPoint(cx, cy).matrixTransform(ctm.inverse())
    return { x: p.x, y: p.y }
  }
  // Client point → viewBox user space (the SVG's own CTM, independent of the
  // pan/zoom transform), where the pan translation and zoom pivot are expressed.
  const clientToViewBox = (cx: number, cy: number): { x: number; y: number } => {
    const svg = svgRef.current
    const ctm = svg?.getScreenCTM?.()
    if (!svg || !ctm || typeof DOMPoint === "undefined") return { x: cx, y: cy }
    const p = new DOMPoint(cx, cy).matrixTransform(ctm.inverse())
    return { x: p.x, y: p.y }
  }

  const beginDrag = (id: string) => (e: ReactPointerEvent) => {
    if (!onNodeMove) return
    e.stopPropagation() // a card press is a drag, never a canvas pan
    const p = clientToScene(e.clientX, e.clientY)
    const cur = offsets?.[id] ?? { dx: 0, dy: 0 }
    dragRef.current = { id, startX: p.x, startY: p.y, baseDx: cur.dx, baseDy: cur.dy, moved: false }
    ;(e.currentTarget as Element).setPointerCapture?.(e.pointerId)
  }
  // A background press starts a canvas pan; a release without movement clears the
  // selection (the reference's "click empty canvas: reset").
  const beginPan = (e: ReactPointerEvent) => {
    if (!onViewChange) return
    bgPressRef.current = e.target === svgRef.current
    panMovedRef.current = false
    const p = clientToViewBox(e.clientX, e.clientY)
    panRef.current = { x: p.x, y: p.y, tx: view.tx, ty: view.ty }
    svgRef.current?.setPointerCapture?.(e.pointerId)
  }
  // Move/up are owned by the SVG so a pointer that leaves the card mid-drag (or
  // pans past the edge) keeps tracking. Node drag takes precedence over pan.
  const onSurfaceMove = (e: ReactPointerEvent) => {
    const d = dragRef.current
    if (d && onNodeMove) {
      const p = clientToScene(e.clientX, e.clientY)
      if (Math.abs(p.x - d.startX) > 2 || Math.abs(p.y - d.startY) > 2) d.moved = true
      onNodeMove(d.id, d.baseDx + (p.x - d.startX), d.baseDy + (p.y - d.startY))
      return
    }
    const pan = panRef.current
    if (!pan || !onViewChange) return
    const p = clientToViewBox(e.clientX, e.clientY)
    const dx = p.x - pan.x
    const dy = p.y - pan.y
    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) panMovedRef.current = true
    onViewChange({ tx: pan.tx + dx, ty: pan.ty + dy, scale: view.scale })
  }
  const onSurfaceUp = () => {
    const d = dragRef.current
    if (d) {
      if (d.moved) suppressClickRef.current = true
      dragRef.current = null
      return
    }
    // Empty-canvas click (no pan) clears selection; a pan does not.
    if (bgPressRef.current && !panMovedRef.current && selected) onSelect(null)
    bgPressRef.current = false
    panMovedRef.current = false
    panRef.current = null
  }
  // Wheel zoom pivots on the pointer: the scene point under the cursor stays put.
  const onWheelZoom = (e: ReactWheelEvent) => {
    if (!onViewChange) return
    e.preventDefault()
    const k = view.scale
    const k2 = Math.min(MAX_SCALE, Math.max(MIN_SCALE, k * (e.deltaY < 0 ? 1.1 : 0.9)))
    if (k2 === k) return
    const vp = clientToViewBox(e.clientX, e.clientY)
    onViewChange({
      scale: k2,
      tx: vp.x - (vp.x - view.tx) * (k2 / k),
      ty: vp.y - (vp.y - view.ty) * (k2 / k),
    })
  }
  const dragProps = (id: string) =>
    onNodeMove
      ? { onPointerDown: beginDrag(id), style: { cursor: "grab", touchAction: "none" as const } }
      : { style: { cursor: "pointer" } }

  if (!m.nodes.length) {
    return (
      <div className="rounded-lg border border-default bg-sunken px-4 py-10 text-center text-sm text-muted">
        No tables or metric views in this space yet.
      </div>
    )
  }
  const byId = nodeById(m)
  const placement = derivePlacement(m, layoutMode)
  const rawBox = layoutBoxes(m, placement, zoom)
  // Apply manual drag nudges on top of the deterministic layout. Everything
  // downstream (edges, lineage, extents) reads these boxes, so a dragged card
  // keeps its relationships attached.
  const box: Record<string, { x: number; y: number; w: number; h: number }> =
    offsets && Object.keys(offsets).length
      ? Object.fromEntries(
          Object.entries(rawBox).map(([id, b]) => {
            const o = offsets[id]
            return [id, o ? { ...b, x: b.x + o.dx, y: b.y + o.dy } : b]
          }),
        )
      : rawBox
  const keep = neighbourhood(m, selected)
  const resolved = resolveEdges(m, placement, box, zoom)
  const chipPos: Record<string, { x: number; y: number }> = {}
  const parts: ReactNode[] = []
  const pick = (id: string) => (e: { stopPropagation: () => void }) => {
    e.stopPropagation()
    // A drag that moved must not also toggle selection on the trailing click.
    if (suppressClickRef.current) {
      suppressClickRef.current = false
      return
    }
    onSelect(selected === id ? null : id)
  }

  const occupied = [...new Set(m.nodes.map((n) => placement.rank[n.id]))].sort((a, b) => a - b)
  occupied.forEach((r) => {
    const anyNode = m.nodes.find((n) => placement.rank[n.id] === r) as (typeof m.nodes)[number]
    parts.push(
      <text
        key={`hdr-${r}`}
        x={box[anyNode.id].x + nodeWidth(anyNode) / 2}
        y={20}
        textAnchor="middle"
        fill="var(--text-muted)"
        fontSize={10}
        fontWeight={700}
        letterSpacing=".05em"
      >
        {rankLabel(m, placement, r).toUpperCase()}
      </text>,
    )
  })

  const un = unmodeledRegion(m, box)
  if (un) {
    parts.push(
      <g key="unmodeled" data-region="unmodeled">
        <rect x={un.x} y={un.y} width={un.w} height={un.h} rx={12} fill="var(--color-danger)" fillOpacity={0.05}
          stroke="var(--color-danger)" strokeWidth={1.25} strokeDasharray="5 4" />
        <text x={un.x + 8} y={un.y - 4} fill="var(--color-danger)" fontSize={9.5} fontWeight={700}>
          UNMODELED · in no metric view
        </text>
      </g>,
    )
  }

  if (selected && byId[selected]?.kind === "mv") {
    const mem = (m.uses[selected] ?? []).map((t) => box[t]).filter(Boolean)
    mem.forEach((b, i) =>
      parts.push(
        <rect key={`bound-${i}`} data-boundary="mv-member" x={b.x - 7} y={b.y - 7} width={b.w + 14} height={b.h + 14}
          rx={12} fill="var(--color-accent)" fillOpacity={0.09} stroke="var(--color-accent)" strokeWidth={2}
          strokeDasharray="7 4" />,
      ),
    )
  }

  // declared joins (§5.2–§5.4) — arrows require proof: only m.joins are drawn.
  resolved.forEach((e, idx) => {
    const d = routePath(e.sx, e.sy, e.dx, e.dy, e.midX, e.hops)
    const active = !!keep && keep.has(e.from) && keep.has(e.to)
    const dim = !!keep && !active
    const stroke = active ? "var(--color-accent)" : "var(--text-muted)"
    const baseOp = active ? 1 : 0.85
    const { crowfoot, oneTick, manyTick } = cardinalityMarkers(e)
    // One line per pair (§ERD best practice): a composite / merged relationship
    // shows the representative key and a "+N keys" hint rather than stacked lines.
    const extraKeys = (e.keyCount ?? 1) - 1
    const onLabel = extraKeys > 0 ? `${e.fromCol} = ${e.toCol} · +${extraKeys} key${extraKeys > 1 ? "s" : ""}` : `${e.fromCol} = ${e.toCol}`
    const lw = onLabel.length * 5.4 + 12
    parts.push(
      <g key={`edge-${idx}`}>
        {active && (
          <path d={d} fill="none" stroke="var(--color-accent)" strokeWidth={6} strokeLinecap="round" opacity={0.16} />
        )}
        <path d={d} fill="none" stroke={stroke} strokeWidth={active ? 2.1 : 1.5} opacity={dim ? 0.1 : baseOp}
          data-edge="join" data-edge-from={e.from} data-edge-to={e.to} data-hops={e.hops.length} />
        {crowfoot && (
          <path d={crowfoot} fill="none" stroke={stroke} strokeWidth={1.4} opacity={dim ? 0.1 : baseOp} data-glyph="crowfoot" />
        )}
        <path d={oneTick} fill="none" stroke={stroke} strokeWidth={1.4} opacity={dim ? 0.1 : baseOp} data-glyph="one-tick" />
        {manyTick && (
          <path d={manyTick} fill="none" stroke={stroke} strokeWidth={1.4} opacity={dim ? 0.1 : baseOp} data-glyph="one-tick" />
        )}
        {active && !dim && (
          <g>
            <rect x={e.midX - lw / 2} y={(e.sy + e.dy) / 2 - 9} width={lw} height={15} rx={4} fill="var(--bg-sunken)"
              stroke="var(--border-color)" strokeWidth={0.75} />
            <text x={e.midX} y={(e.sy + e.dy) / 2 + 2} textAnchor="middle" fontSize={8.5} fontFamily="var(--font-mono)"
              fill="var(--text-secondary)">
              {onLabel}
            </text>
          </g>
        )}
      </g>,
    )
  })

  // nodes
  m.nodes.forEach((n) => {
    const b = box[n.id]
    const dimmed = !!keep && !keep.has(n.id)
    if (n.kind === "table") {
      const t = n as BlueprintTable
      const sel = selected === n.id
      const wide = !!t.columnCount && t.columnCount > 30
      const cold = t.coverage === 0
      parts.push(
        <g key={n.id} opacity={dimmed ? 0.4 : 1} data-node="table" data-node-id={n.id} onClick={pick(n.id)} {...dragProps(n.id)}>
          <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={9} fill="var(--bg-surface)"
            stroke={sel ? "var(--color-accent)" : t.island ? "var(--color-warning)" : "var(--border-color-strong)"}
            strokeWidth={sel ? 2 : 1.4} strokeDasharray={t.cold || t.island ? "4 3" : undefined} />
          {wide && zoom !== "far" && (
            <g data-pill="wide">
              <rect x={b.x + b.w - 72} y={b.y + 6} width={46} height={15} rx={7} fill="var(--color-warning)"
                fillOpacity={0.16} stroke="var(--color-warning)" strokeWidth={0.75} />
              <text x={b.x + b.w - 49} y={b.y + 16} textAnchor="middle" fontSize={8} fontWeight={700} fill="var(--color-warning)">
                {t.columnCount} cols
              </text>
            </g>
          )}
          {t.island && zoom !== "far" && (
            <text x={b.x + b.w - 12} y={b.y + b.h - 6} textAnchor="end" fontSize={8} fontWeight={700}
              fill="var(--color-warning)" data-tag="island">
              no join
            </text>
          )}
          {zoom !== "far" && (
            <text x={b.x + 12} y={b.y + 17} fill="var(--text-muted)" fontSize={8.5} fontWeight={700}
              letterSpacing=".06em" data-caption="role">
              {t.role ?? "TABLE"}
            </text>
          )}
          <text x={b.x + 12} y={zoom === "far" ? b.y + 22 : zoom === "near" ? b.y + 34 : b.y + 37}
            fill="var(--text-primary)" fontSize={12.5} fontWeight={600} fontFamily="var(--font-mono)">
            {shortName(n.id)}
          </text>
          <circle cx={b.x + b.w - 14} cy={b.y + 14} r={8} fill={cold ? "var(--bg-surface)" : "var(--text-muted)"}
            opacity={cold ? 1 : 0.5} stroke={cold ? "var(--color-danger)" : "var(--border-color-strong)"}
            strokeWidth={1} strokeDasharray={cold ? "2 2" : undefined} />
          <text x={b.x + b.w - 14} y={b.y + 17} textAnchor="middle" fontSize={8} fontWeight={700}
            fill={cold ? "var(--color-danger)" : "var(--bg-surface)"}>
            {t.coverage}
          </text>
          {zoom === "near" && (
            <g>
              <line x1={b.x + 10} y1={b.y + COL_TOP - 6} x2={b.x + b.w - 10} y2={b.y + COL_TOP - 6}
                stroke="var(--border-color)" strokeWidth={1} opacity={0.6} />
              {t.cols.map((c, i) => {
                const cy = b.y + COL_TOP + i * COL_H
                const isKey = m.joins.some((j) => (j.from === n.id && j.fromCol === c) || (j.to === n.id && j.toCol === c))
                return (
                  <g key={c}>
                    {isKey && (
                      <rect x={b.x + 8} y={cy + 1} width={b.w - 16} height={COL_H - 2} rx={4}
                        fill="var(--accent-cur, var(--color-accent))" fillOpacity={0.12} data-joinkey={c} />
                    )}
                    <text x={b.x + 14} y={cy + COL_H / 2 + 3.5} fontSize={9.5} fontFamily="var(--font-mono)"
                      fill={isKey ? "var(--accent-cur, var(--color-accent))" : "var(--text-muted)"} fontWeight={isKey ? 700 : 400}>
                      {c}
                    </text>
                  </g>
                )
              })}
            </g>
          )}
        </g>,
      )
    } else {
      const mv = n as BlueprintMv
      const isMv = mv.kind === "mv"
      const sel = selected === n.id
      parts.push(
        <g key={n.id} opacity={dimmed ? 0.4 : 1} data-node={mv.kind} data-node-id={n.id} onClick={pick(n.id)} {...dragProps(n.id)}>
          <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={10}
            fill={isMv ? "var(--color-accent)" : "var(--color-warning)"} fillOpacity={isMv ? 0.07 : 0.08}
            stroke={sel ? "var(--color-accent)" : isMv ? "var(--border-color-strong)" : "var(--color-warning)"}
            strokeWidth={sel ? 2 : 1.5} strokeDasharray={isMv ? undefined : "5 3"} />
          <text x={b.x + 12} y={b.y + 22} fill="var(--text-primary)" fontSize={12.5} fontWeight={700}>
            {isMv ? shortName(n.id) : "Space config"}
          </text>
          <text x={b.x + 12} y={b.y + 38} fill="var(--text-muted)" fontSize={9}>
            {isMv ? `metric view · ${mv.measures.length} measures` : "not in any metric view"}
          </text>
          {zoom !== "far" &&
            mv.measures.map((ms, i) => {
              const mid = `${n.id}::${ms.name}`
              const my = b.y + 44 + i * 22
              const selM = selected === mid
              chipPos[mid] = { x: b.x + 8, y: my + 9 }
              return (
                <g key={mid} data-chip="measure" data-chip-id={mid} onClick={pick(mid)} style={{ cursor: "pointer" }}>
                  <rect x={b.x + 8} y={my} width={b.w - 16} height={18} rx={4} fill={govColor(ms.gov)}
                    fillOpacity={selM ? 0.3 : 0.14}
                    stroke={selM ? "var(--color-accent)" : ms.overlaps ? "var(--color-warning)" : govColor(ms.gov)}
                    strokeWidth={selM ? 2 : 1} />
                  <text x={b.x + 14} y={my + 13} fontSize={9.5} fontWeight={600} fill="var(--text-primary)">
                    {ms.name}
                  </text>
                  {ms.overlaps && (
                    <g data-marker="overlap">
                      <path d={`M ${b.x + b.w - 20} ${my + 4} l 5 9 l -10 0 Z`} fill="var(--color-warning)" />
                      <text x={b.x + b.w - 20} y={my + 13} textAnchor="middle" fontSize={7} fontWeight={800} fill="var(--bg-surface)">
                        !
                      </text>
                    </g>
                  )}
                </g>
              )
            })}
        </g>,
      )
    }
  })

  if (selected) {
    for (const lp of lineagePaths(m, placement, box, resolved, selected, chipPos)) {
      const dash = lp.mode === "mv" ? "0.1 6" : "5 4"
      parts.push(
        <g key={`lin-${lp.srcId}`}>
          <path d={lp.d} fill="none" stroke="var(--color-accent)" strokeWidth={6} strokeLinecap="round" strokeLinejoin="round" opacity={0.12} />
          <path d={lp.d} fill="none" stroke="var(--color-accent)" strokeWidth={lp.mode === "mv" ? 2.25 : 1.9}
            strokeLinecap="round" strokeLinejoin="round" strokeDasharray={dash} data-lineage={lp.mode} data-lineage-src={lp.srcId} />
          <circle cx={lp.sx} cy={lp.sy} r={3.5} fill="var(--color-accent)" />
        </g>,
      )
    }
  }

  // Join Advisor overlay (§7): dashed `proposed_join` edges for checked
  // candidates. These are OVERLAY proposals — never added to the base `resolved`
  // set, so "arrows require proof" (§2) still holds for the base canvas.
  ;(overlay ?? []).forEach((o, i) => {
    const lb = box[o.from]
    const rb = box[o.to]
    if (!lb || !rb) return
    const fromLeft = lb.x <= rb.x
    const L = fromLeft ? lb : rb
    const R = fromLeft ? rb : lb
    const sx = L.x + L.w
    const sy = L.y + L.h / 2
    const dx = R.x
    const dy = R.y + R.h / 2
    const midX = Math.round((sx + dx) / 2)
    parts.push(
      <path
        key={`proposed-${i}`}
        d={routePath(sx, sy, dx, dy, midX, [])}
        fill="none"
        stroke="var(--color-accent)"
        strokeWidth={1.9}
        strokeDasharray="6 4"
        opacity={0.85}
        data-edge="proposed_join"
        data-edge-from={o.from}
        data-edge-to={o.to}
      />,
    )
  })

  const coldSpot = worstColdSpot(m)
  if (coldSpot && zoom !== "far") {
    const hb = box[coldSpot.id]
    const ax = hb.x
    const ay = hb.y + hb.h
    const bx = hb.x - 4
    const by = ay + 24
    parts.push(
      <g key="callout-cold" data-callout="cold-spot">
        <path d={`M ${ax + 20} ${ay} L ${bx + 20} ${by}`} stroke="var(--color-danger)" strokeWidth={1} strokeDasharray="3 2" fill="none" />
        <rect x={bx} y={by} width={178} height={32} rx={6} fill="var(--bg-surface)" stroke="var(--color-danger)" strokeWidth={1} />
        <text x={bx + 10} y={by + 14} fontSize={10} fontWeight={700} fill="var(--color-danger)">
          Cold spot · {shortName(coldSpot.id)}
        </text>
        <text x={bx + 10} y={by + 26} fontSize={9} fill="var(--text-muted)">
          no curated SQL touches it
        </text>
      </g>,
    )
  }

  // Content extent includes the edge geometry, not just the cards: an intra-rank
  // bracket bows into the gutter beside its column, so framing on boxes alone
  // would clip it. Allow a negative left edge for left-bowing brackets.
  const nodeBoxes = m.nodes.map((n) => box[n.id])
  const edgeXs = resolved.flatMap((e) => [e.sx, e.dx, e.midX])
  const minX = Math.min(0, ...nodeBoxes.map((b) => b.x), ...edgeXs) - 16
  const maxX = Math.max(...nodeBoxes.map((b) => b.x + b.w), ...edgeXs) + 40
  const maxY = Math.max(...nodeBoxes.map((b) => b.y + b.h)) + 96
  const vbW = Math.max(720, maxX - minX)
  const vbH = Math.max(360, maxY)
  const interactive = !!onViewChange || !!onNodeMove
  return (
    <svg
      ref={svgRef}
      viewBox={`${minX} 0 ${vbW} ${vbH}`}
      className="w-full rounded-lg border border-default bg-sunken"
      role="img"
      aria-label="Semantic model blueprint"
      onWheel={onViewChange ? onWheelZoom : undefined}
      onPointerDown={onViewChange ? beginPan : undefined}
      onPointerMove={interactive ? onSurfaceMove : undefined}
      onPointerUp={interactive ? onSurfaceUp : undefined}
      onPointerLeave={interactive ? onSurfaceUp : undefined}
      style={interactive ? { touchAction: "none" } : undefined}
    >
      <g ref={sceneRef} transform={`translate(${view.tx} ${view.ty}) scale(${view.scale})`}>
        {parts}
      </g>
    </svg>
  )
}

// ── Legend (prototype parity) ─────────────────────────────────────────────────
function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted">
      <span className="inline-flex items-center gap-1.5">
        <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--border-color-strong)] bg-[var(--bg-surface)]" />
        table
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--color-accent)] bg-[color-mix(in_srgb,var(--color-accent)_10%,transparent)]" />
        metric view
      </span>
      <span className="inline-flex items-center gap-1.5">
        <svg width={30} height={12}>
          <line x1={2} y1={6} x2={26} y2={6} stroke="var(--border-color-strong)" strokeWidth={1.75} />
          <path d="M4 2 L2 6 L4 10 M8 2 L2 6 L8 10" fill="none" stroke="var(--border-color-strong)" strokeWidth={1.25} />
        </svg>
        declared join · crow&apos;s-foot cardinality
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="inline-block h-2.5 w-3.5 rounded-sm border border-dashed border-[var(--color-danger)]" />
        unmodeled / cold spot
      </span>
    </div>
  )
}

// ── Detail inset (mirrors NodeDetail — table / MV / measure / Space config) ──
function InsetSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div>
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">{title}</p>
      {children}
    </div>
  )
}

function Chip({ children }: { children: ReactNode }) {
  return (
    <span className="mb-1 mr-1 inline-block rounded border border-default px-1.5 font-mono text-[11px] text-secondary">
      {children}
    </span>
  )
}

function DetailInset({ model, selected }: { model: BlueprintModel; selected: string | null }) {
  if (!selected) return null
  const m = model
  const byId = nodeById(m)
  const measures = measureIndex(m)
  const ms = measures[selected]

  let head: ReactNode
  let warn: ReactNode = null
  let body: ReactNode

  if (ms) {
    const gl = ms.gov === "governed" ? "Governed" : ms.gov === "curated" ? "Curated" : "Ungoverned"
    head = (
      <>
        <span className="font-mono text-[13px] font-semibold text-primary">{ms.name}</span>
        <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">
          measure · {ms.parentKind === "mv" ? "metric view" : "space config"}
        </span>
        <span className="inline-flex items-center gap-1 rounded-full border border-default px-2 text-[11px] font-semibold">
          <span className="inline-block h-2 w-2 rounded-full" style={{ background: govColor(ms.gov) }} />
          {gl}
        </span>
      </>
    )
    if (ms.overlaps) {
      warn = (
        <div className="border-b border-default px-3 py-2 text-[11.5px] text-[var(--color-warning)]">
          <b>Name collision</b> — <span className="font-mono">{shortName(ms.overlaps)}</span> already exposes a measure named{" "}
          <span className="font-mono">{ms.name}</span>; two definitions, one name.
        </div>
      )
    }
    body = (
      <>
        {ms.expr && (
          <InsetSection title="Definition">
            <code className="block rounded bg-sunken px-2 py-1 font-mono text-[11px] text-secondary">{ms.expr}</code>
          </InsetSection>
        )}
        <InsetSection title="Lineage → source tables">
          {ms.src.length ? (
            ms.src.map((t) => (
              <div key={t} className="mb-0.5 flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                <span className="text-[var(--color-accent-light,var(--color-accent))]">└</span> {shortName(t)}
              </div>
            ))
          ) : (
            <p className="text-[11px] text-muted">no proven source tables</p>
          )}
          <p className="mt-1 text-[11px] text-muted">
            exposed by <b className="text-secondary">{shortName(ms.parent)}</b>
          </p>
        </InsetSection>
      </>
    )
  } else {
    const n = byId[selected]
    if (!n) return null
    if (n.kind === "table") {
      const t = n as BlueprintTable
      const usedBy = Object.keys(m.uses).filter((mv) => m.uses[mv].includes(n.id))
      const joins = m.joins.filter((j) => j.from === n.id || j.to === n.id)
      head = (
        <>
          <span className="font-mono text-[13px] font-semibold text-primary">{shortName(n.id)}</span>
          <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">{t.role ?? "TABLE"} table</span>
        </>
      )
      body = (
        <>
          <InsetSection title="Coverage">
            {t.coverage === 0 ? (
              <p className="text-[11px] text-[var(--color-danger)]">
                <b>cold spot</b> — no curated SQL touches it
              </p>
            ) : (
              <p className="text-[11px] text-muted">
                <b className="text-secondary">{t.coverage}</b> curated statement{t.coverage > 1 ? "s" : ""} touch this table
              </p>
            )}
          </InsetSection>
          {t.cols.length > 0 && (
            <InsetSection title="Participating columns">
              {t.cols.map((c) => (
                <Chip key={c}>{c}</Chip>
              ))}
            </InsetSection>
          )}
          <InsetSection title="Used by metric views">
            {usedBy.length ? usedBy.map((u) => <Chip key={u}>{shortName(u)}</Chip>) : <p className="text-[11px] text-muted">none — this table is unmodeled</p>}
          </InsetSection>
          {joins.length > 0 && (
            <InsetSection title={`Declared joins (${joins.length})`}>
              {joins.map((j, i) => (
                <div key={i} className="mb-1">
                  <div className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                    {shortName(j.from)} <span className="text-[var(--color-accent-light,var(--color-accent))]">→</span> {shortName(j.to)}
                    <Chip>{j.rel}</Chip>
                  </div>
                  <div className="font-mono text-[11px] text-muted">ON {onStr(j)}</div>
                </div>
              ))}
            </InsetSection>
          )}
        </>
      )
    } else if (n.kind === "mv") {
      const mv = n as BlueprintMv
      const srcSet = m.uses[n.id] ?? []
      const joins = m.joins.filter((j) => srcSet.includes(j.from) && srcSet.includes(j.to))
      const targets = new Set(joins.map((j) => j.to))
      const root = joins.map((j) => j.from).find((f) => !targets.has(f)) ?? srcSet[0]
      const gN = mv.measures.filter((mm) => mm.gov === "governed").length
      head = (
        <>
          <span className="font-mono text-[13px] font-semibold text-primary">{shortName(n.id)}</span>
          <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">metric view</span>
          <span className="inline-flex items-center gap-1 rounded-full border border-default px-2 text-[11px] font-semibold">
            <span className="inline-block h-2 w-2 rounded-full bg-[var(--color-success)]" />
            {gN} governed
          </span>
        </>
      )
      body = (
        <>
          {srcSet.length > 0 && (
            <InsetSection title="Join tree">
              <div className="font-mono text-[11px] text-secondary">
                {shortName(root)} <span className="text-muted">source</span>
              </div>
              {joins.map((j, i) => (
                <div key={i} className="pl-3">
                  <div className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                    <span className="text-[var(--color-accent-light,var(--color-accent))]">└</span> {shortName(j.to)}
                    <Chip>{j.rel}</Chip>
                  </div>
                  <div className="pl-4 font-mono text-[11px] text-muted">ON {onStr(j)}</div>
                </div>
              ))}
            </InsetSection>
          )}
          <InsetSection title={`Measures (${mv.measures.length})`}>
            {mv.measures.map((mm) => (
              <div key={mm.name} className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                <span className="inline-block h-2 w-2 rounded-full" style={{ background: govColor(mm.gov) }} />
                {mm.name}
              </div>
            ))}
          </InsetSection>
          {(mv.mv_filter || mv.materialization) && (
            <InsetSection title="Definition">
              <dl className="text-[11px] text-muted">
                {mv.mv_filter && (
                  <div>
                    <b className="text-secondary">filter</b> <span className="font-mono">{mv.mv_filter}</span>
                  </div>
                )}
                {mv.materialization && (
                  <div>
                    <b className="text-secondary">served</b> {mv.materialization}
                  </div>
                )}
              </dl>
            </InsetSection>
          )}
        </>
      )
    } else {
      const cfg = n as BlueprintMv
      head = (
        <>
          <span className="font-mono text-[13px] font-semibold text-primary">Space config</span>
          <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">not in any metric view</span>
        </>
      )
      body = (
        <>
          <InsetSection title={`Measures (${cfg.measures.length})`}>
            {cfg.measures.map((mm) => (
              <div key={mm.name} className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                <span className="inline-block h-2 w-2 rounded-full" style={{ background: govColor(mm.gov) }} />
                {mm.name}
              </div>
            ))}
          </InsetSection>
          <InsetSection title="Note">
            <p className="text-[11px] text-muted">
              Curated and ungoverned measures defined directly in the space config, outside any metric view. Click a
              measure to trace its lineage back to source tables.
            </p>
          </InsetSection>
        </>
      )
    }
  }

  return (
    <div className="overflow-hidden rounded-lg border border-default bg-elevated" data-inset>
      <div className="flex flex-wrap items-center gap-2 border-b border-default px-3 py-2">{head}</div>
      {warn}
      <div className="grid gap-3 p-3 sm:grid-cols-2">{body}</div>
    </div>
  )
}

// ── Insights inset — top 1-2 deal-breakers, click-to-focus (§7.5) ────────────
function InsightsInset({ model, onFocus }: { model: BlueprintModel; onFocus: (id: string | null) => void }) {
  const insights = rankInsights(model)
  return (
    <div className="rounded-lg border border-default bg-elevated p-3" data-insights>
      <div className="mb-2 flex items-center justify-between">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-muted">Insights · what to look at first</p>
        <span className="text-[10.5px] text-muted">Full best-practice checklist lives in the IQ Scan</span>
      </div>
      {insights.length === 0 ? (
        <p className="text-[12px] text-secondary">
          <span className="mr-1.5 inline-block h-2 w-2 rounded-full bg-[var(--color-success)] align-middle" />
          No deal-breakers — this model is clean. See the IQ Scan for the full checklist.
        </p>
      ) : (
        <ul className="space-y-1.5">
          {insights.map((ins, i) => (
            <li key={i}>
              <button
                type="button"
                onClick={() => onFocus(ins.focus)}
                className="flex w-full items-start gap-2 rounded-md px-2 py-1 text-left hover:bg-surface"
                data-insight={ins.severity}
              >
                <span
                  className="mt-1 inline-block h-2 w-2 shrink-0 rounded-full"
                  style={{ background: ins.severity === "fail" ? "var(--color-danger)" : "var(--color-warning)" }}
                />
                <span className="text-[12px] leading-tight">
                  <b className="text-primary">{ins.title}</b> <span className="text-secondary">— {ins.detail}</span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

// ── Join Advisor inset — validated-seed model (§7) ───────────────────────────
function JoinAdvisorInset({
  model,
  candidates,
  checked,
  onToggle,
  onSeed,
  seededCount,
}: {
  model: BlueprintModel
  candidates: JoinCandidate[]
  checked: Set<string>
  onToggle: (id: string, next: boolean) => void
  onSeed: () => void
  seededCount: number
}) {
  const declared = model.joins
  const pendingCount = candidates.filter((c) => checked.has(c.id)).length
  return (
    <div className="rounded-lg border border-default bg-elevated p-3" data-advisor>
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">Join advisor</p>
      <p className="mb-2 text-[11px] text-muted">
        Declared joins are locked — Auto-Optimize can refine one but never removes it. Checking a candidate does not
        write a declared join: it ghosts the relationship onto the canvas and hands it to Auto-Optimize as a{" "}
        <b className="text-secondary">validated seed</b>.
      </p>

      {declared.length > 0 && (
        <div className="mb-2">
          <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">Declared · locked</p>
          {declared.map((j, i) => (
            <div key={i} className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
              🔒 {shortName(j.from)} <span className="text-[var(--color-accent-light,var(--color-accent))]">→</span> {shortName(j.to)}
              <span className="rounded border border-default px-1 text-[10px]">{j.rel}</span>
            </div>
          ))}
        </div>
      )}

      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">Candidates</p>
      {candidates.length === 0 ? (
        <p className="text-[11.5px] text-muted">
          No candidate joins to suggest — the schema is fully connected, or no warehouse is available to probe for new
          relationships.
        </p>
      ) : (
        <ul className="space-y-1.5">
          {candidates.map((c) => {
            const v = verdict(c.probe)
            return (
              <li key={c.id} className="rounded-md border border-default px-2 py-1.5" data-candidate={c.id}>
                <label className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={checked.has(c.id)}
                    onChange={(e) => onToggle(c.id, e.target.checked)}
                    className="accent-[var(--color-accent)]"
                  />
                  <span className="font-mono text-[11.5px] text-secondary">
                    {shortName(c.from)}.{c.fromCol} <span className="text-[var(--color-accent-light,var(--color-accent))]">→</span>{" "}
                    {shortName(c.to)}.{c.toCol}
                  </span>
                  <span className="rounded border border-default px-1 text-[10px] text-muted">{c.match}</span>
                </label>
                <div className="mt-1 flex items-center gap-2 pl-6" data-verdict={v.level}>
                  <span className="h-1.5 w-24 overflow-hidden rounded-full bg-sunken">
                    <span className="block h-full rounded-full" style={{ width: `${v.pct ?? 8}%`, background: v.color }} />
                  </span>
                  <span className="text-[10.5px]" style={{ color: v.color }}>
                    {v.label}
                  </span>
                </div>
              </li>
            )
          })}
        </ul>
      )}

      <div className="mt-2 flex flex-wrap items-center justify-between gap-2 border-t border-default pt-2">
        <p className="text-[11px] text-secondary">
          <b className="text-primary">{pendingCount}</b> pending
          {seededCount > 0 && <span className="text-muted"> · {seededCount} seeded</span>} · proposed to Auto-Optimize →
          re-validated &amp; added there, never written as a locked declared join.
        </p>
        <button
          type="button"
          disabled={pendingCount === 0}
          onClick={onSeed}
          className="inline-flex items-center rounded-md bg-accent px-2.5 py-1 text-xs font-medium text-white disabled:opacity-40"
        >
          Seed to Auto-Optimize
        </button>
      </div>
      <p className="mt-1.5 text-[10.5px] text-[var(--color-warning)]">
        Auto-Optimize can add or refine a seeded join, but never auto-removes a declared one. Seeds stay reversible
        proposals here until the optimizer validates them.
      </p>
    </div>
  )
}

const ZOOM_OPTIONS: { id: BlueprintZoom; label: string }[] = [
  { id: "far", label: "Overview" },
  { id: "mid", label: "Standard" },
  { id: "near", label: "Columns" },
]
// Fact-center is the only layout (§5.12). The prior "Source-left" toggle was
// removed — a fact-anchored star/snowflake is the one honest reading of a
// semantic model, and the toggle only added a worse arrangement to choose.
const LAYOUT_MODE: BlueprintLayoutMode = "fact"

export function SemanticBlueprint({ nodes, edges, label, candidates = [], onSeed, initialSeededCount = 0 }: SemanticBlueprintProps) {
  const model = useMemo(
    () => fromSemanticGraph({ space_id: "", nodes, edges, proposals: [] }),
    [nodes, edges],
  )
  const [zoom, setZoom] = useState<BlueprintZoom>("mid")
  const [selected, setSelected] = useState<string | null>(null)
  const [checked, setChecked] = useState<Set<string>>(() => new Set())
  const [seededCount, setSeededCount] = useState(initialSeededCount)
  // Manual drag nudges (SVG units) keyed by node id; Reset view clears them.
  const [offsets, setOffsets] = useState<Record<string, { dx: number; dy: number }>>({})
  // Viewport pan/zoom, owned here so Reset view can restore it (§5.1).
  const [view, setView] = useState<CanvasView>(IDENTITY_VIEW)

  const moveNode = (id: string, dx: number, dy: number) =>
    setOffsets((prev) => ({ ...prev, [id]: { dx, dy } }))

  const zoomBy = (factor: number) =>
    setView((v) => ({ ...v, scale: Math.min(MAX_SCALE, Math.max(MIN_SCALE, v.scale * factor)) }))

  // Reset view returns to the deterministic layout: drop every manual nudge,
  // recenter/zoom to the initial framing, and clear the selection / focus so the
  // canvas is exactly as first rendered.
  const resetView = () => {
    setOffsets({})
    setSelected(null)
    setView(IDENTITY_VIEW)
  }

  // Persisted advice can arrive after mount (async read in the container) —
  // reflect the authoritative count when it changes.
  useEffect(() => {
    setSeededCount(initialSeededCount)
  }, [initialSeededCount])

  const overlay = useMemo(() => candidates.filter((c) => checked.has(c.id)), [candidates, checked])

  const focusInsight = (id: string | null) => {
    if (id) setSelected(id)
    else setZoom("far")
  }

  const toggleCandidate = (id: string, next: boolean) => {
    const cand = candidates.find((c) => c.id === id)
    // Confirm gate (§7): seeding a weak-containment join can silently produce
    // wrong results, and Auto-Optimize can refine but not remove it.
    if (next && cand && isWeak(cand.probe) && typeof window !== "undefined" && typeof window.confirm === "function") {
      const ok = window.confirm(
        "This candidate has weak/unverified containment. A join here can silently produce wrong results, and Auto-Optimize can refine but not remove it. Seed it anyway?",
      )
      if (!ok) return
    }
    setChecked((prev) => {
      const nextSet = new Set(prev)
      if (next) nextSet.add(id)
      else nextSet.delete(id)
      return nextSet
    })
  }

  const seed = () => {
    const seeds = seedPayload(candidates, checked)
    if (!seeds.length) return
    onSeed?.(seeds)
    setSeededCount((n) => n + seeds.length)
    setChecked(new Set())
  }

  return (
    <div className="space-y-3 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">
          {label ?? "Semantic model · blueprint"}
        </h3>
        <div className="flex flex-wrap items-center gap-2">
          <Seg options={ZOOM_OPTIONS} value={zoom} onChange={setZoom} />
          <span className="inline-flex items-center overflow-hidden rounded-md border border-default text-xs">
            <button
              type="button"
              aria-label="Zoom out"
              onClick={() => zoomBy(0.9)}
              className="px-2 py-1 text-secondary hover:bg-elevated"
            >
              −
            </button>
            <span className="min-w-[3rem] border-x border-default px-1 py-1 text-center tabular-nums text-muted">
              {Math.round(view.scale * 100)}%
            </span>
            <button
              type="button"
              aria-label="Zoom in"
              onClick={() => zoomBy(1.1)}
              className="px-2 py-1 text-secondary hover:bg-elevated"
            >
              +
            </button>
          </span>
          <button
            type="button"
            onClick={resetView}
            className="inline-flex items-center rounded-md border border-default bg-elevated px-2 py-1 text-xs text-secondary hover:bg-surface"
          >
            Reset view
          </button>
        </div>
      </div>
      <Headline model={model} />
      <BlueprintCanvas
        model={model}
        zoom={zoom}
        selected={selected}
        layoutMode={LAYOUT_MODE}
        onSelect={setSelected}
        overlay={overlay}
        offsets={offsets}
        onNodeMove={moveNode}
        view={view}
        onViewChange={setView}
      />
      <Legend />
      <p className="text-xs text-muted">
        Click any card or measure to trace it and open its detail below · scroll to zoom, drag the background to pan, drag
        a card to declutter — then Reset view to restore the framing · switch Overview / Standard / Columns to resolve
        detail · one line per relationship, crossings hop so they stay separable.
      </p>
      <DetailInset model={model} selected={selected} />
      <InsightsInset model={model} onFocus={focusInsight} />
      <JoinAdvisorInset
        model={model}
        candidates={candidates}
        checked={checked}
        onToggle={toggleCandidate}
        onSeed={seed}
        seededCount={seededCount}
      />
    </div>
  )
}

export default SemanticBlueprint
