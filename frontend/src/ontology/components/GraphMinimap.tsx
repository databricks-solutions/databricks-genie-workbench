/* eslint-disable react-refresh/only-export-components */
/**
 * Ontology Map v2 (MV-D75) — lightweight overview minimap. Pure inline SVG (NO new
 * dependency, MV-D45): the map feeds live node positions + the current viewport rect and
 * this scatters them into a small box. Presentational + deterministic for a given input, so
 * it stays byte-stable across re-renders (MV-D72). Degrades to a muted placeholder when the
 * map hasn't laid out yet (MV-D43).
 *
 * MV-D87 (Lane P2, §5/R26) makes it NAVIGABLE: the "you-are-here" viewport box is drawn from
 * the live camera rect, and click/drag inside the box recenters the main view. The projection
 * math is a pair of pure functions ({@link computeMiniGeom}/{@link miniToContent}) so nav is
 * unit-tested without a DOM; the component stays presentational (it only reports a target
 * content point via `onNavigate` — the map owns the camera).
 */
import { type PointerEvent as ReactPointerEvent } from "react"

export interface MiniPoint {
  x: number
  y: number
  color: string
}

export interface MiniViewport {
  x1: number
  y1: number
  x2: number
  y2: number
}

/** Bounding box of a domain/sub-domain container, so the minimap shows structure. */
export interface MiniRect {
  x1: number
  y1: number
  x2: number
  y2: number
  color: string
}

const W = 168
const H = 112
const PAD = 6

/** Deterministic content→minimap projection (pure). Unions points, rects, and the viewport. */
export interface MiniGeom {
  minX: number
  minY: number
  scale: number
}

/**
 * Compute the shared projection covering every plotted node, container rect, AND the current
 * viewport rect, so the whole scene (and the you-are-here box) fits the box. Pure — the same
 * inputs always yield the same geometry (used for both drawing and the click inverse).
 */
export function computeMiniGeom(
  points: MiniPoint[],
  rects: MiniRect[] = [],
  viewport?: MiniViewport | null,
): MiniGeom {
  let minX = Infinity
  let minY = Infinity
  let maxX = -Infinity
  let maxY = -Infinity
  const extend = (x: number, y: number) => {
    if (x < minX) minX = x
    if (y < minY) minY = y
    if (x > maxX) maxX = x
    if (y > maxY) maxY = y
  }
  for (const p of points) extend(p.x, p.y)
  for (const r of rects) {
    extend(r.x1, r.y1)
    extend(r.x2, r.y2)
  }
  if (viewport) {
    extend(viewport.x1, viewport.y1)
    extend(viewport.x2, viewport.y2)
  }
  if (!Number.isFinite(minX)) return { minX: 0, minY: 0, scale: 1 }
  const spanX = Math.max(maxX - minX, 1)
  const spanY = Math.max(maxY - minY, 1)
  const scale = Math.min((W - PAD * 2) / spanX, (H - PAD * 2) / spanY)
  return { minX, minY, scale }
}

/** Inverse projection: minimap-local px → CONTENT-space point (pure). */
export function miniToContent(px: number, py: number, geom: MiniGeom): { x: number; y: number } {
  return { x: geom.minX + (px - PAD) / geom.scale, y: geom.minY + (py - PAD) / geom.scale }
}

export function GraphMinimap({
  points,
  rects = [],
  viewport,
  onNavigate,
}: {
  points: MiniPoint[]
  rects?: MiniRect[]
  viewport?: MiniViewport | null
  /** Recenter the main view on this CONTENT-space point (click/drag pan). Presentational otherwise. */
  onNavigate?: (x: number, y: number) => void
}) {
  if (points.length === 0) {
    return (
      <div
        className="flex items-center justify-center rounded-md border border-default bg-elevated/60 text-[10px] text-muted"
        style={{ width: W, height: H }}
      >
        Overview
      </div>
    )
  }

  const geom = computeMiniGeom(points, rects, viewport)
  const sx = (x: number) => PAD + (x - geom.minX) * geom.scale
  const sy = (y: number) => PAD + (y - geom.minY) * geom.scale

  const navFromEvent = (e: ReactPointerEvent<SVGSVGElement>) => {
    if (!onNavigate) return
    const rect = e.currentTarget.getBoundingClientRect()
    const p = miniToContent(e.clientX - rect.left, e.clientY - rect.top, geom)
    onNavigate(p.x, p.y)
  }
  const onPointerDown = (e: ReactPointerEvent<SVGSVGElement>) => {
    if (!onNavigate) return
    e.currentTarget.setPointerCapture?.(e.pointerId)
    navFromEvent(e)
  }
  const onPointerMove = (e: ReactPointerEvent<SVGSVGElement>) => {
    // Drag-to-pan: only while a button is held (buttons bitmask ≠ 0).
    if (!onNavigate || e.buttons === 0) return
    navFromEvent(e)
  }

  return (
    <svg
      width={W}
      height={H}
      className={`rounded-md border border-default bg-elevated/60${onNavigate ? " cursor-pointer" : ""}`}
      role={onNavigate ? "slider" : "img"}
      aria-label={onNavigate ? "Map overview — click or drag to pan" : "Map overview"}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
    >
      {rects.map((r, i) => (
        <rect
          key={`r${i}`}
          x={sx(r.x1)}
          y={sy(r.y1)}
          width={Math.max((r.x2 - r.x1) * geom.scale, 2)}
          height={Math.max((r.y2 - r.y1) * geom.scale, 2)}
          fill={r.color}
          fillOpacity={0.08}
          stroke={r.color}
          strokeOpacity={0.5}
          strokeWidth={0.75}
          rx={1.5}
        />
      ))}
      {points.map((p, i) => (
        <circle key={i} cx={sx(p.x)} cy={sy(p.y)} r={1.6} fill={p.color} fillOpacity={0.85} />
      ))}
      {viewport && (
        <rect
          x={sx(viewport.x1)}
          y={sy(viewport.y1)}
          width={Math.max((viewport.x2 - viewport.x1) * geom.scale, 2)}
          height={Math.max((viewport.y2 - viewport.y1) * geom.scale, 2)}
          fill="#22D3EE"
          fillOpacity={0.1}
          stroke="#22D3EE"
          strokeWidth={1}
          rx={2}
          pointerEvents="none"
        />
      )}
    </svg>
  )
}
