/**
 * Ontology Map v2 (MV-D75) — lightweight overview minimap. Pure inline SVG (NO new
 * dependency, MV-D45): the map feeds live node positions + the current viewport rect and
 * this scatters them into a small box. Presentational + deterministic for a given input, so
 * it stays byte-stable across re-renders (MV-D72). Degrades to a muted placeholder when the
 * map hasn't laid out yet (MV-D43).
 */
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

const W = 168
const H = 112
const PAD = 6

export function GraphMinimap({
  points,
  viewport,
}: {
  points: MiniPoint[]
  viewport?: MiniViewport | null
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

  // Bounds cover every plotted node and the viewport so the whole scene fits the box.
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
  if (viewport) {
    extend(viewport.x1, viewport.y1)
    extend(viewport.x2, viewport.y2)
  }
  const spanX = Math.max(maxX - minX, 1)
  const spanY = Math.max(maxY - minY, 1)
  const scale = Math.min((W - PAD * 2) / spanX, (H - PAD * 2) / spanY)
  const sx = (x: number) => PAD + (x - minX) * scale
  const sy = (y: number) => PAD + (y - minY) * scale

  return (
    <svg
      width={W}
      height={H}
      className="rounded-md border border-default bg-elevated/60"
      role="img"
      aria-label="Map overview"
    >
      {points.map((p, i) => (
        <circle key={i} cx={sx(p.x)} cy={sy(p.y)} r={1.6} fill={p.color} fillOpacity={0.85} />
      ))}
      {viewport && (
        <rect
          x={sx(viewport.x1)}
          y={sy(viewport.y1)}
          width={Math.max((viewport.x2 - viewport.x1) * scale, 2)}
          height={Math.max((viewport.y2 - viewport.y1) * scale, 2)}
          fill="none"
          stroke="#22D3EE"
          strokeWidth={1}
          rx={2}
        />
      )}
    </svg>
  )
}
