/**
 * Semantic Blueprint (v4) — crow's-foot cardinality markers (§5.4).
 *
 * Orientation-aware endpoint glyphs derived from the resolved edge: the
 * crow's-foot always lands on the physical MANY end (the fact, after
 * fact-center re-ranking) and a single "1" bar on the ONE end, regardless of
 * which column each ended up in. A `1:1` relationship draws a bar on both ends
 * and no foot. Pure geometry of the edge's ports — no marker is ever guessed.
 */
import type { ResolvedEdge } from "./routing"

export interface CardinalityMarkers {
  /** Three-prong crow's-foot on the many/fact end (empty for 1:1). */
  crowfoot: string
  /** One-bar tick on the one end. */
  oneTick: string
  /** Second bar on the many end for a 1:1 relationship (empty otherwise). */
  manyTick: string
}

export function cardinalityMarkers(e: ResolvedEdge): CardinalityMarkers {
  const manyX = e.manyOnLeft ? e.sx : e.dx
  const manyY = e.manyOnLeft ? e.sy : e.dy
  const oneX = e.manyOnLeft ? e.dx : e.sx
  const oneY = e.manyOnLeft ? e.dy : e.sy
  // Point each glyph toward the rest of the line (its midpoint), not by a fixed
  // left/right assumption. A normal L→R gutter leg leaves the box outward; an
  // intra-rank bracket leaves the SAME side both ends — deriving the direction
  // from midX keeps the foot ON the line in both cases instead of stranding it
  // in empty space beside the card.
  const manyDir = e.midX >= manyX ? 1 : -1
  const oneDir = e.midX >= oneX ? 1 : -1
  const footApex = manyX + manyDir * 12
  const oneTickX = oneX + oneDir * 6
  const oneTick = `M ${oneTickX} ${oneY - 5} L ${oneTickX} ${oneY + 5}`

  if (e.rel === "1:1") {
    const manyTickX = manyX + manyDir * 6
    return { crowfoot: "", oneTick, manyTick: `M ${manyTickX} ${manyY - 5} L ${manyTickX} ${manyY + 5}` }
  }
  const crowfoot =
    `M ${footApex} ${manyY} L ${manyX} ${manyY - 6} ` +
    `M ${footApex} ${manyY} L ${manyX} ${manyY} ` +
    `M ${footApex} ${manyY} L ${manyX} ${manyY + 6}`
  return { crowfoot, oneTick, manyTick: "" }
}
