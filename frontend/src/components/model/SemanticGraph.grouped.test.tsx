/**
 * SemanticGraph — 12c Part 2 grouped-layout coverage (semantic-graph-v2-note.md).
 *
 * The v1 defect (§1): every measure was its own node in column 3, so a real
 * space stacked dozens of rows, drove the height, and meet-scaling shrank
 * everything past legibility (finding 4). The redesign groups measures into
 * their owning cards (§2), fits the content instead of zooming to 100% (§3),
 * focuses/searches and collapses above a DERIVED threshold (§4, §9), and anchors
 * edges at card ports (§5). These tests pin the pure layout functions and the
 * card render, per the §8 test plan; the pointer/measurement interactions ride
 * the same helpers the crash test already pins.
 *
 * Node env + renderToStaticMarkup — the repo's frontend test pattern. Effects
 * (ResizeObserver measurement, initial fit) do not run under static markup, so a
 * full-component render uses the SSR fallback threshold (12) deterministically:
 * a 30-table fixture opens collapsed, a 3-table one expanded.
 */
import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import type { SemanticGraphEdge, SemanticGraphNode } from "@/types"
import {
  SemanticGraph,
  buildCards,
  cardPorts,
  collapseThreshold,
  computeFit,
  distributeEdgePorts,
  focusSet,
  isDisplayableMeasureLabel,
  layoutCards,
  matchesSearch,
  relationshipGlyph,
  rollup,
} from "./SemanticGraph"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

// ── Fixture builders ─────────────────────────────────────────────────────────
// A star schema: N tables (1 fact in col 0, the rest dims in col 1), one MV that
// owns `mvMeasures` governed measures, and `standalone` ungoverned/curated
// concepts that belong to no MV. This is the shape §1 quantified at 3/10/30.
function starSchema(
  tables: number,
  opts: { mvMeasures?: number; standalone?: number } = {},
): { nodes: SemanticGraphNode[]; edges: SemanticGraphEdge[] } {
  const { mvMeasures = 3, standalone = 2 } = opts
  const nodes: SemanticGraphNode[] = []
  const edges: SemanticGraphEdge[] = []
  for (let i = 0; i < tables; i++) {
    const id = `cat.sch.t${i}`
    nodes.push({ id, kind: "table", label: `t${i}`, col: i === 0 ? 0 : 1, row: i })
    if (i > 0) edges.push({ from: id, to: "cat.sch.t0", kind: "join", on: `t${i}.fk = t0.pk`, relationship: "many-to-one", scd2: false })
  }
  nodes.push({ id: "cat.sch.mv", kind: "metric_view", label: "orders_metrics", col: 2, row: 0 })
  for (let i = 0; i < mvMeasures; i++) {
    const id = `measure:gov_${i}`
    nodes.push({ id, kind: "measure", label: `gov_measure_${i}`, col: 3, row: i, governance: "governed", origin: "attached MV" })
    edges.push({ from: id, to: "cat.sch.mv", kind: "membership" })
  }
  for (let i = 0; i < standalone; i++) {
    const gov = i % 2 === 0 ? "ungoverned" : "curated"
    nodes.push({ id: `measure:free_${i}`, kind: "measure", label: `free_measure_${i}`, col: 3, row: i, governance: gov, origin: "proposal evidence" })
  }
  return { nodes, edges }
}

describe("buildCards — grouping folds measures into owning cards (§2)", () => {
  it("column 3 carries no per-measure nodes; measures ride inside cards", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 3, standalone: 2 })
    const cards = buildCards(nodes, edges)
    // No card is a measure — the measure explosion is gone.
    expect(cards.map((c) => c.kind as string)).not.toContain("measure")
    // The only col-3 card is the synthetic concepts card.
    const col3 = cards.filter((c) => c.col === 3)
    expect(col3).toHaveLength(1)
    expect(col3[0].kind).toBe("concepts")
  })

  it("membership puts a measure's chip inside its MV card; the rest collect as concepts", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 3, standalone: 2 })
    const cards = buildCards(nodes, edges)
    const mv = cards.find((c) => c.kind === "metric_view")!
    expect(mv.measures.map((m) => m.label).sort()).toEqual(["gov_measure_0", "gov_measure_1", "gov_measure_2"])
    const concepts = cards.find((c) => c.kind === "concepts")!
    expect(concepts.measures).toHaveLength(2)
  })

  it("a space with no standalone concepts grows no concepts card", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 2, standalone: 0 })
    const cards = buildCards(nodes, edges)
    expect(cards.some((c) => c.kind === "concepts")).toBe(false)
  })
})

describe("rollup — the governance traffic light survives grouping (§2/§8)", () => {
  it("counts a card's measures by rung, present rungs only, ladder order", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 3, standalone: 2 })
    const cards = buildCards(nodes, edges)
    const mv = cards.find((c) => c.kind === "metric_view")!
    expect(rollup(mv)).toEqual([{ rung: "governed", count: 3 }])
    const concepts = cards.find((c) => c.kind === "concepts")!
    // standalone alternates ungoverned/curated: free_0 ungoverned, free_1 curated.
    expect(rollup(concepts)).toEqual([
      { rung: "curated", count: 1 },
      { rung: "ungoverned", count: 1 },
    ])
  })
})

describe("layoutCards — collapsed drops chip height, expanded grows with chips", () => {
  it("an expanded MV card is taller than the same card collapsed", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 4, standalone: 0 })
    const cards = buildCards(nodes, edges)
    const expanded = layoutCards(cards, false).placed.get("cat.sch.mv")!.box.h
    const collapsed = layoutCards(cards, true).placed.get("cat.sch.mv")!.box.h
    expect(expanded).toBeGreaterThan(collapsed)
  })

  it("places columns left-to-right and sizes width to the last column", () => {
    const { nodes, edges } = starSchema(3)
    const { placed, width } = layoutCards(buildCards(nodes, edges), false)
    const t0 = placed.get("cat.sch.t0")!.box
    const mv = placed.get("cat.sch.mv")!.box
    expect(mv.x).toBeGreaterThan(t0.x)
    expect(width).toBeGreaterThan(mv.x + mv.w)
  })
})

describe("cardPorts — edges anchor at edge-midpoints, never the center (§5)", () => {
  it("left/right ports sit on the vertical midline of the box", () => {
    const ports = cardPorts({ x: 100, y: 40, w: 180, h: 80 })
    expect(ports.left).toEqual({ x: 100, y: 80 })
    expect(ports.right).toEqual({ x: 280, y: 80 })
  })
})

describe("collapseThreshold — a derivation, not a magic number (§9)", () => {
  it("uses the 12 fallback when no viewport has been measured (SSR/first paint)", () => {
    expect(collapseThreshold(null)).toBe(12)
    expect(collapseThreshold(undefined)).toBe(12)
    expect(collapseThreshold(0)).toBe(12)
  })

  it("derives N_max = floor((H/floorScale - ROW_TOP)/ROW_GAP) from the measured height", () => {
    // 480 / 0.7 = 685.7; (685.7 - 44) / 58 = 11.06 → 11.
    expect(collapseThreshold(480)).toBe(11)
    // A taller container opens denser (more cards before collapse).
    expect(collapseThreshold(900)).toBeGreaterThan(collapseThreshold(480))
    // A shorter one collapses sooner.
    expect(collapseThreshold(300)).toBeLessThan(collapseThreshold(480))
  })
})

describe("computeFit — a real fit, never reset()'s 100% on a tall graph (§3)", () => {
  it("scales a tall graph DOWN to frame it (not identity, not zoom-in)", () => {
    const fit = computeFit(960, 2364, 734, 480)
    expect(fit.scale).toBeLessThan(1)
    expect(fit.scale).toBeGreaterThan(0)
    expect(fit).not.toEqual({ scale: 1, tx: 0, ty: 0 })
  })

  it("caps at 100% so a small graph is framed, never zoomed in", () => {
    const fit = computeFit(300, 200, 734, 480)
    expect(fit.scale).toBe(1)
  })

  it("returns identity for a degenerate (unmeasured) viewport", () => {
    expect(computeFit(960, 2364, 0, 0)).toEqual({ scale: 1, tx: 0, ty: 0 })
  })

  // 12f: framing a 30-table stack in a 520px canvas by height means ~20% scale —
  // every label illegible so that the whole graph "fits". That is the tiny-blob
  // defect arriving by a different road, so the fit prefers WIDTH and lets the
  // reader pan the vertical overflow.
  it("a tall graph fits to width and is not shrunk into illegibility", () => {
    const tall = computeFit(1124, 2436, 780, 520)
    const byHeight = (520 - 48) / 2436
    expect(tall.scale).toBeGreaterThan(byHeight * 2)
    expect(tall.scale).toBeCloseTo((780 - 48) / 1124, 5)
  })

  it("top-aligns a graph that overflows vertically, centers one that fits", () => {
    expect(computeFit(1124, 2436, 780, 520).ty).toBe(24)
    const short = computeFit(812, 300, 780, 520)
    expect(short.ty).toBeGreaterThan(24)
  })

  // The legibility floor must never widen a graph past its own width fit.
  it("the floor yields to the width fit on a very wide graph", () => {
    const wide = computeFit(4000, 200, 780, 520)
    expect(wide.scale).toBeCloseTo((780 - 48) / 4000, 5)
  })
})

describe("matchesSearch — the jump box filters by name (§4)", () => {
  it("is case-insensitive substring; a blank term matches everything", () => {
    expect(matchesSearch("orders_metrics", "ORDER")).toBe(true)
    expect(matchesSearch("customer", "ord")).toBe(false)
    expect(matchesSearch("anything", "  ")).toBe(true)
  })
})

describe("focusSet — selection dims all but the 1-hop neighborhood (§4)", () => {
  it("selecting a table keeps itself and its join neighbors", () => {
    const { nodes, edges } = starSchema(3)
    const cards = buildCards(nodes, edges)
    const keep = focusSet(cards, edges, "cat.sch.t0")!
    // t0 is the fact every dim joins to → its neighborhood is every table.
    expect(keep.has("cat.sch.t0")).toBe(true)
    expect(keep.has("cat.sch.t1")).toBe(true)
    expect(keep.has("cat.sch.t2")).toBe(true)
    // No edge touches the MV card from t0, so it is dimmed out of focus.
    expect(keep.has("cat.sch.mv")).toBe(false)
  })

  it("selecting a measure chip anchors focus on its owning card", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 2, standalone: 0 })
    const cards = buildCards(nodes, edges)
    const keep = focusSet(cards, edges, "measure:gov_0")!
    expect(keep.has("cat.sch.mv")).toBe(true)
  })

  it("returns null when nothing is selected (no dimming)", () => {
    const { nodes, edges } = starSchema(3)
    expect(focusSet(buildCards(nodes, edges), edges, null)).toBeNull()
  })
})

// ── 12d finding 1: data hygiene — internal tokens never render as labels ──────
describe("isDisplayableMeasureLabel — canonical_expr / sug_ ids are not labels", () => {
  it("rejects MV-D29 canonical placeholders and suggestion ids, keeps real names", () => {
    expect(isDisplayableMeasureLabel("count(?n)")).toBe(false)
    expect(isDisplayableMeasureLabel("sum(count(?n))")).toBe(false)
    expect(isDisplayableMeasureLabel("sug_f07l2262f800")).toBe(false)
    expect(isDisplayableMeasureLabel("   ")).toBe(false)
    expect(isDisplayableMeasureLabel(null)).toBe(false)
    expect(isDisplayableMeasureLabel("total_booking_value")).toBe(true)
    expect(isDisplayableMeasureLabel("Net Revenue")).toBe(true)
  })
})

describe("buildCards — internal-token measures are counted, not chipped (12d.1)", () => {
  it("splits displayable chips from unnamed, and the roll-up counts both", () => {
    const nodes: SemanticGraphNode[] = [
      { id: "cat.sch.mv", kind: "metric_view", label: "orders_metrics", col: 2, row: 0 },
      { id: "m_named", kind: "measure", label: "total_bookings", col: 3, row: 0, governance: "governed" },
      { id: "m_expr", kind: "measure", label: "sum(count(?n))", col: 3, row: 1, governance: "governed" },
      { id: "m_sug", kind: "measure", label: "sug_f07l2262f800", col: 3, row: 2, governance: "curated" },
    ]
    const edges: SemanticGraphEdge[] = [
      { from: "m_named", to: "cat.sch.mv", kind: "membership" },
      { from: "m_expr", to: "cat.sch.mv", kind: "membership" },
      { from: "m_sug", to: "cat.sch.mv", kind: "membership" },
    ]
    const mv = buildCards(nodes, edges).find((c) => c.kind === "metric_view")!
    expect(mv.measures.map((m) => m.label)).toEqual(["total_bookings"])
    expect(mv.unnamedMeasures).toHaveLength(2)
    // The governance roll-up still reflects the dropped measures (2 gov + 1 cur).
    expect(rollup(mv)).toEqual([
      { rung: "governed", count: 2 },
      { rung: "curated", count: 1 },
    ])
  })
})

describe("SemanticGraph render — no internal token ever reaches a rendered label (12d.1)", () => {
  it("shows '+N unnamed' and never the canonical_expr or sug_ id", () => {
    const nodes: SemanticGraphNode[] = [
      { id: "cat.sch.mv", kind: "metric_view", label: "orders_metrics", col: 2, row: 0 },
      { id: "m_named", kind: "measure", label: "total_bookings", col: 3, row: 0, governance: "governed" },
      { id: "m_expr", kind: "measure", label: "sum(count(?n))", col: 3, row: 1, governance: "governed" },
      { id: "m_sug", kind: "measure", label: "sug_f07l2262f800", col: 3, row: 2, governance: "curated" },
    ]
    const edges: SemanticGraphEdge[] = [
      { from: "m_named", to: "cat.sch.mv", kind: "membership" },
      { from: "m_expr", to: "cat.sch.mv", kind: "membership" },
      { from: "m_sug", to: "cat.sch.mv", kind: "membership" },
    ]
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("total_bookings")
    expect(html).toContain("+2 unnamed")
    // The validator-style assertion: no ?n/?s placeholder, no sug_ id, anywhere.
    expect(html).not.toContain("?n")
    expect(html).not.toContain("sug_")
  })
})

// ── 12d finding 3: relationship glyphs, full text on hover ────────────────────
describe("relationshipGlyph — compact at rest, never truncated ambiguity", () => {
  it("maps the cardinality vocabulary, format-tolerant", () => {
    expect(relationshipGlyph("many-to-one")).toBe("N:1")
    expect(relationshipGlyph("MANY_TO_ONE")).toBe("N:1")
    expect(relationshipGlyph("one to one")).toBe("1:1")
    expect(relationshipGlyph("one-to-many")).toBe("1:N")
    expect(relationshipGlyph("many-to-many")).toBe("N:N")
  })

  it("returns null for unknown/absent relationships (no wrong glyph)", () => {
    expect(relationshipGlyph("weird")).toBeNull()
    expect(relationshipGlyph(null)).toBeNull()
    expect(relationshipGlyph(undefined)).toBeNull()
  })

  it("the rendered rest label uses the glyph, never the truncated word", () => {
    const { nodes, edges } = starSchema(3)
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // The at-rest join label is the glyph; the full 'many-to-one' text (which
    // truncated to 'many-to-o…') does not appear at rest.
    expect(html).toContain("N:1")
    expect(html).not.toContain("many-to-o")
  })
})

// ── 12d finding 4: fan-out port distribution at the fact column ────────────────
describe("distributeEdgePorts — a fan-in spreads across the fact card's side", () => {
  it("gives each edge in a fan-in a distinct attachment y (no overlap)", () => {
    const factBox = { x: 32, y: 44, w: 176, h: 44 }
    const dim = (row: number) => ({ x: 236, y: 44 + row * 120, w: 188, h: 60 })
    // Three dims → one fact: all three land on the fact's LEFT side.
    const items = [
      { index: 0, fromCardId: "d0", toCardId: "fact", fromBox: dim(0), toBox: factBox },
      { index: 1, fromCardId: "d1", toCardId: "fact", fromBox: dim(1), toBox: factBox },
      { index: 2, fromCardId: "d2", toCardId: "fact", fromBox: dim(2), toBox: factBox },
    ]
    const ports = distributeEdgePorts(items)
    const factYs = [0, 1, 2].map((i) => ports.get(i)!.dst.y)
    // Distinct attachment points, all on the fact card's facing (right) side —
    // the fact sits in col 0 to the LEFT of the dims, so its right edge faces
    // them (x === factBox.x + factBox.w).
    expect(new Set(factYs).size).toBe(3)
    for (const i of [0, 1, 2]) expect(ports.get(i)!.dst.x).toBe(factBox.x + factBox.w)
  })

  it("a lone edge lands on the midpoint — identical to cardPorts (backward compat)", () => {
    const fromBox = { x: 32, y: 44, w: 176, h: 44 }
    const toBox = { x: 236, y: 44, w: 188, h: 60 }
    const ports = distributeEdgePorts([
      { index: 0, fromCardId: "a", toCardId: "b", fromBox, toBox },
    ])
    expect(ports.get(0)!.src).toEqual(cardPorts(fromBox).right)
    expect(ports.get(0)!.dst).toEqual(cardPorts(toBox).left)
  })
})

// ── Render-level: the collapsed governance story (reviewer note, §8) ──────────
describe("SemanticGraph render — 3/10/30-table fixtures (§8)", () => {
  it("3 tables: opens expanded, chips visible, roll-up present", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 3, standalone: 2 })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("gov_measure_0")
    expect(html).toContain("governed")
  })

  it("30 tables: opens COLLAPSED — governance roll-up counts present, chip detail absent", () => {
    const { nodes, edges } = starSchema(30, { mvMeasures: 4, standalone: 3 })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // The governance story survives collapse: the card-level roll-up counts show.
    expect(html).toContain("governed")
    expect(html).toContain("collapsed")
    // Detail is absent: no individual measure chip label is rendered.
    expect(html).not.toContain("gov_measure_0")
    expect(html).not.toContain("free_measure_0")
  })

  it("10 tables: below the fallback threshold, still expanded (chips visible)", () => {
    const { nodes, edges } = starSchema(10, { mvMeasures: 3, standalone: 0 })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("gov_measure_0")
  })
})
