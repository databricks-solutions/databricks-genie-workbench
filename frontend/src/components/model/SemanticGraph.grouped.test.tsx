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
  collapsePairJoins,
  collapseThreshold,
  computeFit,
  distributeEdgePorts,
  edgeGroupSizes,
  edgeBundleAnchors,
  BUNDLE_MIN,
  focusSet,
  isDisplayableMeasureLabel,
  layoutCards,
  matchesSearch,
  relationshipGlyph,
  rollup,
  type RenderedEdge,
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
  it("no measure becomes its own card; the Space-config box gets its own right column", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 3, standalone: 2 })
    const cards = buildCards(nodes, edges)
    // No card is a measure — the measure explosion is gone.
    expect(cards.map((c) => c.kind as string)).not.toContain("measure")
    // Round-5: the concepts (Space-config) box lives in its OWN column to the
    // RIGHT of the metric views (col 3) — the dedicated "Space config · measures"
    // column the reviewer asked for, not buried under the MV boxes in col 2.
    const concepts = cards.filter((c) => c.kind === "concepts")
    expect(concepts).toHaveLength(1)
    expect(concepts[0].col).toBe(3)
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

// Round-5: loose measures are a Space-config BOX in their OWN column to the right
// of the metric views — the dedicated "Space config · measures" column.
describe("loose measures render in their own Space-config column (round-5)", () => {
  it("draws the Space-config box with its loose measures, listed as chips", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 3, standalone: 2 })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("Space config")
    // The subtitle is terse now the column header carries "measures".
    expect(html).toContain("not in any MV")
    expect(html).toContain("free_measure_0")
    // The dedicated column header names it.
    expect(html).toContain("Space config · measures")
  })

  it("collapses like any card once the model is large (chips drop, box remains)", () => {
    const { nodes, edges } = starSchema(30, { mvMeasures: 4, standalone: 3 })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("Space config")
    expect(html).toContain("collapsed")
    expect(html).not.toContain("free_measure_0")
  })

  it("counts unnamed loose measures without printing their internal tokens", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 1, standalone: 0 })
    nodes.push({ id: "measure:u1", kind: "measure", label: "sug_f07l2262f800", col: 3, row: 0, governance: "ungoverned" })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("+1 unnamed")
    expect(html).not.toContain("sug_f07l2262f800")
  })

  it("names the metric view a loose measure's name collides with", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 1, standalone: 0 })
    nodes.push({
      id: "measure:dupe",
      kind: "measure",
      label: "avg_daily_rate",
      col: 3,
      row: 0,
      governance: "ungoverned",
      // The server sends the governing metric view's identifier; the chip shows
      // its leaf so the warning fits beside the measure.
      overlaps: "finance.sales.orders_metrics",
    })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("also in orders_metrics")
    expect(html).not.toContain("finance.sales.orders_metrics")
  })
})

// ── 12f round 4 — selection lights edges; a measure wraps its owning MV (v3 §4) ─
describe("selection interactions match the reference (v3 §4)", () => {
  it("a join edge switches to the accent arrowhead only when an endpoint is selected", () => {
    const { nodes, edges } = starSchema(3) // t0 is the fact every dim joins to
    const idle = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // At rest the join terminates in the neutral arrowhead — the canvas is calm.
    expect(idle).not.toContain("url(#mv-arrow-on)")
    const selected = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="cat.sch.t0" />)
    // Clicking the fact lights its join edges: accent arrowhead now in use.
    expect(selected).toContain("url(#mv-arrow-on)")
  })

  it("selecting a MEASURE wraps the tables of its owning MV (not only an MV click)", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 2, standalone: 0 })
    // The MV must declare the tables it uses for a boundary to form.
    edges.push({ from: "cat.sch.mv", to: "cat.sch.t0", kind: "uses" })
    edges.push({ from: "cat.sch.mv", to: "cat.sch.t1", kind: "uses" })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="measure:gov_0" />)
    expect(html).toContain("Tables used by orders_metrics")
  })
})

// ── Round-5 — proven roles, calmer edges, disambiguated legend, gated Impact ──
describe("round-5 clarity fixes", () => {
  it("captions a table FACT/DIM only when the role is PROVEN, else a neutral TABLE", () => {
    const nodes: SemanticGraphNode[] = [
      { id: "c.s.fact", kind: "table", label: "orders", col: 0, row: 0, role: "fact" },
      { id: "c.s.dim", kind: "table", label: "customer", col: 1, row: 0, role: "dim" },
      // A table with no proven role must NOT be guessed from its column.
      { id: "c.s.neutral", kind: "table", label: "dim_property", col: 0, row: 1 },
    ]
    const html = render(<SemanticGraph nodes={nodes} edges={[]} />)
    expect(html).toContain(">FACT<")
    expect(html).toContain(">DIM<")
    // The unproven table reads TABLE, not a mislabelled FACT.
    expect(html).toContain(">TABLE<")
  })

  it("hides definition (uses) edges at rest and draws them only for the selected MV", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 1, standalone: 0 })
    edges.push({ from: "cat.sch.mv", to: "cat.sch.t0", kind: "uses" })
    const idle = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // At rest the canvas shows the join skeleton only — no dotted MV→table arrows.
    expect(idle).not.toContain("url(#mv-uses-arrow)")
    const selected = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="cat.sch.mv" />)
    expect(selected).toContain("url(#mv-uses-arrow)")
  })

  it("the legend disambiguates the solid join from the dotted MV-definition line", () => {
    const { nodes, edges } = starSchema(3)
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("declared join (N:1)")
    expect(html).toContain("MV definition (on select)")
  })

  it("disables Impact until a table is selected, then enables it", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 1, standalone: 0 })
    const idle = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // No selection → Impact is disabled and says why.
    expect(idle).toContain("Select a source table to see its downstream impact")
    expect(idle).toContain("disabled")
    const onTable = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="cat.sch.t0" />)
    expect(onTable).toContain("Downstream blast radius of the selected table")
  })

  it("dims non-neighbor join edges once something is selected (focus+context, round-6)", () => {
    // A 4-table star: selecting one dim leaf leaves two joins that don't touch it.
    const { nodes, edges } = starSchema(4, { mvMeasures: 1, standalone: 0 })
    const countDimmed = (h: string) => h.split('opacity="0.06"').length - 1
    const idle = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // Selecting t1 lights the t1↔t0 join and dims the t2↔t0 / t3↔t0 joins to the
    // near-invisible 0.06 the reference uses — so a click removes clutter.
    const selected = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="cat.sch.t1" />)
    expect(countDimmed(selected)).toBeGreaterThan(countDimmed(idle))
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

  // Round-7: a loose (Space-config) measure belongs to no MV, so it used to have
  // no edge and lit nothing. A `derives` edge to the table its expression reads
  // gives it lineage — selecting it lights that table.
  it("lights a loose measure's `derives` source table on select (round-7)", () => {
    const nodes: SemanticGraphNode[] = [
      { id: "cat.sch.fact", kind: "table", label: "fact", col: 0, row: 0 },
      { id: "cat.sch.other", kind: "table", label: "other", col: 0, row: 1 },
      { id: "measure:Loose", kind: "measure", label: "Loose", col: 3, row: 0, governance: "curated" },
    ]
    const edges: SemanticGraphEdge[] = [
      { from: "measure:Loose", to: "cat.sch.fact", kind: "derives" },
    ]
    const cards = buildCards(nodes, edges)
    const keep = focusSet(cards, edges, "measure:Loose")!
    expect(keep.has("cat.sch.fact")).toBe(true) // the table it derives from lights
    expect(keep.has("cat.sch.other")).toBe(false) // an unrelated table stays dimmed
  })
})

// ── Round-7: one relationship = one arrow (collapsePairJoins) ─────────────────
describe("collapsePairJoins — reciprocal joins render as a single arrow", () => {
  const box = (x: number) => ({ x, y: 0, w: 100, h: 40 })
  // Two join edges between the same pair, declared in opposite directions (the
  // dim_property ↔ dim_host case): a reader sees one relationship.
  const reciprocal: RenderedEdge[] = [
    { index: 0, fromCardId: "dim_host", toCardId: "dim_property", fromBox: box(400), toBox: box(0), kind: "join" },
    { index: 1, fromCardId: "dim_property", toCardId: "dim_host", fromBox: box(0), toBox: box(400), kind: "join" },
  ]

  it("collapses a reciprocal pair to one join", () => {
    const kept = collapsePairJoins(reciprocal)
    expect(kept).toHaveLength(1)
  })

  it("keeps the left→right edge so the arrowhead points at the dim side", () => {
    const kept = collapsePairJoins(reciprocal)
    // The survivor runs source(left) → dim(right): fromBox.x <= toBox.x.
    expect(kept[0].fromBox.x).toBeLessThanOrEqual(kept[0].toBox.x)
    expect(kept[0].index).toBe(1)
  })

  it("leaves distinct pairs and non-join edges alone", () => {
    const items: RenderedEdge[] = [
      { index: 0, fromCardId: "a", toCardId: "b", fromBox: box(0), toBox: box(100), kind: "join" },
      { index: 1, fromCardId: "a", toCardId: "c", fromBox: box(0), toBox: box(100), kind: "join" },
      { index: 2, fromCardId: "mv", toCardId: "a", fromBox: box(200), toBox: box(0), kind: "uses" },
    ]
    expect(collapsePairJoins(items)).toHaveLength(3)
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

  it("shows a join predicate only on demand — nothing floats at rest (round-6)", () => {
    const { nodes, edges } = starSchema(3)
    const idle = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // Round-6 labels-on-demand: at rest a join carries NO floating label plate,
    // so neither its predicate nor the truncated relationship word appears. (The
    // legend still names the glyph — that's vocabulary, not an edge label.)
    expect(idle).not.toContain("ON t1.fk")
    expect(idle).not.toContain("many-to-o")
    // Selecting an endpoint reveals that edge's full predicate.
    const selected = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="cat.sch.t0" />)
    expect(selected).toContain("ON t")
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

// ── 12f: the rung letter earns its place only when rungs differ ───────────────
describe("measure chips — the non-color rung tag is conditional", () => {
  it("a single-rung metric view drops the repeated letter (the pill already says it)", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 4, standalone: 0 })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // The card's own pill carries the rung once…
    expect(html).toContain("4 measures · governed")
    // …so no chip repeats it as a bare "G". The rung stays in each chip's
    // accessible label, which is where a screen reader reads it.
    expect(html).not.toContain('font-weight="700">G</text>')
    expect(html).toContain("gov_measure_0 — Governed")
  })

  it("a mixed-rung card keeps the letter, since color alone would carry it", () => {
    const { nodes, edges } = starSchema(3, { mvMeasures: 1, standalone: 0 })
    // Same card, two rungs: the membership edge puts both inside the MV.
    nodes.push({
      id: "measure:curated_in_mv", kind: "measure", label: "curated_twin",
      col: 3, row: 1, governance: "curated",
    })
    edges.push({ from: "measure:curated_in_mv", to: "cat.sch.mv", kind: "membership" })
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain('font-weight="700">G</text>')
    expect(html).toContain('font-weight="700">C</text>')
  })
})

// ── 12f: a large fan is bundled, so its labels stop forming a band ────────────
describe("edge bundling — a big fan trades 29 plates for one count", () => {
  const factBox = { x: 32, y: 44, w: 176, h: 56 }
  const fan = (n: number) =>
    Array.from({ length: n }, (_, i) => ({
      index: i,
      fromCardId: `d${i}`,
      toCardId: "fact",
      fromBox: { x: 236, y: 44 + i * 120, w: 188, h: 60 },
      toBox: factBox,
      kind: "join" as const,
    }))

  it("small fans stay unbundled — every edge keeps its own label", () => {
    const sizes = edgeGroupSizes(fan(BUNDLE_MIN - 1))
    for (const size of sizes.values()) expect(size).toBeLessThan(BUNDLE_MIN)
    expect(edgeBundleAnchors(fan(BUNDLE_MIN - 1))).toEqual([])
  })

  it("a fan at the threshold bundles, and reports one anchor with the full count", () => {
    const items = fan(BUNDLE_MIN)
    for (const size of edgeGroupSizes(items).values()) expect(size).toBe(BUNDLE_MIN)
    const anchors = edgeBundleAnchors(items)
    expect(anchors).toHaveLength(1)
    // The chip sits under the hub card, where there is room for it — the gutter
    // the trunk runs through is narrower than the label.
    expect(anchors[0]).toMatchObject({ x: factBox.x, count: BUNDLE_MIN })
    expect(anchors[0].y).toBeGreaterThan(factBox.y + factBox.h)
  })

  it("counts joins only — a `uses` edge on the same side is not a declared join", () => {
    const items = [
      ...fan(BUNDLE_MIN),
      // An MV→table edge lands on the SAME side of the hub card.
      { index: 99, fromCardId: "mv", toCardId: "fact", fromBox: { x: 700, y: 44, w: 200, h: 58 }, toBox: factBox, kind: "uses" as const },
    ]
    expect(edgeBundleAnchors(items)[0].count).toBe(BUNDLE_MIN)
    // …and it is never itself bundled (it carries no label to suppress).
    expect(edgeGroupSizes(items).get(99)).toBeUndefined()
  })

  it("29 edges produce ONE summary count, not 29 per-edge labels", () => {
    const { nodes, edges } = starSchema(30)
    const html = render(<SemanticGraph nodes={nodes} edges={edges} />)
    expect(html).toContain("29 declared joins")
    // Round-6: no edge paints a label at rest (bundled or not), so the only
    // surviving "N:1" on the canvas is the legend's, never an edge's.
    expect(html.split("N:1").length - 1).toBe(1)
  })

  it("a small fan is below the threshold — no summary chip, labels on demand", () => {
    const { nodes, edges } = starSchema(3)
    const idle = render(<SemanticGraph nodes={nodes} edges={edges} />)
    // Not bundled → no "N declared joins" summary chip…
    expect(idle).not.toContain("declared joins")
    // …and round-6 gives it no at-rest predicate either.
    expect(idle).not.toContain("ON t1.fk")
    // Selecting an endpoint reveals the predicate on demand.
    const selected = render(<SemanticGraph nodes={nodes} edges={edges} selectedId="cat.sch.t0" />)
    expect(selected).toContain("ON t")
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
