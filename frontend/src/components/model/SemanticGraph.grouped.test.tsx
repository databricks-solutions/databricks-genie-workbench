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
  focusSet,
  layoutCards,
  matchesSearch,
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
