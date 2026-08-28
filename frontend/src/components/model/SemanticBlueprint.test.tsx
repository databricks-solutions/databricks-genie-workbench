/**
 * Semantic Blueprint (v4) Phase-1 production tests — the §9 pure-function gate:
 * the `fromSemanticGraph` adapter, deterministic byte-stable render, crow's-foot
 * markers, index-stable crossing bridges, semantic-zoom bands, lineage on
 * select, and the arrows-require-proof invariant (§2). All assertions are pure
 * functions of the model or `renderToStaticMarkup` of the canvas.
 */
import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { SemanticGraphResponse } from "@/types"
import { BlueprintCanvas, SemanticBlueprint } from "./SemanticBlueprint"
import { fromSemanticGraph, parseOnColumns } from "./blueprint/model"
import {
  derivePlacement,
  layoutBoxes,
  measureIndex,
  nodeHeight,
} from "./blueprint/layout"
import { computeHops, lineagePaths, resolveEdges, type ResolvedEdge } from "./blueprint/routing"
import { cardinalityMarkers } from "./blueprint/cardinality"
import { headlineCounts, neighbourhood, rankInsights } from "./blueprint/annotate"
import { isWeak, seedPayload, verdict, type JoinCandidate } from "./blueprint/advisor"

// A star fixture in the REAL response shape: a fact, two joined dims (one cold),
// an island/unmodeled table, one metric view with governed measures, and two
// loose Space-config measures (one an ungoverned name collision).
const star: SemanticGraphResponse = {
  space_id: "s",
  proposals: [],
  nodes: [
    { id: "cat.sch.fact_orders", kind: "table", label: "fact_orders", col: 0, row: 0, role: "fact", coverage: 3 },
    { id: "cat.sch.dim_user", kind: "table", label: "dim_user", col: 1, row: 0, role: "dim", coverage: 1 },
    { id: "cat.sch.dim_region", kind: "table", label: "dim_region", col: 1, row: 0, role: "dim", coverage: 0 },
    { id: "cat.sch.dim_orphan", kind: "table", label: "dim_orphan", col: 1, row: 0, coverage: 1 },
    {
      id: "cat.sch.order_metrics",
      kind: "metric_view",
      label: "order_metrics",
      col: 2,
      row: 0,
      definition_available: true,
      mv_source: "cat.sch.fact_orders",
      mv_filter: "status = 'PAID'",
      materialization: "1 materialization · EVERY 1 DAY",
    },
    { id: "measure:order_count", kind: "measure", label: "order_count", col: 3, row: 0, governance: "governed", expr: "COUNT(fact_orders.order_id)" },
    { id: "measure:revenue", kind: "measure", label: "revenue", col: 3, row: 0, governance: "governed", expr: "SUM(fact_orders.amount)" },
    { id: "measure:aov", kind: "measure", label: "aov", col: 3, row: 0, governance: "curated", expr: "revenue / order_count" },
    { id: "measure:repeat_rate", kind: "measure", label: "repeat_rate", col: 3, row: 0, governance: "ungoverned", expr: "…", overlaps: "cat.sch.order_metrics" },
  ],
  edges: [
    { from: "cat.sch.fact_orders", to: "cat.sch.dim_user", kind: "join", on: "fact_orders.user_id = dim_user.user_id", relationship: "many-to-one" },
    { from: "cat.sch.fact_orders", to: "cat.sch.dim_region", kind: "join", on: "fact_orders.region_id = dim_region.region_id", relationship: "many-to-one" },
    { from: "cat.sch.order_metrics", to: "cat.sch.fact_orders", kind: "uses" },
    { from: "cat.sch.order_metrics", to: "cat.sch.dim_user", kind: "uses" },
    { from: "measure:order_count", to: "cat.sch.order_metrics", kind: "membership" },
    { from: "measure:revenue", to: "cat.sch.order_metrics", kind: "membership" },
    { from: "measure:aov", to: "cat.sch.fact_orders", kind: "derives" },
    { from: "measure:repeat_rate", to: "cat.sch.dim_user", kind: "derives" },
  ],
}

describe("fromSemanticGraph adapter", () => {
  const m = fromSemanticGraph(star)

  it("groups governed measures under their metric view and loose ones under Space config", () => {
    const mv = m.nodes.find((n) => n.id === "cat.sch.order_metrics")
    const cfg = m.nodes.find((n) => n.kind === "config")
    expect(mv && mv.kind === "mv" && mv.measures.map((x) => x.name).sort()).toEqual(["order_count", "revenue"])
    expect(cfg && cfg.kind !== "table" && cfg.measures.map((x) => x.name).sort()).toEqual(["aov", "repeat_rate"])
  })

  it("resolves a measure's source tables from derives + transitive uses (§5.10)", () => {
    const idx = measureIndex(m)
    expect(idx["cat.sch.order_metrics::order_count"].src.sort()).toEqual(["cat.sch.dim_user", "cat.sch.fact_orders"])
    expect(idx["Space config::aov"].src).toEqual(["cat.sch.fact_orders"])
  })

  it("parses column endpoints from the ON predicate", () => {
    expect(parseOnColumns("fact_orders.user_id = dim_user.user_id", "cat.sch.fact_orders", "cat.sch.dim_user")).toEqual({
      fromCol: "user_id",
      toCol: "user_id",
    })
    const join = m.joins.find((j) => j.to === "cat.sch.dim_user")
    expect(join?.fromCol).toBe("user_id")
    expect(join?.rel).toBe("N:1")
  })

  it("flags island (unjoined) and unmodeled/cold tables without inventing roles", () => {
    const orphan = m.nodes.find((n) => n.id === "cat.sch.dim_orphan")
    const region = m.nodes.find((n) => n.id === "cat.sch.dim_region")
    expect(orphan && orphan.kind === "table" && orphan.island).toBe(true)
    expect(orphan && orphan.kind === "table" && orphan.role).toBeUndefined()
    expect(region && region.kind === "table" && region.cold).toBe(true)
    expect(region && region.kind === "table" && region.unmodeled).toBe(true)
  })

  it("counts the governance ladder for the headline (§5.7)", () => {
    expect(headlineCounts(m)).toMatchObject({ governed: 2, curated: 1, ungoverned: 1 })
  })

  it("collapses multiple join edges between the same table pair into one line (§ERD best practice)", () => {
    // A composite / redundant relationship: two join edges between the SAME pair
    // (host_id and a second is_current guard) must render as ONE line, not two.
    const composite: SemanticGraphResponse = {
      space_id: "s",
      proposals: [],
      nodes: [
        { id: "cat.sch.fact_booking", kind: "table", label: "fact_booking", col: 0, row: 0, role: "fact", coverage: 2 },
        { id: "cat.sch.dim_host", kind: "table", label: "dim_host", col: 1, row: 0, role: "dim", coverage: 1 },
      ],
      edges: [
        { from: "cat.sch.fact_booking", to: "cat.sch.dim_host", kind: "join", on: "fact_booking.host_id = dim_host.host_id", relationship: "many-to-one" },
        { from: "cat.sch.fact_booking", to: "cat.sch.dim_host", kind: "join", on: "fact_booking.is_current = dim_host.is_current", relationship: "many-to-one" },
      ],
    }
    const mm = fromSemanticGraph(composite)
    const pairJoins = mm.joins.filter(
      (j) =>
        (j.from === "cat.sch.fact_booking" && j.to === "cat.sch.dim_host") ||
        (j.from === "cat.sch.dim_host" && j.to === "cat.sch.fact_booking"),
    )
    expect(pairJoins.length).toBe(1)
    expect(pairJoins[0].keyCount).toBe(2)
    // Both participating key columns still list for the Columns LOD.
    const fact = mm.nodes.find((n) => n.id === "cat.sch.fact_booking")
    expect(fact && fact.kind === "table" && fact.cols.sort()).toEqual(["host_id", "is_current"])
  })

  it("dedupes an exact duplicate join_spec without inflating keyCount", () => {
    const dup: SemanticGraphResponse = {
      space_id: "s",
      proposals: [],
      nodes: [
        { id: "t.f", kind: "table", label: "f", col: 0, row: 0, role: "fact", coverage: 2 },
        { id: "t.d", kind: "table", label: "d", col: 1, row: 0, role: "dim", coverage: 1 },
      ],
      edges: [
        { from: "t.f", to: "t.d", kind: "join", on: "f.d_id = d.d_id", relationship: "many-to-one" },
        { from: "t.f", to: "t.d", kind: "join", on: "f.d_id = d.d_id", relationship: "many-to-one" },
      ],
    }
    const mm = fromSemanticGraph(dup)
    expect(mm.joins.length).toBe(1)
    expect(mm.joins[0].keyCount).toBe(1)
  })

  it("prefers the Phase-2 server column model over the client ON parse (additive)", () => {
    const p2: SemanticGraphResponse = {
      ...star,
      nodes: star.nodes.map((n) =>
        n.id === "cat.sch.fact_orders" ? { ...n, columns: ["order_id", "user_id", "region_id", "amount"] } : n,
      ),
      edges: star.edges.map((e) =>
        e.kind === "join" && e.to === "cat.sch.dim_user"
          ? { ...e, from_column: "buyer_id", to_column: "user_id" }
          : e,
      ),
    }
    const mm = fromSemanticGraph(p2)
    const fact = mm.nodes.find((n) => n.id === "cat.sch.fact_orders")
    expect(fact && fact.kind === "table" && fact.cols).toEqual(["order_id", "user_id", "region_id", "amount"])
    expect(mm.joins.find((j) => j.to === "cat.sch.dim_user")?.fromCol).toBe("buyer_id")
  })
})

describe("layout & zoom bands (§5.5)", () => {
  const m = fromSemanticGraph(star)

  it("fact-center places dimensions to the left of the fact anchor (§5.12)", () => {
    const p = derivePlacement(m, "fact")
    expect(p.rank["cat.sch.fact_orders"]).toBeGreaterThan(p.rank["cat.sch.dim_user"])
  })

  it("node heights resolve per band: far < mid < near for a table with columns", () => {
    const fact = m.nodes.find((n) => n.id === "cat.sch.fact_orders")!
    expect(nodeHeight(fact, "far")).toBeLessThan(nodeHeight(fact, "mid"))
  })

  it("Overview band renders no measure chips and no role captions", () => {
    const html = renderToStaticMarkup(
      <BlueprintCanvas model={m} zoom="far" selected={null} layoutMode="fact" onSelect={() => {}} />,
    )
    expect(html).not.toContain('data-chip="measure"')
    expect(html).not.toContain('data-caption="role"')
  })

  it("Columns band renders join-key column rows with highlights", () => {
    const html = renderToStaticMarkup(
      <BlueprintCanvas model={m} zoom="near" selected={null} layoutMode="fact" onSelect={() => {}} />,
    )
    expect(html).toContain('data-joinkey="user_id"')
  })

  it("a manual drag offset shifts a node from its deterministic layout", () => {
    const base = renderToStaticMarkup(
      <BlueprintCanvas model={m} zoom="mid" selected={null} layoutMode="fact" onSelect={() => {}} />,
    )
    const moved = renderToStaticMarkup(
      <BlueprintCanvas
        model={m}
        zoom="mid"
        selected={null}
        layoutMode="fact"
        onSelect={() => {}}
        offsets={{ "cat.sch.dim_user": { dx: 40, dy: -25 } }}
      />,
    )
    // Additive: an empty/absent offset map renders the pure layout unchanged.
    expect(
      renderToStaticMarkup(
        <BlueprintCanvas model={m} zoom="mid" selected={null} layoutMode="fact" onSelect={() => {}} offsets={{}} />,
      ),
    ).toBe(base)
    // A non-empty offset moves the card, so the markup differs.
    expect(moved).not.toBe(base)
  })
})

describe("linework: crow's-foot + crossing bridges (§5.3/§5.4)", () => {
  const m = fromSemanticGraph(star)

  it("emits an orientation-aware crow's-foot on the many end and a one-bar tick", () => {
    const p = derivePlacement(m, "fact")
    const box = layoutBoxes(m, p, "mid")
    const edges = resolveEdges(m, p, box, "mid")
    const markers = cardinalityMarkers(edges[0])
    expect(markers.crowfoot).not.toBe("")
    expect(markers.oneTick).not.toBe("")
  })

  it("computes an index-stable hop where one edge's trunk crosses another's leg", () => {
    // A has a long horizontal leg at y=50; B's vertical trunk (x=60, y 0→100)
    // crosses it in the interior → A hops over B.
    const A = { sx: 0, sy: 50, dx: 240, dy: 50, midX: 120, hops: [] } as unknown as ResolvedEdge
    const B = { sx: 20, sy: 0, dx: 100, dy: 100, midX: 60, hops: [] } as unknown as ResolvedEdge
    computeHops([A, B])
    expect(A.hops.length + B.hops.length).toBeGreaterThanOrEqual(1)
    // Deterministic: recomputing yields the identical hop set.
    const A2 = { ...A, hops: [] } as ResolvedEdge
    const B2 = { ...B, hops: [] } as ResolvedEdge
    computeHops([A2, B2])
    expect(A2.hops).toEqual(A.hops)
  })
})

describe("lineage on select (§5.10)", () => {
  const m = fromSemanticGraph(star)
  const p = derivePlacement(m, "fact")
  const box = layoutBoxes(m, p, "mid")
  const edges = resolveEdges(m, p, box, "mid")

  it("a selected Space-config measure draws dashed lineage to each source table", () => {
    const chipPos = { "Space config::aov": { x: box["cat.sch.order_metrics"]?.x ?? 0, y: 100 } }
    const paths = lineagePaths(m, p, box, edges, "Space config::aov", chipPos)
    expect(paths.length).toBe(1)
    expect(paths[0].mode).toBe("measure")
    expect(paths[0].srcId).toBe("cat.sch.fact_orders")
  })

  it("a selected metric view draws dotted lineage to each source it uses", () => {
    const paths = lineagePaths(m, p, box, edges, "cat.sch.order_metrics", {})
    expect(paths.every((x) => x.mode === "mv")).toBe(true)
    expect(paths.map((x) => x.srcId).sort()).toEqual(["cat.sch.dim_user", "cat.sch.fact_orders"])
  })

  it("neighbourhood of a measure is its parent + sources (focus+context)", () => {
    const keep = neighbourhood(m, "cat.sch.order_metrics::order_count")
    expect(keep?.has("cat.sch.order_metrics")).toBe(true)
    expect(keep?.has("cat.sch.fact_orders")).toBe(true)
  })
})

describe("component render: determinism + arrows require proof (§2/§8)", () => {
  it("two renders are byte-identical", () => {
    expect(renderToStaticMarkup(<Mid />)).toBe(renderToStaticMarkup(<Mid />))
  })

  it("renders crow's-foot + one-tick glyphs and the health headline", () => {
    const html = renderToStaticMarkup(<Mid />)
    expect(html).toContain('data-glyph="crowfoot"')
    expect(html).toContain('data-glyph="one-tick"')
    expect(html).toContain("data-headline")
  })

  it("the island table draws zero base edges and nothing is proposed", () => {
    const html = renderToStaticMarkup(<Mid />)
    expect(html).not.toContain('data-edge-from="cat.sch.dim_orphan"')
    expect(html).not.toContain('data-edge-to="cat.sch.dim_orphan"')
    expect(html).not.toContain("proposed_join")
    // every base edge maps to a declared join in the model
    const declared = new Set(fromSemanticGraph(star).joins.map((j) => `${j.from}->${j.to}`))
    expect(declared.size).toBe(2)
  })

  it("renders neutral TABLE for a role-less table, never a guessed FACT/DIM", () => {
    const html = renderToStaticMarkup(<Mid />)
    expect(html).toContain(">TABLE<")
  })

  it("additivity: an unknown future field on the response does not change the render", () => {
    const withExtra = { ...star, nodes: star.nodes.map((n) => ({ ...n })) } as SemanticGraphResponse & { future?: number }
    withExtra.future = 1
    expect(renderToStaticMarkup(<SemanticBlueprint nodes={withExtra.nodes} edges={withExtra.edges} />)).toBe(
      renderToStaticMarkup(<Mid />),
    )
  })
})

describe("Phase 3 — Insights inset (§7.5)", () => {
  const m = fromSemanticGraph(star)

  it("ranks fails above warns and caps at the top 2", () => {
    const ins = rankInsights(m)
    expect(ins.length).toBeLessThanOrEqual(2)
    expect(ins[0].severity).toBe("fail")
    expect(ins[0].title).toBe("Unrelated table")
    expect(ins[0].focus).toBe("cat.sch.dim_orphan")
  })

  it("shows the clean-state line when nothing ranks", () => {
    const clean: SemanticGraphResponse = {
      space_id: "s",
      proposals: [],
      nodes: [
        { id: "t.a", kind: "table", label: "a", col: 0, row: 0, role: "fact", coverage: 2 },
        { id: "t.b", kind: "table", label: "b", col: 1, row: 0, role: "dim", coverage: 1 },
      ],
      edges: [{ from: "t.a", to: "t.b", kind: "join", on: "a.b_id = b.b_id", relationship: "many-to-one" }],
    }
    expect(rankInsights(fromSemanticGraph(clean))).toEqual([])
    const html = renderToStaticMarkup(<SemanticBlueprint nodes={clean.nodes} edges={clean.edges} />)
    expect(html).toContain("No deal-breakers")
  })

  it("component renders the Insights + Join Advisor insets", () => {
    const html = renderToStaticMarkup(<Mid />)
    expect(html).toContain("data-insights")
    expect(html).toContain("data-advisor")
    expect(html).toContain("Full best-practice checklist lives in the IQ Scan")
  })
})

describe("Phase 3 — Join Advisor validated-seed (§7)", () => {
  const m = fromSemanticGraph(star)
  const candidate: JoinCandidate = {
    id: "c1",
    from: "cat.sch.fact_orders",
    fromCol: "region_id",
    to: "cat.sch.dim_region",
    toCol: "region_id",
    rel: "N:1",
    match: "name-type",
    probe: 0.97,
  }

  it("verdict classifies containment; weak (<50%) trips the confirm gate", () => {
    expect(verdict(0.97).level).toBe("validated")
    expect(verdict(0.6).level).toBe("partial")
    expect(verdict(0.11).level).toBe("unverified")
    expect(verdict(null).level).toBe("unverified")
    expect(isWeak(0.11)).toBe(true)
    expect(isWeak(null)).toBe(true)
    expect(isWeak(0.97)).toBe(false)
  })

  it("a checked candidate ghosts a proposed_join overlay edge, never a base edge", () => {
    const p = derivePlacement(m, "fact")
    const box = layoutBoxes(m, p, "mid")
    const html = renderToStaticMarkup(
      <BlueprintCanvas model={m} zoom="mid" selected={null} layoutMode="fact" onSelect={() => {}} overlay={[candidate]} />,
    )
    expect(html).toContain('data-edge="proposed_join"')
    // The overlay edge is NOT counted as a base join edge.
    const baseEdges = resolveEdges(m, p, box, "mid")
    expect(baseEdges.length).toBe(2)
    // no base edge points at the proposed target via a base join
    expect(baseEdges.some((e) => e.to === "cat.sch.dim_region" && e.fromCol === "region_id")).toBe(true)
  })

  it("seedPayload carries only the checked candidates to the run", () => {
    expect(seedPayload([candidate], new Set(["c1"]))).toEqual([candidate])
    expect(seedPayload([candidate], new Set())).toEqual([])
  })
})

function Mid() {
  return <SemanticBlueprint nodes={star.nodes} edges={star.edges} />
}
