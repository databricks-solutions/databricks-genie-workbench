import { describe, expect, it } from "vitest"
import type { OntologyGraph } from "@/ontology/types"
import { buildElements, colorForTop, groupTops, nodeFacts } from "@/ontology/estateGraphModel"

// Fixture: two top domains (Revenue, Ops); Revenue has two sub-domains (Bookings,
// Fares); Ops is a bare top with no subs. Assets belong to the sub-domains.
function fixture(): OntologyGraph {
  return {
    domains: {
      nodes: [
        { id: "rev", label: "Revenue", kind: "domain", x: 0, y: 0, size: 1, member_count: 0 },
        { id: "bk", label: "Bookings", kind: "subdomain", parent_id: "rev", parent_name: "Revenue", x: 0, y: 0, size: 1, member_count: 2 },
        { id: "fa", label: "Fares", kind: "subdomain", parent_id: "rev", parent_name: "Revenue", x: 0, y: 0, size: 1, member_count: 1 },
        { id: "ops", label: "Operations", kind: "domain", x: 0, y: 0, size: 1, member_count: 1 },
      ],
      edges: [{ src: "bk", dst: "fa", kind: "lineage" }],
      truncated: false,
    },
    assets: {
      nodes: [
        { id: "asset:c.rev.bookings", label: "bookings", kind: "table", domain_id: "bk", x: 0, y: 0, size: 3 },
        { id: "asset:c.rev.pnr", label: "pnr", kind: "table", domain_id: "bk", x: 0, y: 0, size: 2 },
        { id: "asset:c.rev.fares", label: "fares", kind: "metric_view", domain_id: "fa", x: 0, y: 0, size: 2 },
        { id: "asset:c.ops.flights", label: "flights", kind: "table", domain_id: "ops", x: 0, y: 0, size: 1 },
      ],
      edges: [{ src: "asset:c.rev.bookings", dst: "asset:c.rev.pnr", kind: "coquery", weight: 0.5 }],
      truncated: false,
    },
    layout: "fcose",
    node_count: 8,
    edge_count: 2,
    state: "fresh",
  }
}

describe("groupTops (MV-D71 hierarchy fold)", () => {
  it("folds sub-domains into their parent and sums member counts", () => {
    const tops = groupTops(fixture().domains.nodes)
    expect([...tops.keys()].sort()).toEqual(["ops", "rev"])
    expect(tops.get("rev")!.name).toBe("Revenue")
    expect(tops.get("rev")!.subIds.sort()).toEqual(["bk", "fa"])
    expect(tops.get("rev")!.memberCount).toBe(3) // 2 + 1
    expect(tops.get("ops")!.subIds).toEqual([]) // bare top
  })
})

describe("colorForTop", () => {
  it("is stable and deterministic per id", () => {
    expect(colorForTop("rev")).toBe(colorForTop("rev"))
  })
})

describe("buildElements — Domains LOD", () => {
  it("emits one hub per top domain and aggregates edges to the top level", () => {
    const els = buildElements(fixture(), "domains", null)
    const nodes = els.filter((e) => e.group === "nodes")
    expect(nodes.map((n) => n.data.id).sort()).toEqual(["ops", "rev"])
    expect(nodes.every((n) => n.data.ntype === "domain")).toBe(true)
    // bk→fa is intra-Revenue, so it collapses to a self-loop and is dropped.
    expect(els.filter((e) => e.group === "edges")).toHaveLength(0)
  })
})

describe("buildElements — Sub-domains LOD", () => {
  it("nests sub-domains under compound containers and self-nodes bare tops", () => {
    const els = buildElements(fixture(), "subdomains", null)
    const byId = new Map(els.map((e) => [e.data.id, e.data]))
    // Containers exist for both tops.
    expect(byId.has("top:rev")).toBe(true)
    expect(byId.has("top:ops")).toBe(true)
    expect(byId.get("top:rev")!.ntype).toBe("container")
    // Sub-domains nest under their container.
    expect(byId.get("bk")!.parent).toBe("top:rev")
    expect(byId.get("fa")!.parent).toBe("top:rev")
    // Bare top gets a self leaf so its container isn't empty.
    expect(byId.get("self:ops")!.parent).toBe("top:ops")
    // The bk→fa sub-domain edge survives.
    expect(els.some((e) => e.group === "edges" && e.data.source === "bk" && e.data.target === "fa")).toBe(true)
  })
})

describe("buildElements — Assets LOD (three-level nesting)", () => {
  it("nests assets under their sub-domain container, which nests under the top", () => {
    const els = buildElements(fixture(), "assets", null)
    const byId = new Map(els.map((e) => [e.data.id, e.data]))
    // Assets sit inside their sub-domain container (grouping preserved on drill-in).
    expect(byId.get("asset:c.rev.bookings")!.parent).toBe("sub:bk")
    expect(byId.get("asset:c.rev.fares")!.parent).toBe("sub:fa")
    // An asset whose domain is a bare top (no sub) nests directly under the top.
    expect(byId.get("asset:c.ops.flights")!.parent).toBe("top:ops")
    // Sub-domain containers exist and are nested inside their top container.
    expect(byId.get("sub:bk")!.ntype).toBe("subcontainer")
    expect(byId.get("sub:bk")!.parent).toBe("top:rev")
    expect(byId.get("sub:fa")!.parent).toBe("top:rev")
    // metric_view keeps its kind for accent styling.
    expect(byId.get("asset:c.rev.fares")!.kind).toBe("metric_view")
    // Top containers that actually hold assets are emitted.
    expect(byId.has("top:rev")).toBe(true)
    expect(byId.has("top:ops")).toBe(true)
    // Asset co-query edge survives among visible assets.
    expect(els.some((e) => e.group === "edges" && e.data.etype === "coquery")).toBe(true)
  })

  it("caps assets per sub-domain group and emits a '+N more' chip in that group", () => {
    // bk holds two assets (bookings, pnr); cap 1 → one shown + a more chip, both under sub:bk.
    const els = buildElements(fixture(), "assets", null, { perContainerCap: 1 })
    const bk = els.filter((e) => e.data.parent === "sub:bk" && e.data.ntype === "asset")
    expect(bk).toHaveLength(1) // capped
    expect(els.some((e) => e.data.ntype === "more" && e.data.parent === "sub:bk")).toBe(true)
  })

  it("scopes to a single top domain when focusTop is set (drill-down)", () => {
    const els = buildElements(fixture(), "assets", "ops")
    const containers = els.filter((e) => e.data.ntype === "container")
    expect(containers.map((c) => c.data.id)).toEqual(["top:ops"])
    // Only top:ops content (ops is a bare top → its asset parents directly to it).
    const parents = new Set(els.map((e) => e.data.parent).filter(Boolean))
    expect([...parents]).toEqual(["top:ops"])
  })

  it("gives every leaf a deterministic seed position and is reproducible", () => {
    const a = buildElements(fixture(), "assets", null)
    const b = buildElements(fixture(), "assets", null)
    expect(a).toEqual(b) // same input → identical elements (stable mental map)
    const assets = a.filter((e) => e.data.ntype === "asset")
    expect(assets.length).toBeGreaterThan(0)
    expect(
      assets.every((e) => e.position && Number.isFinite(e.position.x) && Number.isFinite(e.position.y)),
    ).toBe(true)
  })
})

describe("buildElements — edge safety + Domains overview (crash + focus fixes)", () => {
  it("never emits an edge whose endpoint was filtered out of the view", () => {
    const g = fixture()
    // A cross-top edge to a node that will NOT be emitted under an 'ops' focus.
    g.assets.edges.push({ src: "asset:c.rev.bookings", dst: "asset:c.ops.flights", kind: "coquery" })
    const els = buildElements(g, "assets", "ops")
    const nodeIds = new Set(els.filter((e) => e.group === "nodes").map((e) => e.data.id))
    for (const e of els.filter((e) => e.group === "edges")) {
      expect(nodeIds.has(e.data.source as string)).toBe(true)
      expect(nodeIds.has(e.data.target as string)).toBe(true)
    }
  })

  it("Domains LOD ignores focusTop and always shows the whole estate", () => {
    const els = buildElements(fixture(), "domains", "rev")
    const hubs = els.filter((e) => e.data.ntype === "domain")
    expect(hubs.map((h) => h.data.id).sort()).toEqual(["ops", "rev"])
    // Domain hubs carry deterministic centers, and two tops don't collapse onto one point.
    const pos = hubs.map((h) => h.position!)
    expect(pos.every((p) => p && Number.isFinite(p.x) && Number.isFinite(p.y))).toBe(true)
    expect(pos[0].x !== pos[1].x || pos[0].y !== pos[1].y).toBe(true)
  })
})

describe("nodeFacts — plain language, zero jargon (MV-D23)", () => {
  const FORBIDDEN = ["SET TAG", "SELECT", "sql", "canonical_id", "metastore", "asset:", "top:"]

  it("describes a domain with a drill target and no jargon", () => {
    const f = nodeFacts({ ntype: "domain", id: "rev", label: "Revenue", count: 3 })
    expect(f.chip).toBe("Business area")
    expect(f.drillTopId).toBe("rev")
    const blob = JSON.stringify(f).toLowerCase()
    for (const bad of FORBIDDEN) expect(blob).not.toContain(bad.toLowerCase())
  })

  it("describes an asset by human kind, no id leakage", () => {
    const f = nodeFacts({ ntype: "asset", id: "asset:c.rev.fares", label: "fares", kind: "metric_view", domainName: "Revenue", cost: 2400 })
    expect(f.title).toBe("fares")
    expect(f.chip).toBe("Metric view")
    expect(f.drillTopId).toBeNull()
    expect(f.lines.join(" ")).toContain("Revenue")
    const blob = JSON.stringify(f).toLowerCase()
    for (const bad of FORBIDDEN) expect(blob).not.toContain(bad.toLowerCase())
  })
})
