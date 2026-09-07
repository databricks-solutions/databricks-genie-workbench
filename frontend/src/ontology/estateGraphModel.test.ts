import { describe, expect, it } from "vitest"
import type { OntologyGraph, OntologyGraphExpand } from "@/ontology/types"
import {
  buildElements,
  colorForTop,
  groupTops,
  mergeExpand,
  nodeFacts,
  viewElements,
} from "@/ontology/estateGraphModel"

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

describe("origin provenance threading (MV-D74)", () => {
  // Two tops: Revenue is applied (governed-tag backed), Ops is a proposed engine cluster.
  function originFixture(): OntologyGraph {
    return {
      domains: {
        nodes: [
          { id: "rev", label: "Revenue", kind: "domain", x: 0, y: 0, size: 1, member_count: 1, origin: "applied" },
          { id: "bk", label: "Bookings", kind: "subdomain", parent_id: "rev", parent_name: "Revenue", x: 0, y: 0, size: 1, member_count: 1, origin: "applied" },
          { id: "ops", label: "Operations", kind: "domain", x: 0, y: 0, size: 1, member_count: 1, origin: "proposed" },
        ],
        edges: [],
        truncated: false,
      },
      assets: { nodes: [], edges: [], truncated: false },
      layout: "fcose",
      node_count: 3,
      edge_count: 0,
      state: "fresh",
    }
  }

  it("groupTops carries each top's origin from its top-level node", () => {
    const tops = groupTops(originFixture().domains.nodes)
    expect(tops.get("rev")!.origin).toBe("applied")
    expect(tops.get("ops")!.origin).toBe("proposed")
  })

  it("Domains LOD stamps origin so proposed rollups render dashed/Suggested and applied do not", () => {
    const els = buildElements(originFixture(), "domains", null)
    const byId = new Map(els.map((e) => [e.data.id, e.data]))
    expect(byId.get("rev")!.origin).toBe("applied")
    expect(byId.get("ops")!.origin).toBe("proposed")
    // The renderer keys the dashed border off origin="proposed"; only the engine cluster carries it.
    const proposed = els.filter((e) => e.group === "nodes" && e.data.origin === "proposed")
    expect(proposed.map((e) => e.data.id)).toEqual(["ops"])
  })

  it("Sub-domains LOD threads origin onto containers and sub-domain nodes", () => {
    const els = buildElements(originFixture(), "subdomains", null)
    const byId = new Map(els.map((e) => [e.data.id, e.data]))
    expect(byId.get("top:rev")!.origin).toBe("applied")
    expect(byId.get("bk")!.origin).toBe("applied")
    expect(byId.get("top:ops")!.origin).toBe("proposed")
  })
})

describe("viewElements — Assets LOD requires a focused domain (MV-D75)", () => {
  it("yields no elements at the Assets LOD without a focus (the pick-an-area state)", () => {
    const els = viewElements(fixture(), "assets", null)
    expect(els).toHaveLength(0)
    expect(els.some((e) => e.data.ntype === "asset")).toBe(false)
  })

  it("delegates to buildElements once a domain is focused", () => {
    const gated = viewElements(fixture(), "assets", "ops")
    const direct = buildElements(fixture(), "assets", "ops")
    expect(gated).toEqual(direct)
    expect(gated.some((e) => e.data.ntype === "asset")).toBe(true)
  })

  it("does not gate the Domains / Sub-domains overviews", () => {
    expect(viewElements(fixture(), "domains", null).length).toBeGreaterThan(0)
    expect(viewElements(fixture(), "subdomains", null).length).toBeGreaterThan(0)
  })
})

describe("mergeExpand — expand-on-demand satellites (MV-D73)", () => {
  function expandPayload(): OntologyGraphExpand {
    return {
      parent_id: "asset:c.rev.fares",
      as_of: "2026-09-06",
      nodes: [
        { id: "measure:revenue", label: "revenue", kind: "measure", x: 0, y: 0, size: 1 },
        { id: "page:fares-guardrail", label: "Fares guardrail", kind: "page", x: 0, y: 0, size: 1 },
      ],
      edges: [
        { src: "asset:c.rev.fares", dst: "measure:revenue", kind: "mv_measure" },
        { src: "asset:c.rev.fares", dst: "page:fares-guardrail", kind: "page_source" },
      ],
    }
  }

  it("appends measure + Page satellites under the given parent container", () => {
    const base = buildElements(fixture(), "assets", "rev")
    const merged = mergeExpand(base, expandPayload(), "sub:fa")
    const byId = new Map(merged.map((e) => [e.data.id, e.data]))
    expect(byId.get("measure:revenue")!.ntype).toBe("measure")
    expect(byId.get("measure:revenue")!.parent).toBe("sub:fa")
    expect(byId.get("page:fares-guardrail")!.ntype).toBe("page")
    expect(byId.get("page:fares-guardrail")!.parent).toBe("sub:fa")
    // Both satellite edges survive because their endpoints are present.
    expect(merged.filter((e) => e.group === "edges" && e.data.etype === "snippet")).toHaveLength(2)
  })

  it("is idempotent — re-merging the same payload adds nothing (dedupe)", () => {
    const base = buildElements(fixture(), "assets", "rev")
    const once = mergeExpand(base, expandPayload(), "sub:fa")
    const twice = mergeExpand(once, expandPayload(), "sub:fa")
    expect(twice).toEqual(once)
  })

  it("never emits a dangling edge (emitted-only guard)", () => {
    const base = buildElements(fixture(), "assets", "rev")
    const payload = expandPayload()
    // An edge to a node that is NOT in the payload nor the base → must be dropped.
    payload.edges.push({ src: "measure:revenue", dst: "measure:ghost", kind: "mv_measure" })
    const merged = mergeExpand(base, payload, "sub:fa")
    const nodeIds = new Set(merged.filter((e) => e.group === "nodes").map((e) => e.data.id))
    for (const e of merged.filter((e) => e.group === "edges")) {
      expect(nodeIds.has(e.data.source as string)).toBe(true)
      expect(nodeIds.has(e.data.target as string)).toBe(true)
    }
    expect(nodeIds.has("measure:ghost")).toBe(false)
  })

  it("is pure (does not mutate the input array) and deterministic", () => {
    const base = buildElements(fixture(), "assets", "rev")
    const beforeLen = base.length
    const a = mergeExpand(base, expandPayload(), "sub:fa")
    const b = mergeExpand(base, expandPayload(), "sub:fa")
    expect(base).toHaveLength(beforeLen) // input untouched
    expect(a).toEqual(b) // same input → identical output (byte-stable)
  })
})

describe("nodeFacts — satellite copy is plain language (MV-D23/D73)", () => {
  it("describes a measure and a Page with no jargon", () => {
    const m = nodeFacts({ ntype: "measure", id: "measure:revenue", label: "revenue" })
    expect(m.chip).toBe("Measure")
    expect(m.drillTopId).toBeNull()
    const p = nodeFacts({ ntype: "page", id: "page:x", label: "Fares guardrail" })
    expect(p.chip).toBe("Page")
    const blob = (JSON.stringify(m) + JSON.stringify(p)).toLowerCase()
    for (const bad of ["select", "set tag", "sql", "measure:", "page:"]) {
      expect(blob).not.toContain(bad)
    }
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
