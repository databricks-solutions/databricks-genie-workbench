import { describe, expect, it } from "vitest"
import {
  buildEstateModel,
  classForEdge,
  trimCommonPrefix,
  typeForKind,
  verbForEdge,
} from "@/ontology/estateGraphModel"
import type { OntologyGraph, OntologyGraphEdge, OntologyGraphNode } from "@/ontology/types"

// ── Fixtures ─────────────────────────────────────────────────────────────────

function node(p: Partial<OntologyGraphNode> & { id: string }): OntologyGraphNode {
  return {
    label: p.id,
    kind: "table",
    domain_id: null,
    parent_id: null,
    parent_name: null,
    x: 0,
    y: 0,
    size: 1,
    cost: null,
    member_count: null,
    origin: null,
    ...p,
  }
}

/** Full northstar-shaped graph: org root, domain→subdomain, MV⊃table, typed edges, tray. */
function northstarGraph(): OntologyGraph {
  const domains: OntologyGraphNode[] = [
    node({ id: "d_fin", label: "Acme Finance", kind: "domain", origin: "applied" }),
    node({ id: "d_ops", label: "Acme Operations", kind: "domain", origin: "applied" }),
    node({ id: "s_rev", label: "Revenue", kind: "subdomain", parent_id: "d_fin", origin: "applied" }),
    node({ id: "ungrouped", label: "Ungrouped", kind: "ungrouped", member_count: 3, origin: "proposed" }),
  ]
  const assets: OntologyGraphNode[] = [
    node({ id: "agent:a1", label: "Finance Agent", kind: "genie_agent", domain_id: "s_rev", attach_level: "subdomain", origin: "applied" }),
    node({ id: "mv:net_sales", label: "net sales", kind: "metric_view", domain_id: "s_rev", parent_id: "agent:a1", attach_level: "asset", origin: "applied" }),
    node({ id: "measure:ns", label: "net_sales", kind: "measure", domain_id: "s_rev", parent_id: "mv:net_sales", attach_level: "asset", origin: "applied" }),
    node({ id: "table:sales", label: "fact_sales", kind: "table", domain_id: "s_rev", parent_id: "mv:net_sales", attach_level: "asset", origin: "applied" }),
    node({ id: "table:cal", label: "ref_calendar", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
    node({ id: "table:ops1", label: "ops_events", kind: "table", domain_id: "d_ops", attach_level: "domain", origin: "applied" }),
    // tray assets (ungrouped)
    node({ id: "table:u1", label: "orphan_a", kind: "table", domain_id: "ungrouped" }),
    node({ id: "table:u2", label: "orphan_b", kind: "table", domain_id: "ungrouped" }),
    node({ id: "table:u3", label: "orphan_c", kind: "table", domain_id: null }),
  ]
  const edges: OntologyGraphEdge[] = [
    { src: "table:sales", dst: "table:cal", kind: "join_key", weight: 2, verb: "joins calendar", rel_class: "shared" },
    { src: "table:sales", dst: "table:ops1", kind: "co_query", weight: 1, verb: "also queried with", rel_class: "xdom" },
  ]
  return {
    root: node({ id: "org:acme", label: "Acme", kind: "org", member_count: 9 }),
    domains: { nodes: domains, edges: [], truncated: false },
    assets: { nodes: assets, edges, truncated: false },
    layout: "tree",
    node_count: 14,
    edge_count: 2,
    state: "fresh",
    as_of: null,
  }
}

/** Pre-Lane-D (degrade): no root, no attach_level, no verb/rel_class. */
function degradeGraph(): OntologyGraph {
  const domains: OntologyGraphNode[] = [
    node({ id: "d_fin", label: "Alaska Airlines Commercial", kind: "domain", origin: "applied" }),
    node({ id: "d_ops", label: "Alaska Airlines Operations", kind: "domain", origin: "applied" }),
    node({ id: "s_tk", label: "Ticketing", kind: "subdomain", parent_id: "d_fin", origin: "applied" }),
  ]
  const assets: OntologyGraphNode[] = [
    node({ id: "t1", label: "ticket_coupon", kind: "table", domain_id: "s_tk" }),
    node({ id: "t2", label: "flight_leg", kind: "table", domain_id: "d_ops" }),
  ]
  const edges: OntologyGraphEdge[] = [{ src: "t1", dst: "t2", kind: "join_key", weight: null }]
  return {
    domains: { nodes: domains, edges: [], truncated: false },
    assets: { nodes: assets, edges, truncated: false },
    layout: "fr",
    node_count: 5,
    edge_count: 1,
    state: "fresh",
    as_of: null,
  }
}

// ── typeForKind ────────────────────────────────────────────────────────────
describe("typeForKind", () => {
  it("maps backend kinds to tree node types", () => {
    expect(typeForKind("metric_view")).toBe("metric_view")
    expect(typeForKind("genie_agent")).toBe("agent")
    expect(typeForKind("agent")).toBe("agent")
    expect(typeForKind("dashboard")).toBe("dashboard")
    expect(typeForKind("measure")).toBe("measure")
    expect(typeForKind("view")).toBe("table")
    expect(typeForKind("table")).toBe("table")
    expect(typeForKind("weird")).toBe("table")
  })
})

// ── verb + class derivation (degrade) vs explicit (Lane D) ───────────────────
describe("verbForEdge", () => {
  it("honors an explicit Lane-D verb", () => {
    expect(verbForEdge({ src: "a", dst: "b", kind: "join_key", verb: "joins calendar" })).toBe("joins calendar")
  })
  it("derives a plain verb from the signal kind when absent", () => {
    expect(verbForEdge({ src: "a", dst: "b", kind: "mv_membership" })).toBe("reads")
    expect(verbForEdge({ src: "a", dst: "b", kind: "agent_scope" })).toBe("uses")
    expect(verbForEdge({ src: "a", dst: "b", kind: "join_key" })).toBe("shares key with")
    expect(verbForEdge({ src: "a", dst: "b", kind: "co_query" })).toBe("also queried with")
  })
})

describe("classForEdge", () => {
  const domainOf = (id: string) => (id === "x" ? "d1" : id === "y" ? "d2" : "d1")
  it("honors explicit rel_class", () => {
    expect(classForEdge({ src: "x", dst: "y", kind: "k", rel_class: "shared" }, domainOf)).toBe("shared")
  })
  it("derives xdom when endpoints live in different domains", () => {
    expect(classForEdge({ src: "x", dst: "y", kind: "k" }, domainOf)).toBe("xdom")
  })
  it("derives shared within one domain", () => {
    expect(classForEdge({ src: "x", dst: "z", kind: "k" }, domainOf)).toBe("shared")
  })
})

// ── trimCommonPrefix ─────────────────────────────────────────────────────────
describe("trimCommonPrefix", () => {
  it("trims the shared leading words across sibling domains", () => {
    const m = trimCommonPrefix(["Alaska Airlines Commercial", "Alaska Airlines Operations"])
    expect(m.get("Alaska Airlines Commercial")).toBe("Commercial")
    expect(m.get("Alaska Airlines Operations")).toBe("Operations")
  })
  it("leaves names untouched if trimming would empty one", () => {
    const m = trimCommonPrefix(["Acme", "Acme Finance"])
    expect(m.get("Acme")).toBe("Acme")
  })
})

// ── buildEstateModel — northstar path ────────────────────────────────────────
describe("buildEstateModel (northstar)", () => {
  const model = buildEstateModel(northstarGraph())

  it("is not degraded when a root is present", () => {
    expect(model.degraded).toBe(false)
    expect(model.root?.type).toBe("org")
    expect(model.root?.id).toBe("org:acme")
  })

  it("nests domains under the root and subdomains under their domain", () => {
    const kidsOfRoot = model.childrenByParent.get("org:acme")!.map((n) => n.id)
    expect(kidsOfRoot).toContain("d_fin")
    expect(kidsOfRoot).toContain("d_ops")
    const kidsOfFin = model.childrenByParent.get("d_fin")!.map((n) => n.id)
    expect(kidsOfFin).toContain("s_rev")
  })

  it("honors Lane-D asset→asset containment (agent ⊃ mv ⊃ {measure, table})", () => {
    const kidsOfAgent = model.childrenByParent.get("agent:a1")!.map((n) => n.id)
    expect(kidsOfAgent).toContain("mv:net_sales")
    const kidsOfMv = model.childrenByParent.get("mv:net_sales")!.map((n) => n.id)
    expect(kidsOfMv).toEqual(expect.arrayContaining(["measure:ns", "table:sales"]))
  })

  it("attaches a shared reference asset directly to its domain", () => {
    const cal = model.nodes.find((n) => n.id === "table:cal")!
    expect(cal.parentId).toBe("d_fin")
    expect(cal.attachLevel).toBe("domain")
  })

  it("routes ungrouped assets to the tray, never into the tree", () => {
    const trayIds = model.trayItems.map((t) => t.id)
    expect(trayIds).toEqual(expect.arrayContaining(["table:u1", "table:u2", "table:u3"]))
    expect(model.nodes.find((n) => n.id === "table:u1")).toBeUndefined()
  })

  it("classifies cross-edges (shared vs xdom) and carries the verb", () => {
    const shared = model.crossEdges.find((e) => e.verb === "joins calendar")!
    expect(shared.relClass).toBe("shared")
    const xdom = model.crossEdges.find((e) => e.verb === "also queried with")!
    expect(xdom.relClass).toBe("xdom")
  })

  it("computes descendant counts up the tree", () => {
    expect(model.root!.descendantCount).toBeGreaterThan(0)
    const mv = model.nodes.find((n) => n.id === "mv:net_sales")!
    expect(mv.descendantCount).toBe(2) // measure + table
  })
})

// ── buildEstateModel — degrade path ──────────────────────────────────────────
describe("buildEstateModel (degrade)", () => {
  const model = buildEstateModel(degradeGraph(), { estateName: "Alaska" })

  it("synthesizes an org root and flags degraded", () => {
    expect(model.degraded).toBe(true)
    expect(model.root?.type).toBe("org")
    expect(model.root?.label).toBe("Alaska")
  })

  it("keeps assets as leaves under their domain/subdomain", () => {
    const t1 = model.nodes.find((n) => n.id === "t1")!
    expect(t1.parentId).toBe("s_tk")
    expect(t1.attachLevel).toBe("subdomain")
    const t2 = model.nodes.find((n) => n.id === "t2")!
    expect(t2.parentId).toBe("d_ops")
    expect(t2.attachLevel).toBe("domain")
  })

  it("derives verb + class for edges with no Lane-D annotation", () => {
    const e = model.crossEdges[0]
    expect(e.verb).toBe("shares key with")
    expect(e.relClass).toBe("xdom") // t1 in d_fin, t2 in d_ops
  })
})

// ── proposals + tray sources (§3.4) ──────────────────────────────────────────
describe("buildEstateModel proposals", () => {
  it("derives proposals from proposed-origin nodes when no drafts given (band null)", () => {
    const g = northstarGraph()
    g.domains.nodes.push(
      node({ id: "sug_new", label: "Suggested Area", kind: "domain", origin: "proposed" }),
    )
    g.assets.nodes.push(node({ id: "table:u1b", label: "x", kind: "table", domain_id: "sug_new" }))
    const model = buildEstateModel(g)
    const p = model.proposals.find((p) => p.id === "sug_new")!
    expect(p.band).toBeNull()
    expect(p.name).toBe("Suggested Area")
  })

  it("prefers OntologyDrafts (real confidence band) when provided", () => {
    const model = buildEstateModel(northstarGraph(), {
      drafts: {
        domains: [
          {
            proposal_id: "p1",
            kind: "domain",
            name: "Loyalty",
            description: "",
            tag_decision: "create",
            conflict_tag: null,
            subdomains: [],
            members: [{ fqn: "table:u1", asset_type: "table" }],
            why: "",
            evidence: [],
            tier: "high",
            confidence: { band: "High", signals_present: ["usage"], gap: "" },
          },
        ],
        pages: [],
        source: "live",
        as_of: "",
      },
      taxonomy: null,
    })
    const p = model.proposals[0]
    expect(p.band).toBe("High")
    expect(p.memberIds).toContain("table:u1")
  })
})
