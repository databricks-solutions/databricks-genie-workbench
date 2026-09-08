import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologyGraph, OntologyGraphEdge, OntologyGraphNode } from "@/ontology/types"
import { EstateGraph } from "./EstateGraph"

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

const emptyGraph: OntologyGraph = {
  domains: { nodes: [], edges: [], truncated: false },
  assets: { nodes: [], edges: [], truncated: false },
  layout: "tree",
  node_count: 0,
  edge_count: 0,
  state: "cold",
}

function northstar(): OntologyGraph {
  const domains: OntologyGraphNode[] = [
    node({ id: "d_fin", label: "Acme Finance", kind: "domain", member_count: 12, origin: "applied" }),
    node({ id: "s_rev", label: "Revenue", kind: "subdomain", parent_id: "d_fin", origin: "applied" }),
    node({ id: "ungrouped", label: "Ungrouped", kind: "ungrouped", member_count: 2, origin: "proposed" }),
  ]
  const assets: OntologyGraphNode[] = [
    node({ id: "mv:ns", label: "net sales", kind: "metric_view", domain_id: "s_rev", attach_level: "subdomain", origin: "applied" }),
    node({ id: "t:sales", label: "fact_sales", kind: "table", domain_id: "s_rev", parent_id: "mv:ns", attach_level: "asset", origin: "applied" }),
    node({ id: "u1", label: "orphan_a", kind: "table", domain_id: "ungrouped" }),
  ]
  const edges: OntologyGraphEdge[] = []
  return {
    root: node({ id: "org", label: "Acme", kind: "org", member_count: 14 }),
    domains: { nodes: domains, edges: [], truncated: false },
    assets: { nodes: assets, edges, truncated: false },
    layout: "tree",
    node_count: 7,
    edge_count: 0,
    state: "fresh",
  }
}

describe("EstateGraph — Ontology Map north-star renderer (MV-D81/D84)", () => {
  it("renders honest-empty on an empty graph (MV-D43)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={emptyGraph} />)
    expect(html).toContain("No data in the estate graph")
  })

  it("renders the Applied | Proposed | Both provenance toggle, default Applied (MV-D74/§9-F)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain("Applied")
    expect(html).toContain("Proposed")
    expect(html).toContain(">Both<")
    // Applied is the active tab.
    const applied = html.match(/<button[^>]*>Applied<\/button>/)?.[0] ?? ""
    expect(applied).toContain("bg-accent")
  })

  it("renders the colour-by-type legend (§9-B), not an LOD toggle", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain("Genie Agent")
    expect(html).toContain("Metric View")
    expect(html).toContain("Table")
    expect(html).toContain("Business area")
    // The removed LOD control must be gone.
    expect(html).not.toContain("Sub-domains")
  })

  it("renders the shell: search, breadcrumb root, inspector, source line", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain("Find a table") // search box
    expect(html).toContain("Acme") // breadcrumb root name
    expect(html).toContain("Nothing selected") // inspector empty state
    expect(html).toContain("Applied (current governed tags)") // honest source line
  })

  it("renders svg tree nodes with type-colored fills + labels", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain("<svg")
    expect(html).toContain("Acme Finance") // a domain label
    // an org/domain node carries a treeitem role
    expect(html).toContain('role="treeitem"')
  })

  it("hides the Ungrouped tray in Applied mode — no nameless ghost discs beside the tree (R3/R12c)", () => {
    // northstar() carries an ungrouped tray asset; Applied is the default provenance.
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    // The tray divider label ("Ungrouped · N") must NOT render under Applied (§5).
    expect(html).not.toContain("Ungrouped ·")
  })

  it("renders a '+N more' truncation chip for a parent past the child cap (R3)", () => {
    const domains: OntologyGraphNode[] = [
      node({ id: "d_ops", label: "Ops", kind: "domain", origin: "applied" }),
    ]
    const assets: OntologyGraphNode[] = []
    for (let i = 0; i < 40; i++) {
      assets.push(node({ id: `w${i}`, label: `wide_${i}`, kind: "table", domain_id: "d_ops", attach_level: "domain", origin: "applied" }))
    }
    const g: OntologyGraph = {
      root: node({ id: "org", label: "Acme", kind: "org" }),
      domains: { nodes: domains, edges: [], truncated: false },
      assets: { nodes: assets, edges: [], truncated: false },
      layout: "tree",
      node_count: 42,
      edge_count: 0,
      state: "fresh",
    }
    const html = renderToStaticMarkup(<EstateGraph graph={g} />)
    expect(html).toMatch(/\+\d+ more/)
  })

  it("shows an honest-empty applied nudge to view suggested when proposals exist", () => {
    // proposed-origin domain grouping assets → a proposal exists even with empty tree.
    const g: OntologyGraph = {
      domains: {
        nodes: [
          node({ id: "sug", label: "Suggested Area", kind: "domain", origin: "proposed" }),
        ],
        edges: [],
        truncated: false,
      },
      assets: { nodes: [node({ id: "u1", label: "x", kind: "table", domain_id: "sug" })], edges: [], truncated: false },
      layout: "tree",
      node_count: 2,
      edge_count: 0,
      state: "cold",
    }
    const html = renderToStaticMarkup(<EstateGraph graph={g} />)
    expect(html).toContain("View suggested")
  })

  it("de-chromes containers: org/domain/subdomain carry NO glyph; leaf assets keep theirs (R23)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    // Container glyph paths (from GLYPHS) must be gone.
    const ORG_GLYPH = "M12 3 3 8v8l9 5 9-5V8z"
    const DOMAIN_GLYPH = "M4 7h16M4 12h16M4 17h16"
    const SUBDOMAIN_GLYPH = "M6 8h12M6 12h12M9 16h9"
    expect(html).not.toContain(ORG_GLYPH)
    expect(html).not.toContain(DOMAIN_GLYPH)
    expect(html).not.toContain(SUBDOMAIN_GLYPH)
    // A leaf asset (the visible metric view) keeps its type glyph.
    const MV_GLYPH = "M4 20V10M10 20V4M16 20v-8M22 20H2"
    expect(html).toContain(MV_GLYPH)
  })

  it("renders the edge-type legend + interaction hint (R14/R6)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain("Hierarchy")
    expect(html).toContain("Shared key")
    expect(html).toContain("Cross-domain")
    expect(html).toContain("click to drill · drag to move · hover for details")
  })

  // ── MV-D87 (Lane P2) — nav & relationship legibility ───────────────────────
  it("renders the P0-a toolbar: Collapse all + Fullscreen + Domains (R24/R27)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain("Expand all")
    expect(html).toContain("Collapse all")
    expect(html).toContain("Fullscreen")
    expect(html).toContain("Domains") // the show/hide toggle
  })

  it("defines themed direction-arrowhead markers for the two edge classes (R25)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain('id="arrow-shared"')
    expect(html).toContain('id="arrow-xdom"')
    expect(html).toContain("→ shows direction") // the updated interaction hint
  })

  it("renders a relationship-type legend row (verb + count) when cross-links exist (R25)", () => {
    const g = northstar()
    // Two domain-level tables under d_fin joined by a shared key → one rel-type row.
    g.assets.nodes.push(
      node({ id: "t:a", label: "table_a", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
      node({ id: "t:b", label: "table_b", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
    )
    g.assets.edges = [{ src: "t:a", dst: "t:b", kind: "join_key", verb: "shares key with", rel_class: "shared" }]
    const html = renderToStaticMarkup(<EstateGraph graph={g} />)
    expect(html).toContain("shares key with")
  })
})
