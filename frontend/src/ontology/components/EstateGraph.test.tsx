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
})
