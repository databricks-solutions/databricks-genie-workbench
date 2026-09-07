import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologyGraph } from "@/ontology/types"
import { EstateGraph } from "./EstateGraph"

describe("EstateGraph — Phase 3e Step B / Ontology Map (MV-D48, MV-D72)", () => {
  it("renders honest-empty on an empty graph (MV-D43)", () => {
    const emptyGraph: OntologyGraph = {
      domains: { nodes: [], edges: [], truncated: false },
      assets: { nodes: [], edges: [], truncated: false },
      layout: "fcose",
      node_count: 0,
      edge_count: 0,
      state: "cold",
    }

    const html = renderToStaticMarkup(<EstateGraph graph={emptyGraph} />)
    expect(html).toContain("No data in the estate graph")
  })

  it("renders the LOD toggle + counts + legend on a non-empty fixture", () => {
    const fixture: OntologyGraph = {
      domains: {
        nodes: [{ id: "d1", label: "Commercial", kind: "domain", x: 100, y: 100, size: 50, member_count: 3 }],
        edges: [],
        truncated: false,
      },
      assets: {
        nodes: [{ id: "asset:c.s.orders", label: "orders", kind: "table", domain_id: "d1", x: 150, y: 150, size: 20 }],
        edges: [],
        truncated: false,
      },
      layout: "fcose",
      node_count: 2,
      edge_count: 0,
      state: "fresh",
    }

    const html = renderToStaticMarkup(<EstateGraph graph={fixture} />)
    // LOD toggle
    expect(html).toContain("Domains")
    expect(html).toContain("Sub-domains")
    expect(html).toContain("Assets")
    // Legend keys
    expect(html).toContain("Domain (container)")
    expect(html).toContain("Asset")
    expect(html).toContain("Lineage")
    expect(html).toContain("Co-query")
  })

  const shellFixture: OntologyGraph = {
    domains: {
      nodes: [{ id: "d1", label: "Commercial", kind: "domain", x: 100, y: 100, size: 50, member_count: 3 }],
      edges: [],
      truncated: false,
    },
    assets: {
      nodes: [{ id: "asset:c.s.orders", label: "orders", kind: "table", domain_id: "d1", x: 150, y: 150, size: 20 }],
      edges: [],
      truncated: false,
    },
    layout: "fcose",
    node_count: 2,
    edge_count: 0,
    state: "fresh",
  }

  it("renders the Applied|Proposed source toggle, defaulting to Applied (MV-D74)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={shellFixture} />)
    expect(html).toContain("Applied")
    expect(html).toContain("Proposed")
    // Applied is the active tab (accent background); Proposed is not "Suggested" yet.
    const appliedButton = html.match(/<button[^>]*>Applied<\/button>/)?.[0] ?? ""
    expect(appliedButton).toContain("bg-accent")
    // The "Suggested" chip only shows once Proposed is active (default = Applied → absent).
    expect(html).not.toContain(">Suggested<")
  })

  it("renders the modern shell: search, breadcrumb, inspector rail, minimap, source line", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={shellFixture} />)
    expect(html).toContain("Find a table") // search-to-focus box (MV-D75)
    expect(html).toContain("Estate") // breadcrumb root
    expect(html).toContain("Nothing selected") // right-rail inspector, empty selection
    expect(html).toContain("Overview") // minimap placeholder before layout
    expect(html).toContain("Applied (current governed tags)") // honest source line
  })

  it("shows an honest-empty applied map with a one-tap View suggested nudge (MV-D43/D74)", () => {
    const emptyApplied: OntologyGraph = {
      domains: { nodes: [], edges: [], truncated: false },
      assets: { nodes: [], edges: [], truncated: false },
      layout: "fcose",
      node_count: 0,
      edge_count: 0,
      state: "cold",
    }
    const html = renderToStaticMarkup(<EstateGraph graph={emptyApplied} />)
    expect(html).toContain("No data in the estate graph")
    expect(html).toContain("View suggested")
  })
})
