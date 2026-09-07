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
})
