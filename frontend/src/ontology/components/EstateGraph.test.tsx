import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologyGraph } from "@/ontology/types"
import { EstateGraph } from "./EstateGraph"

describe("EstateGraph — Phase 3e Step B (MV-D48)", () => {
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
    // Should render an empty-state notice, not crash.
    expect(html).toContain("No data in the estate graph")
  })

  it("renders the graph with nodes and edges on a non-empty fixture", () => {
    const fixture: OntologyGraph = {
      domains: {
        nodes: [
          {
            id: "d1",
            label: "Commercial",
            kind: "domain",
            x: 100,
            y: 100,
            size: 50,
          },
        ],
        edges: [],
        truncated: false,
      },
      assets: {
        nodes: [
          {
            id: "a1",
            label: "orders",
            kind: "asset",
            x: 150,
            y: 150,
            size: 20,
          },
        ],
        edges: [],
        truncated: false,
      },
      layout: "fcose",
      node_count: 2,
      edge_count: 0,
      state: "fresh",
    }

    const html = renderToStaticMarkup(<EstateGraph graph={fixture} />)
    // Should render the overview and legend without crashing.
    expect(html).toContain("Estate Overview")
    expect(html).toContain("2") // 2 nodes total
  })

  it("renders the legend with domain, asset, lineage, and co-query keys", () => {
    const fixture: OntologyGraph = {
      domains: {
        nodes: [{ id: "d1", label: "Commercial", kind: "domain", x: 100, y: 100, size: 50 }],
        edges: [],
        truncated: false,
      },
      assets: { nodes: [], edges: [], truncated: false },
      layout: "fcose",
      node_count: 1,
      edge_count: 0,
      state: "fresh",
    }

    const html = renderToStaticMarkup(<EstateGraph graph={fixture} />)
    expect(html).toContain("Domain")
    expect(html).toContain("Asset")
    expect(html).toContain("Lineage")
    expect(html).toContain("Co-query")
  })
})
