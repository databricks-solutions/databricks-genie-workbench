import { renderToStaticMarkup } from "react-dom/server"
import { afterEach, describe, expect, it, vi } from "vitest"
import type { OntologyGraph, OntologyGraphEdge, OntologyGraphNode } from "@/ontology/types"
import { graphTokens } from "@/ontology/graphTokens"
import { crossStrokeWidth, CROSS_WEIGHT_MAX } from "@/ontology/ontologyTreeLayout"
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

  it("engages the per-provenance fetch seam when an api is injected (MV-D74 — guards the empty-Proposed regression)", () => {
    // With the api seam, EstateGraph OWNS the origin fetch (fetchState initialises to
    // "loading" iff api is present, EstateGraph.tsx:264) so the Applied/Proposed toggle can
    // refetch origin=proposed. Without it the toggle is inert and Proposed renders empty even
    // when proposed rollups exist — the OntologyPage wiring bug this fix closes.
    const withApi = renderToStaticMarkup(
      <EstateGraph graph={northstar()} api={{ getGraph: vi.fn().mockResolvedValue(emptyGraph), expandNode: vi.fn() }} />,
    )
    expect(withApi).toContain("Building the estate graph…")
    const noApi = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(noApi).not.toContain("Building the estate graph…")
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
    // Domain-attached assets are gated by default — drill them so the child cap applies (R3).
    const html = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll />)
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
    // The default view opens org + domains only and gates leaf assets behind an explicit drill,
    // so render with `initialExpandAll` to make a leaf asset visible and assert its glyph.
    const g: OntologyGraph = {
      root: node({ id: "org", label: "Acme", kind: "org" }),
      domains: {
        nodes: [
          node({ id: "d_fin", label: "Acme Finance", kind: "domain", origin: "applied" }),
          node({ id: "s_rev", label: "Revenue", kind: "subdomain", parent_id: "d_fin", origin: "applied" }),
        ],
        edges: [],
        truncated: false,
      },
      assets: {
        nodes: [
          node({ id: "mv:top", label: "net sales", kind: "metric_view", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
        ],
        edges: [],
        truncated: false,
      },
      layout: "tree",
      node_count: 4,
      edge_count: 0,
      state: "fresh",
    }
    const html = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll />)
    // Container glyph paths (from GLYPHS) must be gone.
    const ORG_GLYPH = "M12 3 3 8v8l9 5 9-5V8z"
    const DOMAIN_GLYPH = "M4 7h16M4 12h16M4 17h16"
    const SUBDOMAIN_GLYPH = "M6 8h12M6 12h12M9 16h9"
    expect(html).not.toContain(ORG_GLYPH)
    expect(html).not.toContain(DOMAIN_GLYPH)
    expect(html).not.toContain(SUBDOMAIN_GLYPH)
    // A leaf asset (the domain-attached metric view) keeps its type glyph.
    const MV_GLYPH = "M4 20V10M10 20V4M16 20v-8M22 20H2"
    expect(html).toContain(MV_GLYPH)
  })

  it("renders the edge-type legend + interaction hint (R14/R6)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain("Hierarchy")
    expect(html).toContain("Shared key")
    expect(html).toContain("Cross-domain")
    expect(html).toContain("click to open · drag to move · hover for details")
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
    // Domain-attached tables are gated in the default view — drill them via initialExpandAll.
    const html = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll />)
    expect(html).toContain("shares key with")
  })

  // ── Buttery interactions — imperative redraw hooks (§1/§3/§4) ────────────────
  // Drag + relayout-glide recompute an edge's `d` from moved endpoints imperatively, keyed by
  // these data-attributes. They must render on the edges so the imperative path can find + redraw
  // them (the interactions themselves need a DOM env; the node-env suite verifies the hooks exist
  // and the geometry via the pure builder tests in ontologyTreeLayout.test.ts).
  it("tags spine links with data-edge-kind/src/dst for imperative redraw", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain('data-edge-kind="spine"')
    // org → d_fin spine carries its endpoint ids.
    expect(html).toMatch(/data-edge-kind="spine"[^>]*data-src="org"/)
  })

  it("tags cross-links with data-edge-kind/relclass + data-cross-line paths for imperative redraw", () => {
    const g = northstar()
    g.assets.nodes.push(
      node({ id: "t:a", label: "table_a", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
      node({ id: "t:b", label: "table_b", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
    )
    g.assets.edges = [{ src: "t:a", dst: "t:b", kind: "join_key", verb: "shares key with", rel_class: "shared" }]
    // Domain-attached tables are gated in the default view — drill them via initialExpandAll.
    // Arcs are focus-gated now (off at rest, R4) — seed a selection on an endpoint to draw it.
    const html = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll initialSelectedId="t:a" />)
    expect(html).toContain('data-edge-kind="cross"')
    expect(html).toContain('data-relclass="shared"')
    expect(html).toContain("data-cross-line")
  })

  it("offers an explicit 'Show N direct assets' drill for a selected domain with gated assets (#1)", () => {
    const g = northstar()
    // d_fin already has a sub-domain (s_rev); add a directly-attached table so its assets are
    // gated behind the explicit inspector drill (a single click won't dump them).
    g.assets.nodes.push(
      node({ id: "t:dir", label: "gl_ledger", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
    )
    // Default view: assets gated → the control reads "Show".
    const closed = renderToStaticMarkup(<EstateGraph graph={g} initialSelectedId="d_fin" />)
    expect(closed).toContain("Show 1 direct asset")
    // Drilled (initialExpandAll seeds the asset-drill set) → the control reads "Hide".
    const open = renderToStaticMarkup(<EstateGraph graph={g} initialSelectedId="d_fin" initialExpandAll />)
    expect(open).toContain("Hide 1 direct asset")
  })

  it("surfaces the edge evidence bag inline in the inspector relationship row (#4, MV-D88)", () => {
    const g = northstar()
    g.assets.nodes.push(
      node({ id: "t:a", label: "table_a", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
      node({ id: "t:b", label: "table_b", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
    )
    // A join_key arc carrying the MV-D88 evidence bag (columns + FK vs proxy).
    g.assets.edges = [
      {
        src: "t:a",
        dst: "t:b",
        kind: "join_key",
        verb: "shares key with",
        rel_class: "shared",
        detail: { columns: "route_id", kind: "foreign key" },
      },
    ]
    // Select an endpoint (docks the inspector) and expand so the other endpoint resolves.
    const html = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll initialSelectedId="t:a" />)
    // The docked inspector row shows the "why" inline (not only on arc hover).
    expect(html).toContain("route_id")
    expect(html).toContain("foreign key")
  })

  it("keeps node <g> tagged with data-node-id (the drag + glide handle)", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain('data-node-id="org"')
  })
})

// ── MV-D106 Phase 1 — viewport culling wiring ────────────────────────────────
// A balanced estate that lays out > 600 nodes when fully expanded (10 domains × 10 sub-areas ×
// 8 tables + 110 containers + org = 911), so the module culling threshold engages. No single
// parent exceeds the child cap (10), so nothing truncates into a "+N more" chip.
function bigEstate(): OntologyGraph {
  const domains: OntologyGraphNode[] = []
  const assets: OntologyGraphNode[] = []
  for (let d = 0; d < 10; d++) {
    const dId = `d${d}`
    domains.push(node({ id: dId, label: `Domain ${d}`, kind: "domain", origin: "applied" }))
    for (let s = 0; s < 10; s++) {
      const sId = `${dId}_s${s}`
      domains.push(node({ id: sId, label: `Sub ${d}.${s}`, kind: "subdomain", parent_id: dId, origin: "applied" }))
      for (let t = 0; t < 8; t++) {
        assets.push(
          node({ id: `${sId}_t${t}`, label: `tbl_${d}_${s}_${t}`, kind: "table", domain_id: sId, attach_level: "subdomain", origin: "applied" }),
        )
      }
    }
  }
  return {
    root: node({ id: "org", label: "Acme", kind: "org" }),
    domains: { nodes: domains, edges: [], truncated: false },
    assets: { nodes: assets, edges: [], truncated: false },
    layout: "tree",
    node_count: 1 + domains.length + assets.length,
    edge_count: 0,
    state: "fresh",
  }
}

const countNodeMarkers = (html: string) => (html.match(/data-node-id="/g) ?? []).length

describe("EstateGraph — viewport culling (MV-D106 Phase 1)", () => {
  it("culls off-screen nodes above the threshold, yet always mounts a selected node outside the viewport", () => {
    const g = bigEstate()
    // A tiny viewport parked far from the tree (which lays out around the origin), so essentially
    // nothing is in view — only the keep-set (the selected node + its ancestors) survives.
    const farRect = { x1: -100000, y1: -100000, x2: -99800, y2: -99800 }
    const full = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll initialSelectedId="d0_s0_t0" />)
    const culled = renderToStaticMarkup(
      <EstateGraph graph={g} initialExpandAll initialSelectedId="d0_s0_t0" initialViewportRect={farRect} />,
    )
    const fullCount = countNodeMarkers(full)
    const culledCount = countNodeMarkers(culled)
    // Without a viewport the whole estate mounts (over the 600 threshold).
    expect(fullCount).toBeGreaterThan(600)
    // Culling drops the off-screen majority — only the keep-set (selected + ancestors) remains.
    expect(culledCount).toBeLessThan(fullCount)
    expect(culledCount).toBeLessThan(50)
    // The selected node is far outside the parked viewport but the keep-set mounts it regardless.
    expect(culled).toContain('data-node-id="d0_s0_t0"')
  })

  it("renders the SAME node count for a small (< threshold) graph with or without a viewport (default-safe)", () => {
    const g = northstar() // ≪ 600 nodes ⇒ referential passthrough regardless of the viewport
    const rect = { x1: 0, y1: 0, x2: 100, y2: 100 }
    const without = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll />)
    const withRect = renderToStaticMarkup(<EstateGraph graph={g} initialExpandAll initialViewportRect={rect} />)
    expect(countNodeMarkers(withRect)).toBe(countNodeMarkers(without))
  })
})

// ── Live theme reactivity (useTheme fix) ─────────────────────────────────────
// The Map picks its palette from `graphTokens(resolvedTheme)`, and `resolvedTheme` now tracks
// the applied `<html>.dark` class. So the SAME graph renders DARK token hexes when the document
// carries `.dark` and LIGHT hexes otherwise — proving the toggle flips the Map live (no stale
// frozen theme). We stub a minimal `document` since the node test env has no DOM.
describe("EstateGraph — palette follows the applied theme (useTheme reactivity)", () => {
  const DARK_DOMAIN_FILL = "#94A3B8" // graphTokens DARK typeFill.domain
  const LIGHT_DOMAIN_FILL = "#234A57" // graphTokens LIGHT typeFill.domain

  function stubHtmlDark(hasDark: boolean) {
    vi.stubGlobal("document", {
      documentElement: { classList: { contains: (c: string) => c === "dark" && hasDark } },
    })
  }
  afterEach(() => vi.unstubAllGlobals())

  it("emits DARK token hexes when <html> has the dark class", () => {
    stubHtmlDark(true)
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain(DARK_DOMAIN_FILL)
    expect(html).not.toContain(LIGHT_DOMAIN_FILL)
  })

  it("emits LIGHT token hexes when <html> has no dark class", () => {
    stubHtmlDark(false)
    const html = renderToStaticMarkup(<EstateGraph graph={northstar()} />)
    expect(html).toContain(LIGHT_DOMAIN_FILL)
    expect(html).not.toContain(DARK_DOMAIN_FILL)
  })
})

// ── MV-D97 §6 — authority visual encoding (certification ring, deprecated muted, arc thickness) ──
describe("EstateGraph — certification ring + deprecated de-emphasis (MV-D97 §6)", () => {
  function withAsset(meta?: Record<string, string> | null): OntologyGraph {
    return {
      root: node({ id: "org", label: "Acme", kind: "org" }),
      domains: {
        nodes: [node({ id: "d_fin", label: "Finance", kind: "domain", origin: "applied" })],
        edges: [],
        truncated: false,
      },
      assets: {
        nodes: [node({ id: "t:gold", label: "gold_table", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied", meta })],
        edges: [],
        truncated: false,
      },
      layout: "tree",
      node_count: 3,
      edge_count: 0,
      state: "fresh",
    }
  }

  it("draws the green certified ring + check badge iff meta.certified (else a plain node)", () => {
    const certified = renderToStaticMarkup(<EstateGraph graph={withAsset({ certified: "true" })} initialExpandAll />)
    expect(certified).toContain('data-certified="true"')
    expect(certified).toContain("data-certified-badge")
    // The ring uses the resolved-theme certified token (light or dark — both clear AA, MV-D79).
    const ringPresent =
      certified.includes(graphTokens("light").certifiedRing) ||
      certified.includes(graphTokens("dark").certifiedRing)
    expect(ringPresent).toBe(true)
    // A plain asset (no meta) carries neither the flag nor the badge.
    const plain = renderToStaticMarkup(<EstateGraph graph={withAsset(null)} initialExpandAll />)
    expect(plain).not.toContain('data-certified="true"')
    expect(plain).not.toContain("data-certified-badge")
  })

  it("applies the muted hatch de-emphasis iff meta.deprecated (else a plain node)", () => {
    const deprecated = renderToStaticMarkup(<EstateGraph graph={withAsset({ deprecated: "true" })} initialExpandAll />)
    expect(deprecated).toContain('data-deprecated="true"')
    expect(deprecated).toContain("url(#ont-deprecated-hatch)")
    const plain = renderToStaticMarkup(<EstateGraph graph={withAsset(null)} initialExpandAll />)
    expect(plain).not.toContain('data-deprecated="true"')
  })

  it("defines the deprecated hatch pattern in defs", () => {
    const html = renderToStaticMarkup(<EstateGraph graph={withAsset(null)} />)
    expect(html).toContain('id="ont-deprecated-hatch"')
  })
})

describe("crossStrokeWidth — join strength → arc thickness (MV-D97 §6)", () => {
  it("missing/non-positive weight ⇒ the fixed base stroke exactly (degrade, MV-D43)", () => {
    expect(crossStrokeWidth(1, undefined)).toBe(1)
    expect(crossStrokeWidth(1, null)).toBe(1)
    expect(crossStrokeWidth(1.4, 0)).toBe(1.4)
    expect(crossStrokeWidth(1, -3)).toBe(1)
  })

  it("grows with weight and is bounded by CROSS_WEIGHT_MAX×", () => {
    expect(crossStrokeWidth(1, 5)).toBeGreaterThan(1)
    expect(crossStrokeWidth(1, 50)).toBeGreaterThan(crossStrokeWidth(1, 5))
    expect(crossStrokeWidth(1, 1e6)).toBeLessThanOrEqual(CROSS_WEIGHT_MAX + 1e-9)
  })
})
