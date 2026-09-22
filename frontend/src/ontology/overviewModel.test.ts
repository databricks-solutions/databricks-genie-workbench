import { describe, expect, it } from "vitest"
import type {
  OntologyDrafts,
  OntologyInventory,
  OntologyPreflight,
  OntologyTaxonomy,
  PermissionTier,
} from "@/ontology/types"
import {
  buildChecklist,
  buildKpis,
  formatAsOf,
  heroCounts,
  nextAction,
  readTiersBlocked,
} from "./overviewModel"

function preflight(overrides: Partial<OntologyPreflight> = {}): OntologyPreflight {
  return {
    tiers: [],
    can_render_taxonomy: true,
    catalog_allowlist: ["finance"],
    as_of: "2026-09-01T00:00:00+00:00",
    ...overrides,
  }
}

function tier(overrides: Partial<PermissionTier>): PermissionTier {
  return {
    id: "inventory",
    label: "Inventory",
    identity: "obo",
    status: "ok",
    grants: [],
    reason: null,
    ...overrides,
  }
}

function drafts(overrides: Partial<OntologyDrafts> = {}): OntologyDrafts {
  return {
    domains: [],
    pages: [],
    source: "live",
    as_of: "2026-09-01T00:00:00+00:00",
    ...overrides,
  }
}

function inventory(overrides: Partial<OntologyInventory> = {}): OntologyInventory {
  return {
    catalogs_scanned: ["finance"],
    metric_view_count: 12,
    genie_agent_count: 4,
    governed_tag_count: 7,
    as_of: "2026-09-01T12:34:56+00:00",
    ...overrides,
  }
}

function taxonomy(overrides: Partial<OntologyTaxonomy> = {}): OntologyTaxonomy {
  return {
    domains: [],
    ungrouped: { metric_views: [], genie_agents: [] },
    as_of: "2026-09-01T00:00:00+00:00",
    ...overrides,
  }
}

describe("nextAction (MV-D107 adaptive CTA)", () => {
  it("empty allowlist ⇒ Choose catalogs → Settings", () => {
    const a = nextAction({ preflight: preflight({ catalog_allowlist: [] }), drafts: null })
    expect(a.kind).toBe("scope")
    expect(a.target).toBe("settings")
    expect(a.label).toBe("Choose catalogs to scan")
  })

  it("cold drafts ⇒ Scan the estate → scan", () => {
    const a = nextAction({ preflight: preflight(), drafts: drafts({ source: "cold" }) })
    expect(a.kind).toBe("scan")
    expect(a.target).toBe("scan")
    expect(a.label).toBe("Scan the estate")
  })

  it("not-yet-loaded drafts (null) with catalogs set ⇒ Scan the estate", () => {
    const a = nextAction({ preflight: preflight(), drafts: null })
    expect(a.kind).toBe("scan")
  })

  it("pending suggestions ⇒ Review N → Drafts (singular/plural)", () => {
    const one = nextAction({
      preflight: preflight(),
      drafts: drafts({ domains: [{} as never], pages: [] }),
    })
    expect(one.kind).toBe("review")
    expect(one.target).toBe("review")
    expect(one.label).toBe("Review 1 suggestion")

    const many = nextAction({
      preflight: preflight(),
      drafts: drafts({ domains: [{} as never], pages: [{} as never, {} as never] }),
    })
    expect(many.label).toBe("Review 3 suggestions")
  })

  it("no pending, warm snapshot ⇒ All caught up → Map (graph)", () => {
    const a = nextAction({ preflight: preflight(), drafts: drafts({ source: "live" }) })
    expect(a.kind).toBe("caught_up")
    expect(a.target).toBe("map")
    expect(a.label).toBe("You're all caught up")
  })
})

describe("buildKpis (MV-D107)", () => {
  it("derives ≤6 counts from the loaded fixture", () => {
    const kpis = buildKpis({
      inventory: inventory(),
      taxonomy: taxonomy({ domains: [{ subdomains: [] } as never, { subdomains: [] } as never] }),
      drafts: drafts({ domains: [{} as never], pages: [{} as never] }),
    })
    expect(kpis.length).toBeLessThanOrEqual(6)
    const byId = Object.fromEntries(kpis.map((k) => [k.id, k.value]))
    expect(byId.metric_views).toBe("12")
    expect(byId.genie_agents).toBe("4")
    expect(byId.tags).toBe("7")
    expect(byId.domains).toBe("2")
    expect(byId.pending).toBe("2")
    expect(byId.scanned).toBe("2026-09-01")
  })

  it("degrades to 3 KPIs while inventory is still loading, still ≤6", () => {
    const kpis = buildKpis({ inventory: null, taxonomy: null, drafts: null })
    expect(kpis.length).toBeLessThanOrEqual(6)
    const byId = Object.fromEntries(kpis.map((k) => [k.id, k.value]))
    expect(byId.domains).toBe("0")
    expect(byId.pending).toBe("0")
    expect(byId.scanned).toBe("—")
  })
})

describe("formatAsOf", () => {
  it("renders a deterministic UTC date, or an em dash for missing/invalid", () => {
    expect(formatAsOf("2026-09-01T12:34:56+00:00")).toBe("2026-09-01")
    expect(formatAsOf(null)).toBe("—")
    expect(formatAsOf("not-a-date")).toBe("—")
  })
})

describe("heroCounts", () => {
  it("M = domains, N = domains + sub-domains", () => {
    const t = taxonomy({
      domains: [
        { subdomains: [{}, {}] } as never,
        { subdomains: [{}] } as never,
      ],
    })
    expect(heroCounts(t)).toEqual({ domains: 2, concepts: 5 })
  })

  it("degrades to zeros when taxonomy is null", () => {
    expect(heroCounts(null)).toEqual({ domains: 0, concepts: 0 })
  })
})

describe("buildChecklist", () => {
  it("nothing scoped ⇒ only 'scope' can be done (it isn't), not complete", () => {
    const { steps, complete } = buildChecklist({
      preflight: preflight({ catalog_allowlist: [] }),
      drafts: null,
      taxonomy: null,
    })
    expect(complete).toBe(false)
    expect(steps.map((s) => s.done)).toEqual([false, false, false, false])
  })

  it("scoped + warm + no pending + domains applied ⇒ all done, complete", () => {
    const { steps, complete } = buildChecklist({
      preflight: preflight(),
      drafts: drafts({ source: "live", domains: [], pages: [] }),
      taxonomy: taxonomy({ domains: [{ subdomains: [] } as never] }),
    })
    expect(complete).toBe(true)
    expect(steps.every((s) => s.done)).toBe(true)
  })
})

describe("readTiersBlocked (auto-expand gate)", () => {
  it("false when required read tiers are ok/not_exercised", () => {
    expect(
      readTiersBlocked(
        preflight({
          tiers: [tier({ id: "inventory", status: "ok" }), tier({ id: "tag_graph", status: "not_exercised" })],
        }),
      ),
    ).toBe(false)
  })

  it("true when a required read tier is blocked or degraded", () => {
    expect(
      readTiersBlocked(preflight({ tiers: [tier({ id: "tag_graph", status: "blocked" })] })),
    ).toBe(true)
    expect(
      readTiersBlocked(preflight({ tiers: [tier({ id: "signals", status: "degraded" })] })),
    ).toBe(true)
  })

  it("ignores the optional write + external-enrichment tiers", () => {
    expect(
      readTiersBlocked(
        preflight({
          tiers: [
            tier({ id: "membership_write", status: "blocked" }),
            tier({ id: "external_enrichment", status: "degraded" }),
          ],
        }),
      ),
    ).toBe(false)
  })

  it("false for a null preflight", () => {
    expect(readTiersBlocked(null)).toBe(false)
  })
})
