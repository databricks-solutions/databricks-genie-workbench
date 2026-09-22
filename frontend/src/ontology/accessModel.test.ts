import { describe, expect, it } from "vitest"
import type { PermissionTier } from "@/ontology/types"
import { bucketTiers, readingBlocked } from "./accessModel"

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

const fixture: PermissionTier[] = [
  tier({ id: "inventory", status: "ok" }),
  tier({ id: "signals", status: "ok" }),
  tier({ id: "tag_graph", status: "ok", grants: ["GRANT SELECT ..."] }),
  tier({ id: "membership_write", identity: "obo", status: "not_exercised" }),
  tier({ id: "external_enrichment", identity: "batch", status: "not_exercised", sources: [] }),
]

describe("bucketTiers (MV-D107 P4 purpose grouping)", () => {
  const b = bucketTiers(fixture)

  it("puts the three OBO reads in 'reading'", () => {
    expect(b.reading.map((t) => t.id)).toEqual(["inventory", "signals", "tag_graph"])
  })

  it("puts read tiers carrying SP grants in 'upgrades'", () => {
    expect(b.upgrades.map((t) => t.id)).toEqual(["tag_graph"])
  })

  it("puts the locked write tier in 'notUsed' and the enrichment tier in 'external'", () => {
    expect(b.notUsed.map((t) => t.id)).toEqual(["membership_write"])
    expect(b.external.map((t) => t.id)).toEqual(["external_enrichment"])
  })

  it("never puts the write or external tier in reading/upgrades", () => {
    const ids = [...b.reading, ...b.upgrades].map((t) => t.id)
    expect(ids).not.toContain("membership_write")
    expect(ids).not.toContain("external_enrichment")
  })
})

describe("readingBlocked (MV-D107 P4 neutral/warn gate)", () => {
  it("false when every read tier is ok / not_exercised", () => {
    expect(readingBlocked(fixture)).toBe(false)
  })

  it("true when a read tier is blocked or degraded", () => {
    expect(readingBlocked([tier({ id: "tag_graph", status: "degraded" })])).toBe(true)
    expect(readingBlocked([tier({ id: "signals", status: "blocked" })])).toBe(true)
  })

  it("ignores the write + external tiers", () => {
    expect(
      readingBlocked([
        tier({ id: "membership_write", status: "blocked" }),
        tier({ id: "external_enrichment", status: "degraded" }),
      ]),
    ).toBe(false)
  })
})
