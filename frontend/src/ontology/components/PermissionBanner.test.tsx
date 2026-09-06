import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologyPreflight, PermissionTier } from "@/ontology/types"
import { PermissionBanner } from "./PermissionBanner"
import { copyButtonLabel, grantCopyText, identityLabel, showGrantCopy } from "./permissionTiers"

function tier(overrides: Partial<PermissionTier>): PermissionTier {
  return {
    id: "tag_graph",
    label: "Governed-tag graph (dedupe)",
    identity: "sp",
    status: "ok",
    grants: [],
    reason: null,
    ...overrides,
  }
}

function preflight(tiers: PermissionTier[]): OntologyPreflight {
  return {
    tiers,
    can_render_taxonomy: true,
    catalog_allowlist: ["finance"],
    as_of: "2026-08-30T00:00:00+00:00",
  }
}

describe("PermissionBanner identity label (MV-D50)", () => {
  it("renders the two read tiers as 'OBO (admin) or SP'", () => {
    expect(identityLabel(tier({ id: "signals" }))).toBe("OBO (admin) or SP")
    expect(identityLabel(tier({ id: "tag_graph" }))).toBe("OBO (admin) or SP")
  })

  it("keeps plain labels for the other tiers", () => {
    expect(identityLabel(tier({ id: "membership_write", identity: "obo" }))).toBe("OBO")
    expect(identityLabel(tier({ id: "external_enrichment", identity: "batch" }))).toBe("Batch")
  })
})

describe("PermissionBanner copy button (MV-D50)", () => {
  it("shows whenever a tier carries grant lines — including a green (ok) tier", () => {
    expect(showGrantCopy(tier({ status: "ok", grants: ["GRANT SELECT ..."] }))).toBe(true)
    expect(showGrantCopy(tier({ status: "ok", grants: [] }))).toBe(false)
    expect(showGrantCopy(tier({ status: "blocked", grants: ["GRANT SELECT ..."] }))).toBe(true)
  })

  it("copies the joined grant lines", () => {
    const t = tier({ grants: ["GRANT USE CATALOG ...", "GRANT SELECT ..."] })
    expect(grantCopyText(t)).toBe("GRANT USE CATALOG ...\nGRANT SELECT ...")
  })

  it("labels the copy by identity", () => {
    expect(copyButtonLabel(tier({ identity: "sp" }))).toBe("Copy GRANT SQL")
    expect(copyButtonLabel(tier({ id: "membership_write", identity: "obo" }))).toBe(
      "Copy entitlement request",
    )
  })
})

describe("PermissionBanner render (MV-D50)", () => {
  it("renders 'OBO (admin) or SP' and a copy button on a green tag_graph tier", () => {
    const html = renderToStaticMarkup(
      <PermissionBanner
        preflight={preflight([
          tier({
            id: "tag_graph",
            status: "ok",
            grants: [
              "GRANT USE CATALOG ON CATALOG system TO `sp`",
              "GRANT SELECT ON TABLE system.tags.governed_tags TO `sp`",
            ],
          }),
        ])}
      />,
    )
    expect(html).toContain("OBO (admin) or SP")
    // Copy button present even though status === "ok".
    expect(html).toContain("Copy GRANT SQL")
  })

  it("frames the SP grants as optional, not required, in the header", () => {
    const html = renderToStaticMarkup(
      <PermissionBanner preflight={preflight([tier({ id: "tag_graph", status: "ok" })])} />,
    )
    const text = html.replace(/\s+/g, " ").toLowerCase()
    expect(text).toContain("optional upgrade")
    expect(text).toContain("no service-principal grant is required to view")
  })
})
