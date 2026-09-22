import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologyPreflight, PermissionTier, SourceStatus } from "@/ontology/types"
import { PermissionBanner } from "./PermissionBanner"
import {
  copyButtonLabel,
  executeStatusToTier,
  grantCopyText,
  identityLabel,
  showGrantCopy,
} from "./permissionTiers"

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

describe("PermissionBanner ready counter (MV-D107 regression)", () => {
  // The header reads "N of M read tiers ready". N must be counted over the SAME
  // read-tier set as M, so it can never exceed the denominator (the old "4 of 3").
  it("never counts more ready tiers than there are read tiers", () => {
    const html = renderToStaticMarkup(
      <PermissionBanner
        preflight={preflight([
          tier({ id: "inventory", status: "ok" }),
          tier({ id: "signals", status: "ok" }),
          tier({ id: "tag_graph", status: "ok" }),
          // Optional tiers are also ok — they must NOT inflate the numerator.
          tier({ id: "membership_write", status: "ok" }),
          tier({ id: "external_enrichment", status: "ok" }),
        ])}
      />,
    )
    expect(html).toContain("3 of 3 read tiers ready")
    expect(html).not.toContain("5 of 3")
    expect(html).not.toContain("4 of 3")
  })

  it("counts only the ready read tiers", () => {
    const html = renderToStaticMarkup(
      <PermissionBanner
        preflight={preflight([
          tier({ id: "inventory", status: "ok" }),
          tier({ id: "signals", status: "degraded" }),
          tier({ id: "tag_graph", status: "blocked" }),
        ])}
      />,
    )
    expect(html).toContain("1 of 3 read tiers ready")
  })
})

// ── Phase 4 Stage C: the per-source Context Sources sub-panel ──────────────────
function source(overrides: Partial<SourceStatus> = {}): SourceStatus {
  return {
    id: "web_search",
    label: "Web search (AI Gateway)",
    klass: "external",
    provenance_tier: "T3",
    influence: "naming, descriptions, synonyms, gap hypotheses, recent context",
    execute_status: "missing",
    grant_line: "GRANT EXECUTE ON `system.ai.web_search` TO `app-sp`",
    reason: "The app service principal has no EXECUTE grant on this source.",
    ...overrides,
  }
}

describe("PermissionBanner execute-status mapping (Stage C)", () => {
  it("maps ok→ok and every non-ok EXECUTE status to the warning (degraded) pill", () => {
    expect(executeStatusToTier("ok")).toBe("ok")
    expect(executeStatusToTier("missing")).toBe("degraded")
    expect(executeStatusToTier("blocked")).toBe("degraded")
    expect(executeStatusToTier("unavailable")).toBe("degraded")
  })
})

describe("PermissionBanner context sources panel (Stage C)", () => {
  it("renders a per-source row (label, class, tier, influence) + a GRANT EXECUTE when missing", () => {
    const html = renderToStaticMarkup(
      <PermissionBanner
        preflight={preflight([
          tier({
            id: "external_enrichment",
            identity: "batch",
            status: "degraded",
            sources: [source()],
          }),
        ])}
      />,
    )
    expect(html).toContain("Web search (AI Gateway)")
    expect(html).toContain("external") // class badge
    expect(html).toContain("T3") // provenance tier badge
    expect(html).toContain("May inform:")
    // A missing EXECUTE surfaces the copy-ready GRANT EXECUTE line + copy button.
    expect(html).toContain("GRANT EXECUTE ON `system.ai.web_search`")
    expect(html).toContain("Copy GRANT EXECUTE")
  })

  it("keeps the plain reason and shows no source panel when the tier reports no sources (off)", () => {
    const html = renderToStaticMarkup(
      <PermissionBanner
        preflight={preflight([
          tier({
            id: "external_enrichment",
            identity: "batch",
            status: "not_exercised",
            sources: [],
            reason: "External context is off (the default) — the engine runs estate-only.",
          }),
        ])}
      />,
    )
    expect(html).toContain("estate-only")
    expect(html).not.toContain("May inform:")
    expect(html).not.toContain("Copy GRANT EXECUTE")
  })
})
