import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { ApplyItem } from "@/ontology/types"
import { describeChange, shortName } from "@/ontology/applyDescribe"
import { ApplyPreview } from "./ApplyPreview"

// Phase 5 (17i) is the subsystem's ONLY governed-tag write surface. The MV-D23
// zero-burden contract is strictest here: the human copy must describe the EFFECT
// of a change, never the SQL that realizes it. If any of these tokens appears, the
// preview has started leaking the write machinery.
const FORBIDDEN = [
  "set tag",
  "unset tag",
  "create governed tag",
  "alter asset",
  "plan_hash",
  "metastore_id",
  "workspace_id",
  "genie_ont_",
  "obo",
  "sql warehouse",
]

function item(overrides: Partial<ApplyItem> = {}): ApplyItem {
  return {
    proposal_id: "sug_a",
    proposal_kind: "domain",
    shape: "set_tag",
    target_fqn: "finance.core.orders",
    tag_key: "business_domain",
    tag_value: "Revenue",
    current_value: null,
    statement: "ALTER ASSET `finance.core.orders` SET TAG `business_domain` = 'Revenue'",
    executable: true,
    blocked_reason: null,
    required_grants: [],
    ...overrides,
  }
}

describe("describeChange — plain-language effect (MV-D23)", () => {
  it("adds: 'Organize <asset> under <grouping>'", () => {
    expect(describeChange(item())).toBe('Organize orders under “Revenue”')
  })

  it("moves: 'Move <asset> from <old> to <new>' when a current value differs", () => {
    expect(describeChange(item({ current_value: "Legacy" }))).toBe(
      'Move orders from “Legacy” to “Revenue”',
    )
  })

  it("create: names the new grouping, not the tag mechanics", () => {
    expect(describeChange(item({ shape: "create_tag", tag_value: "Revenue/Billing" }))).toBe(
      "Create the “Revenue/Billing” grouping",
    )
  })

  it("unset: removes the asset from the old grouping", () => {
    expect(describeChange(item({ shape: "unset_tag", tag_key: "legacy_domain" }))).toBe(
      "Remove orders from “legacy_domain”",
    )
  })

  it("shortName drops the catalog/schema qualifier", () => {
    expect(shortName("finance.core.orders")).toBe("orders")
    expect(shortName("bare")).toBe("bare")
  })

  it("never emits the write mechanics for any shape", () => {
    const phrasings = [
      describeChange(item()),
      describeChange(item({ current_value: "Legacy" })),
      describeChange(item({ shape: "create_tag" })),
      describeChange(item({ shape: "unset_tag" })),
    ].join(" \n ")
    const lower = phrasings.toLowerCase()
    for (const token of FORBIDDEN) {
      expect(lower, `phrasing leaked jargon: "${token}"`).not.toContain(token)
    }
  })
})

describe("ApplyPreview shell", () => {
  it("renders the confirm-gated panel without leaking machinery on first paint", () => {
    // renderToStaticMarkup does not run effects, so this is the pre-preview shell —
    // it must already be jargon-free and must not render any raw statement.
    const html = renderToStaticMarkup(<ApplyPreview onClose={() => {}} />)
    const lower = html.toLowerCase()
    for (const token of FORBIDDEN) {
      expect(lower, `shell leaked jargon: "${token}"`).not.toContain(token)
    }
    expect(html).toContain("Apply approved changes")
  })
})
