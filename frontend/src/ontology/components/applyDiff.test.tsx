import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { ApplyItem } from "@/ontology/types"
import { ApplyDiff } from "./ApplyDiff"

// The diff panel must render adds / moves / blocked-copy-ready rows WITHOUT leaking the
// write machinery (MV-D23). Same forbidden-token contract as the ApplyPreview shell test.
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
    statement: "SET TAG ON TABLE `finance`.`core`.`orders` `business_domain` = `Revenue`",
    executable: true,
    blocked_reason: null,
    required_grants: [],
    ...overrides,
  }
}

describe("ApplyDiff — adds / moves / blocked-copy-ready rows", () => {
  it("renders executable adds and moves in plain language", () => {
    const executable = [
      item(), // add
      item({ target_fqn: "finance.core.ledger", current_value: "Legacy" }), // move
      item({ shape: "create_tag", tag_value: "Revenue/Billing" }), // new grouping
    ]
    const html = renderToStaticMarkup(<ApplyDiff executable={executable} blocked={[]} grants={[]} />)
    expect(html).toContain("Organize orders under “Revenue”")
    expect(html).toContain("Move ledger from “Legacy” to “Revenue”")
    expect(html).toContain("Create the “Revenue/Billing” grouping")
    for (const token of FORBIDDEN) {
      expect(html.toLowerCase(), `diff leaked jargon: "${token}"`).not.toContain(token)
    }
  })

  it("renders the inverse diff (undo) through the SAME component + the notes line", () => {
    // Phase 5 (17j): an undo plan is an apply plan of inverse statements — the same
    // ApplyDiff renders it. The create_tag left in place surfaces as an informational note.
    const inverse = [
      item({ shape: "unset_tag", tag_key: "business_domain" }), // inverse of an add → remove
      item({
        target_fqn: "finance.core.ledger",
        current_value: "Revenue",
        tag_value: "Legacy",
      }), // inverse of a move → restore the prior grouping
    ]
    const notes = ["1 grouping left in place — undo never removes a grouping."]
    const html = renderToStaticMarkup(
      <ApplyDiff executable={inverse} blocked={[]} grants={[]} notes={notes} />,
    )
    expect(html).toContain("Remove orders from “business_domain”")
    expect(html).toContain("Move ledger from “Revenue” to “Legacy”")
    // The note renders as an informational line (never the tag/DDL mechanics, never an error).
    expect(html).toContain("1 grouping left in place — undo never removes a grouping.")
    for (const token of FORBIDDEN) {
      expect(html.toLowerCase(), `inverse diff leaked jargon: "${token}"`).not.toContain(token)
    }
  })

  it("renders no notes block for an apply plan (byte-identical to pre-17j)", () => {
    const html = renderToStaticMarkup(<ApplyDiff executable={[item()]} blocked={[]} grants={[]} />)
    expect(html).not.toContain("left in place")
  })

  it("renders blocked items as copy-ready grant steps (never executed inline)", () => {
    const blocked = [
      item({
        target_fqn: "finance.core.secrets",
        executable: false,
        blocked_reason: "needs a grant",
        required_grants: ["GRANT APPLY TAG ON TABLE `finance`.`core`.`secrets` TO `you`"],
      }),
    ]
    const grants = ["GRANT APPLY TAG ON TABLE `finance`.`core`.`secrets` TO `you`"]
    const html = renderToStaticMarkup(<ApplyDiff executable={[]} blocked={blocked} grants={grants} />)
    // The blocked count + the exact copy-ready grant line render.
    expect(html).toContain("1 change need a permission first")
    expect(html).toContain("GRANT APPLY TAG ON TABLE `finance`.`core`.`secrets` TO `you`")
    expect(html).toContain("Ask an account admin to grant these")
    // …and the raw write statement / blocked_reason machinery never leaks.
    expect(html.toLowerCase()).not.toContain("needs a grant")
  })
})
