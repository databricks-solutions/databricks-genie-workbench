/**
 * MvProposalsSummary — the decision-framing line both suggestion surfaces share
 * (MV-D122): it counts current proposals only, each member measure once, and
 * says apart how many stale proposals need a re-scan.
 */
import { describe, expect, it } from "vitest"
import { isValidElement, type ReactElement, type ReactNode } from "react"
import { renderToStaticMarkup } from "react-dom/server"
import { MvProposalsSummary } from "./MvProposalsSummary"
import type { MvProposal } from "@/types"

const render = (el: React.ReactElement) => renderToStaticMarkup(el)

function proposal(id: string, object: string, measureIds: string[], over: Partial<MvProposal> = {}): MvProposal {
  return {
    suggestion_id: id,
    dedup_fingerprint: `fp_${id}`,
    target_space_id: "space-1",
    run_id: null,
    candidate_type: "NEW_METRIC_VIEW",
    confidence_score: 80,
    tier: "HIGH",
    uncapped_tier: "HIGH",
    tier_capped_by_coverage: false,
    proposed_object: object,
    measures: measureIds.map((m) => ({ display_name: m, expr: `SUM(${m})`, dedup_fingerprint: m })),
    checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" },
    score_components: null,
    evidence: null,
    provenance_labels: null,
    provenance: null,
    alternatives: null,
    conflicts: null,
    requested_mode: null,
    effective_mode: null,
    decision: null,
    decided_by: null,
    decided_at: null,
    suppressed_until: null,
    approved_for_rerun: false,
    created_at: null,
    updated_at: null,
    ...over,
  }
}

function listItemKeys(node: ReactNode): (string | null)[] {
  const keys: (string | null)[] = []
  const walk = (n: ReactNode) => {
    if (Array.isArray(n)) return n.forEach(walk)
    if (!isValidElement(n)) return
    const el = n as ReactElement<{ children?: ReactNode }>
    if (el.type === "li") keys.push(el.key)
    walk(el.props.children)
  }
  walk(node)
  return keys
}

describe("MvProposalsSummary (MV-D122)", () => {
  it("counts a member measure two current proposals share once", () => {
    const html = render(
      <MvProposalsSummary
        proposals={[proposal("a", "finance.sales.orders_metrics", ["m1", "m2"]), proposal("b", "finance.sales.refunds_metrics", ["m2", "m3"])]}
      />,
    )
    expect(html).toContain("Suggesting 2 metric views to govern 3 recurring measures")
    expect(html).not.toContain("4 recurring measures")
  })

  it("counts the current proposal and says the stale one needs a re-scan", () => {
    const html = render(
      <MvProposalsSummary
        proposals={[
          proposal("a", "finance.sales.orders_metrics", ["m1"]),
          proposal("s", "finance.sales.margin_metrics", ["m2", "m3"], { stale_body: true }),
        ]}
      />,
    )
    expect(html).toContain(
      "Suggesting 1 metric view to govern 1 recurring measure · 1 found by an earlier version of the advisor needs a re-scan",
    )
    expect(html).toContain("orders_metrics")
    expect(html).not.toContain("margin_metrics")
  })

  it("with only stale proposals, says how many need a re-scan and suggests nothing", () => {
    const html = render(
      <MvProposalsSummary
        proposals={[
          proposal("s1", "finance.sales.margin_metrics", ["m1"], { stale_body: true }),
          proposal("s2", "finance.sales.aov_metrics", ["m2"], { stale_body: true }),
        ]}
      />,
    )
    expect(html).toContain("2 found by an earlier version of the advisor need a re-scan")
    expect(html).not.toContain("Suggesting")
    expect(html).not.toContain("<li")
  })

  it("renders nothing with no proposals", () => {
    expect(render(<MvProposalsSummary proposals={[]} />)).toBe("")
  })

  it("keys list items by suggestion_id, so two views with one short name render two items", () => {
    const proposals = [
      proposal("a", "finance.sales.orders_metrics", ["m1"]),
      proposal("b", "finance.returns.orders_metrics", ["m2"]),
    ]
    expect(listItemKeys(MvProposalsSummary({ proposals }))).toEqual(["a", "b"])
    const html = render(<MvProposalsSummary proposals={proposals} />)
    expect(html.match(/<li/g)?.length).toBe(2)
  })
})
