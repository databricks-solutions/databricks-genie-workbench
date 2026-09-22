import { describe, expect, it } from "vitest"
import type { DomainDraft, DomainNode, OntologyTaxonomy } from "@/ontology/types"
import type { GovernedTag } from "@/ontology/types"
import {
  countNonDomainTags,
  defaultHideEmpty,
  domainMatches,
  estateNeedsReview,
  filterAndSortDomains,
  filterTags,
  reconcile,
} from "./estateModel"

function tag(overrides: Partial<GovernedTag> = {}): GovernedTag {
  return {
    tag_key: "finance",
    allowed_values: [],
    assignment_count: 0,
    acts_as_domain: false,
    acts_as_subdomain: false,
    ...overrides,
  }
}

function domain(overrides: Partial<DomainNode> = {}): DomainNode {
  return {
    tag_key: "finance",
    name: "Finance",
    member_count: 3,
    subdomains: [],
    members: [],
    ...overrides,
  }
}

function draft(overrides: Partial<DomainDraft> = {}): DomainDraft {
  return {
    proposal_id: "p1",
    kind: "domain",
    name: "Finance",
    description: "",
    tag_decision: "create",
    conflict_tag: null,
    subdomains: [],
    members: [],
    why: "",
    evidence: [],
    tier: "high",
    ...overrides,
  }
}

function taxonomy(domains: DomainNode[]): OntologyTaxonomy {
  return {
    domains,
    ungrouped: { metric_views: [], genie_agents: [] },
    as_of: "2026-09-01T00:00:00+00:00",
  }
}

describe("reconcile (MV-D107 provenance badges)", () => {
  it("reuse draft ⇒ Confirmed", () => {
    const tx = taxonomy([domain({ tag_key: "finance", name: "Finance", member_count: 5 })])
    const r = reconcile(tx, [draft({ tag_decision: "reuse", conflict_tag: null, name: "Finance" })])
    expect(r.get("finance")?.provenance).toBe("confirmed")
  })

  it("reassign OR conflict_tag ⇒ Better proposal, with the deep-link proposalId", () => {
    const tx = taxonomy([domain({ tag_key: "finance" })])
    const byReassign = reconcile(tx, [
      draft({ proposal_id: "px", kind: "reassign", tag_decision: "reassign", name: "Finance" }),
    ])
    expect(byReassign.get("finance")).toEqual({ provenance: "better", proposalId: "px" })

    const byConflict = reconcile(tx, [
      draft({ proposal_id: "py", tag_decision: "create", conflict_tag: "finance", name: "Anything" }),
    ])
    expect(byConflict.get("finance")).toEqual({ provenance: "better", proposalId: "py" })
  })

  it("0 members and no matching draft ⇒ Declared·unpopulated", () => {
    const tx = taxonomy([domain({ tag_key: "hr", name: "HR", member_count: 0 })])
    const r = reconcile(tx, [])
    expect(r.get("hr")?.provenance).toBe("unpopulated")
  })

  it("populated with no draft ⇒ none", () => {
    const tx = taxonomy([domain({ tag_key: "ops", name: "Ops", member_count: 4 })])
    expect(reconcile(tx, []).get("ops")?.provenance).toBe("none")
  })

  it("Better outranks Confirmed when both a reuse and a reassign match", () => {
    const tx = taxonomy([domain({ tag_key: "finance", name: "Finance", member_count: 2 })])
    const r = reconcile(tx, [
      draft({ proposal_id: "r1", tag_decision: "reuse", name: "Finance" }),
      draft({ proposal_id: "r2", kind: "reassign", tag_decision: "reassign", name: "Finance" }),
    ])
    expect(r.get("finance")).toEqual({ provenance: "better", proposalId: "r2" })
  })
})

describe("estateNeedsReview (MV-D107 lead-in)", () => {
  it("all domains empty + drafts present ⇒ show with count", () => {
    const tx = taxonomy([domain({ member_count: 0 }), domain({ tag_key: "hr", member_count: 0 })])
    expect(estateNeedsReview(tx, [draft(), draft({ proposal_id: "p2" })])).toEqual({
      show: true,
      count: 2,
    })
  })

  it("any populated domain ⇒ hidden", () => {
    const tx = taxonomy([domain({ member_count: 3 })])
    expect(estateNeedsReview(tx, [draft()]).show).toBe(false)
  })

  it("no drafts ⇒ hidden even if all empty", () => {
    const tx = taxonomy([domain({ member_count: 0 })])
    expect(estateNeedsReview(tx, []).show).toBe(false)
  })
})

describe("domainMatches / filterAndSortDomains (MV-D107 navigate)", () => {
  const finance = domain({ tag_key: "finance", name: "Finance", member_count: 5 })
  const hrEmpty = domain({ tag_key: "hr", name: "HR", member_count: 0 })
  const ops = domain({
    tag_key: "ops",
    name: "Operations",
    member_count: 2,
    members: [{ fqn: "cat.sch.orders", asset_type: "table" }],
  })

  it("matches on name, tag key, and nested member fqn", () => {
    expect(domainMatches(finance, "fin")).toBe(true)
    expect(domainMatches(ops, "orders")).toBe(true)
    expect(domainMatches(finance, "zzz")).toBe(false)
    expect(domainMatches(finance, "  ")).toBe(true) // blank needle matches all
  })

  it("hide-empty drops 0-member domains", () => {
    const out = filterAndSortDomains([finance, hrEmpty, ops], {
      query: "",
      hideEmpty: true,
      sortBy: "members",
    })
    expect(out.map((d) => d.tag_key)).toEqual(["finance", "ops"])
  })

  it("search filters the tree", () => {
    const out = filterAndSortDomains([finance, hrEmpty, ops], {
      query: "orders",
      hideEmpty: false,
      sortBy: "members",
    })
    expect(out.map((d) => d.tag_key)).toEqual(["ops"])
  })

  it("sorts by members (desc) or name (alpha)", () => {
    const byMembers = filterAndSortDomains([ops, finance], {
      query: "",
      hideEmpty: false,
      sortBy: "members",
    })
    expect(byMembers.map((d) => d.tag_key)).toEqual(["finance", "ops"]) // 5 before 2

    const byName = filterAndSortDomains([ops, finance], {
      query: "",
      hideEmpty: false,
      sortBy: "name",
    })
    expect(byName.map((d) => d.name)).toEqual(["Finance", "Operations"])
  })

  it("members sort reorders even when all counts are 0 (sub-domain tiebreak)", () => {
    const thin = domain({ tag_key: "thin", name: "Zeta", member_count: 0, subdomains: [] })
    const rich = domain({
      tag_key: "rich",
      name: "Alpha",
      member_count: 0,
      subdomains: [
        { tag_value: "a/x", name: "X", member_count: 0, members: [] },
        { tag_value: "a/y", name: "Y", member_count: 0, members: [] },
      ],
    })
    const out = filterAndSortDomains([thin, rich], { query: "", hideEmpty: false, sortBy: "members" })
    // Richer domain (more sub-domains) floats up despite both being 0-member.
    expect(out.map((d) => d.tag_key)).toEqual(["rich", "thin"])
  })
})

describe("defaultHideEmpty (MV-D107 P2 smart default)", () => {
  it("OFF when no domain is populated (freshly-scanned / untagged estate)", () => {
    expect(defaultHideEmpty([domain({ member_count: 0 }), domain({ member_count: 0 })])).toBe(false)
  })
  it("ON once at least one domain has members", () => {
    expect(defaultHideEmpty([domain({ member_count: 0 }), domain({ member_count: 4 })])).toBe(true)
  })
  it("OFF for an empty domain list", () => {
    expect(defaultHideEmpty([])).toBe(false)
  })
})

describe("countNonDomainTags / filterTags (MV-D107 P2 Tags lens)", () => {
  const domainTag = tag({ tag_key: "finance", acts_as_domain: true })
  const subTag = tag({ tag_key: "finance/ap", acts_as_subdomain: true })
  const flat1 = tag({ tag_key: "sensitivity", allowed_values: ["pii", "public"] })
  const flat2 = tag({ tag_key: "3M" })

  it("counts only tags that are neither a domain nor a sub-domain", () => {
    expect(countNonDomainTags([domainTag, subTag, flat1, flat2])).toBe(2)
    expect(countNonDomainTags(null)).toBe(0)
    expect(countNonDomainTags(undefined)).toBe(0)
  })

  it("filters by tag key or allowed value, case-insensitive; blank ⇒ all", () => {
    const all = [domainTag, subTag, flat1, flat2]
    expect(filterTags(all, "finance").map((t) => t.tag_key)).toEqual(["finance", "finance/ap"])
    expect(filterTags(all, "PII").map((t) => t.tag_key)).toEqual(["sensitivity"]) // allowed-value hit
    expect(filterTags(all, "  ")).toHaveLength(4)
    expect(filterTags(all, "zzz")).toHaveLength(0)
  })
})
