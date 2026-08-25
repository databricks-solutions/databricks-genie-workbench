/**
 * Unit tests for the MV surfacing/justification helpers (Prompt 15.3 / MV-D30).
 *
 * These pin the presentation-time invariants the IQ Scan panel relies on:
 *   - a proposal with no proposed_object never renders (blank-never-renders)
 *   - MEDIUM+ surface by default, LOW is partitioned out for the disclosure
 *   - the per-card gain sentence counts members and distinct curated queries
 */
import { describe, expect, it } from "vitest"
import {
  confidenceDisplay,
  evidenceGrowth,
  evidenceSummary,
  factsChecks,
  isCappedStrong,
  isLowConfidence,
  MV_CAPPED_STRONG_LABEL,
  MV_DEFAULT_VISIBLE,
  orthogonalityCallout,
  proposalGainSentence,
  rankProposals,
  recommendedReason,
  splitProposalsByConfidence,
  stageProgressFraction,
} from "./mvFormat"
import type { MvProposal } from "@/types"

function mk(overrides: Partial<MvProposal>): MvProposal {
  return {
    suggestion_id: "s",
    dedup_fingerprint: "fp",
    target_space_id: "space-1",
    run_id: null,
    candidate_type: "NEW_METRIC_VIEW",
    confidence_score: null,
    tier: null,
    uncapped_tier: null,
    tier_capped_by_coverage: null,
    proposed_object: "c.s.v",
    measures: [],
    checks: null,
    score_components: null,
    evidence: null,
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
    ...overrides,
  }
}

describe("splitProposalsByConfidence (MV-D30 surfacing floor)", () => {
  it("drops proposals with no proposed_object — blank cards never render", () => {
    const rows = [
      mk({ suggestion_id: "ok", proposed_object: "c.s.v", tier: "HIGH" }),
      mk({ suggestion_id: "blank", proposed_object: null, tier: "HIGH" }),
      mk({ suggestion_id: "empty", proposed_object: "   ", tier: "MEDIUM" }),
    ]
    const { primary, low } = splitProposalsByConfidence(rows)
    expect(primary.map((p) => p.suggestion_id)).toEqual(["ok"])
    expect(low).toEqual([])
  })

  it("MEDIUM+ (and unlabeled) are primary; only explicit LOW is disclosed", () => {
    const rows = [
      mk({ suggestion_id: "hi", tier: "HIGH" }),
      mk({ suggestion_id: "med", tier: "MEDIUM" }),
      mk({ suggestion_id: "unlabeled", tier: null }),
      mk({ suggestion_id: "lo", tier: "LOW" }),
    ]
    const { primary, low } = splitProposalsByConfidence(rows)
    expect(primary.map((p) => p.suggestion_id)).toEqual(["hi", "med", "unlabeled"])
    expect(low.map((p) => p.suggestion_id)).toEqual(["lo"])
  })

  it("isLowConfidence is case-insensitive and false for absent tiers", () => {
    expect(isLowConfidence("low")).toBe(true)
    expect(isLowConfidence("LOW")).toBe(true)
    expect(isLowConfidence("MEDIUM")).toBe(false)
    expect(isLowConfidence(null)).toBe(false)
    expect(isLowConfidence(undefined)).toBe(false)
  })
})

describe("coverage-capped-strong surfacing (MV-D32 / Prompt 15.7b)", () => {
  // Fresh-space case: strong curated-SQL recurrence, but lineage (L) and
  // usage/demand (D) structurally absent, so MV-D15 capped the served tier to
  // LOW while the score-only tier was MEDIUM+.
  const cappedStrong = mk({
    suggestion_id: "capped",
    tier: "LOW",
    uncapped_tier: "HIGH",
    tier_capped_by_coverage: true,
    confidence_score: 82,
  })
  // Genuinely weak: the evidence itself only earned LOW; coverage did not cap it.
  const genuinelyWeak = mk({
    suggestion_id: "weak",
    tier: "LOW",
    uncapped_tier: "LOW",
    tier_capped_by_coverage: false,
    confidence_score: 30,
  })
  // Legacy row: neither field persisted (pre-15.7b) — must behave exactly as the
  // 15.7 tier-only split did.
  const legacyLow = mk({ suggestion_id: "legacy", tier: "LOW", confidence_score: 28 })

  it("isCappedStrong: true only when uncapped MEDIUM+ AND coverage capped", () => {
    expect(isCappedStrong(cappedStrong)).toBe(true)
    expect(isCappedStrong(mk({ tier: "LOW", uncapped_tier: "MEDIUM", tier_capped_by_coverage: true }))).toBe(true)
    // uncapped LOW is not strong, even if the flag is set.
    expect(isCappedStrong(mk({ tier: "LOW", uncapped_tier: "LOW", tier_capped_by_coverage: true }))).toBe(false)
    // MEDIUM+ uncapped but not capped is just a normal proposal.
    expect(isCappedStrong(mk({ tier: "MEDIUM", uncapped_tier: "MEDIUM", tier_capped_by_coverage: false }))).toBe(false)
    // legacy row (nulls) is never capped-strong.
    expect(isCappedStrong(legacyLow)).toBe(false)
  })

  it("capped-strong joins the default list; genuinely-weak and legacy stay behind the disclosure", () => {
    const { primary, low } = splitProposalsByConfidence([cappedStrong, genuinelyWeak, legacyLow])
    expect(primary.map((p) => p.suggestion_id)).toEqual(["capped"])
    expect(low.map((p) => p.suggestion_id).sort()).toEqual(["legacy", "weak"])
  })

  it("legacy rows split exactly as the 15.7 tier-only rule (no regression)", () => {
    const rows = [
      mk({ suggestion_id: "hi", tier: "HIGH" }),
      mk({ suggestion_id: "lo", tier: "LOW" }),
    ]
    const { primary, low } = splitProposalsByConfidence(rows)
    expect(primary.map((p) => p.suggestion_id)).toEqual(["hi"])
    expect(low.map((p) => p.suggestion_id)).toEqual(["lo"])
  })

  it("ranking orders a capped-strong proposal by its UNCAPPED tier", () => {
    const med = mk({ suggestion_id: "med", tier: "MEDIUM", uncapped_tier: "MEDIUM" })
    // capped-strong with uncapped HIGH must rank ahead of a served-MEDIUM proposal,
    // even though its served tier is LOW.
    const ranked = rankProposals([med, cappedStrong])
    expect(ranked.map((p) => p.suggestion_id)).toEqual(["capped", "med"])
  })

  it("recommendedReason notes a capped-strong pick as evidence-limited — and never says 'confidence' (MV-D35)", () => {
    const reason = recommendedReason(cappedStrong)
    expect(reason).toContain("evidence-limited")
    // MV-D35: the word "confidence" is gone from every rendered rationale.
    expect(reason.toLowerCase()).not.toContain("confidence")
    expect(reason).not.toMatch(/\d+%/)
  })

  it("still exposes the split-logic label constant (badge retired from the card, MV-D35)", () => {
    // The badge no longer renders on the card, but isCappedStrong still drives
    // the default-list promotion (MV-D30 split), so the constant survives.
    expect(MV_CAPPED_STRONG_LABEL).toBe("Strong (evidence-limited)")
  })
})

describe("proposalGainSentence (MV-D30 justification)", () => {
  it("counts members and distinct curated queries across members", () => {
    const p = mk({
      measures: [
        { display_name: "a", expr: "SUM(x)", dedup_fingerprint: "m1", recurrence: 3, provenance_count: 3, benchmark_question_ids: ["q1", "q2"] },
        { display_name: "b", expr: "COUNT(1)", dedup_fingerprint: "m2", recurrence: 2, provenance_count: 2, benchmark_question_ids: ["q2", "q3"] },
      ],
    })
    expect(proposalGainSentence(p)).toBe(
      "These 2 measures recur across 3 curated queries and are ungoverned today.",
    )
  })

  it("uses singular grammar for one measure / one query", () => {
    const p = mk({
      measures: [
        { display_name: "a", expr: "SUM(x)", dedup_fingerprint: "m1", recurrence: 1, provenance_count: 1, benchmark_question_ids: ["q1"] },
      ],
    })
    expect(proposalGainSentence(p)).toBe(
      "These 1 measure recur across 1 curated query and are ungoverned today.",
    )
  })

  it("falls back to bundle-level evidence question ids when members carry none", () => {
    const p = mk({
      measures: [
        { display_name: "a", expr: "SUM(x)", dedup_fingerprint: "m1", recurrence: 1, provenance_count: 1, benchmark_question_ids: null },
      ],
      evidence: { benchmark_question_ids: ["q1", "q2"] },
    })
    expect(proposalGainSentence(p)).toContain("across 2 curated queries")
  })

  it("degrades to a generic gain when no question ids are present anywhere", () => {
    const p = mk({ measures: [], evidence: null })
    expect(proposalGainSentence(p)).toBe(
      "These 1 measure recur in this Agent\u2019s generated SQL and are ungoverned today.",
    )
  })
})

describe("evidenceSummary (Prompt 15.6 finding 3 — humans, never raw ids)", () => {
  it("categorizes prefixed provenance ids into human counts + labels", () => {
    const p = mk({
      measures: [
        {
          display_name: "a", expr: "SUM(x)", dedup_fingerprint: "m1", recurrence: 3, provenance_count: 3,
          benchmark_question_ids: ["sql_snippet:measures:01f13", "trusted_asset:t1", "q_bare"],
        },
        {
          display_name: "b", expr: "COUNT(1)", dedup_fingerprint: "m2", recurrence: 2, provenance_count: 2,
          benchmark_question_ids: ["sql_snippet:measures:02a90", "gso_patch:p1"],
        },
      ],
    })
    const { chips, rawIds } = evidenceSummary(p)
    const byLabel = Object.fromEntries(chips.map((c) => [c.label, c.count]))
    expect(byLabel["curated snippets"]).toBe(2)
    expect(byLabel["trusted asset"]).toBe(1)
    expect(byLabel["generated-SQL match"]).toBe(1)
    expect(byLabel["curated query"]).toBe(1) // the one bare id
    // The raw ids are preserved (behind the details disclosure), sorted.
    expect(rawIds).toEqual([
      "gso_patch:p1",
      "q_bare",
      "sql_snippet:measures:01f13",
      "sql_snippet:measures:02a90",
      "trusted_asset:t1",
    ])
  })

  it("falls back to bundle-level evidence question ids when members carry none", () => {
    const p = mk({ measures: [], evidence: { benchmark_question_ids: ["x", "y"] } })
    const { chips, rawIds } = evidenceSummary(p)
    expect(rawIds).toEqual(["x", "y"])
    expect(chips).toEqual([{ label: "curated queries", count: 2 }])
  })

  it("returns no chips and no raw ids when there is no evidence", () => {
    expect(evidenceSummary(mk({ measures: [], evidence: null }))).toEqual({ chips: [], rawIds: [] })
  })
})

describe("rankProposals + recommendedReason (Prompt 15.6 finding 4)", () => {
  it("orders by tier, then coverage, then curated-query count, then id", () => {
    const lowBig = mk({ suggestion_id: "lowBig", tier: "LOW", measures: [
      { display_name: "a", expr: "x", dedup_fingerprint: "1", recurrence: 1, provenance_count: 1, benchmark_question_ids: [] },
      { display_name: "b", expr: "y", dedup_fingerprint: "2", recurrence: 1, provenance_count: 1, benchmark_question_ids: [] },
    ] })
    const medSmall = mk({ suggestion_id: "medSmall", tier: "MEDIUM", measures: [
      { display_name: "a", expr: "x", dedup_fingerprint: "1", recurrence: 1, provenance_count: 1, benchmark_question_ids: [] },
    ] })
    const hiA = mk({ suggestion_id: "hiA", tier: "HIGH", measures: [
      { display_name: "a", expr: "x", dedup_fingerprint: "1", recurrence: 1, provenance_count: 1, benchmark_question_ids: [] },
    ] })
    const hiB = mk({ suggestion_id: "hiB", tier: "HIGH", measures: [
      { display_name: "a", expr: "x", dedup_fingerprint: "1", recurrence: 1, provenance_count: 1, benchmark_question_ids: [] },
    ] })
    const ranked = rankProposals([lowBig, medSmall, hiB, hiA])
    // HIGH beats coverage: hiA/hiB (tie on tier+coverage+queries) break by id.
    expect(ranked.map((p) => p.suggestion_id)).toEqual(["hiA", "hiB", "medSmall", "lowBig"])
  })

  it("does not mutate the input array", () => {
    const input = [mk({ suggestion_id: "b", tier: "LOW" }), mk({ suggestion_id: "a", tier: "HIGH" })]
    const snapshot = input.map((p) => p.suggestion_id)
    rankProposals(input)
    expect(input.map((p) => p.suggestion_id)).toEqual(snapshot)
  })

  it("recommendedReason assembles facts only — no tier word, no 'confidence', no percent (MV-D35)", () => {
    const p = mk({ tier: "HIGH", measures: [
      { display_name: "a", expr: "x", dedup_fingerprint: "1", recurrence: 1, provenance_count: 1, benchmark_question_ids: ["q1", "q2"] },
    ] })
    expect(recommendedReason(p)).toBe(
      "Strongest candidate — governs 1 measure, recurs across 2 curated queries.",
    )
  })

  it("recommendedReason degrades to a generic line with no facts", () => {
    expect(recommendedReason(mk({ tier: null, measures: [] }))).toBe(
      "Strongest candidate for this Agent.",
    )
  })
})

describe("factsChecks (MV-D35 facts row — gated on real gates)", () => {
  it("renders each check present in proposal.checks, in fixed order", () => {
    const p = mk({ checks: { validated: "PASS", executable: "PASS", no_overlap: "PASS" } })
    expect(factsChecks(p).map((f) => f.key)).toEqual(["validated", "executable", "no_overlap"])
    expect(factsChecks(p).map((f) => f.label)).toEqual([
      "validated",
      "executable",
      "no overlap with existing metric views",
    ])
  })

  it("renders ONLY the checks whose gate ran — a check that lies is worse than the percent", () => {
    // A row that proves validated/executable but carries an overlap conflict:
    // the backend omits no_overlap, so the facts row must not claim it.
    const p = mk({ checks: { validated: "PASS", executable: "PASS" } })
    expect(factsChecks(p).map((f) => f.key)).toEqual(["validated", "executable"])
  })

  it("renders nothing when the row proves no gate (legacy / no checks)", () => {
    expect(factsChecks(mk({ checks: null }))).toEqual([])
    expect(factsChecks(mk({ checks: {} }))).toEqual([])
  })

  it("ignores a non-PASS value — the facts row states passing gates only", () => {
    const p = mk({ checks: { validated: "PASS", no_overlap: "WARN" } })
    expect(factsChecks(p).map((f) => f.key)).toEqual(["validated"])
  })
})

describe("orthogonalityCallout (MV-D35 — callout instead of a forced ranking)", () => {
  const withMeasures = (id: string, fps: string[]) =>
    mk({
      suggestion_id: id,
      measures: fps.map((fp) => ({
        display_name: fp, expr: fp, dedup_fingerprint: fp,
        recurrence: 1, provenance_count: 1, benchmark_question_ids: null,
      })),
    })

  it("fires when 2+ proposals govern pairwise-disjoint measure sets", () => {
    const a = withMeasures("a", ["m1", "m2"])
    const b = withMeasures("b", ["m3"])
    expect(orthogonalityCallout([a, b])).toBe(
      "All 2 are independent — any or all can be created.",
    )
  })

  it("stays null when any measure is shared (a real ranking exists)", () => {
    const a = withMeasures("a", ["m1", "m2"])
    const b = withMeasures("b", ["m2", "m3"])
    expect(orthogonalityCallout([a, b])).toBeNull()
  })

  it("stays null for a single proposal (nothing to be independent of)", () => {
    expect(orthogonalityCallout([withMeasures("a", ["m1"])])).toBeNull()
  })
})

describe("stageProgressFraction (Prompt 15.6 finding 8)", () => {
  it("equal weights: completed stages full, active half", () => {
    // 4 equal stages; on stage 0 the bar is at half of one segment = 1/8.
    expect(stageProgressFraction(4, 0)).toBeCloseTo(0.125)
    // On stage 2: two done (0.5) + half of the third (0.125) = 0.625.
    expect(stageProgressFraction(4, 2)).toBeCloseTo(0.625)
  })

  it("weights bias the bar toward where the time actually goes", () => {
    // A dominant stage-2 (scoring) means finishing stage 1 barely moves the bar.
    const weights = [1, 1, 10, 1]
    // On stage 2 (the long one): done = 2/13, active half = 5/13 → 7/13.
    expect(stageProgressFraction(4, 2, weights)).toBeCloseTo(7 / 13)
  })

  it("ignores malformed weights (wrong length or non-positive) → equal", () => {
    expect(stageProgressFraction(4, 0, [1, 2])).toBeCloseTo(0.125)
    expect(stageProgressFraction(4, 0, [0, 0, 0, 0])).toBeCloseTo(0.125)
  })

  it("clamps out-of-range stage indices and guards zero stages", () => {
    expect(stageProgressFraction(0, 3)).toBe(0)
    expect(stageProgressFraction(4, 99)).toBeCloseTo(0.875) // clamps to last stage
  })
})

describe("MV_DEFAULT_VISIBLE", () => {
  it("is a small, positive default the card list and its test share", () => {
    expect(MV_DEFAULT_VISIBLE).toBeGreaterThan(0)
    expect(MV_DEFAULT_VISIBLE).toBeLessThanOrEqual(5)
  })
})

describe("confidenceDisplay (Prompt 15.7 / MV-D32(1) — coverage-aware, blend untouched)", () => {
  it("keeps the number, captions evidence-poor as evidence-poor (fresh table)", () => {
    // L and D UNAVAILABLE on a fresh table: only curated SQL (Y) + semantic (S).
    const p = mk({
      confidence_score: 34,
      tier: "LOW",
      score_components: { statuses: { L: "UNAVAILABLE", Y: "COMPUTED", S: "COMPUTED", D: "UNAVAILABLE" } },
    })
    const d = confidenceDisplay(p)
    expect(d.percent).toBe(34) // number is unchanged — the blend is byte-untouched
    expect(d.evidencePoor).toBe(true)
    expect(d.caption).toBe("Based on curated SQL only — no usage history yet.")
  })

  it("captions an evidence-rich proposal as backed by usage + lineage — CONTRIBUTION, not just execution (MV-D35 fix #4)", () => {
    const p = mk({
      confidence_score: 88,
      // L and D both COMPUTED AND carrying a value above the floor — they truly
      // contribute, so the caption may claim them.
      score_components: { L: 0.6, D: 0.5, statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" } },
    })
    const d = confidenceDisplay(p)
    expect(d.evidencePoor).toBe(false)
    expect(d.caption).toBe("Backed by usage history and lineage.")
  })

  it("COMPUTED≠SUPPORTIVE: a usage/lineage signal that RAN but measured zero does NOT back the caption (MV-D35 fix #4)", () => {
    // Both producers ran (COMPUTED / EMPTY) but measured nothing — value 0. The
    // pre-15.8 defect captioned this "backed by usage history"; the fix treats
    // execution-without-contribution as evidence-poor.
    const ranZero = mk({
      confidence_score: 40,
      score_components: { L: 0, D: 0, statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "EMPTY" } },
    })
    expect(confidenceDisplay(ranZero).evidencePoor).toBe(true)
  })

  it("says nothing when score_components carries no statuses — never asserts a basis it didn't read", () => {
    expect(confidenceDisplay(mk({ score_components: null })).caption).toBeNull()
    expect(confidenceDisplay(mk({ confidence_score: null, score_components: null })).percent).toBeNull()
  })
})

describe("evidenceGrowth (Prompt 15.7 / MV-D32(3) — cross-surface enrichment, no fabrication)", () => {
  it("a cold scan-only proposal shows NO growth (Y+S only, no history/lineage)", () => {
    const p = mk({ score_components: { statuses: { L: "UNAVAILABLE", Y: "COMPUTED", S: "COMPUTED", D: "UNAVAILABLE" } } })
    expect(evidenceGrowth(p)).toEqual([])
  })

  it("surfaces the enrichment a GSO run added — signals a scan cannot produce", () => {
    const p = mk({
      score_components: { statuses: { L: "COMPUTED", Y: "COMPUTED", S: "COMPUTED", D: "COMPUTED" } },
      evidence: { query_history_statement_ids: ["s1", "s2"] },
    })
    expect(evidenceGrowth(p)).toEqual(["generated-SQL recurrence", "usage signals", "lineage"])
  })

  it("returns [] when there are no statuses to read (nothing to claim)", () => {
    expect(evidenceGrowth(mk({ score_components: null }))).toEqual([])
  })
})
