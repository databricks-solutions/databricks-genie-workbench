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
  evidenceSummary,
  isLowConfidence,
  MV_DEFAULT_VISIBLE,
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
    proposed_object: "c.s.v",
    measures: [],
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

  it("recommendedReason assembles the same facts the ranking used", () => {
    const p = mk({ tier: "HIGH", measures: [
      { display_name: "a", expr: "x", dedup_fingerprint: "1", recurrence: 1, provenance_count: 1, benchmark_question_ids: ["q1", "q2"] },
    ] })
    expect(recommendedReason(p)).toBe(
      "Strongest candidate — high confidence, governs 1 measure, recurs across 2 curated queries.",
    )
  })

  it("recommendedReason degrades to a generic line with no facts", () => {
    expect(recommendedReason(mk({ tier: null, measures: [] }))).toBe(
      "Strongest candidate for this Agent.",
    )
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
