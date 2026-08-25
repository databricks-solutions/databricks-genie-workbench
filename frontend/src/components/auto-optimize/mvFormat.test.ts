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
  isLowConfidence,
  proposalGainSentence,
  splitProposalsByConfidence,
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
