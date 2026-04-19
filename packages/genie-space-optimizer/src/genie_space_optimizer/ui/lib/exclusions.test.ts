/**
 * Unit tests for the Baseline-view exclusion & denominator helpers.
 *
 * These tests protect Bug #2 (denominator mismatch) and Bug #3 (excluded
 * items with no explanation) from regressing at the UI layer. The server
 * side is covered by tests/unit/test_arbiter_adjusted_accuracy.py and
 * tests/unit/test_iteration_api_contract.py — this file locks the mirror
 * contract on the client.
 */

import { describe, expect, test } from "vitest";

import {
  computeBaselineCounts,
  humanizeExclusionReason,
} from "./exclusions";
import type {
  IterationDetail,
  QuarantinedBenchmark,
  QuestionResult,
} from "./transparency-api";

// ── Test fixtures ─────────────────────────────────────────────────────

function q(
  questionId: string,
  opts: Partial<QuestionResult> = {},
): QuestionResult {
  return {
    questionId,
    question: opts.question ?? `Question ${questionId}`,
    resultCorrectness: opts.resultCorrectness ?? "yes",
    judgeVerdicts: opts.judgeVerdicts ?? {},
    failureTypes: opts.failureTypes ?? [],
    matchType: opts.matchType ?? null,
    expectedSql: opts.expectedSql ?? null,
    generatedSql: opts.generatedSql ?? null,
    excluded: opts.excluded,
    exclusionReasonCode: opts.exclusionReasonCode,
    exclusionReasonDetail: opts.exclusionReasonDetail,
  };
}

function iteration(
  partial: Partial<IterationDetail> & {
    questions?: QuestionResult[];
    quarantinedBenchmarks?: QuarantinedBenchmark[];
  } = {},
): IterationDetail {
  return {
    iteration: 0,
    agId: null,
    status: "baseline",
    overallAccuracy: 0,
    judgeScores: {},
    totalQuestions: 0,
    evaluatedCount: 0,
    correctCount: 0,
    excludedCount: 0,
    quarantinedCount: 0,
    mlflowRunId: null,
    modelId: null,
    gates: [],
    patches: [],
    reflection: null,
    questions: [],
    quarantinedBenchmarks: [],
    clusterInfo: null,
    timestamp: null,
    ...partial,
  };
}

// ── humanizeExclusionReason ──────────────────────────────────────────

describe("humanizeExclusionReason", () => {
  test("maps gt_excluded to user-friendly copy", () => {
    expect(humanizeExclusionReason("gt_excluded")).toBe(
      "Ground truth marked as excluded",
    );
  });

  test("maps both_empty", () => {
    expect(humanizeExclusionReason("both_empty")).toBe(
      "Ground truth and Genie both empty",
    );
  });

  test("maps genie_result_unavailable", () => {
    expect(humanizeExclusionReason("genie_result_unavailable")).toBe(
      "Genie result unavailable (query did not run)",
    );
  });

  test("maps quarantined", () => {
    expect(humanizeExclusionReason("quarantined")).toBe(
      "Benchmark failed pre-evaluation validation",
    );
  });

  test("maps temporal_stale", () => {
    expect(humanizeExclusionReason("temporal_stale")).toBe(
      "Question is time-sensitive and ground truth is stale",
    );
  });

  test("empty/null/undefined fall back to generic 'Excluded'", () => {
    expect(humanizeExclusionReason(null)).toBe("Excluded");
    expect(humanizeExclusionReason(undefined)).toBe("Excluded");
    expect(humanizeExclusionReason("")).toBe("Excluded");
  });

  test("unknown code returns the raw code so we never render a blank", () => {
    // Forward-compat: server may add new codes before UI ships. Raw code is
    // diagnostic enough for Support to triage while UI catches up.
    expect(humanizeExclusionReason("some_new_future_code")).toBe(
      "some_new_future_code",
    );
  });

  test("snapshot of the full mapping stays stable", () => {
    const codes = [
      "gt_excluded",
      "both_empty",
      "genie_result_unavailable",
      "quarantined",
      "temporal_stale",
      null,
      undefined,
      "",
      "unknown_future",
    ] as const;

    const mapping = Object.fromEntries(
      codes.map((c) => [String(c), humanizeExclusionReason(c)]),
    );

    expect(mapping).toMatchInlineSnapshot(`
      {
        "": "Excluded",
        "both_empty": "Ground truth and Genie both empty",
        "genie_result_unavailable": "Genie result unavailable (query did not run)",
        "gt_excluded": "Ground truth marked as excluded",
        "null": "Excluded",
        "quarantined": "Benchmark failed pre-evaluation validation",
        "temporal_stale": "Question is time-sensitive and ground truth is stale",
        "undefined": "Excluded",
        "unknown_future": "unknown_future",
      }
    `);
  });
});

// ── computeBaselineCounts ─────────────────────────────────────────────

describe("computeBaselineCounts — Bug #2 denominator contract", () => {
  test("uses evaluatedCount as the denominator, not totalQuestions", () => {
    // The scenario from the bug ticket: 14 total, 2 excluded, 12 correct of
    // the remaining 12. The card must show 12/12, not 12/14.
    const it = iteration({
      totalQuestions: 14,
      evaluatedCount: 12,
      correctCount: 12,
      excludedCount: 2,
      overallAccuracy: 100.0,
    });
    const counts = computeBaselineCounts(it);
    expect(counts.evaluated).toBe(12);
    expect(counts.totalExcluded).toBe(2);
  });

  test("falls back to totalQuestions when evaluatedCount is 0 (legacy rows)", () => {
    const it = iteration({
      totalQuestions: 14,
      evaluatedCount: 0,
      correctCount: 12,
    });
    const counts = computeBaselineCounts(it);
    expect(counts.evaluated).toBe(14);
  });

  test("totalExcluded sums runtime + quarantined", () => {
    const it = iteration({
      totalQuestions: 20,
      evaluatedCount: 15,
      excludedCount: 3,
      quarantinedCount: 2,
    });
    const counts = computeBaselineCounts(it);
    expect(counts.totalExcluded).toBe(5);
  });

  test("undefined excluded/quarantined counts default to zero", () => {
    const it = iteration({
      totalQuestions: 10,
      evaluatedCount: 10,
      // excludedCount & quarantinedCount intentionally default to 0
    });
    const counts = computeBaselineCounts(it);
    expect(counts.totalExcluded).toBe(0);
  });
});

describe("computeBaselineCounts — Bug #3 question partitioning", () => {
  test("splits questions into evaluated vs excluded buckets", () => {
    const it = iteration({
      totalQuestions: 4,
      evaluatedCount: 2,
      excludedCount: 2,
      questions: [
        q("q1"),
        q("q2"),
        q("q3", {
          excluded: true,
          exclusionReasonCode: "gt_excluded",
          exclusionReasonDetail: "Ground truth marked excluded",
        }),
        q("q4", {
          excluded: true,
          exclusionReasonCode: "both_empty",
        }),
      ],
    });
    const counts = computeBaselineCounts(it);
    expect(counts.evaluatedQuestions.map((x) => x.questionId)).toEqual([
      "q1",
      "q2",
    ]);
    expect(counts.excludedQuestions.map((x) => x.questionId)).toEqual([
      "q3",
      "q4",
    ]);
    expect(counts.excludedQuestions[0].exclusionReasonCode).toBe("gt_excluded");
  });

  test("echoes quarantinedBenchmarks array through unchanged", () => {
    const quar: QuarantinedBenchmark[] = [
      {
        questionId: "qA",
        question: "Bad question",
        reasonCode: "quarantined",
        reasonDetail: "Column not found",
      },
    ];
    const it = iteration({ quarantinedBenchmarks: quar });
    expect(computeBaselineCounts(it).quarantinedBenchmarks).toEqual(quar);
  });

  test("handles empty questions array safely", () => {
    const counts = computeBaselineCounts(iteration());
    expect(counts.evaluatedQuestions).toEqual([]);
    expect(counts.excludedQuestions).toEqual([]);
    expect(counts.quarantinedBenchmarks).toEqual([]);
    expect(counts.totalExcluded).toBe(0);
  });

  test("drill-down snapshot: runtime exclusions + pre-eval quarantine together", () => {
    // This is the "UI snapshot" guard the plan calls for: if the shape of
    // what BaselineView renders in its "Excluded from baseline" section
    // changes, this test fires. We snapshot the pure data shape rather than
    // the JSX (no JSDOM) — the JSX is a thin wrapper over this data.
    const it = iteration({
      totalQuestions: 5,
      evaluatedCount: 3,
      correctCount: 3,
      excludedCount: 1,
      quarantinedCount: 1,
      overallAccuracy: 100.0,
      questions: [
        q("q1"),
        q("q2"),
        q("q3"),
        q("q_excluded", {
          excluded: true,
          exclusionReasonCode: "genie_result_unavailable",
          exclusionReasonDetail: "Timeout after 30s",
        }),
      ],
      quarantinedBenchmarks: [
        {
          questionId: "q_quarantined",
          question: "Bad query",
          reasonCode: "quarantined",
          reasonDetail: "EXPLAIN failed: table not found",
        },
      ],
    });
    const counts = computeBaselineCounts(it);

    const drillDown = {
      evaluated: counts.evaluated,
      totalExcluded: counts.totalExcluded,
      runtime: counts.excludedQuestions.map((x) => ({
        id: x.questionId,
        reason: humanizeExclusionReason(x.exclusionReasonCode),
        detail: x.exclusionReasonDetail,
      })),
      preEval: counts.quarantinedBenchmarks.map((x) => ({
        id: x.questionId,
        reason: humanizeExclusionReason(x.reasonCode),
        detail: x.reasonDetail,
      })),
    };

    expect(drillDown).toMatchInlineSnapshot(`
      {
        "evaluated": 3,
        "preEval": [
          {
            "detail": "EXPLAIN failed: table not found",
            "id": "q_quarantined",
            "reason": "Benchmark failed pre-evaluation validation",
          },
        ],
        "runtime": [
          {
            "detail": "Timeout after 30s",
            "id": "q_excluded",
            "reason": "Genie result unavailable (query did not run)",
          },
        ],
        "totalExcluded": 2,
      }
    `);
  });
});
