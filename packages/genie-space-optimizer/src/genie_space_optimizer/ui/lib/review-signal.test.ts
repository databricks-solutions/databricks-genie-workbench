import { describe, expect, test } from "vitest";

import {
  isQuestionNeedsReview,
  needsReviewCount,
  questionOutcome,
  questionPassed,
  reviewReasonText,
} from "./review-signal";
import type { IterationDetail, QuestionResult } from "./transparency-api";

function q(opts: Partial<QuestionResult> = {}): QuestionResult {
  return {
    questionId: opts.questionId ?? "q1",
    question: opts.question ?? "Question",
    resultCorrectness: opts.resultCorrectness ?? "yes",
    judgeVerdicts: opts.judgeVerdicts ?? {},
    failureTypes: opts.failureTypes ?? [],
    matchType: opts.matchType ?? null,
    expectedSql: opts.expectedSql ?? null,
    generatedSql: opts.generatedSql ?? null,
    assessment: opts.assessment,
    manualAssessment: opts.manualAssessment,
    assessmentReasons: opts.assessmentReasons,
    needsReview: opts.needsReview,
  };
}

function iteration(partial: Partial<IterationDetail> = {}): IterationDetail {
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
    needsReviewCount: 0,
    mlflowRunId: null,
    modelId: null,
    gates: [],
    patches: [],
    reflection: null,
    questions: [],
    quarantinedBenchmarks: [],
    clusterInfo: null,
    timestamp: null,
    leakageCountByType: {},
    firewallRejectionCountByType: {},
    secondaryMiningBlocked: 0,
    synthesisSlotsPersisted: 0,
    arbiterRejectionCount: 0,
    clusterFallbackToInstructionCount: 0,
    synthesisArchetypeDistribution: {},
    ...partial,
  };
}

describe("review signal helpers", () => {
  test("NEEDS_REVIEW is a third state, not a failed outcome", () => {
    const review = q({
      resultCorrectness: "no",
      assessment: "NEEDS_REVIEW",
      manualAssessment: true,
      assessmentReasons: ["LLM_JUDGE_OTHER"],
    });

    expect(isQuestionNeedsReview(review)).toBe(true);
    expect(questionPassed(review)).toBe(false);
    expect(questionOutcome(review)).toBe("review");
    expect(reviewReasonText(review)).toBe("LLM_JUDGE_OTHER");
  });

  test("manualAssessment and needsReview booleans also trigger review state", () => {
    expect(questionOutcome(q({ manualAssessment: true }))).toBe("review");
    expect(questionOutcome(q({ needsReview: true }))).toBe("review");
  });

  test("ordinary pass and fail outcomes remain binary", () => {
    expect(questionOutcome(q({ resultCorrectness: "yes" }))).toBe("pass");
    expect(questionOutcome(q({ resultCorrectness: "no" }))).toBe("fail");
  });

  test("needsReviewCount prefers server count and falls back to row flags", () => {
    expect(needsReviewCount(iteration({ needsReviewCount: 3 }))).toBe(3);
    expect(
      needsReviewCount(
        iteration({
          needsReviewCount: undefined as unknown as number,
          questions: [
            q({ questionId: "q1", assessment: "NEEDS_REVIEW" }),
            q({ questionId: "q2", resultCorrectness: "no" }),
          ],
        }),
      ),
    ).toBe(1);
  });
});
