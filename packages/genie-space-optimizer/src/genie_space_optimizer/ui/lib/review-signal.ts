import type { IterationDetail, QuestionResult } from "./transparency-api";

export type QuestionOutcome = "pass" | "fail" | "review" | "unknown";

export function isQuestionNeedsReview(
  question: QuestionResult | undefined,
): boolean {
  if (!question) return false;
  const assessment = question.assessment?.trim().toUpperCase();
  return (
    question.needsReview === true ||
    question.manualAssessment === true ||
    assessment === "NEEDS_REVIEW"
  );
}

export function questionPassed(question: QuestionResult | undefined): boolean {
  if (!question || isQuestionNeedsReview(question)) return false;
  const arbiter = question.judgeVerdicts?.arbiter;
  if (arbiter === "both_correct" || arbiter === "genie_correct") return true;
  return question.resultCorrectness === "yes" || question.resultCorrectness === "pass";
}

export function questionOutcome(
  question: QuestionResult | undefined,
): QuestionOutcome {
  if (!question) return "unknown";
  if (isQuestionNeedsReview(question)) return "review";
  return questionPassed(question) ? "pass" : "fail";
}

export function needsReviewCount(iteration: IterationDetail): number {
  return (
    iteration.needsReviewCount ??
    iteration.questions.filter((q) => isQuestionNeedsReview(q)).length
  );
}

export function reviewReasonText(question: QuestionResult): string {
  const reasons = question.assessmentReasons?.filter(Boolean) ?? [];
  return reasons.length > 0 ? reasons.join(", ") : "Native benchmark marked NEEDS_REVIEW";
}
