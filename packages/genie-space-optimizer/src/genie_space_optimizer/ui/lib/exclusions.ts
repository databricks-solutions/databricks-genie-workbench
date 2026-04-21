import type {
  ExclusionReasonCode,
  IterationDetail,
  QuarantinedBenchmark,
  QuestionResult,
} from "./transparency-api";

/**
 * Bug #3 — Maps stable server-side exclusion reason codes to UI-facing copy.
 *
 * Kept in its own module so it can be unit-tested without pulling in React,
 * Radix, or the rest of IterationExplorer's JSX tree. New codes added by the
 * server must round-trip safely; we fall through to the raw code string so
 * the user still sees *something* diagnostic rather than a silent blank.
 */
export function humanizeExclusionReason(
  code: ExclusionReasonCode | null | undefined,
): string {
  switch (code) {
    case "gt_excluded":
      return "Ground truth marked as excluded";
    case "both_empty":
      return "Ground truth and Genie both empty";
    case "genie_result_unavailable":
      return "Genie result unavailable (query did not run)";
    case "quarantined":
      return "Benchmark failed pre-evaluation validation";
    case "temporal_stale":
      return "Question is time-sensitive and ground truth is stale";
    case null:
    case undefined:
    case "":
      return "Excluded";
    default:
      return code;
  }
}

/**
 * Bug #2 / #3 — single source of truth for the Baseline view's denominator
 * and exclusion partitioning.
 *
 * Returns pre-computed values the UI renders directly, so every caller
 * (BaselineView now, potentially other iteration views later) agrees on the
 * same math and a future regression is caught by unit tests rather than by a
 * customer filing the ticket again.
 */
export interface BaselineCounts {
  /** Denominator of overall_accuracy (excludes runtime exclusions). */
  evaluated: number;
  /** Runtime exclusions (ground-truth excluded, both_empty, genie_unavailable, temporal_stale). */
  excludedAtRuntime: number;
  /** Pre-evaluation quarantine (benchmarks removed before mlflow.genai.evaluate). */
  quarantined: number;
  /** Sum of excludedAtRuntime + quarantined — what the "muted" note reports. */
  totalExcluded: number;
  /** Questions that entered evaluation (drive the accuracy number). */
  evaluatedQuestions: QuestionResult[];
  /** Questions flagged excluded by the server — shown in the drill-down. */
  excludedQuestions: QuestionResult[];
  /** Pre-evaluation quarantine payload — shown in the drill-down. */
  quarantinedBenchmarks: QuarantinedBenchmark[];
}

export function computeBaselineCounts(
  iteration: IterationDetail,
): BaselineCounts {
  // Use `??` (not `||`) so a legitimate 0 — every benchmark excluded or
  // quarantined — is trusted as the denominator rather than silently
  // replaced with totalQuestions (which would reintroduce Bug #2).
  // `||` also fires on legacy TS responses where the field is missing
  // entirely; `??` preserves that behavior only for null/undefined.
  const evaluated = iteration.evaluatedCount ?? iteration.totalQuestions;
  const excludedAtRuntime = iteration.excludedCount ?? 0;
  const quarantined = iteration.quarantinedCount ?? 0;
  const totalExcluded = excludedAtRuntime + quarantined;

  const evaluatedQuestions = iteration.questions.filter((q) => !q.excluded);
  const excludedQuestions = iteration.questions.filter((q) => q.excluded);
  const quarantinedBenchmarks = iteration.quarantinedBenchmarks || [];

  return {
    evaluated,
    excludedAtRuntime,
    quarantined,
    totalExcluded,
    evaluatedQuestions,
    excludedQuestions,
    quarantinedBenchmarks,
  };
}
