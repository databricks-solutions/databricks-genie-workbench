import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import type { UseQueryOptions, UseSuspenseQueryOptions } from "@tanstack/react-query";

class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new ApiError(res.status, `HTTP ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

// ── Types ────────────────────────────────────────────────────────────

export interface AsiResult {
  questionId: string;
  judge: string;
  value: string;
  failureType: string | null;
  severity: string | null;
  confidence: number | null;
  blameSet: string[];
  counterfactualFix: string | null;
  wrongClause: string | null;
  expectedValue: string | null;
  actualValue: string | null;
}

export interface AsiSummary {
  runId: string;
  iteration: number;
  totalResults: number;
  passCount: number;
  failCount: number;
  failureTypeDistribution: Record<string, number>;
  blameDistribution: Record<string, number>;
  judgePassRates: Record<string, number>;
  results: AsiResult[];
}

export interface ProvenanceRecord {
  questionId: string;
  signalType: string;
  judge: string;
  judgeVerdict: string;
  resolvedRootCause: string;
  resolutionMethod: string;
  blameSet: string[];
  counterfactualFix: string | null;
  clusterId: string;
  proposalId: string | null;
  patchType: string | null;
  gateType: string | null;
  gateResult: string | null;
}

export interface ProvenanceSummary {
  runId: string;
  iteration: number;
  lever: number;
  totalRecords: number;
  clusterCount: number;
  proposalCount: number;
  rootCauseDistribution: Record<string, number>;
  gateResults: Record<string, number>;
  records: ProvenanceRecord[];
}

export interface IterationSummary {
  iteration: number;
  lever: number | null;
  evalScope: string;
  overallAccuracy: number;
  // Bug #2 denominator contract. Prefer evaluatedCount for UI math
  // (matches overallAccuracy). totalQuestions is kept for back-compat.
  totalQuestions: number;
  evaluatedCount: number;
  correctCount: number;
  excludedCount: number;
  quarantinedCount: number;
  repeatabilityPct: number | null;
  thresholdsMet: boolean;
  judgeScores: Record<string, number | null>;
  // Bug #4 — benchmark leakage observability.
  leakageCountByType: Record<string, number>;
  firewallRejectionCountByType: Record<string, number>;
  secondaryMiningBlocked: number;
  // Bug #4 Phase 3 — structural synthesis observability.
  synthesisSlotsPersisted: number;
  arbiterRejectionCount: number;
  clusterFallbackToInstructionCount: number;
  synthesisArchetypeDistribution: Record<string, number>;
}

// Stable enum mirroring EXCLUSION_* codes in evaluation.py. Extended on the
// server side first; the UI must accept unknown strings gracefully.
export type ExclusionReasonCode =
  | "gt_excluded"
  | "both_empty"
  | "genie_result_unavailable"
  | "quarantined"
  | "temporal_stale"
  | string;

export interface QuestionResult {
  questionId: string;
  question: string;
  resultCorrectness: string | null;
  judgeVerdicts: Record<string, string>;
  failureTypes: string[];
  matchType: string | null;
  expectedSql: string | null;
  generatedSql: string | null;
  // Bug #3 — runtime exclusion metadata.
  excluded?: boolean;
  exclusionReasonCode?: ExclusionReasonCode | null;
  exclusionReasonDetail?: string | null;
}

export interface QuarantinedBenchmark {
  questionId: string;
  question: string;
  reasonCode: ExclusionReasonCode;
  reasonDetail: string;
}

export interface GateResultEntry {
  gateName: string;
  accuracy: number | null;
  totalQuestions: number | null;
  passed: boolean | null;
  mlflowRunId: string | null;
}

export interface ReflectionEntry {
  iteration: number;
  agId: string;
  accepted: boolean;
  action: string;
  levers: number[];
  targetObjects: string[];
  scoreDeltas: Record<string, number>;
  accuracyDelta: number;
  newFailures: string | null;
  rollbackReason: string | null;
  doNotRetry: string[];
  affectedQuestionIds: string[];
  fixedQuestions: string[];
  stillFailing: string[];
  newRegressions: string[];
  reflectionText: string;
  refinementMode: string;
}

export interface IterationDetail {
  iteration: number;
  agId: string | null;
  status: string;
  overallAccuracy: number;
  judgeScores: Record<string, number | null>;
  // Bug #2 denominator contract.
  totalQuestions: number;
  evaluatedCount: number;
  correctCount: number;
  excludedCount: number;
  quarantinedCount: number;
  mlflowRunId: string | null;
  modelId: string | null;
  gates: GateResultEntry[];
  patches: Record<string, unknown>[];
  reflection: ReflectionEntry | null;
  questions: QuestionResult[];
  // Bug #3 — benchmarks removed pre-evaluation.
  quarantinedBenchmarks: QuarantinedBenchmark[];
  clusterInfo: Record<string, unknown> | null;
  timestamp: string | null;
  // Bug #4 — benchmark leakage observability.
  leakageCountByType: Record<string, number>;
  firewallRejectionCountByType: Record<string, number>;
  secondaryMiningBlocked: number;
  // Bug #4 Phase 3 — structural synthesis observability.
  synthesisSlotsPersisted: number;
  arbiterRejectionCount: number;
  clusterFallbackToInstructionCount: number;
  synthesisArchetypeDistribution: Record<string, number>;
}

export interface ProactiveChanges {
  descriptionsEnriched?: number;
  tablesEnriched?: number;
  // Eligibility + silent-drop counts for description enrichment. When
  // ``descriptionsEligible > descriptionsEnriched + descriptionsFailedLlm``
  // the UI can explain the gap (e.g. "2 batches dropped — LLM returned
  // unparseable JSON after retries"). All optional for backward-compat
  // with older servers that don't emit these keys.
  descriptionsEligible?: number;
  descriptionsFailedLlm?: number;
  tablesEligibleForDescription?: number;
  tablesFailedLlm?: number;
  joinSpecsDiscovered?: number;
  spaceDescriptionGenerated?: boolean;
  sampleQuestionsGenerated?: number;
  instructionsSeeded?: boolean;
  promptsMatched?: number;
  exampleSqlsMined?: number;
}

export interface IterationDetailResponse {
  runId: string;
  spaceId: string;
  baselineScore: number | null;
  // Canonical "arbiter adjusted accuracy" headline. Backend guarantees
  // ``optimizedScore >= baselineScore`` and is null while no full-scope
  // iteration > 0 has been evaluated.
  optimizedScore: number | null;
  // ``0`` means baseline retained (no iter > 0 strictly improved on it,
  // or optimization is still running). ``N > 0`` is the iteration that
  // actually achieved ``optimizedScore``.
  bestIteration: number | null;
  totalIterations: number;
  iterations: IterationDetail[];
  flaggedQuestions: Record<string, unknown>[];
  labelingSessionUrl: string | null;
  proactiveChanges: ProactiveChanges | null;
}

// ── Fetch functions ──────────────────────────────────────────────────

export const getIterationDetail = (runId: string) =>
  fetchJson<IterationDetailResponse>(`/api/genie/runs/${runId}/iteration-detail`);

export const getIterations = (runId: string) =>
  fetchJson<IterationSummary[]>(`/api/genie/runs/${runId}/iterations`);

export const getAsiResults = (runId: string, iteration?: number) => {
  const params = iteration != null ? `?iteration=${iteration}` : "";
  return fetchJson<AsiSummary>(`/api/genie/runs/${runId}/asi-results${params}`);
};

export const getProvenance = (runId: string, iteration?: number, lever?: number) => {
  const parts: string[] = [];
  if (iteration != null) parts.push(`iteration=${iteration}`);
  if (lever != null) parts.push(`lever=${lever}`);
  const qs = parts.length ? `?${parts.join("&")}` : "";
  return fetchJson<ProvenanceSummary[]>(`/api/genie/runs/${runId}/provenance${qs}`);
};

// ── Query keys ───────────────────────────────────────────────────────

export const iterationDetailKey = (runId: string) =>
  ["/api/genie/runs/iteration-detail", runId] as const;

export const iterationsKey = (runId: string) =>
  ["/api/genie/runs/iterations", runId] as const;

export const asiResultsKey = (runId: string, iteration?: number) =>
  ["/api/genie/runs/asi-results", runId, iteration] as const;

export const provenanceKey = (runId: string, iteration?: number, lever?: number) =>
  ["/api/genie/runs/provenance", runId, iteration, lever] as const;

// ── Hooks ────────────────────────────────────────────────────────────

export function useIterationDetail(
  runId: string,
  queryOpts?: Partial<UseQueryOptions<IterationDetailResponse, ApiError>>,
) {
  return useQuery({
    queryKey: iterationDetailKey(runId),
    queryFn: () => getIterationDetail(runId),
    ...queryOpts,
  });
}

export function useIterationDetailSuspense(
  runId: string,
  queryOpts?: Partial<UseSuspenseQueryOptions<IterationDetailResponse, ApiError>>,
) {
  return useSuspenseQuery({
    queryKey: iterationDetailKey(runId),
    queryFn: () => getIterationDetail(runId),
    ...queryOpts,
  });
}

export function useIterations(
  runId: string,
  queryOpts?: Partial<UseQueryOptions<IterationSummary[], ApiError>>,
) {
  return useQuery({
    queryKey: iterationsKey(runId),
    queryFn: () => getIterations(runId),
    ...queryOpts,
  });
}

export function useIterationsSuspense(
  runId: string,
  queryOpts?: Partial<UseSuspenseQueryOptions<IterationSummary[], ApiError>>,
) {
  return useSuspenseQuery({
    queryKey: iterationsKey(runId),
    queryFn: () => getIterations(runId),
    ...queryOpts,
  });
}

export function useAsiResults(
  runId: string,
  iteration?: number,
  queryOpts?: Partial<UseQueryOptions<AsiSummary, ApiError>>,
) {
  return useQuery({
    queryKey: asiResultsKey(runId, iteration),
    queryFn: () => getAsiResults(runId, iteration),
    ...queryOpts,
  });
}

export function useAsiResultsSuspense(
  runId: string,
  iteration?: number,
  queryOpts?: Partial<UseSuspenseQueryOptions<AsiSummary, ApiError>>,
) {
  return useSuspenseQuery({
    queryKey: asiResultsKey(runId, iteration),
    queryFn: () => getAsiResults(runId, iteration),
    ...queryOpts,
  });
}

export function useProvenance(
  runId: string,
  iteration?: number,
  lever?: number,
  queryOpts?: Partial<UseQueryOptions<ProvenanceSummary[], ApiError>>,
) {
  return useQuery({
    queryKey: provenanceKey(runId, iteration, lever),
    queryFn: () => getProvenance(runId, iteration, lever),
    ...queryOpts,
  });
}

export function useProvenanceSuspense(
  runId: string,
  iteration?: number,
  lever?: number,
  queryOpts?: Partial<UseSuspenseQueryOptions<ProvenanceSummary[], ApiError>>,
) {
  return useSuspenseQuery({
    queryKey: provenanceKey(runId, iteration, lever),
    queryFn: () => getProvenance(runId, iteration, lever),
    ...queryOpts,
  });
}
