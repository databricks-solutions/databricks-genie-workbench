import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useGetRun, useGetPendingReviews } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { PipelineStepCard } from "@/components/PipelineStepCard";
import { LeverProgress } from "@/components/LeverProgress";
import { ResourceLinks } from "@/components/ResourceLinks";
import { IterationChart } from "@/components/IterationChart";
import { AsiResultsPanel } from "@/components/AsiResultsPanel";
import { StageTimeline, type StageEvent } from "@/components/StageTimeline";
import { InsightTabs } from "@/components/InsightTabs";
import { IterationExplorer } from "@/components/IterationExplorer";
import { SuggestionsPanel } from "@/components/SuggestionsPanel";
import { useIterationDetail } from "@/lib/transparency-api";
import { Suspense, useEffect, useMemo, useState } from "react";
import {
  ArrowLeft,
  ArrowRight,
  AlertTriangle,
  XCircle,
  Clock,
  Loader2,
  Zap,
  TrendingUp,
  ExternalLink,
  Rocket,
  UserCheck,
  Eye,
  Microscope,
  Lightbulb,
} from "lucide-react";
import { ErrorBoundary } from "react-error-boundary";

export const Route = createFileRoute("/runs/$runId")({
  component: () => (
    <ErrorBoundary fallback={<PipelineError />}>
      <Suspense fallback={<PipelineSkeleton />}>
        <PipelineView />
      </Suspense>
    </ErrorBoundary>
  ),
});

function PipelineError() {
  return (
    <Alert variant="destructive">
      <AlertTitle>Failed to load run</AlertTitle>
      <AlertDescription>
        Could not fetch pipeline status. The run may not exist.
      </AlertDescription>
    </Alert>
  );
}

function PipelineSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-8 w-48" />
      <Skeleton className="h-4 w-full" />
      <div className="space-y-3">
        {Array.from({length: 6}, (_, i) => i + 1).map((i) => (
          <Skeleton key={i} className="h-24 rounded-lg" />
        ))}
      </div>
    </div>
  );
}

const TERMINAL_STATUSES = [
  "COMPLETED", "FAILED", "CANCELLED", "CONVERGED",
  "STALLED", "MAX_ITERATIONS", "APPLIED", "DISCARDED",
];

const STEP_DESCRIPTIONS: Record<number, string> = {
  1: "Reads the Genie Space configuration and queries Unity Catalog for column metadata, tags, and descriptions.",
  2: "Runs all benchmark questions through the Genie API with 9 evaluation judges to establish the current accuracy baseline.",
  3: "Proactively enriches the Genie Space with descriptions, join paths, metadata, instructions, and example SQLs.",
  4: "Applies 5 optimization levers (table/column metadata, metric views, TVFs, join specs, instructions) and evaluates each change.",
  5: "Final evaluation pass on the optimized configuration with repeatability checks to confirm stable improvements.",
  6: "Deploys the optimized configuration to the target environment.",
};

function useElapsedTime(startedAt: string | undefined, isActive: boolean) {
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!isActive || !startedAt) {
      setElapsed(0);
      return;
    }
    const parsed = Date.parse(startedAt);
    if (Number.isNaN(parsed)) {
      setElapsed(0);
      return;
    }
    const tick = () =>
      setElapsed(Math.max(0, Math.floor((Date.now() - parsed) / 1000)));
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startedAt, isActive]);
  return elapsed;
}

function formatElapsed(seconds: number): string {
  const safe = Math.max(0, Math.floor(seconds));
  const h = Math.floor(safe / 3600);
  const m = Math.floor((safe % 3600) / 60);
  const s = safe % 60;
  const pad2 = (v: number) => String(v).padStart(2, "0");
  return `${pad2(h)}:${pad2(m)}:${pad2(s)}`;
}

function statusDisplayLabel(status: string): string {
  const map: Record<string, string> = {
    QUEUED: "Queued",
    RUNNING: "In Progress",
    IN_PROGRESS: "In Progress",
    CONVERGED: "Converged",
    STALLED: "Stalled",
    MAX_ITERATIONS: "Max Iterations Reached",
    FAILED: "Failed",
    CANCELLED: "Cancelled",
    APPLIED: "Applied",
    DISCARDED: "Discarded",
  };
  return map[status] || status;
}

function StatusBanner({
  status,
  elapsed,
  completedSteps,
  totalSteps,
}: {
  status: string;
  elapsed: number;
  completedSteps: number;
  totalSteps: number;
}) {
  if (status === "QUEUED") {
    return (
      <div className="rounded-lg border border-amber-200 bg-amber-50 p-4">
        <div className="flex items-start gap-3">
          <div className="relative mt-0.5">
            <div className="absolute inset-0 animate-ping rounded-full bg-amber-400/30" />
            <Clock className="relative h-5 w-5 text-amber-600" />
          </div>
          <div className="flex-1">
            <h3 className="text-sm font-semibold text-amber-900">
              Queued &mdash; Waiting for Resources
            </h3>
            <p className="mt-0.5 text-xs text-amber-700">
              Your optimization run has been submitted and will start once
              serverless compute is allocated. This typically takes 1-2 minutes.
            </p>
          </div>
          {elapsed > 0 && (
            <Badge variant="outline" className="shrink-0 border-amber-300 text-amber-700">
              {formatElapsed(elapsed)}
            </Badge>
          )}
        </div>
      </div>
    );
  }

  if (status === "RUNNING" || status === "IN_PROGRESS") {
    return (
      <div className="rounded-lg border border-db-blue/20 bg-db-blue/5 p-4">
        <div className="flex items-center gap-3">
          <Loader2 className="h-5 w-5 animate-spin text-db-blue" />
          <div className="flex-1">
            <h3 className="text-sm font-semibold text-db-blue">
              Optimization In Progress
            </h3>
            <p className="text-xs text-db-blue/70">
              Step {completedSteps + 1} of {totalSteps} &middot; Polling for updates every 5 seconds
            </p>
          </div>
          <Badge variant="outline" className="shrink-0 border-db-blue/30 text-db-blue">
            {formatElapsed(elapsed)}
          </Badge>
        </div>
      </div>
    );
  }

  return null;
}

function ReviewBanner({ spaceId }: { spaceId: string }) {
  const { data } = useGetPendingReviews({
    params: { space_id: spaceId },
    query: { enabled: !!spaceId },
  });

  const reviews = data?.data;
  if (!reviews || !reviews.totalPending) return null;

  const parts: string[] = [];
  if (reviews.flaggedQuestions)
    parts.push(`${reviews.flaggedQuestions} flagged question${reviews.flaggedQuestions > 1 ? "s" : ""}`);
  if (reviews.queuedPatches)
    parts.push(`${reviews.queuedPatches} queued patch${reviews.queuedPatches > 1 ? "es" : ""}`);

  const categorized = useMemo(() => {
    const groups = { persistence: 0, tvf: 0, strategist: 0, other: 0 };
    for (const item of reviews.items ?? []) {
      const r = (item.reason ?? "").toLowerCase();
      if (r.includes("persistent") || r.includes("additive_levers_exhausted") || r.includes("consecutive")) {
        groups.persistence++;
      } else if (r.includes("tvf removal") || r.includes("remove_tvf") || r.includes("remove tvf")) {
        groups.tvf++;
      } else if (r.includes("strategist") || r.includes("flag_for_review")) {
        groups.strategist++;
      } else {
        groups.other++;
      }
    }
    return groups;
  }, [reviews.items]);

  const details: string[] = [];
  if (categorized.persistence > 0)
    details.push(`${categorized.persistence} persistently failing`);
  if (categorized.tvf > 0)
    details.push(`${categorized.tvf} TVF removal review${categorized.tvf > 1 ? "s" : ""}`);
  if (categorized.strategist > 0)
    details.push(`${categorized.strategist} strategist-flagged`);

  return (
    <div className="rounded-lg border border-amber-200 bg-amber-50 p-4">
      <div className="flex items-start gap-3">
        <UserCheck className="mt-0.5 h-5 w-5 text-amber-600" />
        <div className="flex-1">
          <h3 className="text-sm font-semibold text-amber-900">
            {reviews.totalPending} item{reviews.totalPending > 1 ? "s" : ""} need
            your review
          </h3>
          <p className="mt-0.5 text-xs text-amber-700">{parts.join(", ")}</p>
          {details.length > 0 && (
            <p className="mt-1 text-xs text-amber-600">{details.join(" · ")}</p>
          )}
        </div>
        {reviews.labelingSessionUrl && (
          <Button
            variant="outline"
            size="sm"
            className="shrink-0 border-amber-300 text-amber-700 hover:bg-amber-100"
            asChild
          >
            <a
              href={reviews.labelingSessionUrl}
              target="_blank"
              rel="noopener noreferrer"
            >
              Open Review Session
              <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
            </a>
          </Button>
        )}
      </div>
    </div>
  );
}

function PipelineView() {
  const { runId } = Route.useParams();
  const navigate = useNavigate();

  const { data: runResponse } = useGetRun({
    params: { run_id: runId },
    query: {
      refetchInterval: (query) => {
        const wrapped = query.state.data as { data?: { status?: string } } | undefined;
        const run = wrapped?.data ?? wrapped;
        const status = (run as { status?: string })?.status;
        if (status && TERMINAL_STATUSES.includes(status)) return false;
        return 5000;
      },
    },
  });

  const run = useMemo(() => {
    if (!runResponse) return null;
    return (runResponse as { data?: unknown }).data ?? runResponse;
  }, [runResponse]) as {
    runId: string;
    spaceId: string;
    spaceName: string;
    status: string;
    startedAt: string;
    steps: {
      stepNumber: number;
      name: string;
      status: string;
      durationSeconds?: number | null;
      summary?: string | null;
      inputs?: Record<string, unknown> | null;
      outputs?: Record<string, unknown> | null;
    }[];
    levers: {
      lever: number;
      name: string;
      status: string;
      patchCount: number;
      scoreBefore?: number | null;
      scoreAfter?: number | null;
      scoreDelta?: number | null;
      rollbackReason?: string | null;
      patches?: {
        patchType?: string;
        scope?: string;
        riskLevel?: string;
        targetObject?: string;
        rolledBack?: boolean;
        rollbackReason?: string | null;
        command?: unknown;
        patch?: unknown;
        appliedAt?: string | null;
      }[];
      iterations?: {
        iteration: number;
        status: string;
        patchCount?: number;
        patchTypes?: string[];
        scoreBefore?: number | null;
        scoreAfter?: number | null;
        scoreDelta?: number | null;
        sliceAccuracy?: number | null;
        p0Accuracy?: number | null;
        fullAccuracy?: number | null;
        totalQuestions?: number | null;
        correctCount?: number | null;
        judgeScores?: Record<string, number | null>;
        mlflowRunId?: string | null;
        rollbackReason?: string | null;
        patches?: {
          patchType?: string;
          scope?: string;
          riskLevel?: string;
          targetObject?: string;
          rolledBack?: boolean;
          rollbackReason?: string | null;
          command?: unknown;
          appliedAt?: string | null;
        }[];
        stageEvents?: {
          stage?: string;
          status?: string;
          durationSeconds?: number | null;
          errorMessage?: string | null;
        }[];
      }[];
    }[];
    baselineScore?: number | null;
    optimizedScore?: number | null;
    convergenceReason?: string | null;
    links?: { label: string; url: string; category: string }[];
    deploymentJobStatus?: string | null;
    deploymentJobUrl?: string | null;
  } | null;

  const isActive = run
    ? !TERMINAL_STATUSES.includes(run.status)
    : false;
  const elapsed = useElapsedTime(run?.startedAt, isActive);
  const workflowLink = run?.links?.find((link) => link.category === "job")?.url;

  const allStageEvents: StageEvent[] = useMemo(() => {
    if (!run) return [];
    const events: StageEvent[] = [];
    for (const step of run.steps) {
      const stepEvents = (step.outputs as Record<string, unknown> | null)
        ?.stageEvents as StageEvent[] | undefined;
      if (stepEvents) events.push(...stepEvents);
    }
    for (const lever of run.levers) {
      for (const iter of lever.iterations ?? []) {
        const iterEvents = iter.stageEvents as StageEvent[] | undefined;
        if (iterEvents) events.push(...iterEvents);
      }
    }
    return events;
  }, [run]);

  const availableIterations: number[] = useMemo(() => {
    if (!run) return [0];
    const iters = new Set<number>([0]);
    for (const lever of run.levers) {
      for (const iter of lever.iterations ?? []) {
        iters.add(iter.iteration);
      }
    }
    return Array.from(iters).sort((a, b) => a - b);
  }, [run]);

  if (!run) return null;

  const completedSteps = run.steps.filter(
    (s) => s.status === "completed",
  ).length;
  const runningStep = run.steps.find((s) => s.status === "running");
  const stepWeight = Math.round(100 / run.steps.length);
  const progressPct = run.steps.reduce((acc, s) => {
    if (s.status === "completed") return acc + stepWeight;
    if (s.status === "running") return acc + Math.round(stepWeight / 2);
    return acc;
  }, 0);
  const isCompleted = ["COMPLETED", "CONVERGED", "STALLED", "MAX_ITERATIONS"].includes(run.status);
  const isFailed = run.status === "FAILED";
  const isCancelled = run.status === "CANCELLED";

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <button
            className="mb-2 flex items-center gap-1 text-sm text-muted hover:text-primary"
            onClick={() =>
              navigate({
                to: "/spaces/$spaceId",
                params: { spaceId: run.spaceId },
              })
            }
          >
            <ArrowLeft className="h-4 w-4" />
            Back to {run.spaceName || "Space"}
          </button>
          <div className="flex items-center gap-3">
            <h1 className="text-2xl font-bold">Optimization Pipeline</h1>
            <Badge
              variant="outline"
              className={
                isActive
                  ? "border-db-blue/30 text-db-blue"
                  : isCompleted
                    ? "border-db-green/30 text-db-green"
                    : isFailed
                      ? "border-db-red-error/30 text-db-red-error"
                      : "text-muted"
              }
            >
              {isActive && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
              {statusDisplayLabel(run.status)}
            </Badge>
            {run.deploymentJobStatus && (
              <DeploymentBadge
                status={run.deploymentJobStatus}
                url={run.deploymentJobUrl}
              />
            )}
          </div>
          <p className="text-sm text-muted">
            Run <code className="rounded bg-elevated px-1 py-0.5 font-mono text-xs">{run.runId.slice(0, 8)}</code>
            {run.startedAt && (
              <> &middot; Started {new Date(run.startedAt).toLocaleString()}</>
            )}
          </p>
        </div>
        {workflowLink && (
          <Button variant="outline" size="sm" asChild>
            <a href={workflowLink} target="_blank" rel="noopener noreferrer">
              Open in Workflows
              <ExternalLink className="ml-2 h-3.5 w-3.5" />
            </a>
          </Button>
        )}
      </div>

      {/* Status Banner */}
      <StatusBanner
        status={run.status}
        elapsed={elapsed}
        completedSteps={completedSteps}
        totalSteps={run.steps.length}
      />

      {/* Review Banner (only for terminal runs) */}
      {(isCompleted || isFailed) && <ReviewBanner spaceId={run.spaceId} />}

      {/* Progress */}
      <div className="space-y-2">
        <div className="flex items-center justify-between text-xs text-muted">
          <span className="flex items-center gap-1.5">
            {isActive && <Loader2 className="h-3 w-3 animate-spin" />}
            {isActive
              ? runningStep
                ? `Running: ${runningStep.name}`
                : "Waiting to start…"
              : isCompleted
                ? "All steps complete"
                : "Progress"}
          </span>
          <span className="tabular-nums font-medium">
            {completedSteps}/{run.steps.length} steps
          </span>
        </div>
        <div className="relative">
          <Progress
            value={progressPct}
            className={`h-2.5 ${isActive ? "[&>div]:bg-db-blue" : isCompleted ? "[&>div]:bg-db-green" : ""}`}
          />
          {isActive && progressPct > 0 && progressPct < 100 && (
            <div
              className="absolute top-0 h-2.5 animate-pulse rounded-full bg-db-blue/30"
              style={{ width: `${progressPct}%` }}
            />
          )}
        </div>
      </div>

      {/* Score Summary Cards (shown when completed) */}
      {isCompleted &&
        run.baselineScore != null &&
        run.optimizedScore != null && (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <Card className="border-db-gray-border bg-white">
              <CardHeader className="pb-1">
                <CardTitle className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-muted">
                  <FlaskConicalIcon />
                  Baseline
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span className="text-2xl font-bold">
                  {run.baselineScore.toFixed(1)}%
                </span>
              </CardContent>
            </Card>
            <Card className="border-db-blue/30 bg-db-blue/5">
              <CardHeader className="pb-1">
                <CardTitle className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-db-blue">
                  <Zap className="h-3.5 w-3.5" />
                  Optimized
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span className="text-2xl font-bold text-db-blue">
                  {run.optimizedScore.toFixed(1)}%
                </span>
              </CardContent>
            </Card>
            <Card className={`border-db-gray-border bg-white ${
              run.optimizedScore > run.baselineScore ? "border-db-green/30 bg-db-green/5" : ""
            }`}>
              <CardHeader className="pb-1">
                <CardTitle className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-muted">
                  <TrendingUp className="h-3.5 w-3.5" />
                  Improvement
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span
                  className={`text-2xl font-bold ${
                    run.optimizedScore > run.baselineScore
                      ? "text-db-green"
                      : "text-muted"
                  }`}
                >
                  {run.optimizedScore > run.baselineScore ? "+" : ""}
                  {(run.optimizedScore - run.baselineScore).toFixed(1)}%
                </span>
              </CardContent>
            </Card>
          </div>
        )}

      {/* Transparency Pane (shown when completed) */}
      {isCompleted && (
        <TransparencyPane
          runId={run.runId}
          stageEvents={allStageEvents}
          availableIterations={availableIterations}
          links={run.links ?? []}
        />
      )}

      {/* Pipeline Steps */}
      <div className="relative space-y-3">
        {/* Vertical connector line */}
        <div className="absolute left-[2.05rem] top-4 -z-10 h-[calc(100%-2rem)] w-px bg-gradient-to-b from-db-gray-border via-db-gray-border to-transparent" />

        {run.steps.map((step) => (
          <PipelineStepCard
            key={step.stepNumber}
            stepNumber={step.stepNumber}
            name={step.name}
            status={step.status}
            description={STEP_DESCRIPTIONS[step.stepNumber]}
            durationSeconds={step.durationSeconds}
            summary={step.summary}
          >
            <StepInsights step={step} links={run.links ?? []} />
            {step.name === "Proactive Enrichment" &&
              run.levers.filter(l => l.lever === 0).length > 0 && (
                <LeverProgress levers={run.levers.filter(l => l.lever === 0)} links={run.links ?? []} runId={run.runId} />
              )}
            {step.name === "Adaptive Optimization" &&
              run.levers.filter(l => l.lever !== 0).length > 0 && (
                <LeverProgress levers={run.levers.filter(l => l.lever !== 0)} links={run.links ?? []} runId={run.runId} />
              )}
          </PipelineStepCard>
        ))}
      </div>

      {/* Resource Links */}
      {run.links && run.links.length > 0 && (
        <ResourceLinks links={run.links} />
      )}

      {/* Completed CTA */}
      {isCompleted && (
        <div className="flex justify-center pt-2">
          <Button
            size="lg"
            className="bg-db-red hover:bg-db-red/90"
            onClick={() =>
              navigate({
                to: "/runs/$runId/comparison",
                params: { runId: run.runId },
              })
            }
          >
            View Results & Compare
            <ArrowRight className="ml-2 h-4 w-4" />
          </Button>
        </div>
      )}

      {/* Failed Alert */}
      {isFailed && (
        <Alert variant="destructive">
          <XCircle className="h-4 w-4" />
          <AlertTitle>Pipeline Failed</AlertTitle>
          <AlertDescription>
            {run.convergenceReason || "An error occurred during optimization."}
            <button
              className="mt-2 block text-sm underline"
              onClick={() =>
                navigate({
                  to: "/spaces/$spaceId",
                  params: { spaceId: run.spaceId },
                })
              }
            >
              Return to Space Detail
            </button>
          </AlertDescription>
        </Alert>
      )}

      {/* Cancelled Alert */}
      {isCancelled && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Pipeline Cancelled</AlertTitle>
          <AlertDescription>
            This optimization run was cancelled.
            <button
              className="mt-2 block text-sm underline"
              onClick={() =>
                navigate({
                  to: "/spaces/$spaceId",
                  params: { spaceId: run.spaceId },
                })
              }
            >
              Return to Space Detail
            </button>
          </AlertDescription>
        </Alert>
      )}
    </div>
  );
}

function TransparencyPane({
  runId,
  stageEvents,
  availableIterations,
  links,
}: {
  runId: string;
  stageEvents: StageEvent[];
  availableIterations: number[];
  links: { label: string; url: string; category: string }[];
}) {
  const { data: iterDetail, isLoading, isError } = useIterationDetail(runId);

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-10 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (isError || !iterDetail) {
    return (
      <div className="space-y-4">
        {isError && (
          <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Could not load full iteration details</AlertTitle>
            <AlertDescription>
              Showing summary charts only. The detailed breakdown may become available once the run completes.
            </AlertDescription>
          </Alert>
        )}
        <div className="grid gap-4 lg:grid-cols-2">
          <IterationChart runId={runId} />
          <StageTimeline stageEvents={stageEvents} />
        </div>
        <AsiResultsPanel runId={runId} availableIterations={availableIterations} />
      </div>
    );
  }

  return (
    <Tabs defaultValue="summary">
      <TabsList>
        <TabsTrigger value="summary" className="gap-1.5">
          <Eye className="h-3.5 w-3.5" />
          Summary
        </TabsTrigger>
        <TabsTrigger value="explorer" className="gap-1.5">
          <Microscope className="h-3.5 w-3.5" />
          Iteration Explorer
        </TabsTrigger>
        <TabsTrigger value="suggestions" className="gap-1.5">
          <Lightbulb className="h-3.5 w-3.5" />
          Suggestions
        </TabsTrigger>
      </TabsList>

      <TabsContent value="summary" className="mt-4 space-y-4">
        <InsightTabs
          runId={runId}
          detail={iterDetail}
          stageEvents={stageEvents}
          links={links}
        />
      </TabsContent>

      <TabsContent value="explorer" className="mt-4">
        <IterationExplorer detail={iterDetail} links={links} />
      </TabsContent>

      <TabsContent value="suggestions" className="mt-4">
        <ErrorBoundary fallback={<Alert variant="destructive"><AlertTitle>Failed to load suggestions</AlertTitle></Alert>}>
          <Suspense fallback={<Skeleton className="h-32 rounded-lg" />}>
            <SuggestionsPanel runId={runId} />
          </Suspense>
        </ErrorBoundary>
      </TabsContent>
    </Tabs>
  );
}

function FlaskConicalIcon() {
  return (
    <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M10 2v7.527a2 2 0 0 1-.211.896L4.72 20.55a1 1 0 0 0 .9 1.45h12.76a1 1 0 0 0 .9-1.45l-5.069-10.127A2 2 0 0 1 14 9.527V2" />
      <path d="M8.5 2h7" />
    </svg>
  );
}

function StepInsights({
  step,
  links,
}: {
  step: {
    stepNumber: number;
    name: string;
    outputs?: Record<string, unknown> | null;
  };
  links: { label: string; url: string; category: string }[];
}) {
  const outputs = step.outputs ?? {};
  if (!Object.keys(outputs).length) return null;

  if (step.name === "Preflight") {
    const tableCount = outputs.tableCount as number | undefined;
    const tables = (outputs.tables as string[] | undefined) ?? [];
    const functionCount = outputs.functionCount as number | undefined;
    const instructionCount = outputs.instructionCount as number | undefined;
    const sampleQuestionCount = outputs.sampleQuestionCount as number | undefined;
    const sampleQuestionsPreview = (outputs.sampleQuestionsPreview as string[] | undefined) ?? [];
    const columnsCollected = outputs.columnsCollected as number | undefined;
    const tagsCollected = outputs.tagsCollected as number | undefined;
    const columnSamples = (outputs.columnSamples as string[] | undefined) ?? [];

    return (
      <div className="space-y-2 text-xs">
        <div className="flex flex-wrap gap-2">
          {tableCount != null && <Badge variant="secondary">Tables: {tableCount}</Badge>}
          {functionCount != null && <Badge variant="secondary">Functions: {functionCount}</Badge>}
          {instructionCount != null && <Badge variant="secondary">Instructions: {instructionCount}</Badge>}
          {sampleQuestionCount != null && <Badge variant="secondary">Sample questions: {sampleQuestionCount}</Badge>}
          {columnsCollected != null && <Badge variant="secondary">Columns: {columnsCollected}</Badge>}
          {tagsCollected != null && <Badge variant="secondary">Tags: {tagsCollected}</Badge>}
        </div>
        {tables.length > 0 && (
          <p className="text-muted">Tables: {tables.slice(0, 8).join(", ")}</p>
        )}
        {columnSamples.length > 0 && (
          <p className="text-muted">Sample columns: {columnSamples.slice(0, 8).join(", ")}</p>
        )}
        {sampleQuestionsPreview.length > 0 && (
          <div className="space-y-1">
            <p className="font-medium text-muted">Sample questions</p>
            {sampleQuestionsPreview.slice(0, 3).map((q, i) => (
              <p key={i} className="text-muted truncate">{q}</p>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (step.name === "Baseline Evaluation") {
    const judgeScores = (outputs.judgeScores as Record<string, number | null> | undefined) ?? {};
    const sampleQuestions = (outputs.sampleQuestions as Record<string, unknown>[] | undefined) ?? [];
    const invalidBenchmarkCount = outputs.invalidBenchmarkCount as number | undefined;
    const permissionBlockedCount = outputs.permissionBlockedCount as number | undefined;
    const unresolvedColumnCount = outputs.unresolvedColumnCount as number | undefined;
    const harnessRetryCount = outputs.harnessRetryCount as number | undefined;
    const evalUrlFromOutput = outputs.evaluationRunUrl as string | undefined;
    const evalUrlFromLinks = links.find(
      (l) => l.category === "mlflow" && l.label.toLowerCase().includes("baseline"),
    )?.url;
    const evalUrl = evalUrlFromOutput ?? evalUrlFromLinks;

    return (
      <div className="space-y-3">
        <div className="flex flex-wrap gap-2">
          {invalidBenchmarkCount != null && (
            <Badge variant="secondary">Invalid benchmarks: {invalidBenchmarkCount}</Badge>
          )}
          {permissionBlockedCount != null && (
            <Badge variant="secondary">Permission blocked: {permissionBlockedCount}</Badge>
          )}
          {unresolvedColumnCount != null && (
            <Badge variant="secondary">Unresolved columns: {unresolvedColumnCount}</Badge>
          )}
          {harnessRetryCount != null && (
            <Badge variant="secondary">Harness retries: {harnessRetryCount}</Badge>
          )}
        </div>

        {!!Object.keys(judgeScores).length && (
          <div className="space-y-1">
            <p className="text-xs font-medium text-muted">Judge scores</p>
            <div className="flex flex-wrap gap-2">
              {Object.entries(judgeScores).map(([judge, score]) => (
                <Badge key={judge} variant="secondary">
                  {judge}: {score != null ? `${score.toFixed(1)}%` : "n/a"}
                </Badge>
              ))}
            </div>
          </div>
        )}

        {sampleQuestions.length > 0 && (
          <div className="space-y-1">
            <p className="text-xs font-medium text-muted">Sample evaluated questions</p>
            <div className="space-y-1">
              {sampleQuestions.slice(0, 3).map((row, idx) => (
                <div key={idx} className="rounded border bg-elevated/30 px-2 py-1 text-xs">
                  <p className="font-medium">{String(row.question ?? "Question")}</p>
                  <p className="text-muted">
                    result_correctness: {String(row.resultCorrectness ?? "n/a")}
                    {row.matchType ? ` · match: ${String(row.matchType)}` : ""}
                  </p>
                </div>
              ))}
            </div>
          </div>
        )}

        {evalUrl && (
          <Button variant="outline" size="sm" asChild>
            <a href={evalUrl} target="_blank" rel="noopener noreferrer">
              Open baseline evaluation run
              <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
            </a>
          </Button>
        )}
      </div>
    );
  }

  if (step.name === "Proactive Enrichment") {
    const proactive = outputs.proactiveChanges as Record<string, unknown> | undefined;
    const totalEnrichments = outputs.totalEnrichments as number | undefined;
    const enrichmentSkipped = outputs.enrichmentSkipped as boolean | undefined;

    return (
      <div className="space-y-2 text-xs">
        {enrichmentSkipped && (
          <p className="text-muted">Enrichment skipped (baseline already meets thresholds)</p>
        )}
        {totalEnrichments != null && (
          <Badge variant="secondary">Total enrichments: {totalEnrichments}</Badge>
        )}
        {proactive && Object.keys(proactive).length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {(proactive.descriptionsEnriched as number) > 0 && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">
                Descriptions: {proactive.descriptionsEnriched as number} cols
              </Badge>
            )}
            {(proactive.tablesEnriched as number) > 0 && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">
                Tables described: {proactive.tablesEnriched as number}
              </Badge>
            )}
            {(proactive.joinSpecsDiscovered as number) > 0 && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">
                Joins discovered: {proactive.joinSpecsDiscovered as number}
              </Badge>
            )}
            {!!proactive.spaceDescriptionGenerated && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">Space description generated</Badge>
            )}
            {(proactive.sampleQuestionsGenerated as number) > 0 && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">
                Sample questions: {proactive.sampleQuestionsGenerated as number}
              </Badge>
            )}
            {!!proactive.instructionsSeeded && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">Instructions seeded</Badge>
            )}
            {(proactive.promptsMatched as number) > 0 && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">
                Prompts matched: {proactive.promptsMatched as number}
              </Badge>
            )}
            {(proactive.exampleSqlsMined as number) > 0 && (
              <Badge variant="outline" className="text-[10px] border-violet-200 text-violet-700">
                Example SQLs: {proactive.exampleSqlsMined as number}
              </Badge>
            )}
          </div>
        )}
      </div>
    );
  }

  if (step.name === "Adaptive Optimization") {
    const patchesApplied = outputs.patchesApplied as number | undefined;
    const leversAccepted = (outputs.leversAccepted as unknown[] | undefined) ?? [];
    const leversRolledBack = (outputs.leversRolledBack as unknown[] | undefined) ?? [];
    const iterationCounter = outputs.iterationCounter as number | undefined;
    const baselineAccuracy = outputs.baselineAccuracy as number | undefined;
    const bestAccuracy = outputs.bestAccuracy as number | undefined;

    return (
      <div className="space-y-2 text-xs">
        <div className="flex flex-wrap gap-2">
          {patchesApplied != null && <Badge variant="secondary">Patches applied: {patchesApplied}</Badge>}
          <Badge variant="secondary">Levers accepted: {leversAccepted.length}</Badge>
          <Badge variant="secondary">Levers rolled back: {leversRolledBack.length}</Badge>
          {iterationCounter != null && <Badge variant="secondary">Iterations: {iterationCounter}</Badge>}
        </div>
        {baselineAccuracy != null && bestAccuracy != null && (
          <p className="text-muted">
            Score: {baselineAccuracy.toFixed(1)}% → {bestAccuracy.toFixed(1)}%
            <span className={bestAccuracy > baselineAccuracy ? "ml-1 text-green-600" : "ml-1"}>
              ({bestAccuracy > baselineAccuracy ? "+" : ""}{(bestAccuracy - baselineAccuracy).toFixed(1)}%)
            </span>
          </p>
        )}
      </div>
    );
  }

  if (step.name === "Finalization") {
    const bestAccuracy = outputs.bestAccuracy as number | undefined;
    const repeatability = outputs.repeatability as number | undefined;
    const convergenceReason = outputs.convergenceReason as string | undefined;
    const ucModelName = outputs.ucModelName as string | undefined;
    const ucModelVersion = outputs.ucModelVersion as string | undefined;
    const ucChampionPromoted = outputs.ucChampionPromoted as boolean | undefined;
    const heldOutAccuracy = outputs.heldOutAccuracy as number | undefined;
    const heldOutCount = outputs.heldOutCount as number | undefined;
    const heldOutDeltaPp = outputs.heldOutDeltaPp as number | undefined;

    return (
      <div className="space-y-2 text-xs">
        <div className="flex flex-wrap gap-2">
          {bestAccuracy != null && <Badge variant="secondary">Best accuracy: {bestAccuracy.toFixed(1)}%</Badge>}
          {repeatability != null && <Badge variant="secondary">Repeatability: {repeatability.toFixed(1)}%</Badge>}
          {heldOutAccuracy != null && (
            <Badge
              variant="secondary"
              className={
                heldOutDeltaPp != null && heldOutDeltaPp > 15
                  ? "border-amber-200 bg-amber-50 text-amber-800"
                  : ""
              }
            >
              Held-out: {heldOutAccuracy.toFixed(1)}% ({heldOutCount ?? "?"} Qs)
              {heldOutDeltaPp != null && (
                <span className="ml-1 opacity-70">
                  {heldOutDeltaPp > 0 ? `${heldOutDeltaPp.toFixed(1)}pp below train` : `${Math.abs(heldOutDeltaPp).toFixed(1)}pp above train`}
                </span>
              )}
            </Badge>
          )}
          {convergenceReason && <Badge variant="secondary">Convergence: {convergenceReason}</Badge>}
        </div>
        {ucModelName && ucModelVersion && (
          <div className={`mt-1 rounded border px-2 py-1.5 ${ucChampionPromoted ? "border-emerald-200 bg-emerald-50" : "border-amber-200 bg-amber-50"}`}>
            <p className={`font-medium ${ucChampionPromoted ? "text-emerald-800" : "text-amber-800"}`}>
              UC Model: {ucModelName} v{ucModelVersion}
              {ucChampionPromoted
                ? " — promoted to @champion"
                : " — registered (existing champion retained)"}
            </p>
          </div>
        )}
      </div>
    );
  }

  if (step.name === "Deploy") {
    const deployStatus = outputs.deployStatus as string | undefined;

    return (
      <div className="space-y-2 text-xs">
        <div className="flex flex-wrap gap-2">
          {deployStatus && <Badge variant="secondary">Deploy: {deployStatus}</Badge>}
        </div>
      </div>
    );
  }

  return null;
}

function DeploymentBadge({
  status,
  url,
}: {
  status: string;
  url?: string | null;
}) {
  const label =
    status === "DEPLOYED"
      ? "Deployed"
      : status === "RUNNING"
        ? "Deploying…"
        : status === "FAILED"
          ? "Deploy failed"
          : status === "SKIPPED"
            ? "Deploy skipped"
            : status === "PENDING_APPROVAL"
              ? "Awaiting approval"
              : status;

  const className =
    status === "DEPLOYED"
      ? "border-db-green/30 text-db-green"
      : status === "RUNNING"
        ? "border-db-blue/30 text-db-blue"
        : status === "FAILED"
          ? "border-db-red-error/30 text-db-red-error"
          : status === "PENDING_APPROVAL"
            ? "border-amber-500/30 text-amber-500"
            : "text-muted";

  const badge = (
    <Badge variant="outline" className={className}>
      {status === "RUNNING" && (
        <Loader2 className="mr-1 h-3 w-3 animate-spin" />
      )}
      <Rocket className="mr-1 h-3 w-3" />
      {label}
      {url && <ExternalLink className="ml-1 h-3 w-3" />}
    </Badge>
  );

  if (url) {
    return (
      <a href={url} target="_blank" rel="noopener noreferrer">
        {badge}
      </a>
    );
  }
  return badge;
}
