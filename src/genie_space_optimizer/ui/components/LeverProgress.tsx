import { Badge } from "@/components/ui/badge";
import { ProvenancePanel } from "@/components/ProvenancePanel";
import {
  CheckCircle2,
  Loader2,
  RotateCcw,
  SkipForward,
  XCircle,
  Circle,
  Wrench,
} from "lucide-react";

interface LeverItem {
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
      patch?: unknown;
      appliedAt?: string | null;
    }[];
    stageEvents?: {
      stage?: string;
      status?: string;
      durationSeconds?: number | null;
      errorMessage?: string | null;
    }[];
  }[];
}

interface LeverProgressProps {
  levers: LeverItem[];
  links?: { label: string; url: string; category: string }[];
  runId?: string;
}

const leverStatusConfig: Record<
  string,
  { label: string; className: string; icon: React.ReactNode }
> = {
  accepted: {
    label: "Accepted",
    className: "bg-db-green/10 text-db-green border-db-green/30",
    icon: <CheckCircle2 className="h-3.5 w-3.5" />,
  },
  rolled_back: {
    label: "Rolled Back",
    className: "bg-db-orange/10 text-db-orange border-db-orange/30",
    icon: <RotateCcw className="h-3.5 w-3.5" />,
  },
  running: {
    label: "Running",
    className: "bg-db-blue/10 text-db-blue border-db-blue/30",
    icon: <Loader2 className="h-3.5 w-3.5 animate-spin" />,
  },
  evaluating: {
    label: "Evaluating",
    className: "bg-db-blue/10 text-db-blue/80 border-db-blue/20",
    icon: <Loader2 className="h-3.5 w-3.5 animate-spin" />,
  },
  skipped: {
    label: "Skipped",
    className: "bg-elevated text-muted border-muted-foreground/20",
    icon: <SkipForward className="h-3.5 w-3.5" />,
  },
  pending: {
    label: "Pending",
    className: "bg-elevated/50 text-muted/50 border-muted-foreground/10",
    icon: <Circle className="h-3.5 w-3.5" />,
  },
  failed: {
    label: "Failed",
    className: "bg-db-red-error/10 text-db-red-error border-db-red-error/30",
    icon: <XCircle className="h-3.5 w-3.5" />,
  },
};

function formatDelta(delta: number | null | undefined): React.ReactNode {
  if (delta == null) return null;
  const sign = delta >= 0 ? "+" : "";
  const color = delta >= 0 ? "text-db-green" : "text-db-red-error";
  return (
    <span className={`text-xs font-semibold tabular-nums ${color}`}>
      {sign}{delta.toFixed(1)}%
    </span>
  );
}

export function LeverProgress({ levers, links = [], runId }: LeverProgressProps) {
  if (!levers.length) return null;

  const accepted = levers.filter((l) => l.status === "accepted").length;
  const total = levers.length;

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-muted">
          <Wrench className="h-3.5 w-3.5" />
          Optimization Levers
        </h4>
        <span className="text-xs text-muted">
          {accepted}/{total} accepted
        </span>
      </div>

      <div className="space-y-2">
        {levers.map((lever) => {
          const cfg = leverStatusConfig[lever.status] ?? leverStatusConfig.pending;
          const isActive = lever.status === "running" || lever.status === "evaluating";
          const patchCount = lever.patchCount || lever.patches?.length || 0;
          const patches = lever.patches ?? [];
          const hasPatchDetails = patches.length > 0;
          const iterations = lever.iterations ?? [];
          const hasIterationDetails = iterations.length > 0;

          return (
            <div
              key={lever.lever}
              className={`space-y-2 rounded-lg border px-3 py-2.5 transition-all ${
                isActive
                  ? "border-db-blue/20 bg-db-blue/5 shadow-sm"
                  : lever.status === "accepted"
                    ? "border-db-green/10 bg-db-green/5"
                    : lever.status === "rolled_back"
                      ? "border-db-orange/10 bg-db-orange/5"
                      : "border-db-gray-border bg-white"
              }`}
            >
              <div className="flex items-center gap-3">
                <div className="flex h-6 w-6 items-center justify-center rounded-full bg-elevated/50 text-xs font-bold text-muted">
                  {lever.lever}
                </div>

                <span className="w-44 truncate text-sm font-medium">
                  {lever.name}
                </span>

                <Badge variant="outline" className={`flex items-center gap-1 ${cfg.className}`}>
                  {cfg.icon}
                  {cfg.label}
                </Badge>

                <div className="ml-auto flex items-center gap-3">
                  {patchCount > 0 && (
                    <span className="text-xs text-muted">
                      {patchCount} patch{patchCount !== 1 ? "es" : ""}
                    </span>
                  )}

                  {lever.scoreAfter != null && lever.scoreBefore != null && (
                    <span className="text-xs tabular-nums text-muted">
                      {lever.scoreBefore.toFixed(0)}% → {lever.scoreAfter.toFixed(0)}%
                    </span>
                  )}

                  {formatDelta(lever.scoreDelta)}

                  {lever.rollbackReason && (
                    <span className="max-w-32 truncate text-xs italic text-db-orange" title={lever.rollbackReason}>
                      {lever.rollbackReason}
                    </span>
                  )}
                </div>
              </div>

              {hasPatchDetails && (
                <div className="space-y-1 border-t border-dashed border-db-gray-border pt-2">
                  <p className="text-xs font-medium text-muted">Changes</p>
                  {patches.slice(0, 8).map((patch, idx) => (
                    <div key={idx} className="rounded border bg-white px-2 py-1 text-xs">
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant="secondary">{patch.patchType ?? "patch"}</Badge>
                        {patch.scope && <span className="text-muted">scope: {patch.scope}</span>}
                        {patch.riskLevel && <span className="text-muted">risk: {patch.riskLevel}</span>}
                        {patch.rolledBack && (
                          <Badge variant="outline" className="border-db-orange/30 text-db-orange">
                            rolled back
                          </Badge>
                        )}
                      </div>
                      {patch.targetObject && (
                        <p className="mt-1 text-muted">target: {patch.targetObject}</p>
                      )}
                      {patch.command != null ? (
                        <pre className="mt-1 max-h-24 overflow-auto rounded bg-elevated/40 p-1 text-[11px]">
                          {JSON.stringify(patch.command, null, 2)}
                        </pre>
                      ) : patch.patch != null ? (
                        <pre className="mt-1 max-h-24 overflow-auto rounded bg-elevated/40 p-1 text-[11px]">
                          {JSON.stringify(patch.patch, null, 2)}
                        </pre>
                      ) : null}
                    </div>
                  ))}
                </div>
              )}

              {hasIterationDetails && (
                <div className="space-y-2 border-t border-dashed border-db-gray-border pt-2">
                  <p className="text-xs font-medium text-muted">Iteration history</p>
                  {iterations.map((iter) => {
                    const iterCfg = leverStatusConfig[iter.status] ?? leverStatusConfig.pending;
                    const iterPatches = iter.patches ?? [];
                    const iterLink = iter.mlflowRunId
                      ? links.find(
                          (l) => l.category === "mlflow" && l.url.includes(`/runs/${iter.mlflowRunId}`),
                        )?.url
                      : undefined;
                    return (
                      <details key={iter.iteration} className="rounded border bg-white">
                        <summary className="flex cursor-pointer list-none items-center gap-2 px-2 py-1.5 text-xs">
                          <Badge variant="outline">Iteration {iter.iteration}</Badge>
                          <Badge variant="outline" className={`flex items-center gap-1 ${iterCfg.className}`}>
                            {iterCfg.icon}
                            {iterCfg.label}
                          </Badge>
                          {iter.scoreBefore != null && iter.scoreAfter != null && (
                            <span className="text-muted tabular-nums">
                              {iter.scoreBefore.toFixed(1)}% → {iter.scoreAfter.toFixed(1)}%
                            </span>
                          )}
                          {formatDelta(iter.scoreDelta)}
                          {iter.patchCount != null && (
                            <span className="ml-auto text-muted">
                              {iter.patchCount} patches
                            </span>
                          )}
                        </summary>
                        <div className="space-y-2 border-t bg-elevated/20 px-2 py-2 text-xs">
                          <div className="flex flex-wrap gap-2">
                            {iter.fullAccuracy != null && (
                              <Badge variant="secondary">full: {iter.fullAccuracy.toFixed(1)}%</Badge>
                            )}
                            {iter.sliceAccuracy != null && (
                              <Badge variant="secondary">slice: {iter.sliceAccuracy.toFixed(1)}%</Badge>
                            )}
                            {iter.p0Accuracy != null && (
                              <Badge variant="secondary">p0: {iter.p0Accuracy.toFixed(1)}%</Badge>
                            )}
                            {iter.totalQuestions != null && (
                              <Badge variant="secondary">questions: {iter.totalQuestions}</Badge>
                            )}
                            {iter.correctCount != null && (
                              <Badge variant="secondary">correct: {iter.correctCount}</Badge>
                            )}
                          </div>

                          {!!iter.patchTypes?.length && (
                            <p className="text-muted">
                              patch types: {iter.patchTypes.join(", ")}
                            </p>
                          )}
                          {iter.rollbackReason && (
                            <p className="italic text-db-orange">rollback reason: {iter.rollbackReason}</p>
                          )}

                          {!!iter.judgeScores && Object.keys(iter.judgeScores).length > 0 && (
                            <div className="flex flex-wrap gap-1">
                              {Object.entries(iter.judgeScores).map(([judge, score]) => (
                                <Badge key={judge} variant="outline">
                                  {judge}: {score != null ? `${score.toFixed(1)}%` : "n/a"}
                                </Badge>
                              ))}
                            </div>
                          )}

                          {!!iterPatches.length && (
                            <div className="space-y-1">
                              {iterPatches.map((patch, idx) => (
                                <div key={idx} className="rounded border bg-white px-2 py-1">
                                  <div className="flex flex-wrap items-center gap-2">
                                    <Badge variant="secondary">{patch.patchType ?? "patch"}</Badge>
                                    {patch.targetObject && (
                                      <span className="text-muted">{patch.targetObject}</span>
                                    )}
                                  </div>
                                  {patch.command != null ? (
                                    <pre className="mt-1 max-h-24 overflow-auto rounded bg-elevated/40 p-1 text-[11px]">
                                      {JSON.stringify(patch.command, null, 2)}
                                    </pre>
                                  ) : patch.patch != null ? (
                                    <pre className="mt-1 max-h-24 overflow-auto rounded bg-elevated/40 p-1 text-[11px]">
                                      {JSON.stringify(patch.patch, null, 2)}
                                    </pre>
                                  ) : null}
                                </div>
                              ))}
                            </div>
                          )}

                          {!!iter.stageEvents?.length && (
                            <div className="space-y-1">
                              <p className="font-medium text-muted">stage events</p>
                              {iter.stageEvents.slice(0, 8).map((event, idx) => (
                                <p key={idx} className="text-muted">
                                  {event.stage} - {event.status}
                                  {event.durationSeconds != null ? ` (${Math.round(event.durationSeconds)}s)` : ""}
                                  {event.errorMessage ? ` - ${event.errorMessage}` : ""}
                                </p>
                              ))}
                            </div>
                          )}

                          {iterLink && (
                            <a
                              href={iterLink}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex text-db-blue underline"
                            >
                              Open evaluation run
                            </a>
                          )}
                        </div>
                      </details>
                    );
                  })}
                </div>
              )}

              {runId && (lever.status === "accepted" || lever.status === "rolled_back") && (
                <ProvenancePanel runId={runId} lever={lever.lever} />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
