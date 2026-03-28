import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { SqlSnippetPatch } from "@/components/SqlSnippetPatch";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ArrowLeft,
  ArrowRight,
  Brain,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  GitBranch,
  Layers,
  Shield,
  Target,
  XCircle,
  AlertTriangle,
  UserCheck,
} from "lucide-react";
import type {
  IterationDetail,
  IterationDetailResponse,
} from "@/lib/transparency-api";

interface IterationExplorerProps {
  detail: IterationDetailResponse;
  links?: { label: string; url: string; category: string }[];
}

export function IterationExplorer({ detail, links = [] }: IterationExplorerProps) {
  const [activeIteration, setActiveIteration] = useState(0);

  const current = detail.iterations.find((i) => i.iteration === activeIteration);
  const experimentLink = links.find(
    (l) => l.category === "mlflow" && l.label.includes("Experiment"),
  );

  return (
    <div className="space-y-4">
      {/* Iteration Selector Strip */}
      <div className="flex items-center gap-2 overflow-x-auto rounded-lg border bg-elevated/30 p-2">
        {detail.iterations.map((iter) => (
          <button
            key={iter.iteration}
            onClick={() => setActiveIteration(iter.iteration)}
            className={`flex shrink-0 flex-col items-center gap-0.5 rounded-md px-3 py-2 text-xs transition-colors ${
              activeIteration === iter.iteration
                ? "bg-surface shadow-sm ring-1 ring-default"
                : "hover:bg-elevated"
            }`}
          >
            <span className="font-medium">
              {iter.iteration === 0
                ? "Baseline"
                : iter.status === "patch_failed"
                  ? `Iter ${iter.iteration} (Failed)`
                  : `Iter ${iter.iteration}`}
            </span>
            <span className="tabular-nums text-muted">
              {iter.status === "patch_failed" ? "—" : `${iter.overallAccuracy.toFixed(1)}%`}
            </span>
            <StatusDot status={iter.status} />
          </button>
        ))}
      </div>

      {current && (
        <IterationView
          iteration={current}
          detail={detail}
          experimentLink={experimentLink}
        />
      )}

      {/* Navigation */}
      <div className="flex items-center justify-between">
        <Button
          variant="outline"
          size="sm"
          disabled={activeIteration <= 0}
          onClick={() => {
            const idx = detail.iterations.findIndex(
              (i) => i.iteration === activeIteration,
            );
            if (idx > 0) setActiveIteration(detail.iterations[idx - 1].iteration);
          }}
        >
          <ArrowLeft className="mr-1.5 h-3.5 w-3.5" />
          Previous
        </Button>
        <span className="text-xs text-muted">
          {activeIteration === 0 ? "Baseline" : `Iteration ${activeIteration}`} of{" "}
          {detail.totalIterations}
        </span>
        <Button
          variant="outline"
          size="sm"
          disabled={
            activeIteration >= (detail.iterations[detail.iterations.length - 1]?.iteration ?? 0)
          }
          onClick={() => {
            const idx = detail.iterations.findIndex(
              (i) => i.iteration === activeIteration,
            );
            if (idx < detail.iterations.length - 1)
              setActiveIteration(detail.iterations[idx + 1].iteration);
          }}
        >
          Next
          <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
        </Button>
      </div>
    </div>
  );
}

function StatusDot({ status }: { status: string }) {
  const color =
    status === "accepted"
      ? "bg-green-500"
      : status === "rolled_back"
        ? "bg-red-500"
        : status === "patch_failed"
          ? "bg-orange-500"
          : status === "baseline"
            ? "bg-blue-500"
            : "bg-gray-400";
  return <div className={`h-1.5 w-1.5 rounded-full ${color}`} />;
}

function PatchFailedView({ iteration }: { iteration: IterationDetail }) {
  const patchError = (iteration.clusterInfo as Record<string, unknown>)?.patch_error;
  const rootCause = (iteration.clusterInfo as Record<string, unknown>)?.root_cause;
  const rationale = (iteration.clusterInfo as Record<string, unknown>)?.rationale;

  return (
    <div className="space-y-4">
      <Card className="border-orange-200">
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-sm">
            <AlertTriangle className="h-4 w-4 text-orange-500" />
            Patch Application Failed
            <Badge variant="outline" className="ml-auto border-orange-300 text-orange-600 text-[10px]">
              Skipped
            </Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-xs">
          {rootCause ? (
            <div className="flex items-center gap-2">
              <Target className="h-3.5 w-3.5 text-orange-500" />
              <span className="text-muted">Root cause targeted:</span>
              <span>{String(rootCause)}</span>
            </div>
          ) : null}
          {rationale ? (
            <div className="rounded border border-blue-200 bg-blue-50/50 p-2">
              <p className="mb-1 text-[10px] font-medium text-blue-700">Strategist Rationale</p>
              <p className="text-[11px] leading-relaxed">{String(rationale)}</p>
            </div>
          ) : null}
          {patchError ? (
            <div className="rounded border border-orange-200 bg-orange-50/50 p-2">
              <p className="mb-1 text-[10px] font-medium text-orange-700">Error</p>
              <pre className="whitespace-pre-wrap text-[10px] leading-relaxed text-orange-900">
                {String(patchError)}
              </pre>
            </div>
          ) : null}
          <p className="text-muted">
            The strategist's proposed patches could not be applied. The optimizer
            automatically advanced to the next iteration with a different approach.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}

function IterationView({
  iteration,
  detail,
  experimentLink,
}: {
  iteration: IterationDetail;
  detail: IterationDetailResponse;
  experimentLink?: { label: string; url: string; category: string };
}) {
  if (iteration.status === "patch_failed") {
    return <PatchFailedView iteration={iteration} />;
  }
  if (iteration.iteration === 0) {
    return <BaselineView iteration={iteration} detail={detail} experimentLink={experimentLink} />;
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-3">
        <Card>
          <CardHeader className="pb-1">
            <CardTitle className="text-xs uppercase tracking-wide text-muted">
              Accuracy
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {iteration.overallAccuracy.toFixed(1)}%
            </span>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-1">
            <CardTitle className="text-xs uppercase tracking-wide text-muted">
              Questions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {iteration.correctCount}/{iteration.totalQuestions}
            </span>
            <span className="ml-1 text-sm text-muted">correct</span>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-1">
            <CardTitle className="text-xs uppercase tracking-wide text-muted">
              Judges
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {Object.keys(iteration.judgeScores).length}
            </span>
          </CardContent>
        </Card>
      </div>

    <div className="grid gap-4 lg:grid-cols-2">
      {/* Strategist Decision Card */}
      <Card className={iteration.status === "rolled_back" ? "border-red-200" : ""}>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-sm">
            <Brain className="h-4 w-4 text-blue-500" />
            Strategist Decision
            <Badge
              variant="outline"
              className={`ml-auto text-[10px] ${
                iteration.status === "accepted"
                  ? "border-green-300 text-green-600"
                  : iteration.status === "rolled_back"
                    ? "border-red-300 text-red-600"
                    : ""
              }`}
            >
              {iteration.status === "accepted" ? "Accepted" : "Rolled Back"}
            </Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-xs">
          {iteration.agId && (
            <div className="flex items-center gap-2">
              <span className="text-muted">Action Group:</span>
              <Badge variant="secondary">{iteration.agId}</Badge>
            </div>
          )}
          {iteration.clusterInfo && (
            <>
              <div className="flex items-center gap-2">
                <Target className="h-3.5 w-3.5 text-orange-500" />
                <span className="text-muted">Root cause:</span>
                <span>{String(iteration.clusterInfo.root_cause || "—")}</span>
              </div>
              {iteration.clusterInfo.impact_score != null && (
                <div className="flex items-center gap-2">
                  <span className="text-muted">Impact score:</span>
                  <span>{Number(iteration.clusterInfo.impact_score).toFixed(2)}</span>
                  <span className="text-muted">
                    (Rank #{String(iteration.clusterInfo.rank || "?")},{" "}
                    {String(iteration.clusterInfo.question_count || "?")} questions)
                  </span>
                </div>
              )}
              {iteration.clusterInfo.rationale && (
                <div className="mt-1.5 rounded border border-blue-200 bg-blue-50/50 p-2">
                  <p className="mb-1 text-[10px] font-medium text-blue-700">Strategist Rationale</p>
                  <p className="text-[11px] leading-relaxed">{String(iteration.clusterInfo.rationale)}</p>
                </div>
              )}
              {(() => {
                const esc = iteration.clusterInfo.escalation as { type?: string; handled?: boolean; detail?: unknown } | undefined;
                if (!esc) return null;
                return (
                  <div className="mt-1.5 rounded border border-amber-200 bg-amber-50/50 p-2">
                    <div className="flex items-center gap-1.5 mb-1">
                      <AlertTriangle className="h-3 w-3 text-amber-600" />
                      <p className="text-[10px] font-medium text-amber-700">
                        Escalation: {String(esc.type || "unknown")}
                      </p>
                      <Badge
                        variant="outline"
                        className={`ml-auto text-[9px] px-1.5 py-0 ${
                          esc.handled
                            ? "border-green-300 text-green-700"
                            : "border-red-300 text-red-700"
                        }`}
                      >
                        {esc.handled ? "Handled" : "Pending"}
                      </Badge>
                    </div>
                    {esc.detail ? (
                      <details className="text-[11px]">
                        <summary className="cursor-pointer text-muted hover:text-primary">
                          Escalation details
                        </summary>
                        <pre className="mt-1 rounded bg-elevated p-1.5 text-[10px] overflow-x-auto">
                          {JSON.stringify(esc.detail, null, 2)}
                        </pre>
                      </details>
                    ) : null}
                  </div>
                );
              })()}
              {iteration.clusterInfo.instruction_rewrite_preview && (
                <details className="mt-1">
                  <summary className="cursor-pointer text-[11px] text-muted hover:text-primary">
                    Instruction rewrite preview
                  </summary>
                  <pre className="mt-1 rounded bg-elevated p-2 text-[10px] whitespace-pre-wrap">
                    {String(iteration.clusterInfo.instruction_rewrite_preview)}
                  </pre>
                </details>
              )}
            </>
          )}
          {iteration.reflection && (
            <div className="mt-2 space-y-2 rounded bg-elevated/50 p-2">
              <p className="mb-1 font-medium text-muted">Reflection</p>
              <p>{iteration.reflection.reflectionText || "—"}</p>
              <div className="flex flex-wrap items-center gap-1.5">
                {iteration.reflection.refinementMode && (
                  <Badge
                    variant="outline"
                    className={`text-[9px] px-1.5 py-0 ${
                      iteration.reflection.refinementMode === "in_plan"
                        ? "border-blue-300 text-blue-700"
                        : "border-orange-300 text-orange-700"
                    }`}
                  >
                    {iteration.reflection.refinementMode.replace("_", " ")}
                  </Badge>
                )}
                {iteration.reflection.fixedQuestions.length > 0 && (
                  <Badge variant="outline" className="text-[9px] px-1.5 py-0 border-green-300 text-green-700">
                    {iteration.reflection.fixedQuestions.length} fixed
                  </Badge>
                )}
                {iteration.reflection.newRegressions.length > 0 && (
                  <Badge variant="outline" className="text-[9px] px-1.5 py-0 border-red-300 text-red-700">
                    {iteration.reflection.newRegressions.length} regressed
                  </Badge>
                )}
              </div>
              {iteration.reflection.rollbackReason && (
                <p className="text-red-500">
                  Rollback: {iteration.reflection.rollbackReason}
                </p>
              )}
              {iteration.reflection.doNotRetry.length > 0 && (
                <details className="text-[11px]">
                  <summary className="cursor-pointer text-muted hover:text-primary">
                    {iteration.reflection.doNotRetry.length} patch type(s) will not be retried
                  </summary>
                  <div className="mt-1 flex flex-wrap gap-1">
                    {iteration.reflection.doNotRetry.map((item) => (
                      <Badge key={item} variant="secondary" className="text-[9px]">
                        {item}
                      </Badge>
                    ))}
                  </div>
                </details>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Gate Results Card */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-sm">
            <Shield className="h-4 w-4 text-indigo-500" />
            Gate Results
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {iteration.gates.length > 0 ? (
              iteration.gates.map((gate) => (
                <div
                  key={gate.gateName}
                  className="flex items-center justify-between rounded border px-3 py-2"
                >
                  <div className="flex items-center gap-2">
                    {gate.passed ? (
                      <CheckCircle2 className="h-4 w-4 text-green-500" />
                    ) : (
                      <XCircle className="h-4 w-4 text-red-400" />
                    )}
                    <span className="text-xs font-medium capitalize">
                      {gate.gateName} Gate
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-xs">
                    {gate.accuracy != null && (
                      <span className="tabular-nums">
                        {gate.accuracy.toFixed(1)}%
                      </span>
                    )}
                    {gate.totalQuestions != null && (
                      <span className="text-muted">
                        ({gate.totalQuestions} questions)
                      </span>
                    )}
                  </div>
                </div>
              ))
            ) : (
              <p className="text-xs text-muted">No gate data available</p>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Patches Card */}
      {iteration.patches.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-sm">
              <GitBranch className="h-4 w-4 text-emerald-500" />
              Patches ({iteration.patches.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-1.5">
              {iteration.patches.map((p, idx) => (
                <PatchRow key={idx} patch={p} />
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Score Breakdown */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-sm">
            <Layers className="h-4 w-4 text-violet-500" />
            Score Breakdown
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="text-xs">Judge</TableHead>
                <TableHead className="text-center text-xs">Score</TableHead>
                {iteration.reflection?.scoreDeltas &&
                  Object.keys(iteration.reflection.scoreDeltas).length > 0 && (
                    <TableHead className="text-center text-xs">Delta</TableHead>
                  )}
              </TableRow>
            </TableHeader>
            <TableBody>
              {Object.entries(iteration.judgeScores)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([judge, score]) => {
                  const delta = iteration.reflection?.scoreDeltas?.[judge];
                  return (
                    <TableRow key={judge}>
                      <TableCell className="text-xs">{judge}</TableCell>
                      <TableCell className="text-center text-xs tabular-nums">
                        {score != null ? `${score.toFixed(1)}%` : "—"}
                      </TableCell>
                      {iteration.reflection?.scoreDeltas &&
                        Object.keys(iteration.reflection.scoreDeltas).length > 0 && (
                          <TableCell className="text-center text-xs">
                            {delta != null ? (
                              <span
                                className={
                                  delta > 0 ? "text-green-600" : delta < 0 ? "text-red-500" : ""
                                }
                              >
                                {delta > 0 ? "+" : ""}
                                {delta.toFixed(1)}
                              </span>
                            ) : (
                              "—"
                            )}
                          </TableCell>
                        )}
                    </TableRow>
                  );
                })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Question Impact */}
      {iteration.reflection && <QuestionImpactCard reflection={iteration.reflection} detail={detail} />}

      {/* MLflow & Human Review Links */}
      <MLflowLinks iteration={iteration} experimentLink={experimentLink} detail={detail} />
    </div>
    </div>
  );
}

function BaselineView({
  iteration,
  detail,
  experimentLink,
}: {
  iteration: IterationDetail;
  detail: IterationDetailResponse;
  experimentLink?: { label: string; url: string; category: string };
}) {
  const [showAll, setShowAll] = useState(false);
  const displayed = showAll ? iteration.questions : iteration.questions.slice(0, 20);

  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-3">
        <Card>
          <CardHeader className="pb-1">
            <CardTitle className="text-xs uppercase tracking-wide text-muted">
              Accuracy
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {iteration.overallAccuracy.toFixed(1)}%
            </span>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-1">
            <CardTitle className="text-xs uppercase tracking-wide text-muted">
              Questions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {iteration.correctCount}/{iteration.totalQuestions}
            </span>
            <span className="ml-1 text-sm text-muted">correct</span>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-1">
            <CardTitle className="text-xs uppercase tracking-wide text-muted">
              Judges
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {Object.keys(iteration.judgeScores).length}
            </span>
          </CardContent>
        </Card>
      </div>

      {/* Score breakdown */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Baseline Judge Scores</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {Object.entries(iteration.judgeScores)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([judge, score]) => (
                <Badge key={judge} variant="secondary" className="text-xs">
                  {judge}: {score != null ? `${score.toFixed(1)}%` : "n/a"}
                </Badge>
              ))}
          </div>
        </CardContent>
      </Card>

      {/* Questions Table */}
      {iteration.questions.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">
              Baseline Question Results ({iteration.questions.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="max-h-[400px] overflow-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-xs">Question</TableHead>
                    <TableHead className="text-center text-xs">Result</TableHead>
                    <TableHead className="text-center text-xs">Match</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {displayed.map((q) => {
                    const passed =
                      q.resultCorrectness === "yes" || q.resultCorrectness === "pass";
                    return (
                      <TableRow key={q.questionId}>
                        <TableCell className="max-w-[400px] truncate text-xs" title={q.question}>
                          {q.question || q.questionId}
                        </TableCell>
                        <TableCell className="text-center">
                          {passed ? (
                            <CheckCircle2 className="mx-auto h-4 w-4 text-green-500" />
                          ) : (
                            <XCircle className="mx-auto h-4 w-4 text-red-400" />
                          )}
                        </TableCell>
                        <TableCell className="text-center text-xs">
                          {q.matchType || "—"}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
            {iteration.questions.length > 20 && !showAll && (
              <Button
                variant="ghost"
                size="sm"
                className="mt-2 w-full text-xs"
                onClick={() => setShowAll(true)}
              >
                Show all {iteration.questions.length} questions
              </Button>
            )}
          </CardContent>
        </Card>
      )}

      <MLflowLinks iteration={iteration} experimentLink={experimentLink} detail={detail} />
    </div>
  );
}

function QuestionImpactCard({
  reflection,
  detail,
}: {
  reflection: NonNullable<IterationDetail["reflection"]>;
  detail: IterationDetailResponse;
}) {
  const flaggedIds = useMemo(
    () => new Set(detail.flaggedQuestions.map((f) => String(f.question_id || ""))),
    [detail.flaggedQuestions],
  );

  const hasContent =
    reflection.fixedQuestions.length > 0 ||
    reflection.stillFailing.length > 0 ||
    reflection.newRegressions.length > 0;

  if (!hasContent) return null;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center gap-2 text-sm">
          <Target className="h-4 w-4 text-orange-500" />
          Question Impact
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {reflection.fixedQuestions.length > 0 && (
          <div>
            <p className="mb-1 text-xs font-medium text-green-600">
              Fixed ({reflection.fixedQuestions.length})
            </p>
            <div className="flex flex-wrap gap-1">
              {reflection.fixedQuestions.map((qid) => (
                <Badge key={qid} className="bg-green-100 text-[10px] text-green-700">
                  {qid.slice(0, 30)}
                </Badge>
              ))}
            </div>
          </div>
        )}
        {reflection.stillFailing.length > 0 && (
          <div>
            <p className="mb-1 text-xs font-medium text-amber-600">
              Still Failing ({reflection.stillFailing.length})
            </p>
            <div className="flex flex-wrap gap-1">
              {reflection.stillFailing.slice(0, 10).map((qid) => (
                <Badge
                  key={qid}
                  variant="outline"
                  className={`text-[10px] ${flaggedIds.has(qid) ? "border-amber-400 bg-amber-50" : ""}`}
                >
                  {qid.slice(0, 30)}
                  {flaggedIds.has(qid) && (
                    <AlertTriangle className="ml-1 h-2.5 w-2.5 text-amber-500" />
                  )}
                </Badge>
              ))}
            </div>
          </div>
        )}
        {reflection.newRegressions.length > 0 && (
          <div>
            <p className="mb-1 text-xs font-medium text-red-600">
              New Regressions ({reflection.newRegressions.length})
            </p>
            <div className="flex flex-wrap gap-1">
              {reflection.newRegressions.map((qid) => (
                <Badge key={qid} className="bg-red-100 text-[10px] text-red-700">
                  {qid.slice(0, 30)}
                </Badge>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function MLflowLinks({
  iteration,
  experimentLink,
  detail,
}: {
  iteration: IterationDetail;
  experimentLink?: { label: string; url: string; category: string };
  detail: IterationDetailResponse;
}) {
  const hasLinks =
    iteration.mlflowRunId || iteration.modelId || experimentLink || detail.labelingSessionUrl;
  if (!hasLinks) return null;

  const expId = experimentLink?.url?.match(/experiments\/(\d+)/)?.[1];

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">Links</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-2">
          {iteration.mlflowRunId && expId && (
            <Button variant="outline" size="sm" asChild>
              <a
                href={`${experimentLink!.url.split("/experiments/")[0]}/ml/experiments/${expId}/runs/${iteration.mlflowRunId}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                View in MLflow
                <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
              </a>
            </Button>
          )}
          {iteration.modelId && (
            <Button variant="outline" size="sm" asChild>
              <a
                href={`#model-${iteration.modelId}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                View LoggedModel
                <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
              </a>
            </Button>
          )}
          {detail.labelingSessionUrl && (
            <Button variant="outline" size="sm" className="border-amber-300" asChild>
              <a
                href={detail.labelingSessionUrl}
                target="_blank"
                rel="noopener noreferrer"
              >
                <UserCheck className="mr-1.5 h-3.5 w-3.5 text-amber-600" />
                Open Human Review Session
                <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
              </a>
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function PatchRow({ patch }: { patch: Record<string, unknown> }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="rounded border text-xs">
      <button
        className="flex w-full items-center gap-2 px-3 py-2 hover:bg-elevated/50"
        onClick={() => setExpanded(!expanded)}
      >
        {expanded ? (
          <ChevronDown className="h-3.5 w-3.5 shrink-0" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 shrink-0" />
        )}
        <span className="font-medium">{String(patch.patchType || "patch")}</span>
        <span className="truncate text-muted">
          {String(patch.targetObject || "")}
        </span>
        <span className="ml-auto">
          {patch.rolledBack ? (
            <Badge variant="destructive" className="text-[10px]">Rolled Back</Badge>
          ) : (
            <Badge className="bg-green-500 text-[10px]">Applied</Badge>
          )}
        </span>
      </button>
      {expanded && (
        <div className="border-t p-2">
          {String(patch.patchType || patch.type || "").includes("sql_snippet") ||
           String(patch.patchType || "") === "proactive_sql_expression" ? (
            <SqlSnippetPatch patch={patch} />
          ) : (
            <pre className="max-h-32 overflow-auto rounded bg-elevated p-2 text-[10px]">
              {JSON.stringify(patch.command || patch.patch || patch, null, 2)}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}
