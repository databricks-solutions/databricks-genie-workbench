import React, { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import {
  BarChart3,
  CheckCircle2,
  XCircle,
  FileText,
  Activity,
  Gavel,
  Filter,
  ExternalLink,
  ChevronDown,
  ChevronRight,
  Layers,
  RotateCcw,
  AlertTriangle,
  Trash2,
  Info,
  UserCheck,
} from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type {
  IterationDetailResponse,
  ProactiveChanges,
  QuestionResult,
} from "@/lib/transparency-api";
import type { StageEvent } from "@/components/StageTimeline";
import { IterationChart } from "@/components/IterationChart";
import { StageTimeline, formatStageDisplayName } from "@/components/StageTimeline";

interface InsightTabsProps {
  runId: string;
  detail: IterationDetailResponse;
  stageEvents: StageEvent[];
  links?: { label: string; url: string; category: string }[];
}

type QuestionFilter = "all" | "failing" | "fixed" | "regressed" | "persistent";

export type FlagCategory = "persistence" | "tvf_removal" | "strategist" | "other";

export function categorizeFlagReason(reason: string): FlagCategory {
  if (!reason) return "other";
  const r = reason.toLowerCase();
  if (r.includes("persistent") || r.includes("additive_levers_exhausted") || r.includes("consecutive"))
    return "persistence";
  if (r.includes("tvf removal") || r.includes("remove_tvf") || r.includes("remove tvf"))
    return "tvf_removal";
  if (r.includes("strategist") || r.includes("flag_for_review"))
    return "strategist";
  return "other";
}

interface FlaggedInfo {
  flag_reason: string;
  iterations_failed: number | null;
  patches_tried: string | null;
  category: FlagCategory;
}

export function InsightTabs({
  runId,
  detail,
  stageEvents,
  links = [],
}: InsightTabsProps) {
  return (
    <Tabs defaultValue="overview" className="w-full">
      <TabsList className="grid w-full grid-cols-5">
        <TabsTrigger value="overview" className="gap-1.5 text-xs">
          <BarChart3 className="h-3.5 w-3.5" />
          Overview
        </TabsTrigger>
        <TabsTrigger value="questions" className="gap-1.5 text-xs">
          <FileText className="h-3.5 w-3.5" />
          Questions
        </TabsTrigger>
        <TabsTrigger value="patches" className="gap-1.5 text-xs">
          <Activity className="h-3.5 w-3.5" />
          Patches
        </TabsTrigger>
        <TabsTrigger value="activity" className="gap-1.5 text-xs">
          <Activity className="h-3.5 w-3.5" />
          Activity
        </TabsTrigger>
        <TabsTrigger value="judges" className="gap-1.5 text-xs">
          <Gavel className="h-3.5 w-3.5" />
          Judges
        </TabsTrigger>
      </TabsList>

      <TabsContent value="overview" className="mt-4">
        <OverviewTab runId={runId} detail={detail} stageEvents={stageEvents} />
      </TabsContent>
      <TabsContent value="questions" className="mt-4 space-y-4">
        <QuestionsTab detail={detail} />
        <RecommendationsCard detail={detail} />
      </TabsContent>
      <TabsContent value="patches" className="mt-4">
        <PatchesTab detail={detail} />
      </TabsContent>
      <TabsContent value="activity" className="mt-4">
        <ActivityTab stageEvents={stageEvents} />
      </TabsContent>
      <TabsContent value="judges" className="mt-4">
        <JudgesTab detail={detail} links={links} />
      </TabsContent>
    </Tabs>
  );
}

const PROACTIVE_LABELS: Record<keyof ProactiveChanges, string> = {
  descriptionsEnriched: "Column descriptions enriched",
  tablesEnriched: "Table descriptions enriched",
  joinSpecsDiscovered: "Join specs discovered",
  spaceDescriptionGenerated: "Space description generated",
  sampleQuestionsGenerated: "Sample questions generated",
  instructionsSeeded: "Instructions seeded",
  promptsMatched: "Prompts matched",
  exampleSqlsMined: "Example SQLs mined",
};

function ProactiveEnrichmentCard({ proactive }: { proactive: ProactiveChanges }) {
  const entries = (Object.entries(proactive) as [keyof ProactiveChanges, unknown][]).filter(
    ([, v]) => v !== false && v !== 0 && v != null,
  );
  if (entries.length === 0) return null;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center gap-2 text-sm">
          <Activity className="h-4 w-4 text-violet-500" />
          Pre-Loop Enrichment
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-2">
          {entries.map(([key, value]) => (
            <Badge key={key} variant="outline" className="border-violet-200 text-violet-700">
              {PROACTIVE_LABELS[key]}
              {typeof value === "number" ? `: ${value}` : ""}
            </Badge>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function OverviewTab({
  runId,
  detail,
  stageEvents,
}: {
  runId: string;
  detail: IterationDetailResponse;
  stageEvents: StageEvent[];
}) {
  const judgeProgressionData = useMemo(() => {
    const judges = new Set<string>();
    for (const iter of detail.iterations) {
      for (const j of Object.keys(iter.judgeScores)) judges.add(j);
    }
    return Array.from(judges)
      .sort()
      .map((judge) => ({
        judge,
        dataPoints: detail.iterations.map((iter) => ({
          iteration: iter.iteration,
          score: iter.judgeScores[judge] ?? null,
        })),
      }));
  }, [detail.iterations]);

  return (
    <div className="space-y-4">
      <div className="grid gap-4 lg:grid-cols-2">
        <IterationChart runId={runId} />
        <StageTimeline stageEvents={stageEvents} />
      </div>

      {detail.proactiveChanges && <ProactiveEnrichmentCard proactive={detail.proactiveChanges} />}

      {judgeProgressionData.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Per-Judge Score Progression</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-xs">Judge</TableHead>
                    {detail.iterations.map((iter) => (
                      <TableHead key={iter.iteration} className="text-center text-xs">
                        {iter.iteration === 0 ? "Baseline" : `Iter ${iter.iteration}`}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {judgeProgressionData.map(({ judge, dataPoints }) => (
                    <TableRow key={judge}>
                      <TableCell className="text-xs font-medium">{judge}</TableCell>
                      {dataPoints.map((dp) => {
                        const prev =
                          dp.iteration > 0
                            ? dataPoints.find((p) => p.iteration === dp.iteration - 1)?.score
                            : null;
                        const delta =
                          dp.score != null && prev != null ? dp.score - prev : null;
                        return (
                          <TableCell key={dp.iteration} className="text-center text-xs">
                            {dp.score != null ? (
                              <span>
                                {dp.score.toFixed(1)}%
                                {delta != null && delta !== 0 && (
                                  <span
                                    className={`ml-1 ${delta > 0 ? "text-green-600" : "text-red-500"}`}
                                  >
                                    ({delta > 0 ? "+" : ""}
                                    {delta.toFixed(1)})
                                  </span>
                                )}
                              </span>
                            ) : (
                              <span className="text-muted">—</span>
                            )}
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Optimization Narrative */}
      {detail.iterations.some(i => i.iteration > 0 && i.reflection) && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-sm">
              <Layers className="h-4 w-4 text-violet-500" />
              Optimization Narrative
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {detail.iterations
                .filter(i => i.iteration > 0 && i.reflection)
                .map(iter => {
                  const r = iter.reflection!;
                  const accepted = r.accepted;
                  return (
                    <div
                      key={iter.iteration}
                      className={`flex items-start gap-3 rounded-lg border px-3 py-2.5 ${
                        accepted
                          ? "border-green-200 bg-green-50/50"
                          : "border-red-200 bg-red-50/50"
                      }`}
                    >
                      <Badge
                        variant="outline"
                        className={`shrink-0 mt-0.5 ${
                          accepted
                            ? "border-green-300 text-green-700"
                            : "border-red-300 text-red-700"
                        }`}
                      >
                        {accepted ? (
                          <CheckCircle2 className="mr-1 h-3 w-3" />
                        ) : (
                          <RotateCcw className="mr-1 h-3 w-3" />
                        )}
                        Iter {iter.iteration}
                      </Badge>
                      <div className="flex-1 min-w-0">
                        <p className="text-xs leading-relaxed">
                          {r.reflectionText || "—"}
                        </p>
                        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                          {r.refinementMode && (
                            <Badge
                              variant="outline"
                              className={`text-[9px] px-1.5 py-0 ${
                                r.refinementMode === "in_plan"
                                  ? "border-blue-300 text-blue-700"
                                  : "border-orange-300 text-orange-700"
                              }`}
                            >
                              {r.refinementMode.replace("_", " ")}
                            </Badge>
                          )}
                          {r.fixedQuestions.length > 0 && (
                            <Badge variant="outline" className="text-[9px] px-1.5 py-0 border-green-300 text-green-700">
                              {r.fixedQuestions.length} fixed
                            </Badge>
                          )}
                          {r.newRegressions.length > 0 && (
                            <Badge variant="outline" className="text-[9px] px-1.5 py-0 border-red-300 text-red-700">
                              {r.newRegressions.length} regressed
                            </Badge>
                          )}
                        </div>
                      </div>
                      <span
                        className={`shrink-0 text-xs font-semibold tabular-nums ${
                          r.accuracyDelta >= 0 ? "text-green-600" : "text-red-500"
                        }`}
                      >
                        {r.accuracyDelta >= 0 ? "+" : ""}
                        {r.accuracyDelta.toFixed(1)}%
                      </span>
                    </div>
                  );
                })}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function QuestionsTab({ detail }: { detail: IterationDetailResponse }) {
  const [filter, setFilter] = useState<QuestionFilter>("all");

  const flaggedMap = useMemo(() => {
    const m = new Map<string, FlaggedInfo>();
    for (const f of detail.flaggedQuestions) {
      const qid = String(f.question_id || "");
      const reason = String(f.flag_reason || "");
      m.set(qid, {
        flag_reason: reason,
        iterations_failed: typeof f.iterations_failed === "number" ? f.iterations_failed : null,
        patches_tried: f.patches_tried ? String(f.patches_tried) : null,
        category: categorizeFlagReason(reason),
      });
    }
    return m;
  }, [detail.flaggedQuestions]);

  const questionJourney = useMemo(() => {
    const qMap = new Map<
      string,
      { question: string; results: Map<number, QuestionResult>; persistentFail: boolean; flagged: boolean; flagInfo: FlaggedInfo | null }
    >();

    for (const iter of detail.iterations) {
      for (const q of iter.questions) {
        if (!qMap.has(q.questionId)) {
          qMap.set(q.questionId, {
            question: q.question,
            results: new Map(),
            persistentFail: false,
            flagged: flaggedMap.has(q.questionId),
            flagInfo: flaggedMap.get(q.questionId) ?? null,
          });
        }
        qMap.get(q.questionId)!.results.set(iter.iteration, q);
      }
    }

    for (const [, data] of qMap) {
      const allFail = Array.from(data.results.values()).every(
        (r) => r.resultCorrectness !== "yes" && r.resultCorrectness !== "pass",
      );
      data.persistentFail = allFail && data.results.size > 1;
    }

    return qMap;
  }, [detail.iterations, flaggedMap]);

  const iters = detail.iterations.map((i) => i.iteration);

  const filteredQuestions = useMemo(() => {
    const entries = Array.from(questionJourney.entries());
    if (filter === "all") return entries;

    const final = detail.iterations[detail.iterations.length - 1];

    return entries.filter(([, data]) => {
      const baseResult = data.results.get(0);
      const finalResult = data.results.get(final?.iteration ?? 0);
      const basePassed =
        baseResult?.resultCorrectness === "yes" || baseResult?.resultCorrectness === "pass";
      const finalPassed =
        finalResult?.resultCorrectness === "yes" || finalResult?.resultCorrectness === "pass";

      if (filter === "failing") return !finalPassed;
      if (filter === "fixed") return !basePassed && finalPassed;
      if (filter === "regressed") return basePassed && !finalPassed;
      if (filter === "persistent") return data.persistentFail || data.flagged;
      return true;
    });
  }, [questionJourney, filter, detail.iterations]);

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm">Question Journey</CardTitle>
          <div className="flex gap-1">
            {(["all", "failing", "fixed", "regressed", "persistent"] as QuestionFilter[]).map(
              (f) => (
                <Button
                  key={f}
                  variant={filter === f ? "default" : "outline"}
                  size="sm"
                  className="h-7 text-xs"
                  onClick={() => setFilter(f)}
                >
                  <Filter className="mr-1 h-3 w-3" />
                  {f.charAt(0).toUpperCase() + f.slice(1)}
                </Button>
              ),
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="max-h-[500px] overflow-auto">
          <Table className="table-fixed">
            <TableHeader>
              <TableRow>
                <TableHead className="min-w-[200px] w-[40%] text-xs">Question</TableHead>
                {iters.map((it) => (
                  <TableHead key={it} className="w-[48px] text-center text-xs">
                    {it === 0 ? "Baseline" : `Iter ${it}`}
                  </TableHead>
                ))}
                <TableHead className="w-[80px] text-center text-xs">Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredQuestions.slice(0, 100).map(([qid, data]) => (
                <TableRow key={qid}>
                  <TableCell className="truncate text-xs" title={data.question || qid}>
                    {data.question || qid}
                  </TableCell>
                  {iters.map((it) => {
                    const r = data.results.get(it);
                    if (!r) return <TableCell key={it} className="text-center text-xs">—</TableCell>;
                    const passed =
                      r.resultCorrectness === "yes" || r.resultCorrectness === "pass";
                    return (
                      <TableCell key={it} className="text-center">
                        {passed ? (
                          <CheckCircle2 className="mx-auto h-4 w-4 text-green-500" />
                        ) : (
                          <XCircle className="mx-auto h-4 w-4 text-red-400" />
                        )}
                      </TableCell>
                    );
                  })}
                  <TableCell className="text-center">
                    <div className="flex items-center justify-center gap-1">
                      {data.persistentFail && !data.flagInfo && (
                        <Badge variant="destructive" className="text-[10px]">
                          Persistent
                        </Badge>
                      )}
                      {data.flagInfo && (
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Badge
                                className={`text-[10px] cursor-help ${
                                  data.flagInfo.category === "tvf_removal"
                                    ? "bg-orange-500"
                                    : data.flagInfo.category === "persistence"
                                      ? "bg-red-500"
                                      : "bg-amber-500"
                                }`}
                              >
                                {data.flagInfo.category === "tvf_removal"
                                  ? "TVF Issue"
                                  : data.flagInfo.category === "persistence"
                                    ? "Persistent"
                                    : "Flagged"}
                              </Badge>
                            </TooltipTrigger>
                            <TooltipContent side="left" className="max-w-xs text-xs space-y-1">
                              <p className="font-medium">{data.flagInfo.flag_reason}</p>
                              {data.flagInfo.iterations_failed != null && (
                                <p className="text-muted">
                                  Failed in {data.flagInfo.iterations_failed} iteration(s)
                                </p>
                              )}
                              {data.flagInfo.patches_tried && (
                                <p className="text-muted">
                                  Patches tried: {data.flagInfo.patches_tried}
                                </p>
                              )}
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      )}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {filteredQuestions.length > 100 && (
            <p className="mt-2 text-center text-xs text-muted">
              Showing 100 of {filteredQuestions.length} questions
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function RecommendationsCard({ detail }: { detail: IterationDetailResponse }) {
  const recommendations = useMemo(() => {
    const items: {
      category: FlagCategory;
      title: string;
      description: string;
      questions: string[];
      icon: "tvf" | "persistence" | "strategist";
    }[] = [];

    const byCategory = new Map<FlagCategory, { reasons: string[]; questions: string[] }>();
    for (const fq of detail.flaggedQuestions) {
      const reason = String(fq.flag_reason || "");
      const cat = categorizeFlagReason(reason);
      if (cat === "other") continue;
      const entry = byCategory.get(cat) ?? { reasons: [], questions: [] };
      entry.reasons.push(reason);
      entry.questions.push(String(fq.question_id || fq.question_text || ""));
      byCategory.set(cat, entry);
    }

    const tvf = byCategory.get("tvf_removal");
    if (tvf) {
      const tvfNames = tvf.reasons
        .map((r) => {
          const match = r.match(/(?:removal[^:]*:\s*)(\S+)/i);
          return match?.[1];
        })
        .filter(Boolean);
      items.push({
        category: "tvf_removal",
        title: `TVF Review Required${tvfNames.length ? `: ${tvfNames.join(", ")}` : ""}`,
        description:
          "One or more table-valued functions were flagged for removal but require manual review. " +
          "Consider verifying whether these TVFs are still needed, and add them back or confirm removal.",
        questions: tvf.questions,
        icon: "tvf",
      });
    }

    const persistence = byCategory.get("persistence");
    if (persistence) {
      items.push({
        category: "persistence",
        title: `${persistence.questions.length} Persistently Failing Question${persistence.questions.length > 1 ? "s" : ""} — Human Review Required`,
        description: detail.labelingSessionUrl
          ? "These questions failed across multiple iterations despite optimization attempts. " +
            "A labeling session has been created for human review."
          : "These questions failed across multiple iterations despite optimization attempts. " +
            "Review the questions — they may need benchmark corrections, additional instructions, or domain-specific guidance.",
        questions: persistence.questions,
        icon: "persistence",
      });
    }

    const strat = byCategory.get("strategist");
    if (strat) {
      items.push({
        category: "strategist",
        title: "Strategist-Flagged Items",
        description:
          "The adaptive strategist flagged these questions for human review. " +
          "The optimization engine determined automated fixes were insufficient.",
        questions: strat.questions,
        icon: "strategist",
      });
    }

    return items;
  }, [detail.flaggedQuestions, detail.labelingSessionUrl]);

  if (recommendations.length === 0) return null;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center gap-2 text-sm">
          <AlertTriangle className="h-4 w-4 text-amber-500" />
          Recommended Actions
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {recommendations.map((rec, idx) => (
          <div
            key={idx}
            className={`rounded-lg border px-4 py-3 ${
              rec.icon === "tvf"
                ? "border-orange-200 bg-orange-50/50"
                : rec.icon === "persistence"
                  ? "border-red-200 bg-red-50/50"
                  : "border-amber-200 bg-amber-50/50"
            }`}
          >
            <div className="flex items-start gap-3">
              {rec.icon === "tvf" ? (
                <Trash2 className="mt-0.5 h-4 w-4 shrink-0 text-orange-600" />
              ) : rec.icon === "persistence" ? (
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-red-600" />
              ) : (
                <Info className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
              )}
              <div className="flex-1 min-w-0">
                <p className="text-xs font-semibold">{rec.title}</p>
                <p className="mt-0.5 text-xs text-muted">{rec.description}</p>
                {rec.questions.length > 0 && (
                  <details className="mt-1.5">
                    <summary className="cursor-pointer text-[11px] text-muted hover:text-primary">
                      {rec.questions.length} affected question{rec.questions.length > 1 ? "s" : ""}
                    </summary>
                    <div className="mt-1 flex flex-wrap gap-1">
                      {rec.questions.slice(0, 10).map((q, i) => (
                        <Badge key={i} variant="secondary" className="text-[9px] max-w-[200px] truncate">
                          {q}
                        </Badge>
                      ))}
                      {rec.questions.length > 10 && (
                        <Badge variant="outline" className="text-[9px]">
                          +{rec.questions.length - 10} more
                        </Badge>
                      )}
                    </div>
                  </details>
                )}
                {rec.icon === "persistence" && detail.labelingSessionUrl && (
                  <div className="mt-2">
                    <Button variant="outline" size="sm" className="border-amber-300" asChild>
                      <a
                        href={detail.labelingSessionUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        <UserCheck className="mr-1.5 h-3.5 w-3.5 text-amber-600" />
                        Open Labeling Session
                        <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
                      </a>
                    </Button>
                  </div>
                )}
              </div>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function PatchesTab({ detail }: { detail: IterationDetailResponse }) {
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

  const allPatches = useMemo(() => {
    const patches: (Record<string, unknown> & { _iteration: number; _key: string })[] = [];
    for (const iter of detail.iterations) {
      for (let i = 0; i < iter.patches.length; i++) {
        patches.push({
          ...iter.patches[i],
          _iteration: iter.iteration,
          _key: `${iter.iteration}-${i}`,
        });
      }
    }
    return patches;
  }, [detail.iterations]);

  const toggleRow = (key: string) => {
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">
          All Patches ({allPatches.length})
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="max-h-[500px] overflow-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8 text-xs" />
                <TableHead className="text-xs">Iter</TableHead>
                <TableHead className="text-xs">Lever</TableHead>
                <TableHead className="text-xs">Type</TableHead>
                <TableHead className="text-xs">Target</TableHead>
                <TableHead className="text-xs">Scope</TableHead>
                <TableHead className="text-xs">Risk</TableHead>
                <TableHead className="text-xs">Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {allPatches.map((p) => {
                const isExpanded = expandedRows.has(p._key);
                return (
                  <React.Fragment key={p._key}>
                    <TableRow
                      className="cursor-pointer hover:bg-elevated/50"
                      onClick={() => toggleRow(p._key)}
                    >
                      <TableCell className="text-xs">
                        {isExpanded ? (
                          <ChevronDown className="h-3.5 w-3.5" />
                        ) : (
                          <ChevronRight className="h-3.5 w-3.5" />
                        )}
                      </TableCell>
                      <TableCell className="text-xs">{p._iteration}</TableCell>
                      <TableCell className="text-xs">
                        {String(p.scope || "—")}
                      </TableCell>
                      <TableCell className="text-xs">
                        {String(p.patchType || "—")}
                      </TableCell>
                      <TableCell className="max-w-[200px] truncate text-xs">
                        {String(p.targetObject || "—")}
                      </TableCell>
                      <TableCell className="text-xs">
                        {String(p.scope || "—")}
                      </TableCell>
                      <TableCell className="text-xs">
                        <Badge
                          variant="outline"
                          className={
                            p.riskLevel === "high"
                              ? "border-red-300 text-red-600"
                              : p.riskLevel === "medium"
                                ? "border-amber-300 text-amber-600"
                                : "text-muted"
                          }
                        >
                          {String(p.riskLevel || "—")}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs">
                        {p.rolledBack ? (
                          <Badge variant="destructive" className="text-[10px]">
                            Rolled Back
                          </Badge>
                        ) : (
                          <Badge className="bg-green-500 text-[10px]">Accepted</Badge>
                        )}
                      </TableCell>
                    </TableRow>
                    {isExpanded && (
                      <TableRow>
                        <TableCell colSpan={8}>
                          <pre className="max-h-40 overflow-auto rounded bg-elevated p-2 text-[10px]">
                            {JSON.stringify(p.command || p.patch || p, null, 2)}
                          </pre>
                        </TableCell>
                      </TableRow>
                    )}
                  </React.Fragment>
                );
              })}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

function ActivityTab({ stageEvents }: { stageEvents: StageEvent[] }) {
  const sortedEvents = useMemo(() => {
    return [...stageEvents].sort((a, b) => {
      const ta = a.startedAt ? new Date(a.startedAt).getTime() : 0;
      const tb = b.startedAt ? new Date(b.startedAt).getTime() : 0;
      return tb - ta;
    });
  }, [stageEvents]);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">
          Stage Events ({sortedEvents.length})
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="max-h-[500px] space-y-1.5 overflow-auto">
          {sortedEvents.map((event, idx) => (
            <div
              key={idx}
              className="flex items-start gap-3 rounded border px-3 py-2 text-xs"
            >
              <div
                className={`mt-0.5 h-2 w-2 shrink-0 rounded-full ${
                  event.status === "completed" || event.status === "complete"
                    ? "bg-green-500"
                    : event.status === "failed"
                      ? "bg-red-500"
                      : event.status === "started"
                        ? "bg-blue-500"
                        : "bg-gray-400"
                }`}
              />
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="font-medium">
                    {formatStageDisplayName(event.stage ?? "Unknown", event.status)}
                  </span>
                  <Badge variant="outline" className="text-[10px]">
                    {event.status}
                  </Badge>
                  {event.durationSeconds != null && (
                    <span className="text-muted">
                      {event.durationSeconds.toFixed(1)}s
                    </span>
                  )}
                </div>
                {event.startedAt && (
                  <span className="text-muted">
                    {new Date(event.startedAt).toLocaleString()}
                  </span>
                )}
                {event.errorMessage && (
                  <p className="mt-1 text-red-500">{event.errorMessage}</p>
                )}
              </div>
            </div>
          ))}
          {sortedEvents.length === 0 && (
            <p className="py-4 text-center text-muted">No stage events recorded</p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function JudgesTab({
  detail,
  links,
}: {
  detail: IterationDetailResponse;
  links: { label: string; url: string; category: string }[];
}) {
  const baseline = detail.iterations.find((i) => i.iteration === 0);
  const final = detail.iterations[detail.iterations.length - 1];

  const judgeComparison = useMemo(() => {
    if (!baseline || !final) return [];
    const allJudges = new Set([
      ...Object.keys(baseline.judgeScores),
      ...Object.keys(final.judgeScores),
    ]);
    return Array.from(allJudges)
      .sort()
      .map((judge) => {
        const b = baseline.judgeScores[judge] ?? null;
        const f = final.judgeScores[judge] ?? null;
        return {
          judge,
          baseline: b,
          final: f,
          delta: b != null && f != null ? f - b : null,
        };
      });
  }, [baseline, final]);

  const experimentLink = links.find((l) => l.category === "mlflow" && l.label.includes("Experiment"));

  return (
    <div className="space-y-4">
      {experimentLink && (
        <div className="flex justify-end">
          <Button variant="outline" size="sm" asChild>
            <a href={experimentLink.url} target="_blank" rel="noopener noreferrer">
              Open MLflow Experiment
              <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
            </a>
          </Button>
        </div>
      )}

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Judge Pass Rates: Baseline vs Final</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="text-xs">Judge</TableHead>
                <TableHead className="text-center text-xs">Baseline</TableHead>
                <TableHead className="text-center text-xs">Final</TableHead>
                <TableHead className="text-center text-xs">Delta</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {baseline && final && (() => {
                const bAcc = baseline.overallAccuracy;
                const fAcc = final.overallAccuracy;
                const accDelta = bAcc != null && fAcc != null ? fAcc - bAcc : null;
                return (
                  <TableRow className="border-b-2 font-semibold">
                    <TableCell className="text-xs font-semibold">Overall Accuracy</TableCell>
                    <TableCell className="text-center text-xs font-semibold">
                      {bAcc != null ? `${bAcc.toFixed(1)}%` : "—"}
                    </TableCell>
                    <TableCell className="text-center text-xs font-semibold">
                      {fAcc != null ? `${fAcc.toFixed(1)}%` : "—"}
                    </TableCell>
                    <TableCell className="text-center text-xs font-semibold">
                      {accDelta != null ? (
                        <span className={accDelta > 0 ? "text-green-600" : accDelta < 0 ? "text-red-500" : ""}>
                          {accDelta > 0 ? "+" : ""}
                          {accDelta.toFixed(1)}%
                        </span>
                      ) : (
                        "—"
                      )}
                    </TableCell>
                  </TableRow>
                );
              })()}
              {judgeComparison.map(({ judge, baseline: b, final: f, delta }) => (
                <TableRow key={judge}>
                  <TableCell className="text-xs font-medium">{judge}</TableCell>
                  <TableCell className="text-center text-xs">
                    {b != null ? `${b.toFixed(1)}%` : "—"}
                  </TableCell>
                  <TableCell className="text-center text-xs">
                    {f != null ? `${f.toFixed(1)}%` : "—"}
                  </TableCell>
                  <TableCell className="text-center text-xs">
                    {delta != null ? (
                      <span className={delta > 0 ? "text-green-600" : delta < 0 ? "text-red-500" : ""}>
                        {delta > 0 ? "+" : ""}
                        {delta.toFixed(1)}%
                      </span>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {detail.iterations.length > 2 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Failure Type Trends</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-xs text-muted">
              Showing questions failing across iterations. Total questions: {baseline?.totalQuestions ?? "?"}.
              Baseline failures: {baseline ? baseline.totalQuestions - baseline.correctCount : "?"}.
              Final failures: {final ? final.totalQuestions - final.correctCount : "?"}.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
