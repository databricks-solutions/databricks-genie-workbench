"use client";

import { useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Cell } from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp } from "lucide-react";
import { useAsiResults, type AsiSummary, type AsiResult } from "@/lib/transparency-api";

const failureChartConfig = {
  count: { label: "Count", color: "hsl(var(--chart-5))" },
} satisfies ChartConfig;

const blameChartConfig = {
  count: { label: "Count", color: "hsl(var(--chart-3))" },
} satisfies ChartConfig;

const judgeChartConfig = {
  passRate: { label: "Pass Rate %", color: "hsl(var(--chart-2))" },
} satisfies ChartConfig;

interface AsiResultsPanelProps {
  runId: string;
  availableIterations?: number[];
}

export function AsiResultsPanel({
  runId,
  availableIterations = [0],
}: AsiResultsPanelProps) {
  const [open, setOpen] = useState(false);
  const [selectedIteration, setSelectedIteration] = useState(0);
  const { data, isLoading, isError } = useAsiResults(runId, selectedIteration, {
    enabled: open,
  });

  return (
    <Card>
      <Collapsible open={open} onOpenChange={setOpen}>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">Judge Feedback (ASI)</CardTitle>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" size="sm" className="gap-1">
                {open ? (
                  <>
                    Hide judge details <ChevronUp className="h-4 w-4" />
                  </>
                ) : (
                  <>
                    View judge details <ChevronDown className="h-4 w-4" />
                  </>
                )}
              </Button>
            </CollapsibleTrigger>
          </div>
        </CardHeader>
        <CollapsibleContent>
          <CardContent className="space-y-6 pt-0">
            {availableIterations.length > 1 && (
              <div className="flex items-center gap-2">
                <span className="text-xs font-medium text-muted">
                  Iteration:
                </span>
                <div className="flex gap-1">
                  {availableIterations.map((iter) => (
                    <Button
                      key={iter}
                      size="sm"
                      variant={
                        selectedIteration === iter ? "default" : "outline"
                      }
                      onClick={() => setSelectedIteration(iter)}
                      className="h-7 px-2 text-xs"
                    >
                      {iter === 0 ? "Baseline" : `Iteration ${iter}`}
                    </Button>
                  ))}
                </div>
              </div>
            )}

            {isLoading && (
              <p className="py-8 text-center text-sm text-muted">
                Loading judge feedback...
              </p>
            )}

            {isError && (
              <p className="py-8 text-center text-sm text-danger">
                Failed to load judge feedback
              </p>
            )}

            {data && <AsiContent data={data} />}
          </CardContent>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  );
}

function AsiContent({ data }: { data: AsiSummary }) {
  if (data.totalResults === 0) {
    return (
      <p className="py-6 text-center text-sm text-muted">
        No ASI results for this iteration
      </p>
    );
  }

  const passRate =
    data.totalResults > 0
      ? ((data.passCount / data.totalResults) * 100).toFixed(1)
      : "0";

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3">
        <div className="rounded-lg border p-3 text-center">
          <p className="text-xs font-medium text-muted">
            Total Verdicts
          </p>
          <p className="text-2xl font-bold">{data.totalResults}</p>
        </div>
        <div className="rounded-lg border border-green-200 bg-green-50 p-3 text-center">
          <p className="text-xs font-medium text-green-700">Pass Rate</p>
          <p className="text-2xl font-bold text-green-700">{passRate}%</p>
        </div>
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-center">
          <p className="text-xs font-medium text-red-700">Failures</p>
          <p className="text-2xl font-bold text-red-700">{data.failCount}</p>
        </div>
      </div>

      {/* Charts row */}
      <div className="grid gap-4 lg:grid-cols-3">
        <FailureTypeChart distribution={data.failureTypeDistribution} />
        <BlameChart distribution={data.blameDistribution} />
        <JudgePassRateChart rates={data.judgePassRates} />
      </div>

      {/* Question table */}
      <QuestionTable results={data.results} />
    </div>
  );
}

function FailureTypeChart({
  distribution,
}: {
  distribution: Record<string, number>;
}) {
  const chartData = Object.entries(distribution)
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  if (chartData.length === 0) {
    return (
      <div className="rounded-lg border p-4">
        <p className="text-xs font-medium text-muted">
          Failure Types
        </p>
        <p className="mt-2 text-xs text-muted">No failures found</p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border p-4">
      <p className="mb-2 text-xs font-medium text-muted">
        Failure Types
      </p>
      <ChartContainer config={failureChartConfig} className="h-[200px] w-full">
        <BarChart
          layout="vertical"
          data={chartData}
          margin={{ top: 0, right: 8, bottom: 0, left: 0 }}
        >
          <XAxis type="number" hide />
          <YAxis
            type="category"
            dataKey="name"
            width={120}
            tick={{ fontSize: 10 }}
          />
          <ChartTooltip content={<ChartTooltipContent />} />
          <Bar dataKey="count" fill="hsl(var(--chart-5))" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ChartContainer>
    </div>
  );
}

function BlameChart({
  distribution,
}: {
  distribution: Record<string, number>;
}) {
  const chartData = Object.entries(distribution)
    .map(([name, count]) => ({
      name: name.length > 30 ? name.slice(0, 27) + "..." : name,
      count,
      fullName: name,
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  if (chartData.length === 0) {
    return (
      <div className="rounded-lg border p-4">
        <p className="text-xs font-medium text-muted">
          Blame Attribution
        </p>
        <p className="mt-2 text-xs text-muted">
          No blame data available
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border p-4">
      <p className="mb-2 text-xs font-medium text-muted">
        Blame Attribution
      </p>
      <ChartContainer config={blameChartConfig} className="h-[200px] w-full">
        <BarChart
          layout="vertical"
          data={chartData}
          margin={{ top: 0, right: 8, bottom: 0, left: 0 }}
        >
          <XAxis type="number" hide />
          <YAxis
            type="category"
            dataKey="name"
            width={120}
            tick={{ fontSize: 10 }}
          />
          <ChartTooltip content={<ChartTooltipContent />} />
          <Bar dataKey="count" fill="hsl(var(--chart-3))" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ChartContainer>
    </div>
  );
}

function JudgePassRateChart({ rates }: { rates: Record<string, number> }) {
  const chartData = Object.entries(rates)
    .map(([name, passRate]) => ({ name, passRate }))
    .sort((a, b) => a.passRate - b.passRate);

  if (chartData.length === 0) {
    return (
      <div className="rounded-lg border p-4">
        <p className="text-xs font-medium text-muted">
          Judge Pass Rates
        </p>
        <p className="mt-2 text-xs text-muted">No judge data</p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border p-4">
      <p className="mb-2 text-xs font-medium text-muted">
        Judge Pass Rates
      </p>
      <ChartContainer config={judgeChartConfig} className="h-[200px] w-full">
        <BarChart
          layout="vertical"
          data={chartData}
          margin={{ top: 0, right: 8, bottom: 0, left: 0 }}
        >
          <XAxis type="number" domain={[0, 100]} hide />
          <YAxis
            type="category"
            dataKey="name"
            width={120}
            tick={{ fontSize: 10 }}
          />
          <ChartTooltip content={<ChartTooltipContent />} />
          <Bar dataKey="passRate" radius={[0, 4, 4, 0]}>
            {chartData.map((entry) => (
              <Cell
                key={entry.name}
                fill={
                  entry.passRate >= 80
                    ? "hsl(var(--chart-2))"
                    : entry.passRate >= 50
                      ? "hsl(var(--chart-3))"
                      : "hsl(var(--chart-5))"
                }
              />
            ))}
          </Bar>
        </BarChart>
      </ChartContainer>
    </div>
  );
}

function QuestionTable({ results }: { results: AsiResult[] }) {
  const grouped = new Map<string, AsiResult[]>();
  for (const r of results) {
    const existing = grouped.get(r.questionId) ?? [];
    existing.push(r);
    grouped.set(r.questionId, existing);
  }

  const questions = Array.from(grouped.entries()).map(
    ([questionId, verdicts]) => {
      const passCount = verdicts.filter((v) => {
        const val = v.value.toLowerCase().trim();
        return ["yes", "genie_correct", "pass", "true"].includes(val);
      }).length;
      return { questionId, verdicts, passCount, total: verdicts.length };
    },
  );

  return (
    <div className="space-y-2">
      <p className="text-xs font-medium text-muted">
        Per-Question Results ({questions.length} questions)
      </p>
      <div className="max-h-[400px] overflow-auto rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[200px]">Question ID</TableHead>
              <TableHead className="text-center">Verdicts</TableHead>
              <TableHead>Failures</TableHead>
              <TableHead>Fix Suggestion</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {questions.map(({ questionId, verdicts, passCount, total }) => {
              const failures = verdicts.filter((v) => {
                const val = v.value.toLowerCase().trim();
                return !["yes", "genie_correct", "pass", "true"].includes(val);
              });
              const mainFix = failures.find((f) => f.counterfactualFix)
                ?.counterfactualFix;

              return (
                <QuestionRow
                  key={questionId}
                  questionId={questionId}
                  passCount={passCount}
                  total={total}
                  failures={failures}
                  mainFix={mainFix ?? null}
                />
              );
            })}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

function QuestionRow({
  questionId,
  passCount,
  total,
  failures,
  mainFix,
}: {
  questionId: string;
  passCount: number;
  total: number;
  failures: AsiResult[];
  mainFix: string | null;
}) {
  const [expanded, setExpanded] = useState(false);
  const allPass = failures.length === 0;

  return (
    <>
      <TableRow
        className="cursor-pointer hover:bg-elevated/50"
        onClick={() => setExpanded(!expanded)}
      >
        <TableCell className="font-mono text-xs">
          {questionId.length > 24
            ? questionId.slice(0, 21) + "..."
            : questionId}
        </TableCell>
        <TableCell className="text-center">
          <Badge
            variant="outline"
            className={
              allPass
                ? "border-green-200 text-green-700"
                : "border-red-200 text-red-700"
            }
          >
            {passCount}/{total} pass
          </Badge>
        </TableCell>
        <TableCell>
          <div className="flex flex-wrap gap-1">
            {failures.slice(0, 3).map((f, i) => (
              <Badge key={i} variant="secondary" className="text-[10px]">
                {f.judge}: {f.failureType ?? f.value}
              </Badge>
            ))}
            {failures.length > 3 && (
              <Badge variant="secondary" className="text-[10px]">
                +{failures.length - 3}
              </Badge>
            )}
          </div>
        </TableCell>
        <TableCell className="max-w-[200px] truncate text-xs text-muted">
          {mainFix ?? "—"}
        </TableCell>
      </TableRow>
      {expanded && failures.length > 0 && (
        <TableRow>
          <TableCell colSpan={4} className="bg-elevated/30 p-3">
            <div className="space-y-2">
              {failures.map((f, i) => (
                <div
                  key={i}
                  className="rounded border bg-white p-2 text-xs"
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="secondary">{f.judge}</Badge>
                    <span className="text-red-600">{f.value}</span>
                    {f.failureType && (
                      <Badge variant="outline">{f.failureType}</Badge>
                    )}
                    {f.severity && (
                      <span className="text-muted">
                        severity: {f.severity}
                      </span>
                    )}
                  </div>
                  {f.wrongClause && (
                    <p className="mt-1 text-muted">
                      Wrong clause: {f.wrongClause}
                    </p>
                  )}
                  {f.expectedValue && (
                    <p className="text-muted">
                      Expected: {f.expectedValue}
                    </p>
                  )}
                  {f.actualValue && (
                    <p className="text-muted">
                      Actual: {f.actualValue}
                    </p>
                  )}
                  {f.counterfactualFix && (
                    <p className="mt-1 text-green-700">
                      Fix: {f.counterfactualFix}
                    </p>
                  )}
                  {f.blameSet.length > 0 && (
                    <p className="text-muted">
                      Blame: {f.blameSet.join(", ")}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </TableCell>
        </TableRow>
      )}
    </>
  );
}
