"use client";

import { useState } from "react";
import { BarChart, Bar, XAxis, YAxis } from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp, GitBranch } from "lucide-react";
import {
  useProvenance,
  type ProvenanceSummary,
  type ProvenanceRecord,
} from "@/lib/transparency-api";

const rootCauseConfig = {
  count: { label: "Count", color: "hsl(var(--chart-3))" },
} satisfies ChartConfig;

interface ProvenancePanelProps {
  runId: string;
  lever: number;
  iteration?: number;
}

export function ProvenancePanel({
  runId,
  lever,
  iteration,
}: ProvenancePanelProps) {
  const [open, setOpen] = useState(false);
  const { data, isLoading, isError } = useProvenance(
    runId,
    iteration,
    lever,
    { enabled: open },
  );

  const summary = data?.[0];

  return (
    <div className="space-y-2 border-t border-dashed border-db-gray-border pt-2">
      <Button
        variant="ghost"
        size="sm"
        className="h-7 gap-1.5 px-2 text-xs text-muted"
        onClick={() => setOpen(!open)}
      >
        <GitBranch className="h-3.5 w-3.5" />
        Provenance
        {open ? (
          <ChevronUp className="h-3 w-3" />
        ) : (
          <ChevronDown className="h-3 w-3" />
        )}
      </Button>

      {open && (
        <div className="space-y-4 rounded-lg border bg-white p-3">
          {isLoading && (
            <p className="py-4 text-center text-xs text-muted">
              Loading provenance...
            </p>
          )}

          {isError && (
            <p className="py-4 text-center text-xs text-danger">
              Failed to load provenance data
            </p>
          )}

          {data && !summary && (
            <p className="py-4 text-center text-xs text-muted">
              No provenance data for this lever
            </p>
          )}

          {summary && <ProvenanceContent summary={summary} />}
        </div>
      )}
    </div>
  );
}

function ProvenanceContent({ summary }: { summary: ProvenanceSummary }) {
  return (
    <div className="space-y-4">
      {/* Flow summary */}
      <FlowSummary summary={summary} />

      {/* Root cause distribution */}
      <RootCauseChart distribution={summary.rootCauseDistribution} />

      {/* Cluster detail table */}
      <ClusterTable records={summary.records} />
    </div>
  );
}

function FlowSummary({ summary }: { summary: ProvenanceSummary }) {
  const uniqueQuestions = new Set(summary.records.map((r) => r.questionId));
  const uniquePatches = new Set(
    summary.records.map((r) => r.patchType).filter(Boolean),
  );

  const gatePass = summary.gateResults["pass"] ?? 0;
  const gateFail = summary.gateResults["fail"] ?? 0;
  const gateRollback = summary.gateResults["rollback"] ?? 0;

  const steps = [
    { label: "Failing Questions", value: uniqueQuestions.size },
    { label: "Clusters", value: summary.clusterCount },
    { label: "Proposals", value: summary.proposalCount },
    { label: "Patch Types", value: uniquePatches.size },
  ];

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2 overflow-x-auto">
        {steps.map((step, i) => (
          <div key={step.label} className="flex items-center gap-2">
            <div className="flex flex-col items-center rounded-lg border bg-elevated/30 px-3 py-1.5">
              <span className="text-lg font-bold tabular-nums">
                {step.value}
              </span>
              <span className="whitespace-nowrap text-[10px] text-muted">
                {step.label}
              </span>
            </div>
            {i < steps.length - 1 && (
              <span className="text-muted">&rarr;</span>
            )}
          </div>
        ))}
        <span className="text-muted">&rarr;</span>
        <div className="flex flex-col items-center rounded-lg border px-3 py-1.5">
          <span className="text-lg font-bold">
            {gatePass > 0 ? (
              <span className="text-green-600">Pass</span>
            ) : gateFail > 0 || gateRollback > 0 ? (
              <span className="text-red-600">Fail</span>
            ) : (
              <span className="text-muted">?</span>
            )}
          </span>
          <span className="whitespace-nowrap text-[10px] text-muted">
            Gate
          </span>
        </div>
      </div>
    </div>
  );
}

function RootCauseChart({
  distribution,
}: {
  distribution: Record<string, number>;
}) {
  const chartData = Object.entries(distribution)
    .map(([name, count]) => ({
      name: name.length > 25 ? name.slice(0, 22) + "..." : name,
      count,
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  if (chartData.length === 0) return null;

  return (
    <div>
      <p className="mb-1 text-xs font-medium text-muted">
        Root Causes
      </p>
      <ChartContainer
        config={rootCauseConfig}
        className="h-[150px] w-full"
      >
        <BarChart
          layout="vertical"
          data={chartData}
          margin={{ top: 0, right: 8, bottom: 0, left: 0 }}
        >
          <XAxis type="number" hide />
          <YAxis
            type="category"
            dataKey="name"
            width={100}
            tick={{ fontSize: 10 }}
          />
          <ChartTooltip content={<ChartTooltipContent />} />
          <Bar
            dataKey="count"
            fill="hsl(var(--chart-3))"
            radius={[0, 4, 4, 0]}
          />
        </BarChart>
      </ChartContainer>
    </div>
  );
}

function ClusterTable({ records }: { records: ProvenanceRecord[] }) {
  const clusters = new Map<string, ProvenanceRecord[]>();
  for (const r of records) {
    const existing = clusters.get(r.clusterId) ?? [];
    existing.push(r);
    clusters.set(r.clusterId, existing);
  }

  const clusterEntries = Array.from(clusters.entries()).sort(
    (a, b) => b[1].length - a[1].length,
  );

  return (
    <div className="space-y-2">
      <p className="text-xs font-medium text-muted">
        Clusters ({clusterEntries.length})
      </p>
      <div className="max-h-[300px] overflow-auto space-y-2">
        {clusterEntries.map(([clusterId, clusterRecords]) => (
          <ClusterCard
            key={clusterId}
            clusterId={clusterId}
            records={clusterRecords}
          />
        ))}
      </div>
    </div>
  );
}

function ClusterCard({
  clusterId: _clusterId,
  records,
}: {
  clusterId: string;
  records: ProvenanceRecord[];
}) {
  const [expanded, setExpanded] = useState(false);
  const sample = records[0];
  const uniqueQuestions = new Set(records.map((r) => r.questionId));

  return (
    <div className="rounded-lg border text-xs">
      <button
        className="flex w-full items-center justify-between px-3 py-2 text-left hover:bg-elevated/30"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-2">
          <Badge variant="secondary" className="text-[10px]">
            {uniqueQuestions.size} questions
          </Badge>
          {sample?.resolvedRootCause && (
            <span className="text-muted">
              {sample.resolvedRootCause}
            </span>
          )}
          {sample?.patchType && (
            <Badge variant="outline" className="text-[10px]">
              {sample.patchType}
            </Badge>
          )}
        </div>
        {expanded ? (
          <ChevronUp className="h-3 w-3 shrink-0" />
        ) : (
          <ChevronDown className="h-3 w-3 shrink-0" />
        )}
      </button>

      {expanded && (
        <div className="border-t bg-elevated/20 px-3 py-2">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="h-7 text-[10px]">Question</TableHead>
                <TableHead className="h-7 text-[10px]">Judge</TableHead>
                <TableHead className="h-7 text-[10px]">Root Cause</TableHead>
                <TableHead className="h-7 text-[10px]">Method</TableHead>
                <TableHead className="h-7 text-[10px]">Fix</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {records.map((r, i) => (
                <TableRow key={i}>
                  <TableCell className="py-1 font-mono text-[10px]">
                    {r.questionId.length > 16
                      ? r.questionId.slice(0, 13) + "..."
                      : r.questionId}
                  </TableCell>
                  <TableCell className="py-1">
                    <Badge
                      variant="outline"
                      className={`text-[10px] ${r.judgeVerdict.toUpperCase() === "PASS" ? "text-green-600" : "text-red-600"}`}
                    >
                      {r.judge}: {r.judgeVerdict}
                    </Badge>
                  </TableCell>
                  <TableCell className="py-1 text-[10px]">
                    {r.resolvedRootCause}
                  </TableCell>
                  <TableCell className="py-1 text-[10px] text-muted">
                    {r.resolutionMethod}
                  </TableCell>
                  <TableCell className="max-w-[150px] truncate py-1 text-[10px] text-muted">
                    {r.counterfactualFix ?? "—"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
