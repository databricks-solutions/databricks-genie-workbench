"use client";

import { useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Cell,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp } from "lucide-react";

export interface StageEvent {
  stage?: string;
  status?: string;
  startedAt?: string | null;
  completedAt?: string | null;
  durationSeconds?: number | null;
  errorMessage?: string | null;
}

interface StageTimelineProps {
  stageEvents: StageEvent[];
}

const chartConfig = {
  duration: { label: "Duration (s)" },
} satisfies ChartConfig;

function shortenStageName(stage: string): string {
  const cleaned = stage
    .replace(/_STARTED$|_COMPLETE$|_COMPLETED$|_FAILED$/i, "")
    .replace(/_/g, " ");
  return cleaned
    .split(" ")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

function formatStatusSuffix(status?: string): string {
  if (!status) return "";
  const s = status.toLowerCase();
  if (s === "complete" || s === "completed") return "Done";
  if (s === "failed" || s === "error") return "Failed";
  if (s === "started" || s === "running") return "Running";
  if (s === "rolled_back") return "Rolled Back";
  if (s === "skipped") return "Skipped";
  return status;
}

export function formatStageDisplayName(stage: string, status?: string): string {
  const base = shortenStageName(stage);
  const s = (status ?? "").toLowerCase();
  if (s === "complete" || s === "completed") {
    if (/_STARTED$/i.test(stage)) return `${base} Ended`;
    return `${base} Complete`;
  }
  if (s === "failed") return `${base} Failed`;
  if (s === "started" || s === "running") return `${base} Running`;
  return base;
}

function getStatusColor(status?: string): string {
  if (!status) return "var(--chart-4)";
  const s = status.toLowerCase();
  if (s === "complete" || s === "completed") return "var(--chart-2)";
  if (s === "failed" || s === "error") return "var(--chart-5)";
  if (s === "started" || s === "running") return "var(--chart-1)";
  if (s === "rolled_back") return "var(--chart-3)";
  if (s === "skipped") return "var(--muted-foreground)";
  return "var(--chart-4)";
}

function formatDateTime(iso?: string | null): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch {
    return iso;
  }
}

export function StageTimeline({ stageEvents }: StageTimelineProps) {
  const [open, setOpen] = useState(stageEvents.length > 0);

  const rawData = stageEvents
    .map((e) => {
      let duration = e.durationSeconds;
      if ((duration == null || duration <= 0) && e.startedAt && e.completedAt) {
        duration =
          (new Date(e.completedAt).getTime() -
            new Date(e.startedAt).getTime()) /
          1000;
      }
      return {
        ...e,
        shortStage: e.stage ? shortenStageName(e.stage) : "Unknown",
        durationSeconds: duration ?? 0,
      };
    })
    .filter((e) => e.durationSeconds > 0);

  // Deduplicate labels: append status suffix when multiple entries share a name
  const nameCounts = new Map<string, number>();
  for (const d of rawData) nameCounts.set(d.shortStage, (nameCounts.get(d.shortStage) ?? 0) + 1);
  const chartData = rawData.map((d) => ({
    ...d,
    shortStage:
      (nameCounts.get(d.shortStage) ?? 1) > 1
        ? `${d.shortStage} (${formatStatusSuffix(d.status)})`
        : d.shortStage,
  }));

  const hasData = chartData.length > 0;
  const chartHeight = Math.max(300, chartData.length * 32);

  return (
    <Card>
      <Collapsible open={open} onOpenChange={setOpen}>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">Stage Timeline</CardTitle>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" size="sm" className="gap-1">
                {open ? (
                  <>
                    Hide stage timeline <ChevronUp className="h-4 w-4" />
                  </>
                ) : (
                  <>
                    Show stage timeline <ChevronDown className="h-4 w-4" />
                  </>
                )}
              </Button>
            </CollapsibleTrigger>
          </div>
        </CardHeader>
        <CollapsibleContent>
          <CardContent className="pt-0">
            {!hasData ? (
              <p className="text-sm text-muted py-6 text-center">
                No timeline data available
              </p>
            ) : (
              <ChartContainer config={chartConfig} className="w-full" style={{ height: `${chartHeight}px` }}>
                <BarChart
                  layout="vertical"
                  data={chartData}
                  margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                >
                  <XAxis type="number" dataKey="durationSeconds" />
                  <YAxis
                    type="category"
                    dataKey="shortStage"
                    width={180}
                    tick={{ fontSize: 11 }}
                  />
                  <ChartTooltip
                    content={
                      <ChartTooltipContent
                        formatter={(_value, _name, item) => {
                          const payload = item.payload as (typeof chartData)[0];
                          return (
                            <div className="grid gap-1.5 min-w-[200px]">
                              <div>
                                <span className="text-muted">
                                  Stage:{" "}
                                </span>
                                <span className="font-medium">
                                  {payload.stage ?? "Unknown"}
                                </span>
                              </div>
                              <div>
                                <span className="text-muted">
                                  Status:{" "}
                                </span>
                                <span className="font-medium capitalize">
                                  {payload.status ?? "—"}
                                </span>
                              </div>
                              <div>
                                <span className="text-muted">
                                  Duration:{" "}
                                </span>
                                <span className="font-medium tabular-nums">
                                  {payload.durationSeconds}s
                                </span>
                              </div>
                              <div>
                                <span className="text-muted">
                                  Started:{" "}
                                </span>
                                <span className="font-mono text-xs">
                                  {formatDateTime(payload.startedAt)}
                                </span>
                              </div>
                              <div>
                                <span className="text-muted">
                                  Completed:{" "}
                                </span>
                                <span className="font-mono text-xs">
                                  {formatDateTime(payload.completedAt)}
                                </span>
                              </div>
                              {payload.errorMessage && (
                                <div>
                                  <span className="text-muted">
                                    Error:{" "}
                                  </span>
                                  <span className="text-danger text-xs">
                                    {payload.errorMessage}
                                  </span>
                                </div>
                              )}
                            </div>
                          );
                        }}
                      />
                    }
                  />
                  <Bar dataKey="durationSeconds" radius={[0, 4, 4, 0]}>
                    {chartData.map((entry, index) => (
                      <Cell
                        key={entry.stage ?? index}
                        fill={getStatusColor(entry.status)}
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ChartContainer>
            )}
          </CardContent>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  );
}
