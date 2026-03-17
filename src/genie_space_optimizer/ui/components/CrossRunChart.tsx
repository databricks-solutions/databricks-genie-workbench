"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Cell,
  ReferenceLine,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartLegend,
  ChartLegendContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export interface RunHistory {
  runId: string;
  status: string;
  baselineScore?: number | null;
  optimizedScore?: number | null;
  timestamp: string;
}

export interface CrossRunChartProps {
  runs: RunHistory[];
}

const BASELINE_COLOR = "#6366f1";
const OPTIMIZED_COLOR = "#10b981";

const chartConfig = {
  baseline: { label: "Baseline", color: BASELINE_COLOR },
  optimized: { label: "Optimized", color: OPTIMIZED_COLOR },
} satisfies ChartConfig;

function formatShortDate(timestamp: string): string {
  const d = new Date(timestamp);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function formatTooltipDate(timestamp: string): string {
  return new Date(timestamp).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function CrossRunChart({ runs }: CrossRunChartProps) {
  const filtered = runs.filter(
    (r) => r.baselineScore != null || r.optimizedScore != null,
  );

  let prevDate = "";
  const chartData = filtered.map((r, idx) => {
    const baseline = r.baselineScore ?? 0;
    const optimized = r.optimizedScore ?? 0;
    const shortDate = formatShortDate(r.timestamp);
    const showDate = shortDate !== prevDate;
    prevDate = shortDate;
    return {
      runId: r.runId,
      label: showDate ? `${shortDate} #${idx + 1}` : `#${idx + 1}`,
      tooltipDate: formatTooltipDate(r.timestamp),
      baseline,
      optimized,
      delta: optimized - baseline,
    };
  });

  if (chartData.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Optimization Trend</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted">
            No optimization data available yet
          </p>
        </CardContent>
      </Card>
    );
  }

  const allValues = chartData.flatMap((d) => [d.baseline, d.optimized].filter(Boolean));
  const dataMin = Math.min(...allValues);
  const yMin = Math.max(0, Math.floor((dataMin - 5) / 5) * 5);
  const yMax = 100;

  const avgBaseline =
    chartData.reduce((s, d) => s + d.baseline, 0) / chartData.length;

  const CustomTooltipContent = ({
    active,
    payload,
  }: {
    active?: boolean;
    payload?: Array<{
      payload: (typeof chartData)[number];
      dataKey: string;
      value: number;
    }>;
  }) => {
    if (!active || !payload?.length) return null;
    const data = payload[0]?.payload;
    if (!data) return null;

    return (
      <div className="grid min-w-[10rem] gap-1.5 rounded-lg border border-default/50 bg-surface px-3 py-2 text-xs shadow-xl">
        <div className="font-medium text-primary">
          Run #{data.runId.slice(0, 8)}
        </div>
        <div className="text-muted">{data.tooltipDate}</div>
        <div className="mt-1 grid gap-1.5">
          <div className="flex justify-between gap-6">
            <span className="flex items-center gap-1.5">
              <span
                className="inline-block h-2.5 w-2.5 rounded-sm"
                style={{ backgroundColor: BASELINE_COLOR }}
              />
              Baseline
            </span>
            <span className="font-mono font-semibold tabular-nums">
              {data.baseline.toFixed(1)}%
            </span>
          </div>
          <div className="flex justify-between gap-6">
            <span className="flex items-center gap-1.5">
              <span
                className="inline-block h-2.5 w-2.5 rounded-sm"
                style={{ backgroundColor: OPTIMIZED_COLOR }}
              />
              Optimized
            </span>
            <span className="font-mono font-semibold tabular-nums">
              {data.optimized.toFixed(1)}%
            </span>
          </div>
          <div className="flex justify-between gap-6 border-t border-default/40 pt-1.5">
            <span className="text-muted">Improvement</span>
            <span
              className={`font-mono font-semibold tabular-nums ${data.delta > 0 ? "text-emerald-600" : data.delta < 0 ? "text-red-600" : "text-muted"}`}
            >
              {data.delta > 0 ? "+" : ""}
              {data.delta.toFixed(1)}%
            </span>
          </div>
        </div>
      </div>
    );
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle>Optimization Trend</CardTitle>
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="text-xs font-normal gap-1.5">
            <span
              className="inline-block h-2 w-2 rounded-sm"
              style={{ backgroundColor: BASELINE_COLOR }}
            />
            Baseline
          </Badge>
          <Badge variant="outline" className="text-xs font-normal gap-1.5">
            <span
              className="inline-block h-2 w-2 rounded-sm"
              style={{ backgroundColor: OPTIMIZED_COLOR }}
            />
            Optimized
          </Badge>
        </div>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="h-[320px] w-full">
          <BarChart
            data={chartData}
            margin={{ top: 12, right: 12, bottom: 4, left: 4 }}
            barGap={2}
            barCategoryGap="20%"
          >
            <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.3} />
            <XAxis
              dataKey="label"
              tickLine={false}
              tickMargin={8}
              axisLine={false}
              fontSize={10}
              interval={0}
            />
            <YAxis
              domain={[yMin, yMax]}
              tickFormatter={(v: number) => `${v}%`}
              tickLine={false}
              axisLine={false}
              fontSize={11}
              width={42}
            />
            <ReferenceLine
              y={avgBaseline}
              stroke={BASELINE_COLOR}
              strokeDasharray="4 4"
              strokeOpacity={0.4}
              label={{
                value: `Avg ${avgBaseline.toFixed(0)}%`,
                position: "right",
                fontSize: 10,
                fill: BASELINE_COLOR,
                opacity: 0.7,
              }}
            />
            <ChartTooltip
              content={<CustomTooltipContent />}
              cursor={{ fill: "hsl(var(--muted))", opacity: 0.3 }}
            />
            <ChartLegend
              content={(props) => (
                <ChartLegendContent
                  payload={props.payload ?? []}
                  verticalAlign={props.verticalAlign ?? "bottom"}
                />
              )}
              wrapperStyle={{ display: "none" }}
            />
            <Bar dataKey="baseline" radius={[4, 4, 0, 0]} maxBarSize={32}>
              {chartData.map((_, i) => (
                <Cell key={`b-${i}`} fill={BASELINE_COLOR} fillOpacity={0.75} />
              ))}
            </Bar>
            <Bar dataKey="optimized" radius={[4, 4, 0, 0]} maxBarSize={32}>
              {chartData.map((entry, i) => (
                <Cell
                  key={`o-${i}`}
                  fill={OPTIMIZED_COLOR}
                  fillOpacity={entry.optimized > 0 ? 0.85 : 0.3}
                />
              ))}
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
