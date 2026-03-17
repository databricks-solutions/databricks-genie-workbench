import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ArrowUp, ArrowDown, Minus } from "lucide-react";

interface DimensionScore {
  dimension: string;
  baseline: number;
  optimized: number;
  delta: number;
}

interface ScoreCardProps {
  baselineScore: number;
  optimizedScore: number;
  improvementPct: number;
  perDimensionScores: DimensionScore[];
}

function ImprovementIcon({ pct }: { pct: number }) {
  if (pct > 0.5) return <ArrowUp className="h-5 w-5 text-db-green" />;
  if (pct < -0.5) return <ArrowDown className="h-5 w-5 text-db-red-error" />;
  return <Minus className="h-5 w-5 text-muted" />;
}

export function ScoreCard({
  baselineScore,
  optimizedScore,
  improvementPct,
  perDimensionScores,
}: ScoreCardProps) {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <Card className="border-db-gray-border bg-white">
          <CardHeader className="pb-1">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted">
              Baseline Score
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-3xl font-bold text-primary">
              {baselineScore.toFixed(1)}%
            </span>
          </CardContent>
        </Card>

        <Card className="border-db-blue/30 bg-white">
          <CardHeader className="pb-1">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-db-blue">
              Optimized Score
            </CardTitle>
          </CardHeader>
          <CardContent>
            <span className="text-3xl font-bold text-db-blue">
              {optimizedScore.toFixed(1)}%
            </span>
          </CardContent>
        </Card>

        <Card className="border-db-gray-border bg-white">
          <CardHeader className="pb-1">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted">
              Improvement
            </CardTitle>
          </CardHeader>
          <CardContent className="flex items-center gap-2">
            <ImprovementIcon pct={improvementPct} />
            <span
              className={`text-3xl font-bold ${
                improvementPct > 0
                  ? "text-db-green"
                  : improvementPct < 0
                    ? "text-db-red-error"
                    : "text-muted"
              }`}
            >
              {improvementPct > 0 ? "+" : ""}
              {improvementPct.toFixed(1)}%
            </span>
          </CardContent>
        </Card>
      </div>

      {perDimensionScores.length > 0 && (
        <Card className="border-db-gray-border bg-white">
          <CardHeader>
            <CardTitle className="text-sm font-semibold">
              Per-Dimension Scores
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {perDimensionScores.map((dim) => (
              <div key={dim.dimension} className="space-y-1">
                <div className="flex items-center justify-between text-sm">
                  <span className="font-medium capitalize">
                    {dim.dimension.replace(/_/g, " ")}
                  </span>
                  <span className="text-xs text-muted">
                    {dim.baseline.toFixed(1)}% &rarr; {dim.optimized.toFixed(1)}
                    %
                    <span
                      className={`ml-2 font-medium ${
                        dim.delta > 0
                          ? "text-db-green"
                          : dim.delta < 0
                            ? "text-db-red-error"
                            : "text-muted"
                      }`}
                    >
                      ({dim.delta > 0 ? "+" : ""}
                      {dim.delta.toFixed(1)})
                    </span>
                  </span>
                </div>
                <div className="flex gap-1">
                  <div className="relative h-2 flex-1 overflow-hidden rounded-full bg-elevated">
                    <div
                      className="absolute inset-y-0 left-0 rounded-full bg-elevated-foreground/30"
                      style={{ width: `${Math.min(dim.baseline, 100)}%` }}
                    />
                  </div>
                  <div className="relative h-2 flex-1 overflow-hidden rounded-full bg-elevated">
                    <div
                      className="absolute inset-y-0 left-0 rounded-full bg-db-blue"
                      style={{ width: `${Math.min(dim.optimized, 100)}%` }}
                    />
                  </div>
                </div>
              </div>
            ))}
            <div className="flex justify-end gap-6 text-xs text-muted">
              <span className="flex items-center gap-1.5">
                <span className="inline-block h-2 w-4 rounded-full bg-elevated-foreground/30" />
                Baseline
              </span>
              <span className="flex items-center gap-1.5">
                <span className="inline-block h-2 w-4 rounded-full bg-db-blue" />
                Optimized
              </span>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
