import { useState } from "react";
import {
  Card,
  CardContent,
  CardHeader,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Circle,
  Loader2,
  CheckCircle2,
  XCircle,
  ChevronDown,
  Settings2,
  FlaskConical,
  Sparkles,
  Wrench,
  BarChart3,
  Rocket,
} from "lucide-react";

interface PipelineStepCardProps {
  stepNumber: number;
  name: string;
  status: string;
  description?: string;
  durationSeconds?: number | null;
  summary?: string | null;
  children?: React.ReactNode;
}

const stepIcons: Record<number, React.ReactNode> = {
  1: <Settings2 className="h-4 w-4" />,
  2: <FlaskConical className="h-4 w-4" />,
  3: <Sparkles className="h-4 w-4" />,
  4: <Wrench className="h-4 w-4" />,
  5: <BarChart3 className="h-4 w-4" />,
  6: <Rocket className="h-4 w-4" />,
};

const statusConfig: Record<
  string,
  { badge: string; badgeClass: string; ringClass: string }
> = {
  pending: {
    badge: "Pending",
    badgeClass: "bg-elevated text-muted border-muted-foreground/20",
    ringClass: "border-muted-foreground/20 bg-elevated/50 text-muted/40",
  },
  running: {
    badge: "Running",
    badgeClass: "bg-db-blue text-white",
    ringClass: "border-db-blue bg-db-blue/10 text-db-blue",
  },
  completed: {
    badge: "Complete",
    badgeClass: "bg-db-green text-white",
    ringClass: "border-db-green bg-db-green/10 text-db-green",
  },
  failed: {
    badge: "Failed",
    badgeClass: "bg-db-red-error text-white",
    ringClass: "border-db-red-error bg-db-red-error/10 text-db-red-error",
  },
};

function StatusIndicator({ status }: { status: string }) {
  if (status === "running") {
    return (
      <div className="relative flex h-8 w-8 items-center justify-center">
        <div className="absolute inset-0 animate-ping rounded-full bg-db-blue/20" />
        <div className="relative flex h-8 w-8 items-center justify-center rounded-full border-2 border-db-blue bg-db-blue/10">
          <Loader2 className="h-4 w-4 animate-spin text-db-blue" />
        </div>
      </div>
    );
  }
  if (status === "completed") {
    return (
      <div className="flex h-8 w-8 items-center justify-center rounded-full border-2 border-db-green bg-db-green/10">
        <CheckCircle2 className="h-4 w-4 text-db-green" />
      </div>
    );
  }
  if (status === "failed") {
    return (
      <div className="flex h-8 w-8 items-center justify-center rounded-full border-2 border-db-red-error bg-db-red-error/10">
        <XCircle className="h-4 w-4 text-db-red-error" />
      </div>
    );
  }
  return (
    <div className="flex h-8 w-8 items-center justify-center rounded-full border-2 border-muted-foreground/20 bg-elevated/50">
      <Circle className="h-4 w-4 text-muted" />
    </div>
  );
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return secs > 0 ? `${mins}m ${secs}s` : `${mins}m`;
}

export function PipelineStepCard({
  stepNumber,
  name,
  status,
  description,
  durationSeconds,
  summary,
  children,
}: PipelineStepCardProps) {
  const [open, setOpen] = useState(status === "running" || status === "failed");
  const cfg = statusConfig[status] ?? statusConfig.pending;
  const hasExpandableContent = summary || children;
  const isExpandable =
    hasExpandableContent && (status === "completed" || status === "running" || status === "failed");

  const cardBorder =
    status === "running"
      ? "border-db-blue/30 shadow-md shadow-db-blue/5"
      : status === "failed"
        ? "border-db-red-error/30"
        : status === "completed"
          ? "border-db-green/20"
          : "border-db-gray-border";

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <Card className={`border transition-all duration-300 ${cardBorder} bg-white`}>
        <CollapsibleTrigger asChild disabled={!isExpandable}>
          <CardHeader
            className={`flex flex-row items-start gap-4 py-4 ${
              isExpandable ? "cursor-pointer" : "cursor-default"
            }`}
          >
            <StatusIndicator status={status} />

            <div className="flex min-w-0 flex-1 flex-col gap-1">
              <div className="flex items-center gap-2">
                <span className="flex items-center gap-1 text-xs font-medium text-muted">
                  {stepIcons[stepNumber]}
                  Step {stepNumber}
                </span>
                <span className={`text-sm font-semibold ${
                  status === "pending" ? "text-secondary" : ""
                }`}>
                  {name}
                </span>
              </div>

              {description && (
                <p className="text-xs leading-relaxed text-muted">
                  {description}
                </p>
              )}

              {status === "running" && !summary && (
                <p className="text-xs text-db-blue">
                  Processing…
                </p>
              )}

              {summary && status !== "pending" && (
                <p className="text-xs font-medium text-muted">
                  {summary}
                </p>
              )}
            </div>

            <div className="flex shrink-0 items-center gap-2">
              {durationSeconds != null && (
                <span className="text-xs tabular-nums text-muted">
                  {formatDuration(durationSeconds)}
                </span>
              )}
              <Badge variant="outline" className={cfg.badgeClass}>
                {status === "running" && (
                  <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                )}
                {cfg.badge}
              </Badge>
              {isExpandable && (
                <ChevronDown
                  className={`h-4 w-4 text-muted transition-transform duration-200 ${
                    open ? "rotate-180" : ""
                  }`}
                />
              )}
            </div>
          </CardHeader>
        </CollapsibleTrigger>

        <CollapsibleContent>
          <CardContent className="space-y-3 border-t border-dashed border-db-gray-border pt-3">
            {children}
          </CardContent>
        </CollapsibleContent>
      </Card>
    </Collapsible>
  );
}
