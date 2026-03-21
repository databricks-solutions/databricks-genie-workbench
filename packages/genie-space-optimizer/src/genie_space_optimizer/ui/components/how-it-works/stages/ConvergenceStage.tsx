"use client";

import { motion } from "motion/react";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import { ExpandableCard } from "../shared/ExpandableCard";
import { RUN_STATUSES, CONVERGENCE_CONDITIONS, SAFETY_CONSTANTS } from "../data";

const STATUS_COLOR_MAP: Record<string, { bg: string; border: string; text: string }> = {
  QUEUED: { bg: "bg-amber-50", border: "border-amber-300", text: "text-amber-800" },
  IN_PROGRESS: { bg: "bg-amber-50", border: "border-amber-300", text: "text-amber-800" },
  CONVERGED: { bg: "bg-emerald-50", border: "border-emerald-400", text: "text-emerald-800" },
  APPLIED: { bg: "bg-emerald-50", border: "border-emerald-400", text: "text-emerald-800" },
  STALLED: { bg: "bg-slate-100", border: "border-slate-300", text: "text-slate-700" },
  MAX_ITERATIONS: { bg: "bg-slate-100", border: "border-slate-300", text: "text-slate-700" },
  DISCARDED: { bg: "bg-slate-100", border: "border-slate-300", text: "text-slate-700" },
  CANCELLED: { bg: "bg-slate-100", border: "border-slate-300", text: "text-slate-700" },
  FAILED: { bg: "bg-red-50", border: "border-red-300", text: "text-red-800" },
};

function AnimatedDownArrow({ delay }: { delay: number }) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ delay, duration: 0.3 }}
      className="flex justify-center py-1"
    >
      <ChevronDown className="h-5 w-5 text-slate-400" />
    </motion.div>
  );
}

export function ConvergenceStage() {
  const statusMap = new Map(RUN_STATUSES.map((s) => [s.name, s]));

  const visual = (
    <div className="space-y-2">
      <div className="text-sm font-medium text-slate-500">
        Run status state machine
      </div>
      <div className="flex flex-col items-center">
        {/* Row 1: QUEUED → IN_PROGRESS */}
        <div className="flex flex-wrap justify-center gap-2">
          {["QUEUED", "IN_PROGRESS"].map((name, i) => (
            <StatusNode key={name} name={name} statusMap={statusMap} delay={i * 0.05} />
          ))}
        </div>
        <AnimatedDownArrow delay={0.15} />
        {/* Row 2: CONVERGED, STALLED, MAX_ITERATIONS, FAILED, CANCELLED */}
        <div className="flex flex-wrap justify-center gap-2">
          {["CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED", "CANCELLED"].map((name, i) => (
            <StatusNode key={name} name={name} statusMap={statusMap} delay={0.2 + i * 0.05} />
          ))}
        </div>
        <AnimatedDownArrow delay={0.5} />
        {/* Row 3: APPLIED, DISCARDED */}
        <div className="flex flex-wrap justify-center gap-2">
          {["APPLIED", "DISCARDED"].map((name, i) => (
            <StatusNode key={name} name={name} statusMap={statusMap} delay={0.55 + i * 0.05} />
          ))}
        </div>
      </div>
    </div>
  );

  const explanation = (
    <p>
      The optimizer knows when to stop. Six exit conditions govern convergence, and ten safety constants act as guardrails preventing the system from making things worse. Every run ends in a well-defined terminal state.
    </p>
  );

  const learnMore = [
    {
      id: "convergence-decision-tree",
      title: "Convergence Decision Tree",
      content: (
        <div className="space-y-3">
          {CONVERGENCE_CONDITIONS.map((c) => (
            <ExpandableCard
              key={c.name}
              title={c.name}
              subtitle={`→ ${c.resultStatus}`}
              accentColor="border-l-db-blue"
            >
              <div className="space-y-2 text-sm">
                <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">{c.formula}</code></p>
                <p className="text-muted">{c.description}</p>
              </div>
            </ExpandableCard>
          ))}
        </div>
      ),
    },
    {
      id: "safety-constants",
      title: "Safety Constants",
      content: (
        <div className="space-y-2">
          {SAFETY_CONSTANTS.map((c) => (
            <div key={c.name} className="rounded-lg border border-db-gray-border bg-white p-3">
              <div className="flex items-center gap-2">
                <span className="font-mono text-sm font-semibold">{c.name}</span>
                <span className="rounded bg-db-gray-bg px-2 py-0.5 text-xs font-medium">{c.value}</span>
              </div>
              <p className="mt-1 text-sm text-muted">{c.description}</p>
            </div>
          ))}
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Convergence & Safety"
      subtitle="Know when to stop, prevent harm"
      pipelineGroup="finalize"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}

function StatusNode({
  name,
  statusMap,
  delay,
}: {
  name: string;
  statusMap: Map<string, (typeof RUN_STATUSES)[0]>;
  delay: number;
}) {
  const status = statusMap.get(name);
  const colors = STATUS_COLOR_MAP[name] ?? {
    bg: "bg-slate-100",
    border: "border-slate-300",
    text: "text-slate-700",
  };

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ delay, duration: 0.25 }}
      className={cn(
        "min-w-[120px] rounded-xl border px-4 py-2.5 text-center shadow-sm",
        colors.bg,
        colors.border
      )}
    >
      <span className={cn("text-sm font-semibold", colors.text)}>
        {status?.name ?? name}
      </span>
    </motion.div>
  );
}
