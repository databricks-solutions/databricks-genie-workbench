"use client";

import { motion } from "motion/react";
import { ArrowDown, ArrowDownRight, RotateCcw } from "lucide-react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";

const GATES = [
  { name: "Best So Far", subtitle: "Champion accuracy", width: "w-[92%]", bg: "bg-blue-50", border: "border-blue-200", text: "text-blue-800" },
  { name: "Candidate", subtitle: "Full benchmark eval", width: "w-[72%]", bg: "bg-amber-50", border: "border-amber-200", text: "text-amber-800" },
  { name: "Decision", subtitle: "Accept or rollback", width: "w-[52%]", bg: "bg-emerald-50", border: "border-emerald-200", text: "text-emerald-800" },
] as const;

export function AcceptanceRollbackStage() {
  const visual = (
    <div className="flex flex-col items-center gap-0">
      {GATES.map((gate, index) => (
        <div key={gate.name} className="flex w-full flex-col items-center">
          <motion.div
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.15, duration: 0.3 }}
            className={cn(
              "flex flex-col items-center justify-center rounded-xl border px-6 py-4 shadow-sm transition-shadow hover:shadow-md",
              gate.width,
              gate.bg,
              gate.border
            )}
          >
            <span className={cn("text-sm font-semibold", gate.text)}>
              {gate.name}
            </span>
            <p className={cn("mt-0.5 text-xs", gate.text.replace("800", "600"))}>
              {gate.subtitle}
            </p>
          </motion.div>
          {index < GATES.length - 1 && (
            <div className="flex w-full items-center justify-between px-2 py-2">
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.3 + index * 0.15, duration: 0.25 }}
                className="flex items-center gap-1.5 text-xs font-medium text-emerald-600"
              >
                <ArrowDown className="h-4 w-4 shrink-0" />
                <span>Evaluate</span>
              </motion.div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.35 + index * 0.15, duration: 0.25 }}
                className="flex items-center gap-1.5 rounded-lg border border-red-200 bg-red-50 px-2.5 py-1.5 text-xs font-medium text-red-700"
              >
                <RotateCcw className="h-3.5 w-3.5 shrink-0" />
                <span>{"No improvement -> rollback"}</span>
              </motion.div>
            </div>
          )}
        </div>
      ))}
      <div className="mt-4 flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-2.5 text-sm">
        <ArrowDownRight className="h-5 w-5 shrink-0 text-amber-600" aria-hidden />
        <span className="text-amber-800">
          <strong>Strict promotion:</strong> a candidate must beat the current best accuracy to become the champion
        </span>
      </div>
    </div>
  );

  const explanation = (
    <p>
      The current loop applies a candidate patch set, evaluates it on the full benchmark corpus,
      and accepts it only if accuracy improves. Otherwise the applier rolls the live space back
      to the previous config snapshot and records the rejected attempt.
    </p>
  );

  const learnMore = [
    {
      id: "candidate-eval",
      title: "Candidate Evaluation",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            Each applied patch set creates one scored iteration with eval_scope=full. The same
            benchmark corpus is used for baseline and candidates, so accuracy deltas are comparable.
          </p>
        </div>
      ),
    },
    {
      id: "acceptance",
      title: "Acceptance Rule",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            Candidate accuracy must be greater than the previous best. Accepted candidates update
            best_iteration and best_accuracy, then feed the next proposal attempt.
          </p>
        </div>
      ),
    },
    {
      id: "rollback",
      title: "Rollback Mechanism",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            Non-improving or invalid candidates are rolled back with the applier&apos;s pre-patch
            snapshot. The patch and iteration rows are marked rolled back for auditability.
          </p>
        </div>
      ),
    },
    {
      id: "terminal-reason",
      title: "Terminal Reason",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            Optimize stamps the champion row with TARGET_REACHED, MAX_ATTEMPTS,
            EVAL_INVALID, or NO_NEW_HYPOTHESIS. Publish & Audit reads that stamped value directly.
          </p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Acceptance & Rollback"
      subtitle="Evaluate the full corpus and keep only improvements"
      pipelineGroup="leverLoop"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
