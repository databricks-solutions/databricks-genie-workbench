"use client";

import { motion } from "motion/react";
import { ArrowDown, ArrowDownRight, RotateCcw } from "lucide-react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import { GATE_DEFINITIONS } from "../data";

const GATES = [
  { name: "Slice", subtitle: "Quick check", width: "w-[92%]", bg: "bg-blue-50", border: "border-blue-200", text: "text-blue-800" },
  { name: "P0", subtitle: "Critical questions", width: "w-[72%]", bg: "bg-amber-50", border: "border-amber-200", text: "text-amber-800" },
  { name: "Full", subtitle: "Comprehensive", width: "w-[52%]", bg: "bg-emerald-50", border: "border-emerald-200", text: "text-emerald-800" },
] as const;

export function ThreeGatesStage() {
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
              {gate.name} Gate
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
                <span>Pass</span>
              </motion.div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.35 + index * 0.15, duration: 0.25 }}
                className="flex items-center gap-1.5 rounded-lg border border-red-200 bg-red-50 px-2.5 py-1.5 text-xs font-medium text-red-700"
              >
                <RotateCcw className="h-3.5 w-3.5 shrink-0" />
                <span>Fail → Rollback</span>
              </motion.div>
            </div>
          )}
        </div>
      ))}
      <div className="mt-4 flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-2.5 text-sm">
        <ArrowDownRight className="h-5 w-5 shrink-0 text-amber-600" aria-hidden />
        <span className="text-amber-800">
          <strong>Fast fail:</strong> Slice Gate failure skips P0 and Full gates — saving compute and catching regressions early
        </span>
      </div>
    </div>
  );

  const explanation = (
    <p>
      Every set of patches must pass through three progressively expensive evaluation gates. If the cheap Slice Gate catches a regression, the expensive Full Gate is never run — saving compute and catching problems early. This builds trust: the system won&apos;t make things worse.
    </p>
  );

  const sliceGate = GATE_DEFINITIONS[0];
  const p0Gate = GATE_DEFINITIONS[1];
  const fullGate = GATE_DEFINITIONS[2];

  const learnMore = [
    {
      id: "slice-gate",
      title: "Slice Gate",
      content: (
        <div className="space-y-2 text-sm">
          <p>{sliceGate?.description}</p>
          <p><strong>Question selection:</strong> {sliceGate?.questionSelection}</p>
          <p><strong>Pass criteria:</strong> {sliceGate?.passCriteria}</p>
          <p><strong>Tolerance:</strong> {sliceGate?.tolerance}</p>
        </div>
      ),
    },
    {
      id: "p0-gate",
      title: "P0 Gate",
      content: (
        <div className="space-y-2 text-sm">
          <p>{p0Gate?.description}</p>
          <p><strong>Question selection:</strong> {p0Gate?.questionSelection}</p>
          <p><strong>Pass criteria:</strong> {p0Gate?.passCriteria}</p>
          <p><strong>Tolerance:</strong> {p0Gate?.tolerance}</p>
        </div>
      ),
    },
    {
      id: "full-gate",
      title: "Full Gate",
      content: (
        <div className="space-y-2 text-sm">
          <p>{fullGate?.description}</p>
          <p><strong>Question selection:</strong> {fullGate?.questionSelection}</p>
          <p><strong>Pass criteria:</strong> {fullGate?.passCriteria}</p>
          <p><strong>Tolerance:</strong> {fullGate?.tolerance}</p>
          <p><strong>Accuracy guard:</strong> detect_regressions with REGRESSION_THRESHOLD=5%.</p>
        </div>
      ),
    },
    {
      id: "rollback",
      title: "Rollback Mechanism",
      content: (
        <div className="space-y-2 text-sm">
          <p><strong>Primary:</strong> Restore from <code className="rounded bg-db-gray-bg px-1.5 py-0.5">pre_snapshot</code> (the config state before patches were applied).</p>
          <p><strong>Fallback:</strong> Individual <code className="rounded bg-db-gray-bg px-1.5 py-0.5">rollback_commands</code> per patch if snapshot restore fails.</p>
        </div>
      ),
    },
    {
      id: "reflection-buffer",
      title: "Reflection Buffer",
      content: (
        <div className="space-y-2 text-sm">
          <p><strong>Entry shape:</strong> 17 fields per reflection entry.</p>
          <p><strong>Rendering for strategist:</strong> REFLECTION_WINDOW_FULL=3 — the strategist sees the last 3 iterations of reflection context.</p>
          <p><strong>DO NOT RETRY block:</strong> Failed strategies are recorded so the strategist won&apos;t retry the same approach.</p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="The 3-Gate Pattern"
      subtitle="Validate improvements, catch regressions"
      pipelineGroup="leverLoop"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
