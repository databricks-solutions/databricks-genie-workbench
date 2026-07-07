"use client";

import {
  Search,
  Layers,
  BrainCircuit,
  Code,
  Wrench,
  ShieldCheck,
  RotateCcw,
  BookOpen,
  CircleDot,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { StageScreen } from "../StageScreen";
import { LoopDiagram } from "../shared/LoopDiagram";
import { PIPELINE_GROUP_COLORS } from "../data";
import { cn } from "@/lib/utils";

const LOOP_STEPS = [
  { label: "Load Failures", icon: Search },
  { label: "Pack Context", icon: Layers },
  { label: "Propose Patch Set", icon: BrainCircuit },
  { label: "Safety Screen", icon: ShieldCheck },
  { label: "Apply Patches", icon: Wrench },
  { label: "Full Benchmark Eval", icon: Code },
  { label: "Accept/Rollback", icon: RotateCcw },
  { label: "Reflect", icon: BookOpen },
  { label: "Stamp Stop Reason", icon: CircleDot },
];

const colors = PIPELINE_GROUP_COLORS.leverLoop;

export function LeverLoopStage() {
  return (
    <StageScreen
      title="The Optimization Loop"
      subtitle="Iteratively improve via failure analysis and targeted patches"
      pipelineGroup="leverLoop"
      visual={
        <div className="space-y-4">
          <p className="text-center text-sm font-semibold uppercase tracking-wider text-slate-500">
            The Optimization Cycle
          </p>
          <div
            className={cn(
              "overflow-x-auto rounded-xl p-6",
              colors.bg,
            )}
          >
            <LoopDiagram steps={LOOP_STEPS} className="min-w-max" />
          </div>
          <div className="flex flex-col items-center gap-3">
            <Badge
              variant="secondary"
              className={cn(
                "border-amber-200 bg-amber-50 px-3 py-1 text-xs font-medium",
                colors.accent,
              )}
            >
              Attempt 1 of up to 3
            </Badge>
            <p className="max-w-md text-center text-xs text-slate-500">
              Each attempt packs current failures, proposes one patch set, screens it,
              evaluates the full corpus, and keeps only accuracy improvements.
            </p>
          </div>
        </div>
      }
      explanation={
        <>
          This is the heart of the optimizer. Each iteration analyzes
          what&apos;s failing, generates a targeted patch set, evaluates the full
          benchmark corpus, and keeps only candidates that improve accuracy. If
          a candidate does not help, it is rolled back and the next attempt uses
          the recorded reflection.
        </>
      }
    />
  );
}
