"use client";

import {
  Search,
  Layers,
  ArrowUpDown,
  BrainCircuit,
  Lightbulb,
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
  { label: "Extract Failures", icon: Search },
  { label: "Cluster", icon: Layers },
  { label: "Rank", icon: ArrowUpDown },
  { label: "Strategize", icon: BrainCircuit },
  { label: "Generate Proposals", icon: Lightbulb },
  { label: "Convert to Patches", icon: Code },
  { label: "Apply Patches", icon: Wrench },
  { label: "3-Gate Eval", icon: ShieldCheck },
  { label: "Accept/Rollback", icon: RotateCcw },
  { label: "Reflect", icon: BookOpen },
  { label: "Check Convergence", icon: CircleDot },
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
              Iteration 1 of up to 5
            </Badge>
            <p className="max-w-md text-center text-xs text-slate-500">
              Each step is detailed in the next stages — from failure extraction
              through clustering, ranking, strategizing, and 3-gate validation.
            </p>
          </div>
        </div>
      }
      explanation={
        <>
          This is the heart of the optimizer. Each iteration analyzes
          what&apos;s failing, clusters those failures by root cause,
          generates targeted patches, and validates them through a rigorous
          3-gate evaluation. If the patches help, they stick. If not,
          they&apos;re rolled back and the system tries a different approach.
        </>
      }
    />
  );
}
