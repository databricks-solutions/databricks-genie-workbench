"use client";

import {
  FileText,
  Globe,
  ShieldCheck,
  BarChart3,
} from "lucide-react";
import { motion } from "motion/react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import { ScoreGauge } from "../shared/ScoreGauge";
import { PIPELINE_GROUP_COLORS } from "../data";

const FLOW_NODES = [
  { label: "Valid Benchmarks", icon: FileText },
  { label: "Genie Benchmark API", icon: Globe },
  { label: "Iteration 0", icon: ShieldCheck },
  { label: "Accuracy Baseline", icon: BarChart3 },
] as const;

const EVALUATION_ORCHESTRATION_STEPS = [
  "Load the EXPLAIN-valid benchmark corpus produced by Benchmark QC & Repair",
  "Call the native Genie Benchmark API against the current live space",
  "Persist iteration 0 with eval_scope=full and reflection phase=baseline",
  "Set the run's initial best_iteration and best_accuracy",
  "Stop immediately if the baseline already reaches the configured target",
];

function AnimatedArrow() {
  return (
    <motion.div
      className="flex shrink-0 items-center"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ delay: 0.3, duration: 0.4 }}
    >
      <svg
        width="36"
        height="20"
        viewBox="0 0 36 20"
        fill="none"
        className="text-slate-300"
      >
        <motion.line
          x1="0"
          y1="10"
          x2="24"
          y2="10"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeDasharray="6 4"
          initial={{ strokeDashoffset: 20 }}
          animate={{ strokeDashoffset: 0 }}
          transition={{ duration: 1.2, repeat: Infinity, ease: "linear" }}
        />
        <path
          d="M24 10 L34 10 L28 4 M34 10 L28 16"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          fill="none"
        />
      </svg>
    </motion.div>
  );
}

export function OptimizeBaselineStage() {
  const baselineColors = PIPELINE_GROUP_COLORS.leverLoop;

  return (
    <StageScreen
      title="Baseline Inside Optimize"
      subtitle="Establish current quality before patch attempts"
      pipelineGroup="leverLoop"
      visual={
        <div className="flex flex-col items-center gap-10">
          {/* Animated flow: 4 nodes as rich cards with icons, connected by arrows */}
          <div className="flex flex-wrap items-center justify-center gap-1">
            {FLOW_NODES.map(({ label, icon: Icon }, i) => (
              <div key={label} className="flex items-center">
                <motion.div
                  className={cn(
                    "flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 shadow-sm transition-shadow hover:shadow",
                    "min-w-[140px]"
                  )}
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                  transition={{ delay: i * 0.12, duration: 0.3 }}
                >
                  <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-600">
                    <Icon className="h-4.5 w-4.5" />
                  </div>
                  <span className="text-sm font-medium text-slate-700">
                    {label}
                  </span>
                </motion.div>
                {i < FLOW_NODES.length - 1 && <AnimatedArrow />}
              </div>
            ))}
          </div>

          {/* Baseline ScoreGauge — prominent with "Baseline: 72%" label */}
          <div className="w-full max-w-[280px] [&_.relative]:!h-5 [&_.h-full]:!h-5">
            <div className="mb-3 text-center">
              <span className="text-xl font-semibold tabular-nums text-slate-800">
                Iteration 0: 72%
              </span>
            </div>
            <ScoreGauge
              value={72}
              label="Native benchmark accuracy"
              threshold={72}
              color={baselineColors.dot}
            />
          </div>
        </div>
      }
      explanation={
        <p>
          The baseline is no longer a standalone job task. It is iteration 0 inside
          the Optimize notebook, using the same full benchmark corpus that later
          candidate patches must beat.
        </p>
      }
      learnMore={[
        {
          id: "evaluation-orchestration",
          title: "Evaluation Orchestration",
          content: (
            <ol className="list-decimal space-y-2 pl-4">
              {EVALUATION_ORCHESTRATION_STEPS.map((step, i) => (
                <li key={i}>{step}</li>
              ))}
            </ol>
          ),
        },
      ]}
    />
  );
}
