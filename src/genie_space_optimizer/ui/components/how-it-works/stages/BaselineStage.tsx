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
import { JUDGES, PIPELINE_GROUP_COLORS } from "../data";

const FLOW_NODES = [
  { label: "Benchmarks", icon: FileText },
  { label: "Genie Space API", icon: Globe },
  { label: "9 Judges", icon: ShieldCheck },
  { label: "Aggregated Scores", icon: BarChart3 },
] as const;

const EVALUATION_ORCHESTRATION_STEPS = [
  "make_predict_fn closure — captures Genie Space config and benchmark dataset",
  "run_evaluation DataFrame — builds input for mlflow.genai.evaluate",
  "mlflow.genai.evaluate with 9 scorers — runs all judges per question",
  "Retry logic: EVAL_MAX_ATTEMPTS=4 with exponential sleep on transient failures",
  "Sequential fallback on total failure — falls back to per-question evaluation",
];

/** Abbreviations for judge icons (for layoutId animation to JudgesStage) */
const JUDGE_ABBREVS: Record<string, string> = {
  syntax_validity: "SV",
  schema_accuracy: "SA",
  logical_accuracy: "LA",
  semantic_equivalence: "SE",
  completeness: "CO",
  response_quality: "RQ",
  result_correctness: "RC",
  asset_routing: "AR",
  arbiter: "AB",
};

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

export function BaselineStage() {
  const baselineColors = PIPELINE_GROUP_COLORS.baseline;

  return (
    <StageScreen
      title="Baseline Evaluation"
      subtitle="Establish quality with 9 judges"
      pipelineGroup="baseline"
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
                Baseline: 72%
              </span>
            </div>
            <ScoreGauge
              value={72}
              label="Overall accuracy"
              threshold={72}
              color={baselineColors.dot}
            />
          </div>

          {/* 9 judge icons — layoutId for shared-element animation to JudgesStage, purple accent */}
          <div className="flex flex-wrap items-center justify-center gap-2.5">
            {JUDGES.map((judge, i) => (
              <motion.div
                key={judge.name}
                layoutId={`judge-${judge.name}`}
                className={cn(
                  "flex h-10 w-10 shrink-0 items-center justify-center rounded-xl text-xs font-semibold",
                  "border shadow-sm transition-shadow hover:shadow",
                  baselineColors.iconBg,
                  baselineColors.iconText,
                  "border-purple-200"
                )}
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.5 + i * 0.05, duration: 0.25 }}
              >
                {JUDGE_ABBREVS[judge.name] ?? judge.number}
              </motion.div>
            ))}
          </div>
        </div>
      }
      explanation={
        <p>
          Every benchmark question is sent to the Genie Space, and the response is
          evaluated by 9 specialized judges. The result is a baseline accuracy
          score that tells us how good the space is before optimization.
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
