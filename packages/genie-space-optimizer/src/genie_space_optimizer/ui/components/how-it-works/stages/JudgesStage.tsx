"use client";

import { motion } from "motion/react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import { ExpandableCard } from "../shared/ExpandableCard";
import { ScoreGauge } from "../shared/ScoreGauge";
import { DataModelCard } from "../shared/DataModelCard";
import {
  JUDGES,
  JUDGE_CATEGORY_COLORS,
  ASI_MODEL_FIELDS,
} from "../data";

/** Card left border by category (syntax=blue, schema=indigo, logic=purple, quality=slate, execution=green, routing=orange, arbiter=amber) */
const JUDGE_CARD_BORDER: Record<string, string> = {
  syntax: "border-l-blue-500",
  schema: "border-l-indigo-500",
  logic: "border-l-purple-500",
  quality: "border-l-slate-400",
  execution: "border-l-green-500",
  routing: "border-l-orange-500",
  arbiter: "border-l-amber-500",
};

/** Gauge fill color to match card accent */
const JUDGE_GAUGE_COLOR: Record<string, string> = {
  syntax: "bg-blue-500",
  schema: "bg-indigo-500",
  logic: "bg-purple-500",
  quality: "bg-slate-500",
  execution: "bg-green-500",
  routing: "bg-orange-500",
  arbiter: "bg-amber-500",
};

const METHOD_BADGE_COLORS: Record<string, string> = {
  CODE: "bg-blue-100 text-blue-800 border border-blue-200",
  LLM: "bg-indigo-100 text-indigo-800 border border-indigo-200",
  CONDITIONAL_LLM: "bg-amber-100 text-amber-800 border border-amber-200",
};

export function JudgesStage() {
  return (
    <StageScreen
      title="The 9 Judges"
      subtitle="How each judge evaluates SQL quality"
      pipelineGroup="baseline"
      visual={
        <div className="grid grid-cols-3 gap-4">
          {JUDGES.map((judge) => (
            <motion.div
              key={judge.name}
              layoutId={`judge-${judge.name}`}
              className={cn(
                "cursor-default overflow-hidden rounded-xl border border-slate-200 bg-white border-l-4 shadow-sm transition-shadow hover:shadow-lg",
                JUDGE_CARD_BORDER[judge.category] ?? "border-l-slate-400"
              )}
            >
              <div className="p-4">
                <div className="mb-3 flex items-center justify-between gap-2">
                  <span className="truncate text-sm font-semibold text-slate-800">
                    {judge.displayName}
                  </span>
                  <span
                    className={cn(
                      "shrink-0 rounded-full px-2.5 py-0.5 text-[10px] font-medium",
                      METHOD_BADGE_COLORS[judge.method] ??
                        "bg-slate-100 text-slate-700 border border-slate-200"
                    )}
                  >
                    {judge.method.replace("_", " ")}
                  </span>
                </div>
                <ScoreGauge
                  value={judge.threshold >= 0 ? judge.threshold : 0}
                  label="Threshold"
                  threshold={judge.threshold >= 0 ? judge.threshold : undefined}
                  color={
                    judge.threshold >= 0
                      ? JUDGE_GAUGE_COLOR[judge.category] ?? "bg-slate-600"
                      : "bg-slate-300"
                  }
                />
              </div>
            </motion.div>
          ))}
        </div>
      }
      explanation={
        <p>
          Nine specialized judges evaluate every Genie response. Some use LLMs
          for nuanced analysis, others use deterministic code checks. Together
          they provide a comprehensive quality assessment with structured
          feedback (ASI) that drives the optimization loop.
        </p>
      }
      learnMore={[
        {
          id: "per-judge-details",
          title: "Per-Judge Details",
          content: (
            <div className="space-y-3">
              {JUDGES.map((judge) => (
                <ExpandableCard
                  key={judge.name}
                  title={judge.displayName}
                  subtitle={judge.description}
                  accentColor={
                    JUDGE_CATEGORY_COLORS[judge.category] ?? "border-l-gray-400"
                  }
                >
                  <div className="space-y-3 text-sm">
                    <p>{judge.detailedDescription}</p>
                    {judge.failureTypes.length > 0 && (
                      <div>
                        <span className="font-medium">Failure types: </span>
                        {judge.failureTypes.join(", ")}
                      </div>
                    )}
                    {judge.overrideRules.length > 0 && (
                      <div>
                        <span className="font-medium">Override rules: </span>
                        {judge.overrideRules.join("; ")}
                      </div>
                    )}
                    {judge.asiFields.length > 0 && (
                      <div>
                        <span className="font-medium">ASI fields: </span>
                        {judge.asiFields.join(", ")}
                      </div>
                    )}
                  </div>
                </ExpandableCard>
              ))}
            </div>
          ),
        },
        {
          id: "score-aggregation",
          title: "Score Aggregation",
          content: (
            <p className="text-sm">
              Per-judge scores come from eval_result.metrics, normalized from
              0-1 to 0-100. Pass/fail is determined by all_thresholds_met() against
              DEFAULT_THRESHOLDS.
            </p>
          ),
        },
        {
          id: "arbiter-adjusted-accuracy",
          title: "Arbiter-Adjusted Accuracy",
          content: (
            <div className="space-y-3 text-sm">
              <p>
                A row is correct if{" "}
                <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                  result_correctness == &quot;yes&quot;
                </code>{" "}
                OR (
                <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                  result_correctness == &quot;no&quot;
                </code>{" "}
                AND arbiter in{" "}
                <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                  genie_correct
                </code>
                ,{" "}
                <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                  both_correct
                </code>
                ).
              </p>
              <p>
                Excluded from the denominator: quarantined, temporal-stale,
                both_empty, genie_result_unavailable.
              </p>
              <p>
                Cascading pass: when arbiter says Genie correct, logical,
                semantic, completeness, and schema judges also pass for that row.
              </p>
            </div>
          ),
        },
        {
          id: "asi",
          title: "Actionable Side Information (ASI)",
          content: (
            <div className="space-y-4">
              <DataModelCard title="ASI Model" fields={ASI_MODEL_FIELDS} />
              <p className="rounded-lg border border-db-blue/30 bg-db-blue/5 p-4 text-sm">
                ASI is what makes the optimization loop intelligent. Each judge
                doesn&apos;t just say pass/fail — it says why it failed and what
                metadata change would fix it.
              </p>
            </div>
          ),
        },
      ]}
    />
  );
}
