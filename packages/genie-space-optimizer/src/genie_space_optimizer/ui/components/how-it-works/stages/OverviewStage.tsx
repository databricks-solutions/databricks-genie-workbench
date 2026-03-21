"use client";

import { motion, useReducedMotion } from "motion/react";
import { ArrowRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import { PIPELINE_STAGES, DATA_FLOW_NODES, PIPELINE_GROUP_COLORS } from "../data";
import { Card, CardContent } from "@/components/ui/card";

const STAGE_TO_GROUP: Record<string, string> = {
  preflight: "preflight",
  baseline: "baseline",
  enrichment: "enrichment",
  "lever-loop": "leverLoop",
  finalize: "finalize",
  deploy: "finalize",
};

function PipelineDiagram() {
  const prefersReducedMotion = useReducedMotion();

  return (
    <div className="space-y-6">
      {/* Headline */}
      <div className="text-center">
        <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          End-to-End Pipeline
        </p>
      </div>

      {/* Pipeline cards */}
      <div className="relative">
        {/* Animated pulse overlay */}
        {!prefersReducedMotion && (
          <motion.div
            className="pointer-events-none absolute inset-0 z-10 flex items-center"
            aria-hidden
          >
            <motion.div
              className="h-20 w-20 rounded-full bg-blue-400/20 blur-3xl"
              animate={{ left: ["0%", "90%", "0%"] }}
              transition={{
                duration: 5,
                repeat: Infinity,
                ease: "easeInOut",
              }}
              style={{ position: "absolute" }}
            />
          </motion.div>
        )}

        <div className="relative z-0 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          {PIPELINE_STAGES.map((stage, index) => {
            const groupId = STAGE_TO_GROUP[stage.id] ?? "neutral";
            const colors = PIPELINE_GROUP_COLORS[groupId] ?? PIPELINE_GROUP_COLORS.neutral;
            const Icon = stage.icon;

            return (
              <motion.div
                key={stage.id}
                initial={prefersReducedMotion ? false : { opacity: 0, y: 16 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.08, duration: 0.35 }}
              >
                <div
                  className={cn(
                    "group relative flex flex-col items-center rounded-xl border p-4 transition-all hover:shadow-lg",
                    colors.bg,
                    "border-slate-200 hover:border-slate-300",
                  )}
                >
                  {/* Step number */}
                  <span className="absolute -top-2 left-3 rounded-full bg-white px-1.5 py-0.5 text-[10px] font-bold text-slate-400 shadow-sm ring-1 ring-slate-200">
                    {index + 1}
                  </span>

                  {/* Icon */}
                  <div
                    className={cn(
                      "mb-3 flex h-11 w-11 items-center justify-center rounded-xl transition-transform group-hover:scale-110",
                      colors.iconBg,
                    )}
                  >
                    <Icon className={cn("h-5.5 w-5.5", colors.iconText)} />
                  </div>

                  {/* Title */}
                  <span className="text-center text-sm font-bold text-slate-800">
                    {stage.title}
                  </span>

                  {/* Summary */}
                  <p className="mt-1 line-clamp-2 text-center text-[11px] leading-snug text-slate-500">
                    {stage.summary}
                  </p>

                  {/* Flow arrow (shown between cards on lg) */}
                  {index < PIPELINE_STAGES.length - 1 && (
                    <div className="absolute -right-3 top-1/2 z-20 hidden -translate-y-1/2 lg:block">
                      <ArrowRight className="h-4 w-4 text-slate-300" />
                    </div>
                  )}
                </div>
              </motion.div>
            );
          })}
        </div>
      </div>

      {/* Flow indicator on smaller screens */}
      <div className="flex items-center justify-center gap-1 lg:hidden">
        {PIPELINE_STAGES.map((stage, i) => {
          const groupId = STAGE_TO_GROUP[stage.id] ?? "neutral";
          const colors = PIPELINE_GROUP_COLORS[groupId] ?? PIPELINE_GROUP_COLORS.neutral;
          return (
            <div key={stage.id} className="flex items-center gap-1">
              <span className={cn("h-1.5 w-1.5 rounded-full", colors.dot)} />
              {i < PIPELINE_STAGES.length - 1 && (
                <span className="h-px w-3 bg-slate-300" />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function DataFlowContent() {
  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {DATA_FLOW_NODES.map((node) => (
        <Card
          key={node.id}
          className="overflow-hidden border-slate-200 transition-shadow hover:shadow-md"
        >
          <CardContent className="p-4">
            <div className="flex items-start gap-2">
              <span
                className={cn(
                  "mt-0.5 inline-block h-2 w-2 shrink-0 rounded-full",
                  node.type === "input"
                    ? "bg-blue-400"
                    : node.type === "storage"
                      ? "bg-emerald-400"
                      : "bg-amber-400",
                )}
              />
              <div className="min-w-0">
                <p className="truncate font-mono text-xs font-bold text-slate-700">
                  {node.deltaTable ?? node.label}
                </p>
                {node.keyColumns && node.keyColumns.length > 0 && (
                  <p className="mt-0.5 text-[11px] text-slate-400">
                    {node.keyColumns.slice(0, 4).join(", ")}
                    {node.keyColumns.length > 4 ? " ..." : ""}
                  </p>
                )}
              </div>
            </div>
            <p className="mt-2 text-[12px] leading-relaxed text-slate-500">
              {node.description}
            </p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

export function OverviewStage() {
  return (
    <StageScreen
      title="The Big Picture"
      subtitle="How Genie Space Optimizer works end-to-end"
      pipelineGroup="neutral"
      visual={<PipelineDiagram />}
      explanation={
        <>
          Genie Space Optimizer automatically improves the quality of Databricks
          Genie Spaces — the natural-language-to-SQL interface for business
          users. It runs a closed-loop optimization pipeline: generating
          benchmarks, evaluating accuracy with 9 specialized judges, diagnosing
          failures, and applying targeted metadata patches until quality
          thresholds are met.
        </>
      }
      learnMore={[
        {
          id: "data-flow",
          title: "Data Flow Transparency — 12 Delta Tables",
          content: <DataFlowContent />,
        },
      ]}
    />
  );
}
