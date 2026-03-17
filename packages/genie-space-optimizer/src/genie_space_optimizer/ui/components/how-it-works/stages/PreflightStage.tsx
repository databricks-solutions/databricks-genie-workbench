"use client";

import { motion, useReducedMotion } from "motion/react";
import { StageScreen } from "../StageScreen";
import { AnimatedChecklist } from "../shared/AnimatedChecklist";

const PREFLIGHT_ITEMS = [
  { id: "1", label: "Fetch Genie Space config" },
  { id: "2", label: "Collect UC metadata" },
  { id: "3", label: "Profile data columns" },
  { id: "4", label: "Generate benchmarks" },
  { id: "5", label: "Validate benchmarks" },
  { id: "6", label: "Load human feedback & setup experiment" },
];

const PROFILING_STEPS = [
  { id: "1", label: "COUNT(*) per table (up to 20 tables)" },
  { id: "2", label: "TABLESAMPLE 100 rows — cardinality, min/max per column" },
  { id: "3", label: "COLLECT_SET for low-cardinality columns (≤20 distinct values)" },
  { id: "4", label: "FK constraint extraction via REST API" },
  { id: "5", label: "Join overlap analysis via sampled FK/PK joins" },
];

export function PreflightStage() {
  const prefersReducedMotion = useReducedMotion();
  // Last item completes at ~(6-1)*400ms; show Ready badge after
  const readyDelayMs = prefersReducedMotion ? 0 : 2200;

  const visual = (
    <div className="space-y-5">
      <AnimatedChecklist
        items={PREFLIGHT_ITEMS}
        staggerDelay={400}
        variant="card"
        accentBorderClass="border-l-blue-500"
      />
      <motion.div
        className="flex flex-wrap items-center gap-3 pt-1"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: readyDelayMs / 1000, duration: 0.4 }}
      >
        <span
          className="inline-flex items-center gap-1.5 rounded-full border border-emerald-200/80 bg-emerald-50/90 px-3 py-1 text-sm font-medium text-emerald-700 shadow-[0_0_12px_rgba(16,185,129,0.15)]"
          aria-label="System ready"
        >
          <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" aria-hidden />
          System Ready
        </span>
        <span className="text-xs text-slate-500">
          Preflight Duration: ~30–60s
        </span>
      </motion.div>
    </div>
  );

  return (
    <StageScreen
      title="Preflight"
      subtitle="Collect metadata, profile data, prepare the run"
      pipelineGroup="preflight"
      visual={visual}
      explanation={
        <>
          Before optimization begins, the system collects everything it needs: the current Genie
          Space configuration, Unity Catalog metadata with column profiling, benchmark questions,
          and any human feedback from prior runs.
        </>
      }
      learnMore={[
        {
          id: "fetch-config",
          title: "Fetch Genie Space Config",
          content: (
            <p className="text-sm">
              REST API fetch stores the current configuration as config_snapshot — the rollback
              baseline for safe optimization.
            </p>
          ),
        },
        {
          id: "data-profiling",
          title: "Data Profiling",
          content: <AnimatedChecklist items={PROFILING_STEPS} />,
        },
        {
          id: "human-feedback",
          title: "Human Feedback & Experiment",
          content: (
            <p className="text-sm">
              Correction types: benchmark SQL fixes (genie_correct verdicts), judge overrides, and
              quarantines. MLflow experiment setup creates or resolves the experiment, registers
              the evaluation dataset, and sets up the initial instruction version.
            </p>
          ),
        },
      ]}
    />
  );
}
