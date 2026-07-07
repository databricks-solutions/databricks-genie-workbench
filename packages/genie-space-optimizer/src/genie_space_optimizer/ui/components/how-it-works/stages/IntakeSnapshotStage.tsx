"use client";

import { motion, useReducedMotion } from "motion/react";
import { StageScreen } from "../StageScreen";
import { AnimatedChecklist } from "../shared/AnimatedChecklist";

const INTAKE_ITEMS = [
  { id: "1", label: "Fetch current Genie Space config" },
  { id: "2", label: "Persist original rollback snapshot" },
  { id: "3", label: "Compute baseline config fingerprint" },
  { id: "4", label: "Write run_manifest artifact" },
  { id: "5", label: "Ensure optimizer Delta state tables" },
];

const MANIFEST_FIELDS = [
  { id: "1", label: "run_id, space_id, domain" },
  { id: "2", label: "catalog, schema, warehouse_id" },
  { id: "3", label: "apply_mode, levers, max_attempts" },
  { id: "4", label: "target_accuracy, benchmark repair budget" },
  { id: "5", label: "baseline_config_hash and trigger identity" },
];

export function IntakeSnapshotStage() {
  const prefersReducedMotion = useReducedMotion();
  // Last checklist item completes at roughly 4 * 400ms; show Ready after that.
  const readyDelayMs = prefersReducedMotion ? 0 : 1800;

  const visual = (
    <div className="space-y-5">
      <AnimatedChecklist
        items={INTAKE_ITEMS}
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
          Snapshot and manifest ready
        </span>
      </motion.div>
    </div>
  );

  return (
    <StageScreen
      title="Intake & Snapshot"
      subtitle="Capture the original config and durable run envelope"
      pipelineGroup="preflight"
      visual={visual}
      explanation={
        <>
          The first notebook task captures the original Genie Space configuration before any
          optimizer mutation. That snapshot becomes the discard rollback anchor, while the
          run manifest records the parameters every later task reads from Delta by run_id.
        </>
      }
      learnMore={[
        {
          id: "fetch-config",
          title: "Rollback Snapshot",
          content: (
            <p className="text-sm">
              The task reads the current serialized_space from the run-row snapshot or Genie API
              fallback, then persists the original config before optimization can apply patches.
            </p>
          ),
        },
        {
          id: "run-manifest",
          title: "Run Manifest",
          content: <AnimatedChecklist items={MANIFEST_FIELDS} />,
        },
        {
          id: "handoff",
          title: "Durable Handoff",
          content: (
            <p className="text-sm">
              There is no task-value plumbing between notebooks. Each task rehydrates the run from
              job parameters plus Delta state keyed by run_id.
            </p>
          ),
        },
      ]}
    />
  );
}
