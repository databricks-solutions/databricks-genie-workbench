"use client";

import { motion } from "motion/react";
import { AlertTriangle, Award, FileCheck2, ShieldCheck } from "lucide-react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";

const CARDS = [
  {
    title: "Publish Gate",
    subtitle: "Stamped reason checked",
    iconArea: (
      <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-blue-100">
        <ShieldCheck className="h-5 w-5 text-blue-600" />
      </div>
    ),
    topBorder: "border-t-4 border-t-blue-400",
    accent: "text-blue-700",
  },
  {
    title: "Champion",
    subtitle: "Best iteration promoted",
    iconArea: (
      <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-amber-100 shadow-sm ring-2 ring-amber-200/60">
        <Award className="h-7 w-7 text-amber-600" />
      </div>
    ),
    topBorder: "border-t-4 border-t-amber-400",
    accent: "text-amber-700",
  },
  {
    title: "Audit",
    subtitle: "publish_record written",
    iconArea: (
      <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-emerald-100">
        <FileCheck2 className="h-5 w-5 text-emerald-600" />
      </div>
    ),
    topBorder: "border-t-4 border-t-emerald-400",
    accent: "text-emerald-700",
  },
  {
    title: "Concerns",
    subtitle: "Human review surfaced",
    iconArea: (
      <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-purple-100">
        <AlertTriangle className="h-5 w-5 text-purple-600" />
      </div>
    ),
    topBorder: "border-t-4 border-t-purple-400",
    accent: "text-purple-700",
  },
] as const;

export function PublishAuditStage() {
  const visual = (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
      {CARDS.map((card, index) => (
        <motion.div
          key={card.title}
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: index * 0.08, duration: 0.3, ease: "easeOut" }}
          className={cn(
            "overflow-hidden rounded-xl border border-slate-200 bg-white pt-4 shadow-sm transition-shadow hover:shadow-md",
            card.topBorder
          )}
        >
          <div className="flex flex-col items-center p-6 pb-5 text-center">
            <div className="mb-3 flex justify-center">{card.iconArea}</div>
            <h3 className={cn("font-semibold", card.accent)}>{card.title}</h3>
            <p className="mt-1 text-sm text-slate-600">{card.subtitle}</p>
          </div>
        </motion.div>
      ))}
    </div>
  );

  const explanation = (
    <p>
      The final task reads the stamped terminal reason from the champion row. Eligible runs
      promote the champion in optimizer state, while every run writes a publish_record with
      trajectory, outcome, audit summary, and any concerns.
    </p>
  );

  const learnMore = [
    {
      id: "terminal-reason-gate",
      title: "Terminal Reason Gate",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            TARGET_REACHED and MAX_ATTEMPTS publish the champion. EVAL_INVALID,
            NO_NEW_HYPOTHESIS, and EVAL_BUDGET_EXHAUSTED skip promotion but still record concerns.
          </p>
        </div>
      ),
    },
    {
      id: "champion-promotion",
      title: "Delta Promotion",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            <code className="rounded bg-db-gray-bg px-1.5 py-0.5">promote_best_model()</code>
            restamps optimizer Delta state and the run&apos;s best fields. It does not run a
            separate live-space mutation; accepted patches were already applied by Optimize.
          </p>
        </div>
      ),
    },
    {
      id: "publish-record",
      title: "Publish Record",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            The publish_record carries the champion pointer, accuracy trajectory, patch families,
            terminal reason, publish outcome, final status, concerns, and best-effort audit summary.
          </p>
        </div>
      ),
    },
    {
      id: "terminal-status",
      title: "Terminal Status",
      content: (
        <div className="space-y-2 text-sm">
          <p>Terminal reasons map to CONVERGED, MAX_ITERATIONS, STALLED, or FAILED without inventing a new run status.</p>
        </div>
      ),
    },
    {
      id: "concerns",
      title: "Concerns",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            Non-publishing stop reasons, unstamped champion diagnostics, residual failures, and
            audit-summary failures are written as structured concerns for human review.
          </p>
        </div>
      ),
    },
    {
      id: "audit-summary",
      title: "Audit Summary",
      content: (
        <div className="space-y-2 text-sm">
          <p>
            The LLM summary uses only structural and aggregate context. Benchmark question text,
            expected SQL, generated SQL, and answer-key material are excluded.
          </p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Publish & Audit"
      subtitle="Promote, record concerns, and write the audit artifact"
      pipelineGroup="finalize"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
