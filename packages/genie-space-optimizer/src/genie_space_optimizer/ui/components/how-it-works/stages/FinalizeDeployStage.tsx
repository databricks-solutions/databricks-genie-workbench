"use client";

import { motion } from "motion/react";
import { CheckCheck, Award, Rocket, Lock, ShieldCheck } from "lucide-react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import {
  REVIEW_SESSION_FIELDS,
  FEEDBACK_INGESTION_STEPS,
} from "../data";

const CARDS = [
  {
    title: "Repeatability",
    subtitle: "1 eval pass verified",
    iconArea: (
      <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-blue-100">
        <CheckCheck className="h-5 w-5 text-blue-600" />
      </div>
    ),
    topBorder: "border-t-4 border-t-blue-400",
    accent: "text-blue-700",
  },
  {
    title: "Generalization",
    subtitle: "Held-out questions tested",
    iconArea: (
      <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-purple-100">
        <ShieldCheck className="h-5 w-5 text-purple-600" />
      </div>
    ),
    topBorder: "border-t-4 border-t-purple-400",
    accent: "text-purple-700",
  },
  {
    title: "Champion",
    subtitle: "Best model crowned",
    iconArea: (
      <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-amber-100 shadow-sm ring-2 ring-amber-200/60">
        <Award className="h-7 w-7 text-amber-600" />
      </div>
    ),
    topBorder: "border-t-4 border-t-amber-400",
    accent: "text-amber-700",
  },
  {
    title: "Deploy",
    subtitle: "Source → Target",
    iconArea: (
      <div className="flex items-center gap-2">
        <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-emerald-100">
          <Rocket className="h-5 w-5 text-emerald-600" />
        </div>
        <Lock className="h-5 w-5 text-slate-500" aria-hidden />
      </div>
    ),
    topBorder: "border-t-4 border-t-emerald-400",
    accent: "text-emerald-700",
  },
] as const;

export function FinalizeDeployStage() {
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
      The final stage validates that improvements are repeatable, runs a held-out generalization check on questions the optimizer never saw, promotes the best configuration to &quot;champion&quot; status, creates a human review session, and optionally deploys to a target workspace.
    </p>
  );

  const learnMore = [
    {
      id: "repeatability",
      title: "Repeatability Testing",
      content: (
        <div className="space-y-2 text-sm">
          <p>Runs 1 pass on train benchmarks; compares SQL hashes per benchmark. REPEATABILITY_TARGET=90%.</p>
        </div>
      ),
    },
    {
      id: "held-out-check",
      title: "Held-Out Generalization Check",
      content: (
        <div className="space-y-2 text-sm">
          <p>Evaluates ~3-4 benchmark questions the optimizer never saw during the lever loop. Compares held-out accuracy to train accuracy. A gap over 15pp may indicate instruction overfitting. This check is directional only and does not affect optimization decisions.</p>
        </div>
      ),
    },
    {
      id: "champion-promotion",
      title: "Champion Promotion",
      content: (
        <div className="space-y-2 text-sm">
          <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">promote_best_model()</code> selects the iteration with highest overall_accuracy. Sets MLflow alias <code className="rounded bg-db-gray-bg px-1.5 py-0.5">champion</code>.</p>
        </div>
      ),
    },
    {
      id: "uc-model-registration",
      title: "UC Model Registration",
      content: (
        <div className="space-y-2 text-sm">
          <p>Guarded by <code className="rounded bg-db-gray-bg px-1.5 py-0.5">ENABLE_UC_MODEL_REGISTRATION</code>. Uses competitive promotion: new metrics must beat existing champion to update the alias.</p>
        </div>
      ),
    },
    {
      id: "terminal-status",
      title: "Terminal Status",
      content: (
        <div className="space-y-2 text-sm">
          <p>CONVERGED / MAX_ITERATIONS / STALLED / FAILED. FINALIZE_TIMEOUT_SECONDS=6600s (~110 min).</p>
        </div>
      ),
    },
    {
      id: "deployment",
      title: "Deployment (optional)",
      content: (
        <div className="space-y-2 text-sm">
          <p>Source workspace → approval gate (model tag check) → target workspace. Per-space jobs.</p>
        </div>
      ),
    },
    {
      id: "human-review-session",
      title: "Human Review Session",
      content: (
        <div className="space-y-2 text-sm">
          <p>MLflow labeling session. 3 review schema fields:</p>
          <ul className="list-inside list-disc space-y-1">
            {REVIEW_SESSION_FIELDS.map((f) => (
              <li key={f.name}>
                <strong>{f.name}</strong> ({f.type}): {f.label}
                {f.options && ` — ${f.options.join(" | ")}`}
              </li>
            ))}
          </ul>
        </div>
      ),
    },
    {
      id: "feedback-ingestion",
      title: "Feedback Ingestion",
      content: (
        <ol className="list-inside list-decimal space-y-2 text-sm">
          {FEEDBACK_INGESTION_STEPS.map((s) => (
            <li key={s.step}>
              <strong>{s.title}:</strong> {s.description}
            </li>
          ))}
        </ol>
      ),
    },
    {
      id: "ui-surfaces",
      title: "UI Surfaces",
      content: (
        <div className="space-y-2 text-sm">
          <p>ReviewBanner, SpaceCard badge, InsightTabs, Suggestions panel.</p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Finalize & Deploy"
      subtitle="Test, promote, and optionally deploy"
      pipelineGroup="finalize"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
