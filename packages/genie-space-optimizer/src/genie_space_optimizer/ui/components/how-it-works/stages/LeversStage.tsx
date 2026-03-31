"use client";

import { motion } from "motion/react";
import { Settings2, BarChart3, Code, Link2, BookOpen, FlaskConical } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { StageScreen } from "../StageScreen";
import { LEVERS, FICTIONAL_EXAMPLE } from "../data";
import { cn } from "@/lib/utils";

const LEVER_ICONS = [Settings2, BarChart3, Code, Link2, BookOpen, FlaskConical] as const;

const LEVER_ACCENTS: Record<number, string> = {
  1: "from-blue-500/90 to-blue-400/70",
  2: "from-indigo-500/90 to-indigo-400/70",
  3: "from-emerald-500/90 to-emerald-400/70",
  4: "from-orange-500/90 to-orange-400/70",
  5: "from-purple-500/90 to-purple-400/70",
  6: "from-teal-500/90 to-teal-400/70",
};

const LEVER_PILL_COLORS: Record<number, string> = {
  1: "bg-blue-100 text-blue-700 border-blue-200",
  2: "bg-indigo-100 text-indigo-700 border-indigo-200",
  3: "bg-emerald-100 text-emerald-700 border-emerald-200",
  4: "bg-orange-100 text-orange-700 border-orange-200",
  5: "bg-purple-100 text-purple-700 border-purple-200",
  6: "bg-teal-100 text-teal-700 border-teal-200",
};

export function LeversStage() {
  const visual = (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-6">
      {LEVERS.map((lever, index) => {
        const Icon = LEVER_ICONS[lever.number - 1];
        const accent = LEVER_ACCENTS[lever.number] ?? "from-slate-500 to-slate-400";
        const pillColor = LEVER_PILL_COLORS[lever.number] ?? "bg-slate-100 text-slate-700 border-slate-200";
        return (
          <motion.div
            key={lever.number}
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: index * 0.05, duration: 0.25 }}
            whileHover={{ scale: 1.02, y: -2 }}
            className="h-full"
          >
            <div className="flex h-full min-h-[140px] flex-col overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm transition-shadow hover:shadow-md">
              <div className={cn("h-1.5 w-full bg-gradient-to-r", accent)} />
              <div className="flex flex-1 flex-col gap-3 p-4">
                <div className="flex items-start gap-3">
                  <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-slate-100 font-bold text-slate-700">
                    {lever.number}
                  </div>
                  <div className="min-w-0 flex-1">
                    <h3 className="truncate font-semibold text-slate-800">
                      {lever.name}
                    </h3>
                    <Icon className="mt-0.5 h-4 w-4 text-slate-400" />
                  </div>
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {lever.failureTypes.map((ft) => (
                    <Badge
                      key={ft}
                      variant="outline"
                      className={cn(
                        "text-[10px] font-medium",
                        pillColor,
                      )}
                    >
                      {ft.replace(/_/g, " ")}
                    </Badge>
                  ))}
                </div>
              </div>
            </div>
          </motion.div>
        );
      })}
    </div>
  );

  const explanation = (
    <p>
      Each lever is a specialized tool for a specific class of problem. The strategist decides which levers to pull based on the failure clusters. Levers operate on different parts of the Genie Space metadata — from column descriptions to join specifications to instructions.
    </p>
  );

  const patch = FICTIONAL_EXAMPLE.patch;

  const learnMore = [
    {
      id: "per-lever-proposals",
      title: "Per-Lever Proposal Generation",
      content: (
        <div className="space-y-3 text-sm">
          <p><strong>L1 & L2:</strong> Use <code className="rounded bg-db-gray-bg px-1.5 py-0.5">LEVER_1_2_COLUMN_PROMPT</code> for structured metadata changes. L1: table/column descriptions, synonyms, visibility. L2: MV-specific (aggregation, filters, MEASURE).</p>
          <p><strong>L3:</strong> TVF docs + <code className="rounded bg-db-gray-bg px-1.5 py-0.5">remove_tvf</code> for persistently failing TVFs.</p>
          <p><strong>L4:</strong> <code className="rounded bg-db-gray-bg px-1.5 py-0.5">LEVER_4_JOIN_SPEC_PROMPT</code> — analyzes SQL diffs for missing/incorrect JOINs, generates Genie-format join specs.</p>
          <p><strong>L5:</strong> Instruction rewrites + example SQL. Budget-aware, respects character limits.</p>
          <p><strong>L6:</strong> SQL Expressions — reusable measures, filters, and dimensions. Adds structured business concept definitions to the knowledge store (no instruction budget cost).</p>
        </div>
      ),
    },
    {
      id: "section-ownership",
      title: "Section Ownership",
      content: (
        <div className="space-y-2 text-sm">
          <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">update_section()</code> raises <code className="rounded bg-db-gray-bg px-1.5 py-0.5">LeverOwnershipError</code> if a lever tries to modify a section it doesn&apos;t own.</p>
          <p><strong>L0</strong> owns all 14 metadata sections (for non-lever updates).</p>
          <p><strong>L5</strong> owns no metadata sections — it edits instructions and example SQL only.</p>
        </div>
      ),
    },
    {
      id: "patch-lifecycle",
      title: "Patch Lifecycle",
      content: (
        <div className="space-y-3 text-sm">
          <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">proposals_to_patches()</code> converts strategist proposals into patches.</p>
          <p><strong>Patch DSL format:</strong> <code className="rounded bg-db-gray-bg px-1.5 py-0.5">action_type</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">target</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">command</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">rollback_command</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">risk_level</code>. 35 patch types.</p>
          <p><strong>Fictional example patch:</strong></p>
          <pre className="overflow-x-auto rounded-lg border border-db-gray-border bg-db-gray-bg p-3 text-xs">
            {JSON.stringify({ action_type: patch.actionType, target: patch.target, sections: patch.sections }, null, 2)}
          </pre>
        </div>
      ),
    },
    {
      id: "patch-application",
      title: "Patch Application",
      content: (
        <ol className="list-inside list-decimal space-y-2 text-sm">
          <li>Load pre_snapshot (config before patches)</li>
          <li>Apply patches in dependency order</li>
          <li>Propagate changes (format assistance, entity matching)</li>
          <li>Wait for propagation (30s default)</li>
          <li>Run Slice Gate (if enabled)</li>
          <li>Run P0 Gate</li>
          <li>Run Full Gate</li>
          <li>Accept or rollback based on gate results</li>
        </ol>
      ),
    },
  ];

  return (
    <StageScreen
      title="The 6 Levers"
      subtitle="Targeted tools for specific problem types"
      pipelineGroup="leverLoop"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
