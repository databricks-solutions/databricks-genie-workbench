"use client";

import type { ReactNode } from "react";
import { motion } from "motion/react";
import { ChevronDown, ArrowRight, Info } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { StageScreen } from "../StageScreen";
import {
  FICTIONAL_EXAMPLE,
  FAILURE_TAXONOMY,
  PERSISTENT_FAILURE_CLASSIFICATIONS,
  ESCALATION_TYPES,
  CLUSTER_IMPACT_WEIGHTS,
} from "../data";
import { cn } from "@/lib/utils";

const LEVER_COLORS: Record<number, string> = {
  0: "bg-gray-100 text-gray-800 border-gray-200",
  1: "bg-blue-100 text-blue-800 border-blue-200",
  2: "bg-indigo-100 text-indigo-800 border-indigo-200",
  3: "bg-violet-100 text-violet-800 border-violet-200",
  4: "bg-emerald-100 text-emerald-800 border-emerald-200",
  5: "bg-amber-100 text-amber-800 border-amber-200",
};

const ROOT_CAUSE_BADGE: Record<string, string> = {
  wrong_column: "bg-blue-100 text-blue-800 border-blue-200",
  missing_join_spec: "bg-emerald-100 text-emerald-800 border-emerald-200",
};

const impactData = [
  {
    id: "C001",
    rootCause: "wrong_column",
    judge: "schema_accuracy",
    qCount: 3,
    causal: 4.0,
    severity: 0.9,
    fixability: 1.0,
    impact: 10.8,
    pct: 82,
  },
  {
    id: "C002",
    rootCause: "missing_join_spec",
    judge: "logical_accuracy",
    qCount: 2,
    causal: 3.0,
    severity: 0.85,
    fixability: 0.4,
    impact: 2.04,
    pct: 18,
  },
];

function FailureTriageVisual() {
  const { cluster, benchmark } = FICTIONAL_EXAMPLE;
  const questionBadges = [
    benchmark.question,
    ...cluster.affectedQuestions.slice(1, 3),
  ];

  const clusterCards = [
    { id: "C001", rootCause: "wrong_column", count: 3, judge: "schema_accuracy", hasCF: true },
    { id: "C002", rootCause: "missing_join_spec", count: 2, judge: "logical_accuracy", hasCF: false },
  ];

  return (
    <div className="space-y-5">
      {/* Failing Questions */}
      <div>
        <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-slate-500">
          Failing Questions
        </p>
        <div className="space-y-2">
          {questionBadges.map((q, i) => (
            <div
              key={i}
              className="rounded-lg border border-slate-100 border-l-4 border-l-red-400 bg-red-50 px-4 py-3"
            >
              <p className="text-sm font-medium text-slate-800">
                {q.length > 55 ? `${q.slice(0, 55)}...` : q}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Flow arrow */}
      <div className="flex justify-center">
        <motion.div
          animate={{ y: [0, 4, 0] }}
          transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut" }}
          className="flex flex-col items-center gap-0.5"
        >
          <ChevronDown className="h-6 w-6 text-slate-300" />
          <span className="text-[10px] font-medium uppercase tracking-widest text-slate-400">
            cluster
          </span>
        </motion.div>
      </div>

      {/* Clusters */}
      <div>
        <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-slate-500">
          Root-Cause Clusters
        </p>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          {clusterCards.map((c) => (
            <div
              key={c.id}
              className="rounded-xl border border-amber-100 bg-amber-50 p-4 transition-shadow hover:shadow-sm"
            >
              <div className="flex items-center justify-between">
                <p className="font-mono text-sm font-bold text-slate-800">
                  {c.id}
                </p>
                {c.hasCF && (
                  <Badge variant="outline" className="border-emerald-200 bg-emerald-50 text-[10px] text-emerald-700">
                    Has Counterfactual
                  </Badge>
                )}
              </div>
              <Badge
                variant="outline"
                className={cn(
                  "mt-2 text-xs",
                  ROOT_CAUSE_BADGE[c.rootCause] ?? "bg-slate-100 text-slate-700 border-slate-200",
                )}
              >
                {c.rootCause}
              </Badge>
              <p className="mt-1 text-[11px] text-slate-500">
                Judge: <span className="font-medium text-slate-600">{c.judge}</span>
              </p>
              <p className="mt-1 text-xs text-slate-600">
                {c.count} questions affected
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Flow arrow */}
      <div className="flex justify-center">
        <motion.div
          animate={{ y: [0, 4, 0] }}
          transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut", delay: 0.2 }}
          className="flex flex-col items-center gap-0.5"
        >
          <ChevronDown className="h-6 w-6 text-slate-300" />
          <span className="text-[10px] font-medium uppercase tracking-widest text-slate-400">
            rank by impact
          </span>
        </motion.div>
      </div>

      {/* Impact ranking with formula breakdown */}
      <div>
        <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-slate-500">
          Impact Ranking
        </p>

        {/* Formula */}
        <div className="mb-4 rounded-lg border border-slate-200 bg-slate-50 p-3">
          <p className="text-center font-mono text-sm font-medium text-slate-700">
            impact = question_count × causal_weight × severity × fixability
          </p>
        </div>

        {/* Detailed breakdown per cluster */}
        <div className="space-y-4">
          {impactData.map((d, idx) => (
            <div key={d.id} className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="flex h-5 w-5 items-center justify-center rounded-full bg-amber-500 text-[10px] font-bold text-white">
                    {idx + 1}
                  </span>
                  <span className="font-mono text-sm font-bold text-slate-700">{d.id}</span>
                  <Badge variant="outline" className={cn("text-[10px]", ROOT_CAUSE_BADGE[d.rootCause])}>
                    {d.rootCause}
                  </Badge>
                </div>
                <span className="text-sm font-bold text-amber-600">{d.impact.toFixed(1)}</span>
              </div>

              {/* Factor breakdown */}
              <div className="ml-7 grid grid-cols-4 gap-1.5 text-[11px]">
                <div className="rounded bg-slate-100 px-2 py-1 text-center">
                  <p className="text-slate-400">Questions</p>
                  <p className="font-bold text-slate-700">{d.qCount}</p>
                </div>
                <div className="rounded bg-blue-50 px-2 py-1 text-center">
                  <p className="text-blue-400">Causal</p>
                  <p className="font-bold text-blue-700">{d.causal}</p>
                </div>
                <div className="rounded bg-purple-50 px-2 py-1 text-center">
                  <p className="text-purple-400">Severity</p>
                  <p className="font-bold text-purple-700">{d.severity}</p>
                </div>
                <div className={cn(
                  "rounded px-2 py-1 text-center",
                  d.fixability === 1.0 ? "bg-emerald-50" : "bg-red-50",
                )}>
                  <p className={d.fixability === 1.0 ? "text-emerald-400" : "text-red-400"}>Fixability</p>
                  <p className={cn("font-bold", d.fixability === 1.0 ? "text-emerald-700" : "text-red-700")}>
                    {d.fixability}
                  </p>
                </div>
              </div>

              {/* Impact bar */}
              <div className="ml-7">
                <div className="h-2 overflow-hidden rounded-full bg-slate-100">
                  <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: `${d.pct}%` }}
                    transition={{ duration: 0.6, delay: 0.3 + idx * 0.15 }}
                    className="h-full rounded-full bg-amber-500"
                  />
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Flow to action groups */}
      <div className="flex justify-center">
        <motion.div
          animate={{ y: [0, 4, 0] }}
          transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut", delay: 0.4 }}
          className="flex flex-col items-center gap-0.5"
        >
          <ChevronDown className="h-6 w-6 text-slate-300" />
          <span className="text-[10px] font-medium uppercase tracking-widest text-slate-400">
            strategist
          </span>
        </motion.div>
      </div>

      {/* Action Group preview */}
      <div>
        <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-slate-500">
          Action Groups
        </p>
        <div className="rounded-xl border border-indigo-100 bg-indigo-50/60 p-4">
          <div className="flex items-center gap-2">
            <span className="flex h-6 w-6 items-center justify-center rounded-full bg-indigo-500 text-[10px] font-bold text-white">
              AG1
            </span>
            <p className="text-sm font-bold text-indigo-800">
              Wrong column references in revenue queries
            </p>
          </div>
          <p className="ml-8 mt-1 text-xs text-indigo-600">
            From clusters: C001 | 3 questions | Priority 1
          </p>
          <div className="ml-8 mt-2 flex flex-wrap gap-1.5">
            {[
              { lever: 1, label: "Column descriptions" },
              { lever: 4, label: "Join specs" },
              { lever: 5, label: "Instructions" },
            ].map((l) => (
              <Badge
                key={l.lever}
                variant="outline"
                className={cn("text-[10px]", LEVER_COLORS[l.lever])}
              >
                L{l.lever}: {l.label}
              </Badge>
            ))}
          </div>
          <p className="ml-8 mt-2 text-[11px] italic text-indigo-500">
            Coordination: Column L1 descriptions reference the join spec added by L4,
            and L5 instructions route users to the correct table.
          </p>
        </div>
      </div>
    </div>
  );
}

function CausalWeightExplainer() {
  const sortedWeights = Object.entries(CLUSTER_IMPACT_WEIGHTS.causal)
    .sort(([, a], [, b]) => b - a);

  return (
    <div className="space-y-4">
      <p className="text-sm leading-relaxed text-slate-600">
        Causal weight reflects a judge's position in the <strong>causal failure chain</strong>.
        If Genie produces syntactically invalid SQL, every downstream judge (schema, logic,
        semantic, completeness) will also fail. Fixing an upstream judge has disproportionate
        leverage — a single syntax fix can "unlock" multiple downstream improvements.
      </p>
      <div className="rounded-lg border border-blue-100 bg-blue-50/50 p-3">
        <div className="mb-2 flex items-center gap-1.5">
          <Info className="h-3.5 w-3.5 text-blue-500" />
          <p className="text-xs font-semibold text-blue-700">
            Judge Causal Chain (upstream → downstream)
          </p>
        </div>
        <div className="flex items-center gap-1 overflow-x-auto text-[10px] font-medium">
          {sortedWeights.map(([judge, w], i) => (
            <div key={judge} className="flex items-center gap-1">
              <span className={cn(
                "whitespace-nowrap rounded px-1.5 py-0.5",
                w >= 4 ? "bg-red-100 text-red-700" : w >= 2 ? "bg-amber-100 text-amber-700" : "bg-slate-100 text-slate-600",
              )}>
                {judge.replace(/_/g, " ")} ({w})
              </span>
              {i < sortedWeights.length - 1 && (
                <ArrowRight className="h-3 w-3 shrink-0 text-slate-300" />
              )}
            </div>
          ))}
        </div>
      </div>
      <p className="text-xs text-slate-500">
        A cluster affecting the <code>syntax_validity</code> judge (weight 5.0) is scored
        10× higher than one affecting <code>response_quality</code> (weight 0.5) — because
        syntax errors cascade through the entire evaluation pipeline.
      </p>
    </div>
  );
}

function SeverityWeightExplainer() {
  const sortedSeverity = Object.entries(CLUSTER_IMPACT_WEIGHTS.severity)
    .sort(([, a], [, b]) => b - a);

  const tiers = [
    { label: "Critical", min: 0.8, color: "bg-red-100 text-red-700 border-red-200" },
    { label: "High", min: 0.6, color: "bg-amber-100 text-amber-700 border-amber-200" },
    { label: "Medium", min: 0.4, color: "bg-yellow-100 text-yellow-700 border-yellow-200" },
    { label: "Low", min: 0, color: "bg-slate-100 text-slate-600 border-slate-200" },
  ];

  return (
    <div className="space-y-4">
      <p className="text-sm leading-relaxed text-slate-600">
        Severity weight measures <strong>how damaging a specific failure type is</strong>.
        Using the wrong table entirely (1.0) is catastrophic — the SQL is fundamentally wrong.
        A missing synonym (0.3) is cosmetic — the Space can still answer, it just might not
        understand a particular phrasing.
      </p>
      <div className="space-y-3">
        {tiers.map((tier) => {
          const tierItems = sortedSeverity.filter(([, v]) => {
            const tierIdx = tiers.indexOf(tier);
            const upper = tierIdx > 0 ? tiers[tierIdx - 1].min : 999;
            return v >= tier.min && v < upper;
          });
          if (tierItems.length === 0) return null;
          return (
            <div key={tier.label}>
              <p className="mb-1.5 text-xs font-semibold text-slate-500">{tier.label}</p>
              <div className="flex flex-wrap gap-1">
                {tierItems.map(([name, w]) => (
                  <Badge key={name} variant="outline" className={cn("text-[10px]", tier.color)}>
                    {name.replace(/_/g, " ")} ({w})
                  </Badge>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function FixabilityExplainer() {
  return (
    <div className="space-y-4">
      <p className="text-sm leading-relaxed text-slate-600">
        Fixability is a <strong>binary confidence multiplier</strong> based on whether the
        cluster has ASI (Actionable Side Information) counterfactual fixes. During evaluation,
        judges produce structured feedback including "what would fix this" suggestions.
      </p>
      <div className="grid grid-cols-2 gap-3">
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-3">
          <p className="text-sm font-bold text-emerald-700">With Counterfactual: 1.0</p>
          <p className="mt-1 text-xs text-emerald-600">
            The system already knows the concrete fix (e.g., "replace <code>total_amount</code> with{" "}
            <code>total_revenue_usd</code>"). High confidence it can be resolved.
          </p>
        </div>
        <div className="rounded-lg border border-red-200 bg-red-50 p-3">
          <p className="text-sm font-bold text-red-700">Without Counterfactual: 0.4</p>
          <p className="mt-1 text-xs text-red-600">
            The failure was detected but no specific fix was identified.
            The system must infer a fix from schema context alone — a 60% confidence penalty.
          </p>
        </div>
      </div>
      <p className="text-xs text-slate-500">
        This dramatically deprioritizes vague failures where the system would be guessing.
        A cluster of 5 questions with no counterfactual (impact ×0.4) ranks below a cluster
        of 2 questions with a concrete fix (impact ×1.0).
      </p>
    </div>
  );
}

function ActionGroupExplainer() {
  return (
    <div className="space-y-4">
      <p className="text-sm leading-relaxed text-slate-600">
        An <strong>Action Group</strong> is the strategist's output — a coordinated, multi-lever
        action plan that addresses one root cause across multiple levers simultaneously. The key
        insight is that levers must <em>reinforce each other</em>. For example, fixing a{" "}
        <code>wrong_join</code> requires simultaneously updating column descriptions (L1),
        adding join specs (L4), and updating instructions (L5) — doing only one would leave
        the Space partially broken.
      </p>

      <div className="rounded-lg border border-slate-200 bg-slate-900 p-4">
        <p className="mb-2 text-xs font-semibold text-slate-400">Action Group JSON Schema</p>
        <pre className="overflow-x-auto text-xs leading-relaxed text-emerald-400">
{`{
  "id": "AG1",
  "root_cause_summary": "Revenue queries use wrong column",
  "source_cluster_ids": ["C001", "C003"],
  "affected_questions": ["q12", "q15", "q22"],
  "priority": 1,
  "lever_directives": {
    "1": { "columns": [{ "column": "total_revenue_usd",
             "sections": { "definition": "...",
                           "synonyms": "revenue, total_sales" }}]},
    "4": { "join_specs": [{ "left_table": "fact_sales",
             "right_table": "dim_product",
             "join_guidance": "ON fact_sales.product_id = ..." }]},
    "5": { "instruction_guidance": "When asked about revenue...",
           "example_sqls": [{ "question": "...", "sql_sketch": "..." }]}
  },
  "coordination_notes": "L1 descriptions reference the
    join spec from L4; L5 routes users to fact_sales"
}`}
        </pre>
      </div>

      <div className="space-y-2">
        <p className="text-xs font-semibold text-slate-500">Key Contracts</p>
        <ul className="space-y-1.5 text-xs text-slate-600">
          <li className="flex items-start gap-2">
            <span className="mt-0.5 h-1.5 w-1.5 shrink-0 rounded-full bg-amber-400" />
            <span><strong>No cross-AG conflicts</strong> — Two AGs must not touch the same object. Overlapping failures are merged into one AG.</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="mt-0.5 h-1.5 w-1.5 shrink-0 rounded-full bg-amber-400" />
            <span><strong>Coordination notes mandatory</strong> — Each AG must explain how changes across levers reference each other.</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="mt-0.5 h-1.5 w-1.5 shrink-0 rounded-full bg-amber-400" />
            <span><strong>Anti-hallucination</strong> — Every change must cite cluster_id(s). No invented root causes.</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="mt-0.5 h-1.5 w-1.5 shrink-0 rounded-full bg-amber-400" />
            <span><strong>All instruments of power</strong> — For each root cause, the strategist specifies every lever that should act (e.g., <code>wrong_column</code> → Primary L1, also L5).</span>
          </li>
        </ul>
      </div>

      <div className="rounded-lg border border-indigo-100 bg-indigo-50 p-3">
        <p className="text-xs font-semibold text-indigo-700">Two-Phase Strategist Pipeline</p>
        <div className="mt-2 flex items-center gap-2 text-xs">
          <div className="rounded bg-indigo-100 px-2 py-1 font-medium text-indigo-700">Phase 1a: Triage</div>
          <ArrowRight className="h-3 w-3 text-indigo-300" />
          <div className="rounded bg-indigo-100 px-2 py-1 font-medium text-indigo-700">Phase 1b: Detail</div>
        </div>
        <p className="mt-2 text-xs text-indigo-600">
          Triage sees all clusters compactly and produces AG skeletons with{" "}
          <code>levers_needed</code>, <code>focus_tables</code>, and <code>focus_columns</code>.
          Then Detail receives each skeleton with scoped metadata and produces full{" "}
          <code>lever_directives</code>.
        </p>
      </div>
    </div>
  );
}

export function FailureAnalysisStage() {
  const learnMore: { id: string; title: string; content: ReactNode }[] = [
    {
      id: "root-cause-resolution",
      title: "Root-Cause Resolution (3-Tier Cascade)",
      content: (
        <div className="space-y-4">
          <p className="text-sm leading-relaxed text-slate-600">
            Each failing question's root cause is diagnosed via a 3-tier cascade.
            The first tier that produces a result wins:
          </p>
          <ol className="list-inside list-decimal space-y-3 text-sm">
            <li>
              <strong>ASI Metadata</strong> — If the judge produced structured{" "}
              <code>asi_failure_type</code> (and it's not "other"), use it directly.
              This is the most reliable since it comes from the judge that found the failure.
            </li>
            <li>
              <strong>Rationale Pattern Matching</strong> — Keyword matching on the judge's
              free-text rationale to classify into known failure types (e.g., "wrong table",
              "missing join").
            </li>
            <li>
              <strong>SQL Diff Classification</strong> — Compare expected SQL vs generated SQL
              structurally to infer what changed (e.g., different column, different WHERE clause).
            </li>
          </ol>
        </div>
      ),
    },
    {
      id: "causal-weight",
      title: "Causal Weight — Judge Cascade Position",
      content: <CausalWeightExplainer />,
    },
    {
      id: "severity-weight",
      title: "Severity Weight — Failure Type Impact",
      content: <SeverityWeightExplainer />,
    },
    {
      id: "fixability",
      title: "Fixability — Counterfactual Confidence",
      content: <FixabilityExplainer />,
    },
    {
      id: "action-groups",
      title: "Action Groups — Coordinated Multi-Lever Plans",
      content: <ActionGroupExplainer />,
    },
    {
      id: "adaptive-strategist",
      title: "Adaptive Strategist Context (13 Blocks)",
      content: (
        <div className="space-y-4">
          <p className="text-sm text-slate-600">
            The strategist LLM receives 13 context blocks to make informed decisions.
            This is one of the largest prompt assemblies in the system:
          </p>
          <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3">
            {[
              { block: "progress_summary", desc: "Current accuracy and pass rate" },
              { block: "priority_analysis", desc: "Top-10 ranked clusters with impact scores" },
              { block: "reflection_history", desc: "What worked and what didn't in previous iterations" },
              { block: "proven_patterns", desc: "Successful fixes to replicate" },
              { block: "persistent_failures", desc: "Questions that keep failing" },
              { block: "human_suggestions", desc: "Manual review feedback" },
              { block: "schema", desc: "Full Genie Space schema" },
              { block: "failure_clusters", desc: "Detailed cluster breakdown" },
              { block: "soft_signal_clusters", desc: "Correct-but-suboptimal signals" },
              { block: "structured_metadata", desc: "Current table/column/function metadata" },
              { block: "join_specifications", desc: "Existing join specs" },
              { block: "current_instructions", desc: "General instructions text" },
              { block: "existing_example_sqls", desc: "Example SQL entries" },
            ].map((item) => (
              <div key={item.block} className="rounded border border-slate-200 bg-slate-50 p-2">
                <code className="text-[10px] font-bold text-slate-700">{item.block}</code>
                <p className="mt-0.5 text-[10px] text-slate-500">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      ),
    },
    {
      id: "failure-taxonomy",
      title: "Failure Taxonomy (21 Types)",
      content: (
        <div className="space-y-3">
          <p className="text-sm text-slate-600">
            Every failure is classified into one of 21 types, each mapped to a primary lever:
          </p>
          <div className="flex flex-wrap gap-1.5">
            {FAILURE_TAXONOMY.map((ft) => (
              <Badge
                key={ft.name}
                variant="outline"
                className={cn("text-[10px]", LEVER_COLORS[ft.lever] ?? "border-gray-300")}
              >
                {ft.name} (L{ft.lever})
              </Badge>
            ))}
          </div>
        </div>
      ),
    },
    {
      id: "persistent-failure-detection",
      title: "Persistent Failure Detection",
      content: (
        <ul className="space-y-3">
          {PERSISTENT_FAILURE_CLASSIFICATIONS.map((c) => (
            <li key={c.name} className="rounded-lg border border-slate-200 p-3">
              <div className="flex items-center gap-2">
                <Badge
                  variant="outline"
                  className={
                    c.color === "amber"
                      ? "border-amber-300 bg-amber-50"
                      : c.color === "orange"
                        ? "border-orange-300 bg-orange-50"
                        : "border-red-300 bg-red-50"
                  }
                >
                  {c.name}
                </Badge>
              </div>
              <p className="mt-1 font-mono text-xs text-slate-500">{c.condition}</p>
              <p className="mt-1 text-sm text-slate-600">{c.meaning}</p>
            </li>
          ))}
        </ul>
      ),
    },
    {
      id: "arbiter-corrections",
      title: "Arbiter Corrections Pipeline",
      content: (
        <div className="space-y-4">
          <div className="rounded-lg border border-slate-200 p-3">
            <p className="font-medium text-slate-700">Phase 1: genie_correct confirmations</p>
            <p className="mt-1 text-sm text-slate-500">
              Threshold: 2 evaluations must say genie_correct before auto-correcting benchmark SQL.
            </p>
          </div>
          <div className="rounded-lg border border-slate-200 p-3">
            <p className="font-medium text-slate-700">Phase 2: neither_correct repair/quarantine</p>
            <p className="mt-1 text-sm text-slate-500">
              Repair threshold: 2. Quarantine threshold: 3 consecutive neither_correct verdicts.
            </p>
          </div>
        </div>
      ),
    },
    {
      id: "escalation-flagging",
      title: "Escalation & Flagging",
      content: (
        <div className="space-y-4">
          <p className="mb-2 text-sm font-medium text-slate-700">3 escalation types</p>
          <ul className="space-y-2">
            {ESCALATION_TYPES.map((e) => (
              <li key={e.type} className="rounded-lg border border-slate-200 p-3">
                <code className="text-sm font-bold text-slate-700">{e.type}</code>
                <p className="mt-1 text-xs text-slate-500">Trigger: {e.trigger}</p>
                <p className="mt-1 text-sm text-slate-600">Action: {e.action}</p>
              </li>
            ))}
          </ul>
          <p className="text-xs text-slate-500">
            End-of-run sweep flags persistent failures. High-risk patches are queued for human approval.
          </p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Failure Analysis"
      subtitle="Diagnose root causes, rank by impact, and plan coordinated fixes"
      pipelineGroup="leverLoop"
      visual={<FailureTriageVisual />}
      explanation={
        <>
          After evaluation, the system extracts every failing question and diagnoses{" "}
          <em>why</em> it failed using a 3-tier root-cause resolution cascade. Failures
          with the same root cause and blamed objects form clusters, which are ranked by
          a 4-factor impact score. The highest-impact clusters are then fed to the Adaptive
          Strategist, which produces coordinated Action Groups — multi-lever plans that
          ensure fixes reinforce each other rather than conflict.
        </>
      }
      learnMore={learnMore}
    />
  );
}
