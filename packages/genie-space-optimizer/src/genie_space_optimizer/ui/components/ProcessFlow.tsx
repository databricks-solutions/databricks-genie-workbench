import React, { useEffect, useRef, useState, useCallback } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Search,
  Database,
  BarChart3,
  Wrench,
  CheckCircle2,
  Play,
  Pause,
  RotateCcw,
  ArrowRight,
  ArrowDown,
  Brain,
  Scale,
  Code2,
  FileText,
  Table2,
  Link2,
  MessageSquare,
  Filter,
  Sparkles,
  ShieldCheck,
  Layers,
  GitBranch,
  Repeat,
  Target,
  ChevronDown,
  Flag,
  FlaskConical,
  Activity,
  BookMarked,
  Tag,
  Box,
  ClipboardList,
  Gauge,
  Award,
  MessageCircle,
  UserCheck,
  RotateCw,
  Ban,
  type LucideIcon,
} from "lucide-react";

/* ------------------------------------------------------------------ */
/*  Data model                                                         */
/* ------------------------------------------------------------------ */

interface LeafDef {
  id: string;
  title: string;
  description: string;
  icon: LucideIcon;
  variant?: "proactive" | "optional";
  detail?: React.ComponentType;
}

interface MlflowFeature {
  label: string;
  api: string;
  icon: LucideIcon;
}

interface StepDef {
  number: number;
  title: string;
  description: string;
  icon: LucideIcon;
  leaves: LeafDef[];
  mlflow: MlflowFeature[];
}

/* ------------------------------------------------------------------ */
/*  Judge / Lever / Failure data                                       */
/* ------------------------------------------------------------------ */

const JUDGES = [
  { name: "syntax_validity", label: "Syntax Validity", type: "CODE" as const, threshold: 98, description: "Validates SQL parses correctly using Spark EXPLAIN", icon: Code2 },
  { name: "schema_accuracy", label: "Schema Accuracy", type: "LLM" as const, threshold: 95, description: "Checks correct tables, columns, and joins vs expected SQL", icon: Table2 },
  { name: "logical_accuracy", label: "Logical Accuracy", type: "LLM" as const, threshold: 90, description: "Checks aggregations, filters, GROUP BY, ORDER BY, WHERE", icon: Brain },
  { name: "semantic_equivalence", label: "Semantic Equivalence", type: "LLM" as const, threshold: 90, description: "Checks if two SQL queries answer the same business question", icon: Scale },
  { name: "completeness", label: "Completeness", type: "LLM" as const, threshold: 90, description: "Ensures no missing dimensions, measures, or filters", icon: CheckCircle2 },
  { name: "response_quality", label: "Response Quality", type: "LLM" as const, threshold: null, description: "Checks if natural-language response accurately describes SQL", icon: MessageSquare },
  { name: "asset_routing", label: "Asset Routing", type: "CODE" as const, threshold: 95, description: "Verifies Genie chose the right asset type (MV, TVF, Table)", icon: GitBranch },
  { name: "result_correctness", label: "Result Correctness", type: "CODE" as const, threshold: 85, description: "Compares ground-truth vs Genie result sets directly", icon: Target },
  { name: "arbiter", label: "Arbiter", type: "LLM" as const, threshold: null, description: "Tie-breaker that runs only when results disagree", icon: Scale },
];

const LEVERS = [
  { number: 1, name: "Tables & Columns", description: "Descriptions, aliases, synonyms", examples: ["update_column_description", "add_column_synonym"], icon: Table2 },
  { number: 2, name: "Metric Views", description: "MV measures, dimensions, YAML definitions", examples: ["update_mv_measure", "add_mv_dimension"], icon: BarChart3 },
  { number: 3, name: "Table-Valued Functions", description: "TVF SQL, parameters, function signatures", examples: ["update_tvf_sql", "add_tvf_parameter"], icon: Code2 },
  { number: 4, name: "Join Specifications", description: "Table relationships, join columns, cardinality", examples: ["add_join_spec", "update_join_spec"], icon: Link2 },
  { number: 5, name: "Instructions & Examples", description: "Routing rules, disambiguation, example SQL", examples: ["add_example_sql", "add_instruction"], icon: FileText },
  { number: 6, name: "SQL Expressions", description: "Reusable measures, filters, and dimensions", examples: ["add_sql_snippet_measure", "add_sql_snippet_filter"], icon: FlaskConical },
];

const FAILURE_TO_LEVER = [
  { failure: "wrong_column", lever: 1 }, { failure: "wrong_table", lever: 1 }, { failure: "missing_synonym", lever: 1 },
  { failure: "wrong_aggregation", lever: 2 }, { failure: "tvf_parameter_error", lever: 3 },
  { failure: "wrong_join", lever: 4 }, { failure: "missing_join_spec", lever: 4 },
  { failure: "asset_routing_error", lever: 5 }, { failure: "ambiguous_question", lever: 5 }, { failure: "missing_instruction", lever: 5 },
];

/* ------------------------------------------------------------------ */
/*  Leaf detail renderers                                              */
/* ------------------------------------------------------------------ */

function JudgesDetail() {
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-3 sm:grid-cols-5 lg:grid-cols-5">
        {JUDGES.map((judge) => (
          <div key={judge.name} className="flex flex-col items-center text-center rounded-xl border border-default/50 bg-surface p-3">
            <div className={`flex h-9 w-9 items-center justify-center rounded-lg ${judge.type === "CODE" ? "bg-emerald-100 text-emerald-600" : "bg-violet-100 text-violet-600"}`}>
              <judge.icon className="h-4.5 w-4.5" />
            </div>
            <p className="mt-2 text-xs font-semibold text-primary leading-tight">{judge.label}</p>
            <Badge variant="outline" className={`mt-1 text-[9px] px-1.5 py-0 ${judge.type === "CODE" ? "border-emerald-300 text-emerald-700" : "border-violet-300 text-violet-700"}`}>{judge.type}</Badge>
            <p className="mt-1.5 text-[11px] text-muted leading-snug">{judge.description}</p>
            {judge.threshold != null && (
              <p className="mt-1 text-[11px] text-muted">Threshold: <span className="font-semibold text-primary">{judge.threshold}%</span></p>
            )}
          </div>
        ))}
      </div>
      <div className="rounded-lg border border-blue-200 bg-blue-50/50 p-3">
        <p className="text-xs text-blue-800">
          <span className="font-semibold">Overall accuracy</span> is the weighted average across all judges. Each scores every question as <code className="rounded bg-blue-100 px-1 text-[10px]">yes</code>/<code className="rounded bg-blue-100 px-1 text-[10px]">no</code>.
        </p>
      </div>
    </div>
  );
}

function StrategistDetail() {
  const loopSteps = [
    { label: "Analyze", desc: "Pack the latest full-benchmark failures with the relevant space context and prior reflections", icon: Filter, color: "amber" as const },
    { label: "Propose", desc: "Ask the LLM for one targeted patch set against the enabled optimization levers", icon: Brain, color: "blue" as const },
    { label: "Screen", desc: "Drop unsafe, leaky, duplicate, or unsupported patches before touching the live space", icon: ShieldCheck, color: "orange" as const },
    { label: "Evaluate", desc: "Apply the candidate, run the full Genie benchmark set, and persist the scored attempt", icon: Sparkles, color: "green" as const },
    { label: "Decide", desc: "Keep accuracy improvements, rollback non-improving candidates, and carry reflection forward", icon: Layers, color: "violet" as const },
  ];

  const colorMap = {
    amber: { border: "border-amber-200", bg: "bg-amber-50/50", circle: "bg-amber-100", iconColor: "text-amber-700", title: "text-amber-900", text: "text-amber-800" },
    orange: { border: "border-orange-200", bg: "bg-orange-50/50", circle: "bg-orange-100", iconColor: "text-orange-700", title: "text-orange-900", text: "text-orange-800" },
    blue: { border: "border-blue-200", bg: "bg-blue-50/50", circle: "bg-blue-100", iconColor: "text-blue-700", title: "text-blue-900", text: "text-blue-800" },
    green: { border: "border-green-200", bg: "bg-green-50/50", circle: "bg-green-100", iconColor: "text-green-700", title: "text-green-900", text: "text-green-800" },
    violet: { border: "border-violet-200", bg: "bg-violet-50/50", circle: "bg-violet-100", iconColor: "text-violet-700", title: "text-violet-900", text: "text-violet-800" },
  };

  return (
    <div className="space-y-3">
      <div className="flex flex-col items-center gap-3 sm:flex-row sm:items-stretch sm:gap-4">
        {loopSteps.map((step, idx) => {
          const c = colorMap[step.color];
          return (
            <React.Fragment key={step.label}>
              <div className={`flex-1 rounded-xl border-2 ${c.border} ${c.bg} p-4 text-center`}>
                <div className={`mx-auto mb-2 flex h-10 w-10 items-center justify-center rounded-full ${c.circle}`}>
                  <step.icon className={`h-5 w-5 ${c.iconColor}`} />
                </div>
                <p className={`text-sm font-semibold ${c.title}`}>{step.label}</p>
                <p className={`mt-1 text-xs leading-snug ${c.text}`}>{step.desc}</p>
              </div>
              {idx < loopSteps.length - 1 && (
                <div className="flex items-center justify-center">
                  <ArrowRight className="hidden h-5 w-5 text-muted/40 sm:block" />
                  <ArrowDown className="block h-5 w-5 text-muted/40 sm:hidden" />
                </div>
              )}
            </React.Fragment>
          );
        })}
      </div>
      <div className="flex items-center justify-center gap-2 text-xs text-muted">
        <RotateCw className="h-3.5 w-3.5" />
        <span>Repeats until the target is reached, no new hypothesis exists, evaluation fails, or the attempt budget is exhausted</span>
      </div>
    </div>
  );
}

function LeversDetail() {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
      {LEVERS.map((lever) => (
        <div key={lever.number} className="flex flex-col items-center text-center rounded-xl border border-default/50 bg-surface p-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-blue-100 text-blue-600">
            <lever.icon className="h-4.5 w-4.5" />
          </div>
          <Badge variant="outline" className="mt-1.5 text-[9px] px-1.5 py-0">L{lever.number}</Badge>
          <p className="mt-1.5 text-xs font-semibold text-primary leading-tight">{lever.name}</p>
          <p className="mt-1 text-[11px] text-muted leading-snug">{lever.description}</p>
          <div className="mt-2 flex flex-wrap gap-1 justify-center">
            {lever.examples.map((ex) => (
              <code key={ex} className="rounded bg-elevated px-1.5 py-0.5 text-[9px] font-mono text-muted">{ex}</code>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function FailureRoutingDetail() {
  return (
    <div className="flex flex-wrap gap-2">
      {FAILURE_TO_LEVER.map((f) => (
        <div key={f.failure} className="inline-flex items-center gap-1.5 rounded-lg border border-default/50 bg-surface px-3 py-2 text-xs">
          <code className="font-mono text-muted">{f.failure}</code>
          <ArrowRight className="h-3 w-3 text-muted/50" />
          <span className="font-bold text-blue-600">L{f.lever}</span>
        </div>
      ))}
    </div>
  );
}

function FullBenchmarkDetail() {
  const gates = [
    { name: "Baseline", desc: "Iteration 0 evaluates the current live space through the native Genie Benchmark API", icon: BarChart3, color: "amber" as const },
    { name: "Candidate", desc: "Every accepted proposal is tested against the full benchmark corpus, not a partial slice", icon: ShieldCheck, color: "orange" as const },
    { name: "Decision", desc: "The candidate is accepted only when accuracy improves; otherwise the applier restores the previous config", icon: CheckCircle2, color: "green" as const },
  ];
  const colorMap = {
    amber: { border: "border-amber-200", bg: "bg-amber-50/50", circle: "bg-amber-100", iconColor: "text-amber-700", title: "text-amber-900", text: "text-amber-800" },
    orange: { border: "border-orange-200", bg: "bg-orange-50/50", circle: "bg-orange-100", iconColor: "text-orange-700", title: "text-orange-900", text: "text-orange-800" },
    green: { border: "border-green-200", bg: "bg-green-50/50", circle: "bg-green-100", iconColor: "text-green-700", title: "text-green-900", text: "text-green-800" },
    blue: { border: "border-blue-200", bg: "bg-blue-50/50", circle: "bg-blue-100", iconColor: "text-blue-700", title: "text-blue-900", text: "text-blue-800" },
  };

  return (
    <div className="space-y-3">
      <div className="flex flex-col items-center gap-3 sm:flex-row sm:items-stretch sm:gap-4">
        {gates.map((gate, idx) => {
          const c = colorMap[gate.color];
          return (
            <React.Fragment key={gate.name}>
              <div className={`flex-1 rounded-xl border-2 ${c.border} ${c.bg} p-4 text-center`}>
                <div className={`mx-auto mb-2 flex h-10 w-10 items-center justify-center rounded-full ${c.circle}`}>
                  <gate.icon className={`h-5 w-5 ${c.iconColor}`} />
                </div>
                <p className={`text-sm font-semibold ${c.title}`}>{idx + 1}. {gate.name}</p>
                <p className={`mt-1 text-xs leading-snug ${c.text}`}>{gate.desc}</p>
              </div>
              {idx < gates.length - 1 && (
                <div className="flex items-center justify-center">
                  <ArrowRight className="hidden h-5 w-5 text-muted/40 sm:block" />
                  <ArrowDown className="block h-5 w-5 text-muted/40 sm:hidden" />
                </div>
              )}
            </React.Fragment>
          );
        })}
      </div>
      <div className="rounded-lg border border-green-200 bg-green-50/50 p-3">
        <p className="text-xs text-green-800">
          <span className="font-semibold">Rollback protection:</span> Non-improving candidates are rolled back immediately. The best scored iteration is stamped with the terminal reason that Publish & Audit uses for gating.
        </p>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Step definitions with leaves                                       */
/* ------------------------------------------------------------------ */

const STEPS: StepDef[] = [
  {
    number: 1,
    title: "Intake & Snapshot",
    description: "Captures the original Genie Space configuration as the rollback anchor and writes the durable run manifest.",
    icon: Database,
    leaves: [
      { id: "s1-snapshot", title: "Rollback Snapshot", description: "Fetch and persist the original serialized_space before any optimizer mutation", icon: Database },
      { id: "s1-manifest", title: "Run Manifest", description: "Record run_id, space_id, domain, apply mode, levers, target accuracy, attempt budget, and warehouse", icon: ClipboardList },
      { id: "s1-config-hash", title: "Config Fingerprint", description: "Compute the baseline config hash used to identify the original state", icon: Tag },
      { id: "s1-table-refs", title: "Table References", description: "Extract the Genie table references needed by downstream metadata and benchmark tasks", icon: Table2 },
      { id: "s1-state", title: "Delta State Tables", description: "Ensure the optimizer tables and additive migrations exist before the run proceeds", icon: Layers },
    ],
    mlflow: [],
  },
  {
    number: 2,
    title: "Benchmark QC & Repair",
    description: "Builds the benchmark working set, EXPLAIN-validates it, repairs or prunes invalid rows, and pushes the valid set to the live space.",
    icon: ShieldCheck,
    leaves: [
      { id: "s2-metadata", title: "UC Metadata", description: "Collect column comments, tags, routines, and table context used by benchmark generation", icon: Database },
      { id: "s2-generate", title: "Benchmark Generation", description: "Load or synthesize the default 30-40 question working set for the selected domain", icon: MessageSquare },
      { id: "s2-explain", title: "EXPLAIN Validation", description: "Partition questions into valid and invalid sets using SQL validation before optimization starts", icon: Code2 },
      { id: "s2-repair", title: "Bounded Repair", description: "Prune invalid rows and synthesize replacements for up to three repair sweeps", icon: Wrench },
      { id: "s2-push", title: "Live Benchmark Push", description: "Merge the EXPLAIN-valid set into the live Genie Space benchmark store", icon: CheckCircle2 },
      { id: "s2-artifact", title: "QC Artifact", description: "Write valid counts, repair tries, window status, and terminal reason when the set is unrepairable", icon: FileText },
    ],
    mlflow: [
      { label: "Experiment Setup", api: "preflight_setup_experiment()", icon: FlaskConical },
      { label: "Evaluation Dataset", api: "mlflow.genai.datasets.create_dataset()", icon: ClipboardList },
      { label: "Feedback Carry-forward", api: "preflight_load_human_feedback()", icon: MessageCircle },
    ],
  },
  {
    number: 3,
    title: "Optimize",
    description: "Runs baseline evaluation and bounded native full-benchmark patch attempts inside one notebook task.",
    icon: Wrench,
    leaves: [
      { id: "s3-baseline", title: "Baseline Benchmark", description: "Iteration 0 scores the current live space through the native Genie Benchmark API", icon: BarChart3, detail: FullBenchmarkDetail },
      { id: "s3-context", title: "Failure Context Pack", description: "Build a compact, leakage-safe context from current failures, assets, snippets, functions, joins, and instruction sections", icon: Box },
      { id: "s3-proposal", title: "Targeted Patch Attempt", description: "The LLM proposes one patch set for the enabled lever set, informed by prior accepted and rolled-back attempts", icon: Brain, detail: StrategistDetail },
      { id: "s3-levers", title: "6 Optimization Levers", description: "Tables, metric views, TVFs, join specs, instructions, and SQL snippets are available as patch families", icon: Wrench, detail: LeversDetail },
      { id: "s3-screen", title: "Safety Screen", description: "Reject unsupported, duplicate, unsafe, or benchmark-leaking patch entries before applying", icon: Ban },
      { id: "s3-full-eval", title: "Full Benchmark Eval", description: "Every candidate is evaluated against the full benchmark corpus before it can become the new best iteration", icon: ShieldCheck, detail: FullBenchmarkDetail },
      { id: "s3-rollback", title: "Accept / Rollback", description: "Improving candidates become the current config; non-improving candidates are rolled back immediately", icon: Repeat },
      { id: "s3-terminal", title: "Terminal Reason", description: "Stamp TARGET_REACHED, MAX_ATTEMPTS, EVAL_INVALID, or NO_NEW_HYPOTHESIS on the champion row", icon: Flag },
    ],
    mlflow: [
      { label: "Experiment", api: "mlflow.set_experiment()", icon: FlaskConical },
      { label: "OpenAI Autolog", api: "mlflow.openai.autolog()", icon: Activity },
      { label: "Tracing Spans", api: "mlflow.start_span()", icon: Activity },
      { label: "Prompt Version", api: "mlflow.genai.register_prompt()", icon: BookMarked },
      { label: "Attempt Metrics", api: "write_iteration()", icon: Gauge },
    ],
  },
  {
    number: 4,
    title: "Publish & Audit",
    description: "Reads the stamped champion state, conditionally publishes the champion, writes the audit record, and surfaces concerns.",
    icon: Flag,
    leaves: [
      { id: "s4-champion", title: "Champion State", description: "Resolve the champion iteration and read its stamped terminal reason", icon: Award },
      { id: "s4-gate", title: "Publish Gate", description: "Publish only for TARGET_REACHED or MAX_ATTEMPTS; fail closed for invalid or stalled runs", icon: ShieldCheck },
      { id: "s4-publish", title: "Delta Promotion", description: "Promote the best model in optimizer state without a separate deploy task or extra live-space mutation", icon: CheckCircle2 },
      { id: "s4-audit", title: "Publish Record", description: "Write champion pointer, trajectory, publish outcome, final status, and terminal reason", icon: FileText },
      { id: "s4-summary", title: "Audit Summary", description: "Generate a best-effort leakage-safe summary over aggregate run structure", icon: MessageSquare },
      { id: "s4-concerns", title: "Concerns", description: "Record residual failures, unstamped champion diagnostics, or non-publishing stop reasons for human review", icon: UserCheck },
    ],
    mlflow: [
      { label: "Audit Prompt", api: "AUDIT_SUMMARY_PROMPT", icon: BookMarked },
      { label: "Trace Linking", api: "_link_prompt_to_trace()", icon: Activity },
      { label: "Publish Artifact", api: "write_artifact(\"publish_record\")", icon: FileText },
    ],
  },
];

const TOTAL_STEPS = STEPS.length;
const AUTO_PLAY_INTERVAL = 5000;

/* ------------------------------------------------------------------ */
/*  MLflow Gen AI strip                                                */
/* ------------------------------------------------------------------ */

function MlflowStrip({ features }: { features: MlflowFeature[] }) {
  if (features.length === 0) return null;

  return (
    <div className="border-t border-teal-200/60 bg-gradient-to-r from-teal-50/60 via-indigo-50/40 to-teal-50/60 px-5 py-3">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-1.5 shrink-0">
          <div className="flex h-5 w-5 items-center justify-center rounded bg-teal-600 text-white">
            <FlaskConical className="h-3 w-3" />
          </div>
          <span className="text-[11px] font-bold text-teal-800 tracking-wide uppercase">MLflow Gen AI</span>
        </div>
        <div className="h-4 w-px bg-teal-300/60 shrink-0" />
        {features.map((f) => (
          <div key={f.label} className="group relative inline-flex items-center gap-1.5 rounded-lg border border-teal-200/80 bg-white/80 px-2.5 py-1.5 transition-colors hover:border-teal-400 hover:bg-teal-50/50">
            <f.icon className="h-3.5 w-3.5 text-teal-600 shrink-0" />
            <span className="text-[11px] font-medium text-teal-900">{f.label}</span>
            <div className="pointer-events-none absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 hidden group-hover:block z-20">
              <div className="rounded-md bg-gray-900 px-2.5 py-1.5 shadow-lg">
                <code className="text-[10px] font-mono text-teal-300 whitespace-nowrap">{f.api}</code>
              </div>
              <div className="mx-auto h-1.5 w-1.5 -mt-0.5 rotate-45 bg-gray-900" />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Leaf node component                                                */
/* ------------------------------------------------------------------ */

function LeafNode({ leaf, isExpanded, onToggle }: { leaf: LeafDef; isExpanded: boolean; onToggle: () => void }) {
  const hasDetail = !!leaf.detail;
  const variantBadge = leaf.variant === "proactive"
    ? <Badge variant="outline" className="text-[8px] px-1 py-0 border-purple-300 text-purple-700 bg-purple-50">Proactive</Badge>
    : leaf.variant === "optional"
      ? <Badge variant="outline" className="text-[8px] px-1 py-0 border-amber-300 text-amber-700 bg-amber-50">Optional</Badge>
      : null;

  const card = (
    <div className="flex flex-col items-center text-center p-4">
      <div className={`flex h-10 w-10 items-center justify-center rounded-lg ${leaf.variant === "proactive" ? "bg-purple-100 text-purple-600" : "bg-blue-100 text-blue-600"}`}>
        <leaf.icon className="h-5 w-5" />
      </div>
      <div className="mt-2.5 flex items-center gap-1 flex-wrap justify-center">
        <p className="text-sm font-semibold text-primary leading-tight">{leaf.title}</p>
        {variantBadge}
      </div>
      <p className="text-xs text-muted leading-snug mt-1.5">{leaf.description}</p>
      {hasDetail && (
        <ChevronDown className={`mt-2 h-4 w-4 text-muted/60 transition-transform duration-200 ${isExpanded ? "rotate-0" : "-rotate-90"}`} />
      )}
    </div>
  );

  const borderColor = isExpanded ? "border-blue-300 shadow-sm shadow-blue-100" : "border-default/40";

  if (!hasDetail) {
    return (
      <div className={`rounded-xl border ${borderColor} bg-surface`}>
        {card}
      </div>
    );
  }

  return (
    <button
      onClick={onToggle}
      className={`rounded-xl border ${borderColor} bg-surface cursor-pointer hover:border-blue-200 hover:shadow-sm transition-all text-left`}
    >
      {card}
    </button>
  );
}

function getDetailLeafIds(step: StepDef): Set<string> {
  return new Set(step.leaves.filter((l) => l.detail).map((l) => l.id));
}

/* ------------------------------------------------------------------ */
/*  Main component                                                     */
/* ------------------------------------------------------------------ */

export function ProcessFlow() {
  const [activeStep, setActiveStep] = useState(1);
  const [isPlaying, setIsPlaying] = useState(true);
  const [expandedLeaves, setExpandedLeaves] = useState<Set<string>>(
    () => getDetailLeafIds(STEPS[0])
  );
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const clearTimer = useCallback(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  const advance = useCallback(() => {
    setActiveStep((prev) => {
      const next = prev >= TOTAL_STEPS ? 1 : prev + 1;
      setExpandedLeaves(getDetailLeafIds(STEPS[next - 1]));
      return next;
    });
  }, []);

  useEffect(() => {
    if (isPlaying) {
      clearTimer();
      intervalRef.current = setInterval(advance, AUTO_PLAY_INTERVAL);
    } else {
      clearTimer();
    }
    return clearTimer;
  }, [isPlaying, advance, clearTimer]);

  const handleStepClick = (stepNumber: number) => {
    setIsPlaying(false);
    setActiveStep(stepNumber);
    setExpandedLeaves(getDetailLeafIds(STEPS[stepNumber - 1]));
  };

  const handleToggleLeaf = (leafId: string) => {
    setIsPlaying(false);
    setExpandedLeaves((prev) => {
      const next = new Set(prev);
      if (next.has(leafId)) {
        next.delete(leafId);
      } else {
        next.add(leafId);
      }
      return next;
    });
  };

  const handlePlayPause = () => setIsPlaying((p) => !p);

  const handleRestart = () => {
    setActiveStep(1);
    setExpandedLeaves(getDetailLeafIds(STEPS[0]));
    setIsPlaying(true);
  };

  const current = STEPS[activeStep - 1];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="text-center">
        <h2 className="text-xl font-semibold text-primary">How the Optimizer Works</h2>
        <p className="mt-1 text-sm text-muted">
          A {TOTAL_STEPS}-step pipeline that analyzes, benchmarks, and improves your Genie Space configuration using 9 judges and 6 optimization levers.
        </p>
      </div>

      {/* Horizontal spine */}
      <div className="flex items-start justify-center gap-0 px-2 pt-3 overflow-x-auto">
        {STEPS.map((step, idx) => {
          const isActive = step.number === activeStep;
          const isPast = step.number < activeStep;
          const StepIcon = step.icon;

          return (
            <div key={step.number} className="flex items-center">
              <button
                onClick={() => handleStepClick(step.number)}
                className="group relative flex flex-col items-center gap-1.5 overflow-visible focus:outline-none"
              >
                <div
                  className={`
                    relative flex h-14 w-14 items-center justify-center rounded-full
                    border-2 transition-all duration-500 cursor-pointer
                    ${isActive
                      ? "border-blue-500 bg-blue-500 text-white shadow-lg shadow-blue-500/30 scale-110"
                      : isPast
                        ? "border-green-500 bg-green-50 text-green-600"
                        : "border-muted-foreground/30 bg-elevated/50 text-muted group-hover:border-blue-300 group-hover:bg-blue-50 group-hover:text-blue-500"
                    }
                  `}
                >
                  {isActive && (
                    <div className="absolute inset-0 rounded-full animate-ping bg-blue-400 opacity-20" />
                  )}
                  <StepIcon className="h-6 w-6 relative z-10" />
                </div>

                <span
                  className={`
                    text-[10px] font-medium text-center max-w-[90px] leading-tight transition-colors duration-300
                    ${isActive ? "text-blue-600" : isPast ? "text-green-600" : "text-muted"}
                  `}
                >
                  {step.title}
                </span>

                <Badge
                  variant="outline"
                  className={`
                    absolute -top-1.5 -right-1.5 h-5 w-5 rounded-full p-0 text-[10px]
                    flex items-center justify-center transition-all duration-300
                    ${isActive
                      ? "border-blue-500 bg-blue-600 text-white"
                      : isPast
                        ? "border-green-500 bg-green-500 text-white"
                        : "border-muted-foreground/30 bg-surface text-muted"
                    }
                  `}
                >
                  {step.number}
                </Badge>
              </button>

              {idx < STEPS.length - 1 && (
                <div className="flex items-center mx-2 mb-6">
                  <div
                    className={`h-0.5 w-8 transition-colors duration-500 ${isPast ? "bg-green-400" : "bg-elevated-foreground/20"}`}
                  />
                  <ArrowRight
                    className={`h-3.5 w-3.5 -ml-1 transition-colors duration-500 ${isPast ? "text-green-400" : "text-muted/20"}`}
                  />
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Detail panel for the active step */}
      <div
        key={activeStep}
        className="animate-in fade-in slide-in-from-bottom-2 duration-500 rounded-lg border border-default/50 bg-surface overflow-hidden"
      >
        <div className="flex items-center gap-3 border-b border-default/30 px-5 py-3">
          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-blue-100 text-blue-600">
            <current.icon className="h-5 w-5" />
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-semibold text-primary">{current.title}</h3>
              <Badge variant="outline" className="text-[9px] px-1.5 py-0 border-blue-300 text-blue-700">
                Step {current.number} of {TOTAL_STEPS}
              </Badge>
            </div>
            <p className="text-xs text-muted leading-snug mt-0.5">{current.description}</p>
          </div>
        </div>

        {/* Layer 2: Leaf cards */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4 px-5 py-4">
          {current.leaves.map((leaf) => (
            <LeafNode
              key={leaf.id}
              leaf={leaf}
              isExpanded={expandedLeaves.has(leaf.id)}
              onToggle={() => handleToggleLeaf(leaf.id)}
            />
          ))}
        </div>

        {/* MLflow Gen AI strip */}
        <MlflowStrip features={current.mlflow} />

        {/* Layer 3: Expanded detail panels (full width) */}
        {current.leaves
          .filter((leaf) => leaf.detail && expandedLeaves.has(leaf.id))
          .map((leaf) => {
            const DetailComp = leaf.detail!;
            return (
              <div key={leaf.id} className="border-t border-default/30 px-5 py-4 animate-in fade-in slide-in-from-top-2 duration-300">
                <div className="flex items-center gap-2 mb-3">
                  <div className="flex h-7 w-7 items-center justify-center rounded-md bg-blue-100 text-blue-600">
                    <leaf.icon className="h-4 w-4" />
                  </div>
                  <h4 className="text-sm font-semibold text-primary">{leaf.title}</h4>
                  <div className="h-px flex-1 bg-border/40" />
                  <button
                    onClick={() => handleToggleLeaf(leaf.id)}
                    className="text-xs text-muted hover:text-primary transition-colors cursor-pointer"
                  >
                    Collapse
                  </button>
                </div>
                <DetailComp />
              </div>
            );
          })}

        {/* Progress bar */}
        <div className="h-1 bg-elevated">
          <div
            className="h-full bg-blue-500 transition-all duration-500 ease-out"
            style={{ width: `${(activeStep / TOTAL_STEPS) * 100}%` }}
          />
        </div>
      </div>

      {/* Controls */}
      <div className="flex items-center justify-center gap-3">
        <Button variant="outline" size="sm" onClick={handleRestart} className="gap-1.5">
          <RotateCcw className="h-3.5 w-3.5" />
          Restart
        </Button>
        <Button variant="outline" size="sm" onClick={handlePlayPause} className="gap-1.5">
          {isPlaying ? (
            <><Pause className="h-3.5 w-3.5" /> Pause</>
          ) : (
            <><Play className="h-3.5 w-3.5" /> Play</>
          )}
        </Button>
        <div className="flex items-center gap-1.5 ml-2">
          {STEPS.map((step) => (
            <button
              key={step.number}
              onClick={() => handleStepClick(step.number)}
              className={`h-2 w-2 rounded-full transition-all duration-300 focus:outline-none ${
                step.number === activeStep
                  ? "bg-blue-500 scale-125"
                  : step.number < activeStep
                    ? "bg-green-400"
                    : "bg-elevated-foreground/30 hover:bg-blue-300"
              }`}
              aria-label={`Go to step ${step.number}`}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
