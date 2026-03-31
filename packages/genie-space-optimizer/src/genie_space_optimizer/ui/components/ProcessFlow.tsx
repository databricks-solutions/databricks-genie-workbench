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
  Upload,
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
  Rocket,
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
  { number: 5, name: "Genie Space Instructions", description: "Routing rules, disambiguation, example SQL", examples: ["add_example_sql", "add_instruction"], icon: FileText },
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
    { label: "Re-cluster", desc: "Re-cluster failures from the latest evaluation results, removing already-tried clusters", icon: Filter, color: "amber" as const },
    { label: "Rank", desc: "Score clusters by question_count × causal_weight × severity × fixability; pick highest-impact", icon: Target, color: "orange" as const },
    { label: "Strategist", desc: "Single LLM call produces exactly one action group with concrete lever directives", icon: Brain, color: "blue" as const },
    { label: "Apply", desc: "Execute lever patches and run the 3-gate evaluation system", icon: Sparkles, color: "green" as const },
    { label: "Reflect", desc: "Record outcome (accepted/rolled back, score delta) into the reflection buffer for the next iteration", icon: Layers, color: "violet" as const },
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
        <span>Repeats each iteration until convergence criteria are met</span>
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

function ThreeGateDetail() {
  const gates = [
    { name: "Slice Eval", desc: "Quick check on affected questions only (currently disabled)", icon: Filter, color: "amber" as const, disabled: true },
    { name: "P0 Eval", desc: "Evaluate high-priority questions for critical accuracy", icon: ShieldCheck, color: "orange" as const, disabled: false },
    { name: "Full Eval", desc: "Complete benchmark re-evaluation. Runs a confirmation pass if accuracy did not improve, averaging both to smooth Genie non-determinism", icon: CheckCircle2, color: "green" as const, disabled: false },
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
                <p className={`text-sm font-semibold ${c.title}`}>Gate {idx + 1}: {gate.name}</p>
                {gate.disabled && <Badge variant="outline" className="mt-1 text-[8px] px-1.5 py-0 border-muted-foreground/30 text-muted">Disabled</Badge>}
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
          <span className="font-semibold">Rollback protection:</span> Regressions on any gate cause automatic rollback. Single-question noise is filtered — if all regressions fall within one question's weight band, they are treated as Genie variance and the changes are kept.
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
    title: "Preflight",
    description: "Loads the Genie Space configuration, discovers benchmarks, and initialises the MLflow experiment, evaluation dataset, and model registrations.",
    icon: Search,
    leaves: [
      { id: "s1-tables", title: "Table Definitions", description: "Parse all table schemas, column names, and data types", icon: Table2 },
      { id: "s1-instructions", title: "Instructions", description: "Extract existing natural-language instructions and routing rules", icon: FileText },
      { id: "s1-questions", title: "Sample Questions", description: "Catalog example questions and their ground-truth SQL pairs", icon: MessageSquare },
      { id: "s1-functions", title: "Functions", description: "Identify registered TVFs and metric views", icon: Code2 },
      { id: "s1-joins", title: "Join Specifications", description: "Map existing join specifications between tables", icon: Link2 },
      { id: "s1-coverage", title: "Description Coverage", description: "Measure how many columns and tables have descriptions", icon: Filter },
      { id: "s1-columns", title: "Column Metadata", description: "Fetch descriptions, comments, and tags from Unity Catalog", icon: Database },
      { id: "s1-profiling", title: "Data Profiling", description: "Profile data distributions, cardinality, and sample values", icon: BarChart3 },
      { id: "s1-fk", title: "FK Discovery", description: "Detect foreign-key relationships between tables using UC constraints", icon: Link2 },
      { id: "s1-graph", title: "Metadata Graph", description: "Build a comprehensive metadata graph for optimization decisions", icon: Layers },
    ],
    mlflow: [
      { label: "Experiment Creation", api: "mlflow.set_experiment()", icon: FlaskConical },
      { label: "Experiment Tags", api: "mlflow.set_experiment_tags()", icon: Tag },
      { label: "Evaluation Dataset", api: "mlflow.genai.datasets.create_dataset()", icon: ClipboardList },
      { label: "Prompt Registry", api: "mlflow.genai.register_prompt()", icon: BookMarked },
      { label: "LoggedModel Init", api: "mlflow.initialize_logged_model()", icon: Box },
      { label: "Label Schemas", api: "mlflow.genai.label_schemas.create_label_schema()", icon: UserCheck },
      { label: "Feedback Ingestion", api: "mlflow.genai.labeling.get_labeling_sessions()", icon: MessageCircle },
    ],
  },
  {
    number: 2,
    title: "Baseline Evaluation",
    description: "Runs all benchmark questions through the Genie API with 9 evaluation judges to establish the current accuracy baseline.",
    icon: BarChart3,
    leaves: [
      { id: "s2-judges", title: "9 Evaluation Judges", description: "3 deterministic code judges and 6 LLM reasoning judges score every question independently", icon: Scale, detail: JudgesDetail },
      { id: "s2-arbiter", title: "Arbiter Corrections", description: "When 3+ benchmark questions have arbiter verdict 'genie_correct', the benchmark dataset is updated with Genie's SQL as the new ground truth before the strategist runs", icon: ShieldCheck, variant: "proactive" },
    ],
    mlflow: [
      { label: "MLflow Run", api: "mlflow.start_run()", icon: FlaskConical },
      { label: "GenAI Evaluate", api: "mlflow.genai.evaluate()", icon: Scale },
      { label: "Tracing", api: "@mlflow.trace", icon: Activity },
      { label: "Params & Metrics", api: "mlflow.log_params() / log_metric()", icon: Gauge },
      { label: "LoggedModel Linking", api: "mlflow.set_active_model()", icon: Box },
    ],
  },
  {
    number: 3,
    title: "Proactive Enrichment",
    description: "Before the optimization loop, the pipeline runs a series of deterministic and LLM-powered stages that proactively enrich the Genie Space metadata.",
    icon: Sparkles,
    leaves: [
      { id: "s3-prompt", title: "Prompt Matching", description: "Enable format assistance on visible columns and entity matching on STRING columns so Genie shows example values. Deterministic, no LLM. Capped at 120 columns.", icon: Sparkles, variant: "proactive" },
      { id: "s3-enrich", title: "Description Enrichment", description: "Find columns where both Genie description and UC comment are blank, generate structured descriptions (definition, values, join hints) via Databricks LLM", icon: Sparkles, variant: "proactive" },
      { id: "s3-joindisco", title: "Join Discovery", description: "Parse JOINs from baseline SQL results, merge with UC FK constraints, validate type compatibility, and patch new join specifications into the Genie Space", icon: Sparkles, variant: "proactive" },
      { id: "s3-instrseed", title: "Instruction Seeding", description: "For spaces with empty or minimal instructions, generates conservative routing instructions via LLM and patches them before the lever loop", icon: Sparkles, variant: "proactive" },
      { id: "s3-spacemeta", title: "Space Metadata", description: "Generate or refine the Genie Space description and sample questions using LLM analysis of the space's tables and columns", icon: Sparkles, variant: "proactive" },
      { id: "s3-examplesql", title: "Benchmark Example Mining", description: "Extract correct SQL from baseline evaluation results and inject them as example question-SQL pairs into the Genie Space", icon: Sparkles, variant: "proactive" },
    ],
    mlflow: [
      { label: "Instruction Versioning", api: "mlflow.genai.register_prompt()", icon: BookMarked },
    ],
  },
  {
    number: 4,
    title: "Adaptive Lever Loop",
    description: "An adaptive loop re-clusters failures each iteration, ranks them by impact, generates one targeted action group, applies it through 3-gate evaluation, and reflects on the outcome to guide the next iteration.",
    icon: Wrench,
    leaves: [
      { id: "s4-strategist", title: "Adaptive Strategist", description: "Each iteration: re-cluster from latest eval, filter already-tried patches, rank by impact score, then a single LLM call produces exactly one action group", icon: Brain, detail: StrategistDetail },
      { id: "s4-ranking", title: "Cluster Ranking", description: "Clusters scored by question_count × causal_weight × severity × fixability; highest-impact cluster addressed first", icon: Target },
      { id: "s4-reflection", title: "Reflection Buffer", description: "Memory of prior iterations (accepted/rolled back, score deltas, do-not-retry list) passed to the strategist to avoid repeating mistakes", icon: Layers },
      { id: "s4-levers", title: "5 Optimization Levers", description: "Tables & Columns, Metric Views, TVFs, Join Specs, and Instructions — each lever is an executor that generates patch proposals", icon: Wrench, detail: LeversDetail },
      { id: "s4-routing", title: "Failure-to-Lever Routing", description: "Judge failure types are automatically mapped to the appropriate optimization lever", icon: GitBranch, detail: FailureRoutingDetail },
      { id: "s4-gates", title: "3-Gate Evaluation", description: "Slice Eval, P0 Eval, and Full Eval with confirmation — the Full gate runs a confirmation pass if accuracy did not improve, averaging both to smooth Genie non-determinism", icon: ShieldCheck, detail: ThreeGateDetail },
      { id: "s4-rollback", title: "Rollback Protection", description: "Regressions cause automatic rollback, but single-question noise is filtered — if all regressions fall within one question's weight band, they're treated as Genie variance and the changes are kept", icon: Repeat },
      { id: "s4-arbiter", title: "Per-Iteration Arbiter Corrections", description: "After each evaluation, arbiter corrections update the benchmark ground truth for questions Genie answered correctly — this runs every iteration, not just at baseline", icon: ShieldCheck, variant: "proactive" },
      { id: "s4-quarantine", title: "Persistence Quarantine", description: "Questions that persistently fail across multiple iterations are quarantined from future analysis and flagged for human review", icon: Ban },
      { id: "s4-fallback", title: "Holistic Fallback", description: "On the first iteration, if the adaptive strategist returns zero action groups, a holistic single-lever fallback strategy is attempted", icon: RotateCw },
      { id: "s4-convergence", title: "Convergence Criteria", description: "Loop stops when: (1) all thresholds met, (2) diminishing returns (last 2 iterations each < 2% gain), (3) 2 consecutive rollbacks, (4) no actionable clusters remain, or (5) max iterations reached (default: 3)", icon: Target },
    ],
    mlflow: [
      { label: "Tracing Spans", api: "mlflow.start_span()", icon: Activity },
      { label: "LoggedModel per Iteration", api: "mlflow.initialize_logged_model()", icon: Box },
      { label: "3-Gate Evaluations", api: "mlflow.genai.evaluate()", icon: Scale },
      { label: "Gate Feedback", api: "mlflow.log_feedback()", icon: MessageCircle },
      { label: "ASI Feedback", api: "mlflow.log_feedback()", icon: MessageCircle },
      { label: "Instruction Versioning", api: "mlflow.genai.register_prompt()", icon: BookMarked },
    ],
  },
  {
    number: 5,
    title: "Finalization",
    description: "After the optimization loop completes, the pipeline runs repeatability tests, promotes the champion model, creates a review session, and optionally writes descriptions back to Unity Catalog.",
    icon: Flag,
    leaves: [
      { id: "s5-repeat", title: "Repeatability Tests", description: "2 additional full evaluation passes confirm improvements are stable and non-flaky across independent runs", icon: Repeat },
      { id: "s5-publish", title: "Benchmark Publishing", description: "Push updated benchmark questions and ground-truth SQL back to the Genie Space for future evaluations", icon: Upload },
      { id: "s5-review", title: "Review Session", description: "Creates an MLflow labeling session from the final evaluation so domain experts can review flagged questions", icon: UserCheck },
      { id: "s5-uc", title: "UC Write-backs", description: "When apply_mode includes uc_artifact, column and table descriptions are written back to Unity Catalog via ALTER TABLE ... COMMENT. Executes under the user's OBO identity, only replays non-rolled-back patches.", icon: Database, variant: "optional" },
    ],
    mlflow: [
      { label: "Repeatability Evaluation", api: "mlflow.genai.evaluate()", icon: Scale },
      { label: "Champion Promotion", api: 'mlflow.set_logged_model_alias("champion")', icon: Award },
      { label: "Review Session", api: "mlflow.genai.labeling.create_labeling_session()", icon: UserCheck },
    ],
  },
  {
    number: 6,
    title: "Deploy",
    description: "Optionally deploys the optimized configuration to the target Genie Space environment.",
    icon: Rocket,
    leaves: [
      { id: "s6-deploy", title: "Optional Deployment", description: "When enabled, the optimized configuration is pushed to the target Genie Space, replacing the previous configuration with the champion model's metadata", icon: Rocket, variant: "optional" },
    ],
    mlflow: [],
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
