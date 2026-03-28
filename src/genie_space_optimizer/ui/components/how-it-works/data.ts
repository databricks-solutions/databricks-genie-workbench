import {
  Shield,
  BarChart3,
  Sparkles,
  RefreshCw,
  Award,
  Rocket,
  Database,
  BrainCircuit,
  Settings2,
  Zap,
  BookOpen,
  Link2,
  Globe,
  MessageSquare,
  Code,
  FlaskConical,
  Table2,
  CheckCircle2,
  UserCheck,
  PenLine,
  Camera,
  Eye,
  Target,
  GitBranch,
  Filter,
  Layers,
  ShieldCheck,
  TrendingUp,
  CheckCheck,
} from "lucide-react";
import type {
  PipelineStage,
  Judge,
  Lever,
  FailureType,
  RunStatus,
  SafetyConstant,
  DataFlowNode,
  GateDefinition,
  ConvergenceCondition,
  EnrichmentSubstage,
  PreflightStep,
  PersistentFailureClassification,
  EscalationType,
  ReviewField,
  ClusterImpactWeights,
  WalkthroughStage,
  FictionalExample,
} from "./types";

/* ═══════════════════════════════════════════════════════════════════
   PIPELINE STAGES
   ═══════════════════════════════════════════════════════════════════ */

export const PIPELINE_STAGES: PipelineStage[] = [
  {
    id: "preflight",
    title: "Preflight",
    icon: Shield,
    summary: "Collect metadata, generate & validate benchmarks",
    description:
      "Fetches the Genie Space configuration, collects Unity Catalog metadata with data profiling, generates benchmark questions via LLM, validates them through EXPLAIN and execution, loads human feedback from prior sessions, and sets up the MLflow experiment.",
  },
  {
    id: "baseline",
    title: "Baseline Eval",
    icon: BarChart3,
    summary: "Establish quality baseline with 9 judges",
    description:
      "Runs every benchmark question through the Genie Space, then evaluates each response with 9 specialized judges. Produces per-judge scores, an overall accuracy figure, and Actionable Side Information (ASI) that pinpoints exactly why each failure occurred.",
  },
  {
    id: "enrichment",
    title: "Enrichment",
    icon: Sparkles,
    summary: "Proactively fill metadata gaps before optimization",
    description:
      "Applies non-destructive improvements: fills blank column/table descriptions using LLM, discovers join specifications from baseline queries and FK constraints, seeds routing instructions, mines example SQLs from benchmarks, and configures format assistance and entity matching.",
  },
  {
    id: "lever-loop",
    title: "Lever Loop",
    icon: RefreshCw,
    summary: "Iteratively optimize via failure analysis & targeted patches",
    description:
      "The core optimization cycle. Extracts failures, clusters them by root cause, ranks by impact, generates targeted metadata patches via an LLM strategist, applies them, evaluates through a 3-gate pattern (Slice → P0 → Full), and accepts or rolls back. Repeats until convergence, stall, or max iterations.",
  },
  {
    id: "finalize",
    title: "Finalize",
    icon: Award,
    summary: "Validate repeatability, create review session, promote model",
    description:
      "Runs a 2-pass repeatability test comparing SQL hashes, creates an MLflow labeling session for human review, promotes the best iteration's model to 'champion', registers it in Unity Catalog, and sets the terminal run status.",
  },
  {
    id: "deploy",
    title: "Deploy",
    icon: Rocket,
    summary: "Optionally deploy to a target workspace",
    description:
      "If a deploy target was configured, loads the optimized config from the UC registered model, gates deployment on human approval via a model tag check, and patches the target workspace's Genie Space configuration.",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   THE 9 JUDGES
   ═══════════════════════════════════════════════════════════════════ */

export const JUDGE_CATEGORY_COLORS: Record<string, string> = {
  syntax: "border-l-blue-500",
  schema: "border-l-indigo-500",
  logic: "border-l-purple-500",
  quality: "border-l-gray-400",
  execution: "border-l-emerald-500",
  routing: "border-l-orange-500",
  arbiter: "border-l-amber-500",
};

export const JUDGES: Judge[] = [
  {
    number: 1,
    name: "syntax_validity",
    displayName: "Syntax Validity",
    description: "Checks that the generated SQL parses without syntax errors.",
    detailedDescription:
      "Runs EXPLAIN {sql} via Spark. If SQL is empty → FAIL with severity 'critical'. If EXPLAIN throws an error → FAIL with wrong_clause='SELECT' and a counterfactual fix suggestion. This is deterministic — no LLM involved.",
    method: "CODE",
    threshold: 98,
    category: "syntax",
    failureTypes: ["syntax_error"],
    overrideRules: [],
    asiFields: ["wrong_clause", "actual_value", "counterfactual_fix"],
  },
  {
    number: 2,
    name: "schema_accuracy",
    displayName: "Schema Accuracy",
    description:
      "Verifies that all referenced tables, columns, and joins exist.",
    detailedDescription:
      "LLM judge that checks if the generated SQL references correct tables, columns, and joins against Unity Catalog metadata. Override: if execution results match (result_matched) and LLM said fail → force PASS. Confidence: 0.95 normally, 0.5 on result-match override.",
    method: "LLM",
    threshold: 95,
    category: "schema",
    failureTypes: ["wrong_table", "wrong_column", "wrong_join", "missing_column"],
    overrideRules: ["result_matched → force PASS"],
    asiFields: ["failure_type", "wrong_clause", "blame_set", "counterfactual_fix"],
    promptSnippet:
      "You are a SQL schema expert. Determine if the GENERATED SQL references the correct tables, columns, and joins compared to the EXPECTED SQL.",
  },
  {
    number: 3,
    name: "logical_accuracy",
    displayName: "Logical Accuracy",
    description: "Validates SQL logic: JOINs, WHERE, GROUP BY, ORDER BY.",
    detailedDescription:
      "LLM judge comparing expected vs. generated SQL for logical correctness. Same result_matched override as schema_accuracy. Failure types target specific SQL clauses.",
    method: "LLM",
    threshold: 90,
    category: "logic",
    failureTypes: [
      "wrong_aggregation",
      "wrong_filter",
      "wrong_groupby",
      "wrong_orderby",
    ],
    overrideRules: ["result_matched → force PASS"],
    asiFields: ["failure_type", "wrong_clause", "blame_set", "counterfactual_fix"],
    promptSnippet:
      "You are a SQL logic expert. Determine if the GENERATED SQL applies correct aggregations, filters, GROUP BY, ORDER BY, and WHERE clauses.",
  },
  {
    number: 4,
    name: "semantic_equivalence",
    displayName: "Semantic Equivalence",
    description:
      "Checks if both SQLs answer the same business question.",
    detailedDescription:
      "LLM judge with SQL diff analysis. Extra override: row_cap_hit — when Genie hits the 5000-row cap and ground truth has more rows → force PASS (Genie's answer is correct, just truncated).",
    method: "LLM",
    threshold: 90,
    category: "logic",
    failureTypes: ["different_metric", "different_grain", "different_scope"],
    overrideRules: ["result_matched → force PASS", "row_cap_hit → force PASS"],
    asiFields: ["failure_type", "blame_set", "counterfactual_fix"],
    promptSnippet:
      "You are a SQL semantics expert. Determine if the two SQL queries measure the SAME business metric at the SAME grain.",
  },
  {
    number: 5,
    name: "completeness",
    displayName: "Completeness",
    description:
      "Ensures the response includes all required facts and columns.",
    detailedDescription:
      "LLM judge checking whether the generated SQL fully answers the user's question without missing dimensions, measures, or filters. Both result_match and row_cap overrides apply.",
    method: "LLM",
    threshold: 90,
    category: "logic",
    failureTypes: [
      "missing_column",
      "missing_filter",
      "missing_temporal_filter",
      "missing_aggregation",
      "partial_answer",
    ],
    overrideRules: ["result_matched → force PASS", "row_cap_hit → force PASS"],
    asiFields: ["failure_type", "blame_set", "counterfactual_fix"],
    promptSnippet:
      "You are a SQL completeness expert. Determine if the generated SQL fully answers the user's question without missing dimensions, measures, or filters.",
  },
  {
    number: 6,
    name: "response_quality",
    displayName: "Response Quality",
    description: "Evaluates the natural language response quality.",
    detailedDescription:
      "Informational-only judge (0% threshold — never blocks). If no analysis_text is present → 'unknown' (never penalize). Evaluates accuracy of descriptions, correctness vs. question, and whether claims are supported by data.",
    method: "LLM",
    threshold: 0,
    category: "quality",
    failureTypes: [
      "inaccurate_description",
      "unsupported_claim",
      "misleading_summary",
    ],
    overrideRules: ["No analysis_text → 'unknown' (no penalty)"],
    asiFields: ["failure_type", "counterfactual_fix"],
  },
  {
    number: 7,
    name: "result_correctness",
    displayName: "Result Correctness",
    description:
      "Compares execution results via hash — do both SQLs produce the same data?",
    detailedDescription:
      "Deterministic hash comparison of query results from the predict function. Special cases: both 0 rows → PASS; Genie rows = 5000 and GT > 5000 → PASS (row cap); GT infra error → 'excluded'. Does NOT re-execute SQL — uses precomputed comparison.",
    method: "CODE",
    threshold: 85,
    category: "execution",
    failureTypes: ["wrong_aggregation"],
    overrideRules: [
      "both 0 rows → PASS",
      "row_cap (5000) → PASS",
      "GT infra error → excluded",
    ],
    asiFields: ["failure_type", "expected_value", "actual_value"],
  },
  {
    number: 8,
    name: "asset_routing",
    displayName: "Asset Routing",
    description:
      "Checks if Genie used the correct asset type (TABLE, MV, or TVF).",
    detailedDescription:
      "Deterministic comparison: detect_asset_type(genie_sql) vs. expected_asset. Override: if execution results matched → PASS. Failure type is always 'asset_routing_error' with blame_set pointing to the expected asset type.",
    method: "CODE",
    threshold: 95,
    category: "routing",
    failureTypes: ["asset_routing_error"],
    overrideRules: ["result_matched → force PASS"],
    asiFields: ["failure_type", "blame_set", "counterfactual_fix"],
  },
  {
    number: 9,
    name: "arbiter",
    displayName: "Arbiter",
    description:
      "Final synthesizer — resolves disagreements and triggers corrections.",
    detailedDescription:
      "Conditional LLM judge, only triggered when result_correctness reports a mismatch. Verdicts: genie_correct (Genie is right, benchmark is wrong), ground_truth_correct, both_correct, neither_correct. If results match → both_correct (no LLM call). Default on LLM error: ground_truth_correct. Drives benchmark auto-correction and quarantine.",
    method: "CONDITIONAL_LLM",
    threshold: -1,
    category: "arbiter",
    failureTypes: [],
    overrideRules: ["result_matched → both_correct (skip LLM)"],
    asiFields: ["failure_type", "blame_set", "counterfactual_fix"],
    promptSnippet:
      "You are a senior SQL arbiter. Compare the two SQL queries and determine: genie_correct, ground_truth_correct, both_correct, or neither_correct.",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   OPTIMIZATION LEVERS
   ═══════════════════════════════════════════════════════════════════ */

export const LEVERS: Lever[] = [
  {
    number: 1,
    name: "Tables & Columns",
    description:
      "Optimizes table descriptions, column descriptions, column synonyms, and column visibility. Uses LEVER_1_2_COLUMN_PROMPT to generate structured metadata changes targeting wrong column/table selection failures.",
    patchTypes: [
      "update_description",
      "update_column_description",
      "add_column_synonym",
    ],
    failureTypes: [
      "wrong_column",
      "wrong_table",
      "description_mismatch",
      "missing_synonym",
    ],
    ownedSections: [
      "purpose",
      "best_for",
      "grain",
      "scd",
      "definition",
      "values",
      "synonyms",
    ],
  },
  {
    number: 2,
    name: "Metric Views",
    description:
      "Optimizes metric view column descriptions, aggregation hints, filter annotations, and MEASURE() syntax guidance. Shares the LEVER_1_2_COLUMN_PROMPT with Lever 1 but focuses on MV-specific context.",
    patchTypes: ["update_column_description"],
    failureTypes: [
      "wrong_aggregation",
      "wrong_measure",
      "missing_filter",
      "wrong_filter_condition",
    ],
    ownedSections: [
      "definition",
      "values",
      "aggregation",
      "grain_note",
      "important_filters",
      "synonyms",
    ],
  },
  {
    number: 3,
    name: "Table-Valued Functions",
    description:
      "Optimizes TVF descriptions, parameter documentation, routing guidance, and example SQL queries for TVF routing. Can propose removal of persistently failing TVFs after TVF_REMOVAL_MIN_ITERATIONS (2) consecutive failures.",
    patchTypes: ["add_tvf_parameter", "remove_tvf", "add_example_sql", "update_example_sql"],
    failureTypes: ["tvf_parameter_error", "asset_routing_error"],
    ownedSections: [
      "purpose",
      "best_for",
      "use_instead_of",
      "parameters",
      "example",
    ],
  },
  {
    number: 4,
    name: "Join Specifications",
    description:
      "Optimizes join specs between tables using LEVER_4_JOIN_SPEC_PROMPT. Analyzes SQL diffs for missing/incorrect JOINs, validates column type compatibility, and generates Genie-format join specs.",
    patchTypes: ["add_join_spec", "update_join_spec"],
    failureTypes: ["wrong_join", "missing_join_spec", "wrong_join_spec"],
    ownedSections: ["relationships", "join"],
  },
  {
    number: 5,
    name: "Instructions & Examples",
    description:
      "Optimizes free-text instructions that guide Genie's SQL generation and example SQL queries. Budget-aware — respects instruction character limits. Addresses routing errors, ambiguous question handling, and temporal filter rules.",
    patchTypes: [
      "add_instruction",
      "update_instruction",
      "remove_instruction",
      "rewrite_instruction",
      "add_example_sql",
      "update_example_sql",
      "remove_example_sql",
    ],
    failureTypes: [
      "asset_routing_error",
      "missing_instruction",
      "ambiguous_question",
    ],
    ownedSections: [],
  },
  {
    number: 6,
    name: "SQL Expressions",
    description:
      "Adds reusable SQL expressions (measures, filters, dimensions) that define business concepts like KPIs, common conditions, and grouping attributes. These structured definitions teach Genie about business terms and do not count toward the instruction budget.",
    patchTypes: [
      "add_sql_snippet_measure",
      "update_sql_snippet_measure",
      "remove_sql_snippet_measure",
      "add_sql_snippet_filter",
      "update_sql_snippet_filter",
      "remove_sql_snippet_filter",
      "add_sql_snippet_expression",
      "update_sql_snippet_expression",
      "remove_sql_snippet_expression",
    ],
    failureTypes: [
      "wrong_aggregation",
      "wrong_measure",
      "missing_filter",
      "missing_dimension",
      "wrong_grouping",
      "ambiguous_question",
    ],
    ownedSections: ["AGGREGATION RULES", "QUERY PATTERNS"],
  },
];

/* ═══════════════════════════════════════════════════════════════════
   FAILURE TAXONOMY (22 types)
   ═══════════════════════════════════════════════════════════════════ */

export const FAILURE_TAXONOMY: FailureType[] = [
  { name: "wrong_column", lever: 1, description: "Genie selected the wrong column" },
  { name: "wrong_table", lever: 1, description: "Genie used the wrong table" },
  { name: "description_mismatch", lever: 1, description: "Column/table description is misleading" },
  { name: "missing_synonym", lever: 1, description: "Column name not recognized — needs a synonym" },
  { name: "wrong_aggregation", lever: 2, description: "Incorrect aggregation function (SUM vs COUNT, etc.)" },
  { name: "wrong_measure", lever: 2, description: "Wrong MEASURE() in metric view" },
  { name: "missing_filter", lever: 2, description: "Missing WHERE clause that should be present" },
  { name: "wrong_filter_condition", lever: 2, description: "Incorrect filter value or operator" },
  { name: "tvf_parameter_error", lever: 3, description: "Wrong parameter passed to a table-valued function" },
  { name: "wrong_join", lever: 4, description: "Incorrect JOIN condition between tables" },
  { name: "missing_join_spec", lever: 4, description: "Join specification not defined for a needed relationship" },
  { name: "wrong_join_spec", lever: 4, description: "Existing join specification is incorrect" },
  { name: "asset_routing_error", lever: 5, description: "Genie used the wrong asset type (TABLE vs MV vs TVF)" },
  { name: "missing_instruction", lever: 5, description: "No routing instruction for this scenario" },
  { name: "ambiguous_question", lever: 5, description: "Question interpretation error — needs disambiguation" },
  { name: "format_difference", lever: 0, description: "Cosmetic difference only (not actionable)" },
  { name: "extra_columns_only", lever: 0, description: "Extra columns in output (not actionable)" },
  { name: "select_star", lever: 0, description: "SELECT * vs specific columns (not actionable)" },
  { name: "missing_scd_filter", lever: 1, description: "Missing slowly-changing dimension filter" },
  { name: "wrong_join_type", lever: 4, description: "Wrong JOIN type (INNER vs LEFT, etc.)" },
  { name: "wrong_orderby", lever: 2, description: "Incorrect ORDER BY clause" },
  { name: "missing_temporal_filter", lever: 5, description: "Missing date/time filter" },
];

/* ═══════════════════════════════════════════════════════════════════
   RUN STATUSES & STATE MACHINE
   ═══════════════════════════════════════════════════════════════════ */

export const RUN_STATUSES: RunStatus[] = [
  {
    name: "QUEUED",
    color: "yellow",
    description: "Run created, waiting for job to start",
    transitions: [{ target: "IN_PROGRESS", label: "job starts" }],
  },
  {
    name: "IN_PROGRESS",
    color: "yellow",
    description: "Job is actively running the pipeline",
    transitions: [
      { target: "CONVERGED", label: "all thresholds met" },
      { target: "STALLED", label: "no improvement" },
      { target: "MAX_ITERATIONS", label: "iteration limit" },
      { target: "FAILED", label: "error in any stage" },
      { target: "CANCELLED", label: "job cancelled" },
    ],
  },
  {
    name: "CONVERGED",
    color: "green",
    description: "All quality thresholds met",
    transitions: [
      { target: "APPLIED", label: "user clicks Apply" },
      { target: "DISCARDED", label: "user clicks Discard" },
    ],
  },
  {
    name: "STALLED",
    color: "gray",
    description: "No further improvement possible",
    transitions: [
      { target: "APPLIED", label: "user clicks Apply" },
      { target: "DISCARDED", label: "user clicks Discard" },
    ],
  },
  {
    name: "MAX_ITERATIONS",
    color: "gray",
    description: "Reached iteration limit without convergence",
    transitions: [
      { target: "APPLIED", label: "user clicks Apply" },
      { target: "DISCARDED", label: "user clicks Discard" },
    ],
  },
  {
    name: "FAILED",
    color: "red",
    description: "Error in pipeline execution",
    transitions: [],
  },
  {
    name: "CANCELLED",
    color: "gray",
    description: "Job was cancelled externally",
    transitions: [],
  },
  {
    name: "APPLIED",
    color: "green",
    description: "Optimized config applied to Genie Space",
    transitions: [],
  },
  {
    name: "DISCARDED",
    color: "gray",
    description: "Optimization results discarded, original config restored",
    transitions: [],
  },
];

/* ═══════════════════════════════════════════════════════════════════
   SAFETY CONSTANTS
   ═══════════════════════════════════════════════════════════════════ */

export const SAFETY_CONSTANTS: SafetyConstant[] = [
  {
    name: "REGRESSION_THRESHOLD",
    value: "5.0%",
    description:
      "Maximum allowed accuracy regression per judge before triggering a rollback. If any judge's score drops by more than this, the iteration is rolled back.",
  },
  {
    name: "CONSECUTIVE_ROLLBACK_LIMIT",
    value: "2",
    description:
      "Maximum consecutive rollbacks before the loop is stopped as STALLED. Prevents infinite retry cycles.",
  },
  {
    name: "PLATEAU_ITERATIONS",
    value: "2",
    description:
      "Window for plateau detection. If no meaningful improvement for this many iterations, the run is marked STALLED.",
  },
  {
    name: "MAX_NOISE_FLOOR",
    value: "5.0%",
    description:
      "Maximum noise floor for score comparison. Scores within this range of each other may be due to evaluation variance rather than real change.",
  },
  {
    name: "SLICE_GATE_TOLERANCE",
    value: "15.0%",
    description:
      "Regression tolerance for the slice gate (the fast first gate). Higher tolerance because only a subset of questions is evaluated.",
  },
  {
    name: "DIMINISHING_RETURNS_EPSILON",
    value: "2.0%",
    description:
      "Minimum accuracy improvement per iteration to be considered meaningful. Below this, the iteration counts toward diminishing returns.",
  },
  {
    name: "DIMINISHING_RETURNS_LOOKBACK",
    value: "2",
    description:
      "Number of recent iterations to check for diminishing returns. If all are below epsilon, the loop stops.",
  },
  {
    name: "MAX_ITERATIONS",
    value: "5",
    description:
      "Hard cap on the number of lever loop iterations. The run stops as MAX_ITERATIONS if this is reached.",
  },
  {
    name: "GENIE_CORRECT_CONFIRMATION",
    value: "2 evals",
    description:
      "Number of independent evaluations where the arbiter must say 'genie_correct' before auto-correcting a benchmark's expected SQL.",
  },
  {
    name: "QUARANTINE_THRESHOLD",
    value: "3 consecutive",
    description:
      "Number of consecutive 'neither_correct' verdicts before a benchmark question is quarantined (excluded from accuracy).",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   DATA FLOW NODES
   ═══════════════════════════════════════════════════════════════════ */

export const DATA_FLOW_NODES: DataFlowNode[] = [
  {
    id: "genie-api",
    label: "Genie Space API",
    description: "Source and target for space configuration — tables, columns, instructions, join specs",
    type: "input",
  },
  {
    id: "uc-metadata",
    label: "Unity Catalog",
    description: "Column types, tags, foreign keys, routines, data profiles",
    type: "input",
  },
  {
    id: "benchmarks",
    label: "Benchmarks",
    deltaTable: "genie_benchmarks_{domain}",
    keyColumns: ["question", "expected_sql", "expected_asset", "category", "split", "validation_status"],
    description: "LLM-generated and curated (question, expected_sql) pairs",
    type: "storage",
  },
  {
    id: "iterations",
    label: "Evaluations",
    deltaTable: "genie_opt_iterations",
    keyColumns: ["run_id", "iteration", "lever", "eval_scope", "overall_accuracy", "scores_json"],
    description: "Per-iteration evaluation results with per-judge scores",
    type: "storage",
  },
  {
    id: "asi-results",
    label: "ASI Results",
    deltaTable: "genie_eval_asi_results",
    keyColumns: ["question_id", "judge", "failure_type", "severity", "blame_set", "counterfactual_fix"],
    description: "Actionable Side Information — structured judge feedback per question",
    type: "storage",
  },
  {
    id: "provenance",
    label: "Provenance",
    deltaTable: "genie_opt_provenance",
    keyColumns: ["question_id", "judge", "resolved_root_cause", "cluster_id", "proposal_id", "gate_result"],
    description: "End-to-end chain linking patches to originating judge verdicts",
    type: "storage",
  },
  {
    id: "patches",
    label: "Patches",
    deltaTable: "genie_opt_patches",
    keyColumns: ["run_id", "iteration", "lever", "patch_type", "target_object", "rolled_back"],
    description: "Audit trail of every metadata change with rollback commands",
    type: "storage",
  },
  {
    id: "runs",
    label: "Runs",
    deltaTable: "genie_opt_runs",
    keyColumns: ["run_id", "space_id", "status", "best_accuracy", "convergence_reason"],
    description: "One row per optimization attempt with full lifecycle tracking",
    type: "storage",
  },
  {
    id: "stages",
    label: "Stages",
    deltaTable: "genie_opt_stages",
    keyColumns: ["run_id", "task_key", "stage", "status", "lever", "duration_seconds"],
    description: "Timeline of stage transitions with duration tracking",
    type: "storage",
  },
  {
    id: "suggestions",
    label: "Suggestions",
    deltaTable: "genie_opt_suggestions",
    keyColumns: ["suggestion_id", "type", "title", "rationale", "status"],
    description: "METRIC_VIEW and FUNCTION improvement proposals for human review",
    type: "storage",
  },
  {
    id: "flagged",
    label: "Flagged Questions",
    deltaTable: "genie_opt_flagged_questions",
    keyColumns: ["question_id", "question_text", "flag_reason", "iterations_failed"],
    description: "Persistent failures routed for human review",
    type: "storage",
  },
  {
    id: "queued-patches",
    label: "Queued Patches",
    deltaTable: "genie_opt_queued_patches",
    keyColumns: ["run_id", "iteration", "patch_type", "target_identifier", "confidence_tier", "status"],
    description: "High-risk patches pending human approval",
    type: "storage",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   PREFLIGHT STEPS
   ═══════════════════════════════════════════════════════════════════ */

export const PREFLIGHT_STEPS: PreflightStep[] = [
  {
    id: "fetch-config",
    title: "Fetch Space Config",
    icon: Globe,
    description:
      "Fetches current Genie Space configuration (tables, metric views, TVFs, join specs, instructions) and stores it as config_snapshot — the rollback baseline.",
  },
  {
    id: "collect-metadata",
    title: "Collect UC Metadata & Profile",
    icon: Database,
    description:
      "Collects column names/types/descriptions, tags, routines, FK constraints. Profiles data via TABLESAMPLE: row counts, cardinality, min/max ranges, and COLLECT_SET for low-cardinality columns (≤20 distinct values).",
  },
  {
    id: "generate-benchmarks",
    title: "Generate Benchmarks",
    icon: BrainCircuit,
    description:
      "Generates benchmark (question, expected_sql) pairs via LLM with data profile context, column allowlist, and asset metadata. Targets 20 benchmarks across 8 categories. Fills coverage gaps for uncovered assets.",
  },
  {
    id: "validate-benchmarks",
    title: "Validate Benchmarks",
    icon: CheckCircle2,
    description:
      "4-phase validation: EXPLAIN (syntax), table existence (SELECT LIMIT 0), execution sanity (LIMIT 1), and LLM alignment check (EXTRA_FILTER, EXTRA_COLUMNS, MISSING_AGGREGATION, WRONG_INTERPRETATION).",
  },
  {
    id: "load-feedback",
    title: "Load Human Feedback",
    icon: UserCheck,
    description:
      "Loads corrections from prior labeling sessions: benchmark SQL fixes (genie_correct verdicts), judge overrides, and quarantines (excluding questions from accuracy).",
  },
  {
    id: "setup-experiment",
    title: "Setup MLflow Experiment",
    icon: FlaskConical,
    description:
      "Creates or resolves the MLflow experiment, creates the evaluation dataset in Unity Catalog, and registers the initial instruction version as an MLflow prompt.",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   ENRICHMENT SUBSTAGES
   ═══════════════════════════════════════════════════════════════════ */

export const ENRICHMENT_SUBSTAGES: EnrichmentSubstage[] = [
  {
    id: "config-prep",
    title: "Config Preparation",
    icon: Settings2,
    description: "Load config, fetch UC columns, annotate with data types",
    details: [
      "Load config from Delta snapshot or Genie API",
      "Fetch UC columns via REST API",
      "Annotate columns with UC data types",
    ],
  },
  {
    id: "prompt-matching",
    title: "Prompt Matching",
    icon: Zap,
    description:
      "Enable format assistance and entity matching for better SQL generation",
    details: [
      "Format Assistance: adds data-type hints to help Genie generate valid SQL",
      "Entity Matching: builds value dictionaries so Genie maps phrases to column values",
      "Propagation wait: 30s normal, 90s if entity matching changed",
    ],
  },
  {
    id: "column-descriptions",
    title: "Column Descriptions",
    icon: PenLine,
    description:
      "Fill blank column descriptions with LLM-generated structured metadata",
    details: [
      "Blank detection: both Genie description AND UC comment < 10 chars",
      "Column classification: dimension / measure / key",
      "Sections by type: dim→(definition, values, synonyms), measure→(definition, aggregation, grain_note, synonyms), key→(definition, join, synonyms)",
      "Batching: ≤30 columns = single LLM call, >30 = per-table batches",
    ],
  },
  {
    id: "table-descriptions",
    title: "Table Descriptions",
    icon: Table2,
    description: "Generate structured descriptions for undocumented tables",
    details: [
      "Sections: PURPOSE, BEST_FOR, GRAIN, SCD, RELATIONSHIPS",
      "Tables with < 10 chars of description get LLM-generated descriptions",
    ],
  },
  {
    id: "join-discovery",
    title: "Join Discovery",
    icon: Link2,
    description: "Discover join specifications from FK constraints and baseline queries",
    details: [
      "Tier 0: FK constraints from Unity Catalog",
      "Tier 1: Execution-proven JOINs parsed from baseline queries",
      "Type corroboration validates column compatibility",
      "Relationship annotation (MANY_TO_ONE for fact→dim)",
    ],
  },
  {
    id: "space-metadata",
    title: "Space Metadata",
    icon: BookOpen,
    description: "Generate space description and sample questions if missing",
    details: [
      "Space description: 150-300 words covering data coverage, analytics, use cases",
      "Sample questions: generated via LLM, wrapped with Genie IDs",
    ],
  },
  {
    id: "instruction-seeding",
    title: "Instruction Seeding",
    icon: MessageSquare,
    description:
      "Generate routing instructions if the space has none (< 50 chars)",
    details: [
      "Sections: PURPOSE, ASSET ROUTING, TEMPORAL FILTERS, DATA QUALITY NOTES, JOIN GUIDANCE",
      "Budget: 500-1500 characters, no Markdown",
    ],
  },
  {
    id: "example-sql",
    title: "Example SQL Mining",
    icon: Code,
    description: "Extract validated SQL from benchmarks as example queries",
    details: [
      "Similarity filter: skip if n-gram Jaccard > 0.85 vs existing examples",
      "Validation: EXPLAIN + execution must return rows",
      "Respects instruction slot budget",
    ],
  },
  {
    id: "model-snapshot",
    title: "LoggedModel Snapshot",
    icon: Camera,
    description: "Save enriched config as MLflow model at iteration=-1",
    details: [
      "Captures space_config.json and metadata_snapshot.json",
      "Tagged with domain, space_id, iteration=-1",
      "Serves as 'before lever loop' checkpoint",
    ],
  },
];

/* ═══════════════════════════════════════════════════════════════════
   3-GATE DEFINITIONS
   ═══════════════════════════════════════════════════════════════════ */

export const GATE_DEFINITIONS: GateDefinition[] = [
  {
    name: "Slice Gate",
    questionSelection:
      "Only benchmarks referencing patched objects + explicitly affected questions",
    passCriteria: "No regression per judge exceeding tolerance",
    tolerance:
      "max(SLICE_GATE_TOLERANCE=15%, noise_floor + 2%, question_weight + 0.5%)",
    description:
      "Fast first check on the subset of questions directly affected by the patches. Skipped if the slice would cover >50% of all benchmarks (too broad to be useful as a fast-fail). Optional — controlled by ENABLE_SLICE_GATE.",
  },
  {
    name: "P0 Gate",
    questionSelection: "Only priority='P0' benchmarks (typically top 3)",
    passCriteria: "Zero P0 failures",
    tolerance: "N/A — any P0 failure triggers rollback",
    description:
      "Quick regression check on the highest-priority questions. If any P0 question fails, the iteration is immediately rolled back without running the expensive full evaluation.",
  },
  {
    name: "Full Gate",
    questionSelection: "All benchmarks",
    passCriteria:
      "No per-judge regression > threshold AND overall accuracy maintained",
    tolerance: "max(REGRESSION_THRESHOLD=5%, noise_floor)",
    description:
      "The authoritative evaluation. If accuracy did NOT improve, a confirmation eval (2nd pass) is run and scores are averaged to reduce variance. Even with no per-judge regressions, if overall accuracy decreased, the iteration can be rolled back (accuracy guard).",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   CONVERGENCE CONDITIONS
   ═══════════════════════════════════════════════════════════════════ */

export const CONVERGENCE_CONDITIONS: ConvergenceCondition[] = [
  {
    name: "Thresholds Met",
    resultStatus: "CONVERGED",
    formula: "all_thresholds_met(best_scores)",
    description:
      "All 8 scored judges meet their quality thresholds simultaneously. The optimization achieved its goal.",
  },
  {
    name: "Diminishing Returns",
    resultStatus: "STALLED",
    formula:
      "Last 2 non-escalation iterations all had accuracy_delta < 2.0%",
    description:
      "The optimizer is making changes but they aren't producing meaningful accuracy improvements.",
  },
  {
    name: "Consecutive Rollbacks",
    resultStatus: "STALLED",
    formula: "≥2 consecutive iterations rolled back",
    description:
      "Every recent attempt made things worse. The optimizer cannot find a productive direction.",
  },
  {
    name: "Max Iterations",
    resultStatus: "MAX_ITERATIONS",
    formula: "iteration_count ≥ MAX_ITERATIONS (5)",
    description:
      "Hard safety cap reached. The optimizer may have improved quality but didn't fully converge.",
  },
  {
    name: "No Actionable Clusters",
    resultStatus: "STALLED",
    formula: "All failure clusters already tried or empty",
    description:
      "All known failure patterns have been attempted. No new strategies available.",
  },
  {
    name: "Empty Strategy",
    resultStatus: "STALLED",
    formula: "Strategist returns 0 action groups",
    description:
      "The LLM strategist could not find any actionable improvement given the current state.",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   PERSISTENT FAILURE CLASSIFICATIONS
   ═══════════════════════════════════════════════════════════════════ */

export const PERSISTENT_FAILURE_CLASSIFICATIONS: PersistentFailureClassification[] =
  [
    {
      name: "INTERMITTENT",
      condition: "max_consecutive failures < 2",
      color: "amber",
      meaning: "Fails sometimes but not consistently — may resolve with different strategies",
    },
    {
      name: "PERSISTENT",
      condition:
        "max_consecutive failures ≥ 2 AND additive levers NOT exhausted",
      color: "orange",
      meaning:
        "Keeps failing consistently, but add_instruction and add_example_sql haven't been fully tried yet",
    },
    {
      name: "ADDITIVE_LEVERS_EXHAUSTED",
      condition:
        "max_consecutive failures ≥ 2 AND both add_instruction + add_example_sql tried ≥ 2 times each",
      color: "red",
      meaning:
        "All automated fix strategies exhausted — requires human review to resolve",
    },
  ];

/* ═══════════════════════════════════════════════════════════════════
   ESCALATION TYPES
   ═══════════════════════════════════════════════════════════════════ */

export const ESCALATION_TYPES: EscalationType[] = [
  {
    type: "remove_tvf",
    trigger: "TVF fails for ≥2 consecutive iterations",
    action:
      "Remove TVF from Genie Space config. Confidence tiers: auto_apply (just do it), apply_and_flag (apply + flag for review), flagged_only (don't apply, just flag).",
  },
  {
    type: "gt_repair",
    trigger: "Strategist believes benchmark SQL is wrong",
    action:
      "Trigger arbiter corrections with force_adopt_qids to bypass the 2-iteration confirmation threshold.",
  },
  {
    type: "flag_for_review",
    trigger: "Strategist cannot determine an automated fix",
    action:
      "Write affected questions to genie_opt_flagged_questions for human review in the labeling session.",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   REVIEW SESSION SCHEMA
   ═══════════════════════════════════════════════════════════════════ */

export const REVIEW_SESSION_FIELDS: ReviewField[] = [
  {
    name: "judge_verdict",
    type: "Feedback (categorical)",
    label: "Is this question truly failing?",
    options: [
      "Yes — Genie answer is wrong",
      "No — Genie answer is actually correct",
      "Benchmark is wrong — expected SQL needs fixing",
      "Ambiguous — question is unclear",
    ],
  },
  {
    name: "corrected_sql",
    type: "Expectation (text)",
    label: "Provide the correct expected SQL (if benchmark is wrong)",
  },
  {
    name: "improvements",
    type: "Expectation (text)",
    label: "Suggest improvements for this Genie Space",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   HIGH-RISK PATCH TYPES
   ═══════════════════════════════════════════════════════════════════ */

export const HIGH_RISK_PATCH_TYPES = [
  "add_table",
  "remove_table",
  "update_tvf_sql",
  "remove_tvf",
  "update_mv_yaml",
] as const;

/* ═══════════════════════════════════════════════════════════════════
   CLUSTER IMPACT WEIGHTS
   ═══════════════════════════════════════════════════════════════════ */

export const CLUSTER_IMPACT_WEIGHTS: ClusterImpactWeights = {
  causal: {
    syntax_validity: 5.0,
    schema_accuracy: 4.0,
    asset_routing: 3.5,
    logical_accuracy: 3.0,
    semantic_equivalence: 2.0,
    completeness: 1.5,
    result_correctness: 1.0,
    response_quality: 0.5,
  },
  severity: {
    wrong_table: 1.0,
    wrong_column: 0.9,
    wrong_join: 0.9,
    missing_join_spec: 0.85,
    wrong_join_spec: 0.85,
    wrong_aggregation: 0.8,
    wrong_measure: 0.8,
    asset_routing_error: 0.9,
    missing_filter: 0.7,
    missing_temporal_filter: 0.7,
    tvf_parameter_error: 0.7,
    missing_instruction: 0.6,
    compliance_violation: 0.5,
    repeatability_issue: 0.4,
    description_mismatch: 0.4,
    missing_synonym: 0.3,
    ambiguous_question: 0.3,
    stale_data: 0.3,
    data_freshness: 0.3,
    missing_format_assistance: 0.3,
    missing_entity_matching: 0.3,
  },
  fixability: {
    withCounterfactual: 1.0,
    without: 0.4,
  },
};

/* ═══════════════════════════════════════════════════════════════════
   STRUCTURED METADATA SECTIONS
   ═══════════════════════════════════════════════════════════════════ */

export const STRUCTURED_METADATA_SECTIONS = [
  "purpose",
  "best_for",
  "grain",
  "scd",
  "relationships",
  "definition",
  "values",
  "synonyms",
  "aggregation",
  "grain_note",
  "join",
  "important_filters",
  "use_instead_of",
  "parameters",
  "example",
] as const;

export const ENTITY_TYPE_SECTIONS: Record<string, string[]> = {
  table: ["purpose", "best_for", "grain", "scd", "relationships"],
  column_dim: ["definition", "values", "synonyms"],
  column_measure: ["definition", "aggregation", "grain_note", "synonyms"],
  column_key: ["definition", "join", "synonyms"],
  function: ["purpose", "best_for", "use_instead_of", "parameters", "example"],
  mv_table: ["purpose", "best_for", "grain", "important_filters"],
};

export const LEVER_SECTION_OWNERSHIP: Record<number, string[]> = {
  0: [...STRUCTURED_METADATA_SECTIONS],
  1: ["purpose", "best_for", "grain", "scd", "definition", "values", "synonyms"],
  2: ["definition", "values", "aggregation", "grain_note", "important_filters", "synonyms"],
  3: ["purpose", "best_for", "use_instead_of", "parameters", "example"],
  4: ["relationships", "join"],
  5: [],
};

/* ═══════════════════════════════════════════════════════════════════
   FEEDBACK INGESTION STEPS
   ═══════════════════════════════════════════════════════════════════ */

export const FEEDBACK_INGESTION_STEPS = [
  {
    step: 1,
    title: "Load Prior Session",
    description:
      "Preflight finds the latest completed run for the same space and loads its labeling session name.",
  },
  {
    step: 2,
    title: "Sync Corrections",
    description:
      "Calls session.sync(to_dataset) to propagate human corrections from the labeling session to the evaluation dataset.",
  },
  {
    step: 3,
    title: "Ingest Feedback",
    description:
      "Reads human labels → judge_override (genie_correct/benchmark_wrong/ambiguous), benchmark_correction (corrected SQL), improvement (suggestions).",
  },
  {
    step: 4,
    title: "Apply in Lever Loop",
    description:
      "Corrections applied via apply_benchmark_corrections() and quarantines via quarantine_benchmark_question() before evaluation begins.",
  },
];

/* ═══════════════════════════════════════════════════════════════════
   BENCHMARK DATA MODEL (for display)
   ═══════════════════════════════════════════════════════════════════ */

export const BENCHMARK_MODEL_FIELDS = [
  { name: "id", type: "string", example: '"domain_gs_001"' },
  { name: "question", type: "string", example: '"What is total revenue by region?"' },
  { name: "expected_sql", type: "string", example: '"SELECT region, SUM(revenue) …"' },
  { name: "expected_asset", type: "enum", example: '"TABLE" | "MV" | "TVF"' },
  { name: "category", type: "enum", example: '"aggregation" | "ranking" | "time_series" | …' },
  { name: "required_tables", type: "string[]", example: '["sales_fact"]' },
  { name: "required_columns", type: "string[]", example: '["region", "revenue"]' },
  { name: "priority", type: "enum", example: '"P0" | "P1"' },
  { name: "split", type: "enum", example: '"train" | "held_out"' },
  { name: "provenance", type: "enum", example: '"curated" | "synthetic" | "coverage_gap_fill"' },
  { name: "validation_status", type: "enum", example: '"valid" | "invalid"' },
];

export const ASI_MODEL_FIELDS = [
  { name: "failure_type", type: "string", example: '"wrong_column"' },
  { name: "severity", type: "enum", example: '"critical" | "major" | "minor"' },
  { name: "confidence", type: "number", example: "0.95" },
  { name: "wrong_clause", type: "string?", example: '"SELECT" | "WHERE" | "JOIN"' },
  { name: "blame_set", type: "string[]", example: '["sales_fact.total_revenue"]' },
  { name: "counterfactual_fix", type: "string?", example: '"Add synonym \'revenue\' to total_sales"' },
  { name: "ambiguity_detected", type: "boolean", example: "false" },
];

/* ═══════════════════════════════════════════════════════════════════
   PIPELINE GROUP COLORS
   ═══════════════════════════════════════════════════════════════════ */

export const PIPELINE_GROUP_COLORS: Record<
  string,
  { bg: string; border: string; dot: string; iconBg: string; iconText: string; ring: string; accent: string; fadedDot: string }
> = {
  neutral: { bg: "bg-slate-50/60", border: "border-l-slate-400", dot: "bg-slate-500", fadedDot: "bg-slate-200", iconBg: "bg-slate-100", iconText: "text-slate-600", ring: "ring-slate-300", accent: "text-slate-600" },
  preflight: { bg: "bg-blue-50/80", border: "border-l-blue-500", dot: "bg-blue-500", fadedDot: "bg-blue-200", iconBg: "bg-blue-100", iconText: "text-blue-700", ring: "ring-blue-300", accent: "text-blue-700" },
  baseline: { bg: "bg-purple-50/80", border: "border-l-purple-500", dot: "bg-purple-500", fadedDot: "bg-purple-200", iconBg: "bg-purple-100", iconText: "text-purple-700", ring: "ring-purple-300", accent: "text-purple-700" },
  enrichment: { bg: "bg-teal-50/80", border: "border-l-teal-500", dot: "bg-teal-500", fadedDot: "bg-teal-200", iconBg: "bg-teal-100", iconText: "text-teal-700", ring: "ring-teal-300", accent: "text-teal-700" },
  leverLoop: { bg: "bg-amber-50/80", border: "border-l-amber-500", dot: "bg-amber-500", fadedDot: "bg-amber-200", iconBg: "bg-amber-100", iconText: "text-amber-700", ring: "ring-amber-300", accent: "text-amber-700" },
  finalize: { bg: "bg-emerald-50/80", border: "border-l-emerald-500", dot: "bg-emerald-500", fadedDot: "bg-emerald-200", iconBg: "bg-emerald-100", iconText: "text-emerald-700", ring: "ring-emerald-300", accent: "text-emerald-700" },
};

/* ═══════════════════════════════════════════════════════════════════
   WALKTHROUGH STAGES (12 granular steps)
   ═══════════════════════════════════════════════════════════════════ */

export const WALKTHROUGH_STAGES: WalkthroughStage[] = [
  { id: "overview", title: "The Big Picture", subtitle: "How Genie Space Optimizer works end-to-end", pipelineGroup: "neutral", icon: Eye },
  { id: "preflight", title: "Preflight", subtitle: "Collect metadata, profile data, prepare the run", pipelineGroup: "preflight", icon: Shield },
  { id: "benchmarks", title: "Generating Benchmarks", subtitle: "Create and validate evaluation questions", pipelineGroup: "preflight", icon: Target },
  { id: "baseline", title: "Baseline Evaluation", subtitle: "Establish quality with 9 judges", pipelineGroup: "baseline", icon: BarChart3 },
  { id: "judges", title: "The 9 Judges", subtitle: "How each judge evaluates SQL quality", pipelineGroup: "baseline", icon: ShieldCheck },
  { id: "enrichment", title: "Enrichment", subtitle: "Proactively fill metadata gaps", pipelineGroup: "enrichment", icon: Sparkles },
  { id: "lever-loop", title: "The Optimization Loop", subtitle: "Iteratively improve via targeted patches", pipelineGroup: "leverLoop", icon: RefreshCw },
  { id: "failure-analysis", title: "Failure Analysis", subtitle: "Diagnose and cluster root causes", pipelineGroup: "leverLoop", icon: Filter },
  { id: "levers", title: "The 6 Levers", subtitle: "Targeted tools for specific problem types", pipelineGroup: "leverLoop", icon: Layers },
  { id: "three-gates", title: "The 3-Gate Pattern", subtitle: "Validate improvements, catch regressions", pipelineGroup: "leverLoop", icon: GitBranch },
  { id: "convergence", title: "Convergence & Safety", subtitle: "Know when to stop, prevent harm", pipelineGroup: "finalize", icon: TrendingUp },
  { id: "finalize", title: "Finalize & Deploy", subtitle: "Test, promote, and optionally deploy", pipelineGroup: "finalize", icon: CheckCheck },
];

/* ═══════════════════════════════════════════════════════════════════
   FICTIONAL EXAMPLE — "Revenue Analytics" space (used throughout walkthrough)
   ═══════════════════════════════════════════════════════════════════ */

export const FICTIONAL_EXAMPLE: FictionalExample = {
  spaceName: "Revenue Analytics",
  tables: [
    {
      fqn: "main.analytics.fact_sales",
      alias: "fact_sales",
      columns: [
        { name: "sale_id", type: "BIGINT" },
        { name: "product_id", type: "BIGINT" },
        { name: "date_key", type: "INT" },
        { name: "total_revenue_usd", type: "DECIMAL(18,2)" },
        { name: "quantity", type: "INT" },
        { name: "region", type: "STRING" },
      ],
    },
    {
      fqn: "main.analytics.dim_product",
      alias: "dim_product",
      columns: [
        { name: "product_id", type: "BIGINT" },
        { name: "product_name", type: "STRING" },
        { name: "category", type: "STRING" },
        { name: "subcategory", type: "STRING" },
        { name: "brand", type: "STRING" },
      ],
    },
    {
      fqn: "main.analytics.dim_date",
      alias: "dim_date",
      columns: [
        { name: "date_key", type: "INT" },
        { name: "calendar_date", type: "DATE" },
        { name: "month_name", type: "STRING" },
        { name: "quarter", type: "STRING" },
        { name: "fiscal_year", type: "INT" },
      ],
    },
  ],
  benchmark: {
    question: "What is the total revenue by product category?",
    expectedSql:
      "SELECT dp.category, SUM(fs.total_revenue_usd) AS total_revenue\\nFROM main.analytics.fact_sales fs\\nJOIN main.analytics.dim_product dp ON fs.product_id = dp.product_id\\nGROUP BY dp.category\\nORDER BY total_revenue DESC",
    category: "aggregation",
    expectedAsset: "TABLE",
  },
  failure: {
    type: "wrong_column",
    judgeMessage:
      'SQL uses "revenue" but the column is named "total_revenue_usd". The alias is not recognized without a synonym.',
    blameSet: ["fact_sales.total_revenue_usd"],
    counterfactualFix: 'Add synonym "revenue" to the total_revenue_usd column description',
  },
  patch: {
    actionType: "update_column_description",
    target: "fact_sales.total_revenue_usd",
    sections: {
      DEFINITION: "Total gross revenue in USD for the transaction",
      SYNONYMS: "revenue, total_sales, gross_revenue",
      AGGREGATION: "SUM for totals, AVG for per-unit analysis",
    },
  },
  cluster: {
    id: "C001",
    rootCause: "wrong_column",
    questionCount: 3,
    affectedQuestions: [
      "What is the total revenue by product category?",
      "Show revenue trends by quarter",
      "Compare revenue across regions",
    ],
  },
  enrichment: {
    table: "dim_product",
    column: "category",
    before: "",
    after: "VALUES:\nElectronics, Clothing, Home & Garden, Sports, Books\n\nSYNONYMS:\nproduct category, product type, product group",
    sections: {
      DEFINITION: "Product classification grouping",
      VALUES: "Electronics, Clothing, Home & Garden, Sports, Books",
      SYNONYMS: "product category, product type, product group",
    },
  },
};
