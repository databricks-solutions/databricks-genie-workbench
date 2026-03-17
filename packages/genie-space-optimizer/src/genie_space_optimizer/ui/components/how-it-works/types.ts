import type { LucideIcon } from "lucide-react";

export interface PipelineStage {
  id: string;
  title: string;
  icon: LucideIcon;
  summary: string;
  description: string;
}

export interface Judge {
  number: number;
  name: string;
  displayName: string;
  description: string;
  detailedDescription: string;
  method: "CODE" | "LLM" | "CONDITIONAL_LLM";
  threshold: number;
  category: JudgeCategory;
  failureTypes: string[];
  overrideRules: string[];
  asiFields: string[];
  promptSnippet?: string;
}

export type JudgeCategory =
  | "syntax"
  | "schema"
  | "logic"
  | "quality"
  | "execution"
  | "routing"
  | "arbiter";

export interface Lever {
  number: number;
  name: string;
  description: string;
  patchTypes: string[];
  failureTypes: string[];
  ownedSections: string[];
}

export interface FailureType {
  name: string;
  lever: number;
  description: string;
}

export interface RunStatus {
  name: string;
  color: "green" | "red" | "yellow" | "gray" | "blue";
  description: string;
  transitions: { target: string; label: string }[];
}

export interface SafetyConstant {
  name: string;
  value: string;
  description: string;
}

export interface DataFlowNode {
  id: string;
  label: string;
  deltaTable?: string;
  keyColumns?: string[];
  description: string;
  type: "input" | "storage" | "process";
}

export interface GateDefinition {
  name: string;
  questionSelection: string;
  passCriteria: string;
  tolerance: string;
  description: string;
}

export interface ConvergenceCondition {
  name: string;
  resultStatus: string;
  formula: string;
  description: string;
}

export interface EnrichmentSubstage {
  id: string;
  title: string;
  icon: LucideIcon;
  description: string;
  details: string[];
}

export interface PreflightStep {
  id: string;
  title: string;
  icon: LucideIcon;
  description: string;
}

export interface PersistentFailureClassification {
  name: string;
  condition: string;
  color: string;
  meaning: string;
}

export interface EscalationType {
  type: string;
  trigger: string;
  action: string;
}

export interface ReviewField {
  name: string;
  type: string;
  label: string;
  options?: string[];
}

export interface ClusterImpactWeights {
  causal: Record<string, number>;
  severity: Record<string, number>;
  fixability: { withCounterfactual: number; without: number };
}

export type PipelineGroup = "preflight" | "baseline" | "enrichment" | "leverLoop" | "finalize";

export interface WalkthroughStage {
  id: string;
  title: string;
  subtitle: string;
  pipelineGroup: PipelineGroup | "neutral";
  icon: LucideIcon;
}

export interface FictionalExample {
  spaceName: string;
  tables: { fqn: string; alias: string; columns: { name: string; type: string }[] }[];
  benchmark: { question: string; expectedSql: string; category: string; expectedAsset: string };
  failure: { type: string; judgeMessage: string; blameSet: string[]; counterfactualFix: string };
  patch: { actionType: string; target: string; sections: Record<string, string> };
  cluster: { id: string; rootCause: string; questionCount: number; affectedQuestions: string[] };
  enrichment: { table: string; column: string; before: string; after: string; sections: Record<string, string> };
}
