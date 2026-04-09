import { useState, useEffect, useCallback } from "react"
import {
  Sparkles,
  MessageSquarePlus,
  ShieldCheck,
  Wrench,
  Zap,
  Lock,
  Layers,
  ChevronLeft,
  ChevronRight,
  Search,
  Database,
  FileText,
  Settings,
  CheckCircle2,
  AlertTriangle,
  XCircle,
  Target,
  BarChart3,
  GitBranch,
  RefreshCw,
  ArrowRightLeft,
  Gauge,
  Box,
  Globe,
  HardDrive,
  Network,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { FeatureCard } from "@/components/how-it-works/FeatureCard"
import { StageCard } from "@/components/how-it-works/StageCard"
import { PipelineDiagram } from "@/components/how-it-works/PipelineDiagram"
import type { PipelineStep } from "@/components/how-it-works/PipelineDiagram"
import { PermissionDiagram } from "@/components/how-it-works/PermissionDiagram"

const ACCENT = "#4F46E5"
const CYAN = "#06B6D4"
const SUCCESS = "#10B981"
const WARNING = "#F59E0B"
const DANGER = "#EF4444"
const INFO = "#3B82F6"

interface Stage {
  id: string
  label: string
  icon: React.ReactNode
  color: string
}

const stages: Stage[] = [
  { id: "overview", label: "Overview", icon: <Sparkles className="h-4 w-4" />, color: ACCENT },
  { id: "create", label: "Create Agent", icon: <MessageSquarePlus className="h-4 w-4" />, color: CYAN },
  { id: "score", label: "IQ Scanner", icon: <ShieldCheck className="h-4 w-4" />, color: SUCCESS },
  { id: "fix", label: "Fix Agent", icon: <Wrench className="h-4 w-4" />, color: WARNING },
  { id: "optimize", label: "Auto-Optimize", icon: <Zap className="h-4 w-4" />, color: DANGER },
  { id: "permissions", label: "Permissions", icon: <Lock className="h-4 w-4" />, color: INFO },
  { id: "architecture", label: "Architecture", icon: <Layers className="h-4 w-4" />, color: ACCENT },
]

export function HowItWorks() {
  const [activeStage, setActiveStage] = useState(0)

  const goNext = useCallback(() => setActiveStage((s) => Math.min(s + 1, stages.length - 1)), [])
  const goPrev = useCallback(() => setActiveStage((s) => Math.max(s - 1, 0)), [])

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "ArrowRight") goNext()
      else if (e.key === "ArrowLeft") goPrev()
    }
    window.addEventListener("keydown", handleKey)
    return () => window.removeEventListener("keydown", handleKey)
  }, [goNext, goPrev])

  return (
    <div className="animate-fade-in">
      {/* Hero */}
      <div className="relative mb-8 rounded-2xl hero-mesh overflow-hidden border border-default">
        <div className="absolute inset-0 hero-grid pointer-events-none" />
        <div className="relative px-8 py-10 text-center">
          <div className="inline-flex items-center gap-2 rounded-full bg-accent/10 border border-accent/20 px-4 py-1.5 text-xs font-semibold text-accent mb-4">
            <Sparkles className="h-3.5 w-3.5" /> Interactive Guide
          </div>
          <h1 className="text-3xl md:text-4xl font-display font-bold text-primary mb-3">
            How <span className="text-gradient">Genie Workbench</span> Works
          </h1>
          <p className="text-secondary text-base max-w-2xl mx-auto leading-relaxed">
            Create, score, fix, and optimize your Genie Spaces — all from one intelligent interface.
            Explore each capability below.
          </p>
        </div>
      </div>

      {/* Stage navigation pills */}
      <div className="mb-8 flex flex-wrap justify-center gap-1.5">
        {stages.map((stage, i) => (
          <button
            key={stage.id}
            onClick={() => setActiveStage(i)}
            className={cn(
              "flex items-center gap-1.5 rounded-full px-3.5 py-2 text-xs font-medium transition-all duration-200",
              i === activeStage
                ? "shadow-md scale-105"
                : "bg-elevated text-muted hover:text-secondary hover:bg-surface border border-default",
            )}
            style={
              i === activeStage
                ? { background: `${stage.color}18`, color: stage.color, boxShadow: `0 4px 12px ${stage.color}25` }
                : undefined
            }
          >
            {stage.icon}
            <span className="hidden sm:inline">{stage.label}</span>
          </button>
        ))}
      </div>

      {/* Stage content */}
      <div key={stages[activeStage].id} className="animate-slide-up">
        {activeStage === 0 && <OverviewContent />}
        {activeStage === 1 && <CreateAgentContent />}
        {activeStage === 2 && <IQScannerContent />}
        {activeStage === 3 && <FixAgentContent />}
        {activeStage === 4 && <AutoOptimizeContent />}
        {activeStage === 5 && <PermissionsContent />}
        {activeStage === 6 && <ArchitectureContent />}
      </div>

      {/* Prev / Next navigation */}
      <div className="mt-8 flex items-center justify-between">
        <button
          onClick={goPrev}
          disabled={activeStage === 0}
          className={cn(
            "flex items-center gap-1.5 rounded-lg px-4 py-2 text-sm font-medium transition-colors",
            activeStage === 0
              ? "text-muted/40 cursor-not-allowed"
              : "text-secondary hover:text-primary hover:bg-elevated",
          )}
        >
          <ChevronLeft className="h-4 w-4" /> {activeStage > 0 ? stages[activeStage - 1].label : "Back"}
        </button>

        <span className="text-xs text-muted">
          {activeStage + 1} / {stages.length} &middot; arrow keys to navigate
        </span>

        <button
          onClick={activeStage === stages.length - 1 ? () => setActiveStage(0) : goNext}
          className="flex items-center gap-1.5 rounded-lg px-4 py-2 text-sm font-medium transition-colors text-secondary hover:text-primary hover:bg-elevated"
        >
          {activeStage < stages.length - 1 ? stages[activeStage + 1].label : "Back to Overview"} <ChevronRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  )
}

/* ================================================================
   STAGE 0 — Overview
   ================================================================ */
function OverviewContent() {
  return (
    <div className="space-y-6">
      <div className="text-center mb-2">
        <h2 className="text-xl font-display font-bold text-primary">Everything You Need for Genie Spaces</h2>
        <p className="text-sm text-muted mt-1">Four powerful capabilities, one streamlined workflow</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 animate-stagger">
        <FeatureCard
          icon={<MessageSquarePlus className="h-6 w-6" />}
          title="Create"
          description="Describe what you need in plain English. Our AI agent discovers your data, builds a plan, and creates a fully configured Genie Space."
          accentColor={CYAN}
          glowColor={`${CYAN}15`}
        />
        <FeatureCard
          icon={<ShieldCheck className="h-6 w-6" />}
          title="Score"
          description="Instantly scan any Genie Space against 12 quality checks. Get a maturity tier (Not Ready → Ready → Trusted) and actionable findings."
          accentColor={SUCCESS}
          glowColor={`${SUCCESS}15`}
        />
        <FeatureCard
          icon={<Wrench className="h-6 w-6" />}
          title="Fix"
          description="Turn scan findings into concrete fixes. The Fix Agent generates JSON patches and applies them directly — no manual editing required."
          accentColor={WARNING}
          glowColor={`${WARNING}15`}
        />
        <FeatureCard
          icon={<Zap className="h-6 w-6" />}
          title="Optimize"
          description="Run a full benchmark-driven optimization pipeline. Tests real questions, evaluates with 9 judges, and applies improvements automatically."
          accentColor={DANGER}
          glowColor={`${DANGER}15`}
        />
      </div>

      {/* End-to-end flow */}
      <StageCard title="End-to-End Workflow" icon={<ArrowRightLeft className="h-4 w-4" />}>
        <PipelineDiagram
          steps={[
            { icon: <MessageSquarePlus className="h-5 w-5" />, label: "Create", description: "AI builds your space", color: CYAN },
            { icon: <ShieldCheck className="h-5 w-5" />, label: "Score", description: "12-check quality scan", color: SUCCESS },
            { icon: <Wrench className="h-5 w-5" />, label: "Fix", description: "Auto-apply patches", color: WARNING },
            { icon: <Zap className="h-5 w-5" />, label: "Optimize", description: "Benchmark & refine", color: DANGER },
            { icon: <CheckCircle2 className="h-5 w-5" />, label: "Trusted", description: "Production-ready", color: SUCCESS },
          ]}
        />
      </StageCard>
    </div>
  )
}

/* ================================================================
   STAGE 1 — Create Agent
   ================================================================ */
function CreateAgentContent() {
  const agentSteps: PipelineStep[] = [
    { icon: <FileText className="h-5 w-5" />, label: "Requirements", description: "Describe your goal", color: CYAN },
    { icon: <Database className="h-5 w-5" />, label: "Data Sources", description: "Pick catalogs & tables", color: INFO },
    { icon: <Search className="h-5 w-5" />, label: "Inspection", description: "AI reads your schema", color: ACCENT },
    { icon: <GitBranch className="h-5 w-5" />, label: "Plan", description: "Parallel plan generation", color: SUCCESS },
    { icon: <Settings className="h-5 w-5" />, label: "Configure", description: "Build the Genie Space", color: WARNING },
    { icon: <CheckCircle2 className="h-5 w-5" />, label: "Created", description: "Ready to score", color: SUCCESS },
  ]

  const tools = [
    { name: "list_catalogs", desc: "Browse available Unity Catalog catalogs" },
    { name: "list_schemas", desc: "List schemas within a selected catalog" },
    { name: "list_tables", desc: "Discover tables and their descriptions" },
    { name: "get_table_schema", desc: "Read column names, types, and comments" },
    { name: "run_sql", desc: "Sample data to verify table contents" },
    { name: "create_genie_space", desc: "Build and configure the final space" },
  ]

  return (
    <div className="space-y-6">
      <div className="text-center mb-2">
        <h2 className="text-xl font-display font-bold text-primary">Create Agent</h2>
        <p className="text-sm text-muted mt-1">A multi-turn AI conversation that builds your Genie Space step by step</p>
      </div>

      <StageCard title="6-Step Progression" subtitle="The agent walks you through each phase" icon={<MessageSquarePlus className="h-4 w-4" />}>
        <PipelineDiagram steps={agentSteps} />
      </StageCard>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <StageCard title="Agent Tools" subtitle="What the AI can do" icon={<Settings className="h-4 w-4" />}>
          <div className="space-y-2.5">
            {tools.map((t) => (
              <div key={t.name} className="flex items-start gap-3">
                <code className="text-xs font-mono bg-accent/10 text-accent px-2 py-0.5 rounded shrink-0 mt-0.5">{t.name}</code>
                <span className="text-sm text-secondary">{t.desc}</span>
              </div>
            ))}
          </div>
        </StageCard>

        <StageCard title="How It Feels" subtitle="What you experience" icon={<Sparkles className="h-4 w-4" />}>
          <div className="space-y-4">
            <div className="rounded-lg bg-elevated border border-default p-4">
              <p className="text-sm text-secondary italic">"I need a Genie Space for our retail analytics — we have sales transactions and inventory tables in the commerce catalog."</p>
              <div className="mt-3 flex items-center gap-2 text-xs text-muted">
                <div className="h-5 w-5 rounded-full bg-accent/20 flex items-center justify-center">
                  <MessageSquarePlus className="h-3 w-3 text-accent" />
                </div>
                Agent discovers tables, reads schemas, suggests a plan...
              </div>
            </div>
            <div className="space-y-2 text-sm">
              <div className="flex items-center gap-2 text-secondary">
                <CheckCircle2 className="h-4 w-4 text-success shrink-0" />
                Real-time streaming — see the agent think and act
              </div>
              <div className="flex items-center gap-2 text-secondary">
                <CheckCircle2 className="h-4 w-4 text-success shrink-0" />
                Parallel plan generation for speed
              </div>
              <div className="flex items-center gap-2 text-secondary">
                <CheckCircle2 className="h-4 w-4 text-success shrink-0" />
                Session persistence — pick up where you left off
              </div>
            </div>
          </div>
        </StageCard>
      </div>
    </div>
  )
}

/* ================================================================
   STAGE 2 — IQ Scanner
   ================================================================ */
function IQScannerContent() {
  const configChecks = [
    { name: "Tables exist", desc: "At least one table attached to the space" },
    { name: "Table descriptions (≥80%)", desc: "80%+ of tables have meaningful descriptions" },
    { name: "Column descriptions (≥50%)", desc: "50%+ of columns are documented" },
    { name: "Text instructions (>50 chars)", desc: "Business context and terminology explained" },
    { name: "Join specifications", desc: "Join paths defined for multi-table spaces" },
    { name: "Table count 1–12", desc: "Optimal number of tables for accuracy" },
    { name: "8+ example SQLs", desc: "Diverse query patterns for the model to learn" },
    { name: "SQL snippets", desc: "Functions, expressions, measures, or filters defined" },
    { name: "Entity/format matching", desc: "Categorical and date/number columns annotated" },
    { name: "10+ benchmark questions", desc: "Ground-truth questions to measure accuracy" },
  ]

  const optimizationChecks = [
    { name: "Optimization workflow completed", desc: "Space has been through the optimization pipeline" },
    { name: "Optimization accuracy ≥ 85%", desc: "Benchmark accuracy meets the trusted threshold" },
  ]

  const tiers = [
    { name: "Not Ready", criteria: "Config gaps remain", color: DANGER, icon: <XCircle className="h-5 w-5" />, desc: "One or more of the 10 config checks fail" },
    { name: "Ready to Optimize", criteria: "All config checks pass", color: INFO, icon: <AlertTriangle className="h-5 w-5" />, desc: "All 10 config checks pass — optimization pending" },
    { name: "Trusted", criteria: "All 12 checks pass", color: SUCCESS, icon: <CheckCircle2 className="h-5 w-5" />, desc: "Config + optimization checks all pass" },
  ]

  return (
    <div className="space-y-6">
      <div className="text-center mb-2">
        <h2 className="text-xl font-display font-bold text-primary">IQ Scanner</h2>
        <p className="text-sm text-muted mt-1">Instant, rule-based quality scoring — no LLM needed</p>
      </div>

      {/* Maturity tiers */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 animate-stagger">
        {tiers.map((tier) => (
          <div
            key={tier.name}
            className="rounded-xl border-2 p-5 text-center transition-shadow hover:shadow-lg"
            style={{ borderColor: `${tier.color}40`, background: `${tier.color}08` }}
          >
            <div className="flex justify-center mb-3">
              <div
                className="h-12 w-12 rounded-full flex items-center justify-center"
                style={{ background: `${tier.color}20`, color: tier.color }}
              >
                {tier.icon}
              </div>
            </div>
            <h3 className="font-display font-bold text-primary text-base">{tier.name}</h3>
            <div className="text-sm font-semibold mt-1" style={{ color: tier.color }}>{tier.criteria}</div>
            <p className="text-xs text-muted mt-1.5">{tier.desc}</p>
          </div>
        ))}
      </div>

      {/* Score gauge illustration */}
      <StageCard title="12 Quality Checks" subtitle="Each check contributes 1 point" icon={<Gauge className="h-4 w-4" />}>
        <div className="mb-3">
          <h4 className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">Config Checks (1–10)</h4>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {configChecks.map((check) => (
              <div key={check.name} className="flex items-start gap-2.5 py-1.5">
                <div className="h-2 w-2 rounded-full shrink-0 mt-1.5" style={{ background: ACCENT }} />
                <div>
                  <span className="text-sm text-primary font-medium block">{check.name}</span>
                  <span className="text-xs text-muted">{check.desc}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="pt-3 border-t border-default">
          <h4 className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">Optimization Checks (11–12)</h4>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {optimizationChecks.map((check) => (
              <div key={check.name} className="flex items-start gap-2.5 py-1.5">
                <div className="h-2 w-2 rounded-full shrink-0 mt-1.5" style={{ background: SUCCESS }} />
                <div>
                  <span className="text-sm text-primary font-medium block">{check.name}</span>
                  <span className="text-xs text-muted">{check.desc}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </StageCard>
    </div>
  )
}

/* ================================================================
   STAGE 3 — Fix Agent
   ================================================================ */
function FixAgentContent() {
  const fixSteps: PipelineStep[] = [
    { icon: <ShieldCheck className="h-5 w-5" />, label: "Scan Results", description: "Findings from IQ Scanner", color: SUCCESS },
    { icon: <Search className="h-5 w-5" />, label: "Analyze", description: "LLM reviews each finding", color: INFO },
    { icon: <FileText className="h-5 w-5" />, label: "Generate Patches", description: "JSON patches per finding", color: WARNING },
    { icon: <Target className="h-5 w-5" />, label: "Validate", description: "Check patch safety", color: ACCENT },
    { icon: <CheckCircle2 className="h-5 w-5" />, label: "Apply", description: "Write to Genie API", color: SUCCESS },
  ]

  return (
    <div className="space-y-6">
      <div className="text-center mb-2">
        <h2 className="text-xl font-display font-bold text-primary">Fix Agent</h2>
        <p className="text-sm text-muted mt-1">Automatically generates and applies fixes from IQ Scanner findings</p>
      </div>

      <StageCard title="Scan → Fix Pipeline" icon={<Wrench className="h-4 w-4" />}>
        <PipelineDiagram steps={fixSteps} />
      </StageCard>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <StageCard title="What Gets Fixed" icon={<Target className="h-4 w-4" />}>
          <div className="space-y-3">
            {[
              { label: "Missing instructions", fix: "Generates contextual instructions from table metadata" },
              { label: "Missing sample questions", fix: "Creates realistic questions users would actually ask" },
              { label: "Empty table descriptions", fix: "Writes descriptions from column analysis" },
              { label: "Missing column docs", fix: "Documents columns based on names, types, and samples" },
              { label: "Weak naming", fix: "Suggests clearer display names" },
            ].map((item) => (
              <div key={item.label} className="flex items-start gap-3">
                <div className="mt-1 h-5 w-5 rounded bg-warning/10 flex items-center justify-center shrink-0">
                  <Wrench className="h-3 w-3 text-warning" />
                </div>
                <div>
                  <span className="text-sm font-medium text-primary">{item.label}</span>
                  <p className="text-xs text-muted">{item.fix}</p>
                </div>
              </div>
            ))}
          </div>
        </StageCard>

        <StageCard title="Patch Format" subtitle="JSON Patch (RFC 6902)" icon={<FileText className="h-4 w-4" />}>
          <div className="rounded-lg bg-sunken border border-default p-4 font-mono text-xs leading-relaxed">
            <div className="text-muted">{"// Example: adding a table description"}</div>
            <div className="mt-2">
              <span className="text-accent">{"{"}</span><br />
              <span className="ml-3 text-cyan">"op"</span>: <span className="text-success">"replace"</span>,<br />
              <span className="ml-3 text-cyan">"path"</span>: <span className="text-success">"/tables/0/description"</span>,<br />
              <span className="ml-3 text-cyan">"value"</span>: <span className="text-success">"Daily sales transactions..."</span><br />
              <span className="text-accent">{"}"}</span>
            </div>
          </div>
          <div className="mt-4 space-y-2 text-sm">
            <div className="flex items-center gap-2 text-secondary">
              <CheckCircle2 className="h-4 w-4 text-success shrink-0" />
              Patches run in parallel for speed
            </div>
            <div className="flex items-center gap-2 text-secondary">
              <CheckCircle2 className="h-4 w-4 text-success shrink-0" />
              ID sanitization prevents targeting errors
            </div>
            <div className="flex items-center gap-2 text-secondary">
              <CheckCircle2 className="h-4 w-4 text-success shrink-0" />
              Applied via Genie Space API — no file editing
            </div>
          </div>
        </StageCard>
      </div>
    </div>
  )
}

/* ================================================================
   STAGE 4 — Auto-Optimize (GSO)
   ================================================================ */
function AutoOptimizeContent() {
  const pipelineSteps: PipelineStep[] = [
    { icon: <ShieldCheck className="h-5 w-5" />, label: "Preflight", description: "Validate space readiness", color: INFO },
    { icon: <BarChart3 className="h-5 w-5" />, label: "Baseline", description: "Run benchmark questions", color: ACCENT },
    { icon: <Database className="h-5 w-5" />, label: "Enrich", description: "Gather metadata context", color: CYAN },
    { icon: <RefreshCw className="h-5 w-5" />, label: "Lever Loop", description: "Apply & evaluate levers", color: WARNING },
    { icon: <Target className="h-5 w-5" />, label: "Finalize", description: "Select best combination", color: SUCCESS },
    { icon: <CheckCircle2 className="h-5 w-5" />, label: "Deploy", description: "Apply winning config", color: SUCCESS },
  ]

  const levers = [
    { name: "Instructions", desc: "Rewrite or enhance space-level instructions", color: ACCENT },
    { name: "Table Descriptions", desc: "Improve how tables are described to the model", color: CYAN },
    { name: "Column Descriptions", desc: "Add or refine column-level documentation", color: SUCCESS },
    { name: "Sample Questions", desc: "Generate better example queries", color: WARNING },
    { name: "Certified Queries", desc: "Add verified SQL for common questions", color: DANGER },
  ]

  return (
    <div className="space-y-6">
      <div className="text-center mb-2">
        <h2 className="text-xl font-display font-bold text-primary">Auto-Optimize (GSO)</h2>
        <p className="text-sm text-muted mt-1">Benchmark-driven optimization — tests real questions, picks what actually works</p>
      </div>

      <StageCard title="6-Task Pipeline" icon={<Zap className="h-4 w-4" />}>
        <PipelineDiagram steps={pipelineSteps} />
      </StageCard>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <StageCard title="5 Lever Categories" subtitle="What gets tuned" icon={<Settings className="h-4 w-4" />}>
          <div className="space-y-3">
            {levers.map((l) => (
              <div key={l.name} className="flex items-center gap-3">
                <div
                  className="h-8 w-8 rounded-lg flex items-center justify-center shrink-0"
                  style={{ background: `${l.color}15` }}
                >
                  <div className="h-3 w-3 rounded-sm" style={{ background: l.color }} />
                </div>
                <div>
                  <span className="text-sm font-medium text-primary">{l.name}</span>
                  <p className="text-xs text-muted">{l.desc}</p>
                </div>
              </div>
            ))}
          </div>
        </StageCard>

        <StageCard title="3-Gate Evaluation" subtitle="How quality is measured" icon={<BarChart3 className="h-4 w-4" />}>
          <div className="space-y-4">
            {[
              { gate: "Gate 1 — SQL Validity", desc: "Does the generated SQL parse and execute?", color: INFO },
              { gate: "Gate 2 — Schema Match", desc: "Do columns and tables match the expected output?", color: WARNING },
              { gate: "Gate 3 — Semantic Accuracy", desc: "Do results match ground-truth answers? (9 judges)", color: SUCCESS },
            ].map((g) => (
              <div key={g.gate} className="rounded-lg border p-3" style={{ borderColor: `${g.color}30`, background: `${g.color}05` }}>
                <h4 className="text-sm font-semibold" style={{ color: g.color }}>{g.gate}</h4>
                <p className="text-xs text-muted mt-0.5">{g.desc}</p>
              </div>
            ))}
            <div className="flex items-center gap-2 pt-2 text-xs text-muted border-t border-default">
              <RefreshCw className="h-3.5 w-3.5" />
              Iterates until accuracy converges or max rounds reached
            </div>
          </div>
        </StageCard>
      </div>
    </div>
  )
}

/* ================================================================
   STAGE 5 — Permissions
   ================================================================ */
function PermissionsContent() {
  return (
    <div className="space-y-6">
      <div className="text-center mb-2">
        <h2 className="text-xl font-display font-bold text-primary">Permissions Model</h2>
        <p className="text-sm text-muted mt-1">Dual-identity architecture — your token for interactive work, app identity for background jobs</p>
      </div>
      <PermissionDiagram />
    </div>
  )
}

/* ================================================================
   STAGE 6 — Architecture
   ================================================================ */
function ArchitectureContent() {
  const layers = [
    {
      name: "Frontend",
      desc: "React 19 + TypeScript + Tailwind CSS v4",
      color: CYAN,
      icon: <Globe className="h-5 w-5" />,
      items: ["Single-page app served by FastAPI", "SSE streaming for real-time updates", "Dark/light theme with CSS variables"],
    },
    {
      name: "Backend",
      desc: "FastAPI + Pydantic + asyncio",
      color: ACCENT,
      icon: <Box className="h-5 w-5" />,
      items: ["4 routers: analysis, spaces, admin, create", "OBO middleware (ContextVar-based)", "LLM calls via Databricks serving endpoints"],
    },
    {
      name: "Persistence",
      desc: "Lakebase (PostgreSQL) + Delta Tables",
      color: SUCCESS,
      icon: <HardDrive className="h-5 w-5" />,
      items: ["Scans, stars, sessions in Lakebase", "Optimization state in 12 Delta tables", "Graceful fallback to in-memory storage"],
    },
    {
      name: "Integrations",
      desc: "Databricks platform services",
      color: WARNING,
      icon: <Network className="h-5 w-5" />,
      items: ["Unity Catalog for data discovery", "Genie API for space management", "Lakeflow Jobs for optimization pipeline"],
    },
  ]

  return (
    <div className="space-y-6">
      <div className="text-center mb-2">
        <h2 className="text-xl font-display font-bold text-primary">Architecture</h2>
        <p className="text-sm text-muted mt-1">Full-stack Databricks App — React frontend, FastAPI backend, platform integrations</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 animate-stagger">
        {layers.map((layer) => (
          <div
            key={layer.name}
            className="rounded-xl border-2 p-5 transition-shadow hover:shadow-lg"
            style={{ borderColor: `${layer.color}30`, background: `${layer.color}05` }}
          >
            <div className="flex items-center gap-3 mb-3">
              <div
                className="h-10 w-10 rounded-lg flex items-center justify-center"
                style={{ background: `${layer.color}20`, color: layer.color }}
              >
                {layer.icon}
              </div>
              <div>
                <h3 className="font-display font-bold text-primary text-sm">{layer.name}</h3>
                <p className="text-xs text-muted">{layer.desc}</p>
              </div>
            </div>
            <div className="space-y-1.5">
              {layer.items.map((item) => (
                <div key={item} className="flex items-center gap-2 text-sm text-secondary">
                  <div className="h-1.5 w-1.5 rounded-full shrink-0" style={{ background: layer.color }} />
                  {item}
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Data flow diagram */}
      <StageCard title="Data Flow" subtitle="How requests move through the system" icon={<ArrowRightLeft className="h-4 w-4" />}>
        <PipelineDiagram
          steps={[
            { icon: <Globe className="h-5 w-5" />, label: "Browser", description: "React SPA", color: CYAN },
            { icon: <Box className="h-5 w-5" />, label: "FastAPI", description: "OBO middleware", color: ACCENT },
            { icon: <Database className="h-5 w-5" />, label: "Genie API", description: "Space CRUD", color: INFO },
            { icon: <HardDrive className="h-5 w-5" />, label: "Lakebase", description: "State storage", color: SUCCESS },
            { icon: <Network className="h-5 w-5" />, label: "Unity Catalog", description: "Data governance", color: WARNING },
          ]}
        />
      </StageCard>
    </div>
  )
}
