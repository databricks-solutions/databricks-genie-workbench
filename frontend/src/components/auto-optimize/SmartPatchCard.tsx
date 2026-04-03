import { useState } from "react"
import { ChevronDown } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import type { GSOPatchDetail } from "@/types"

interface SmartPatchCardProps {
  patch: GSOPatchDetail
}

function parseJson(raw: Record<string, unknown> | string | null): Record<string, unknown> {
  if (!raw) return {}
  if (typeof raw === "object") return raw
  if (typeof raw === "string") {
    try {
      let parsed = JSON.parse(raw)
      if (typeof parsed === "string") parsed = JSON.parse(parsed)
      if (typeof parsed === "string") parsed = JSON.parse(parsed)
      return typeof parsed === "object" && parsed !== null ? parsed : {}
    } catch { return {} }
  }
  return {}
}

const OP_STYLE: Record<string, { variant: "success" | "danger" | "warning" | "info"; label: string }> = {
  add: { variant: "success", label: "add" },
  update: { variant: "info", label: "update" },
  remove: { variant: "danger", label: "remove" },
  rewrite: { variant: "warning", label: "rewrite" },
}

function OpBadge({ op }: { op: string }) {
  const s = OP_STYLE[op] ?? { variant: "info" as const, label: op }
  return <Badge variant={s.variant} className="text-[10px] py-0 px-1.5 font-mono">{s.label}</Badge>
}

function Target({ value }: { value: string }) {
  const short = value.split(".").slice(-1)[0] || value
  return (
    <span className="text-xs font-mono text-primary truncate" title={value}>
      {short !== value ? <span className="text-muted">{value.split(".").slice(0, -1).join(".")}.</span> : null}
      {short}
    </span>
  )
}

function SqlBlock({ sql }: { sql: string }) {
  return (
    <pre className="mt-1.5 p-2 text-[11px] font-mono bg-elevated/50 rounded-md text-primary overflow-x-auto whitespace-pre-wrap leading-relaxed">
      {sql}
    </pre>
  )
}

function FieldRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex gap-2 text-xs mt-1">
      <span className="text-muted shrink-0 w-20 text-right">{label}:</span>
      <span className="text-primary min-w-0">{children}</span>
    </div>
  )
}

const STRUCTURED_LABELS: Record<string, string> = {
  purpose: "Purpose", definition: "Definition", best_for: "Best for",
  grain: "Grain", values: "Values", aggregation: "Aggregation",
  scd: "SCD", relationships: "Relationships", join: "Join",
  grain_note: "Grain note", important_filters: "Important filters",
  synonyms: "Synonyms", use_instead_of: "Use instead of",
  parameters: "Parameters", example: "Example",
}

function DescriptionCard({ cmd }: { cmd: Record<string, unknown> }) {
  const target = String(cmd.target || "")
  const structured = cmd.structured_sections as Record<string, string> | undefined

  if (structured && Object.keys(structured).length > 0) {
    const entries = Object.entries(structured).filter(([, v]) => v && typeof v === "string")
    if (entries.length > 0) {
      return (
        <div>
          <Target value={target} />
          {entries.map(([key, val]) => (
            <FieldRow key={key} label={STRUCTURED_LABELS[key] || key}>{String(val)}</FieldRow>
          ))}
        </div>
      )
    }
  }

  const oldText = String(cmd.old_text || "")
  const newText = String(cmd.new_text || "")
  return (
    <div>
      <Target value={target} />
      {newText && <p className="text-xs text-primary mt-1.5 leading-relaxed">{newText}</p>}
      {oldText && cmd.op === "update" && (
        <p className="text-[11px] text-muted mt-1 line-through">{oldText.slice(0, 120)}{oldText.length > 120 ? "…" : ""}</p>
      )}
    </div>
  )
}

function ColumnConfigCard({ cmd }: { cmd: Record<string, unknown> }) {
  const table = String(cmd.table || "")
  const column = String(cmd.column || "")
  const structured = cmd.structured_sections as Record<string, string> | undefined
  const purpose = structured?.purpose

  if (cmd.visible !== undefined) {
    return (
      <div>
        <span className="text-xs font-mono text-primary">{column}</span>
        <span className="text-xs text-muted ml-1">on</span>
        <span className="text-xs font-mono text-muted ml-1">{table.split(".").slice(-1)[0]}</span>
        <span className="text-xs text-muted ml-2">→ {cmd.visible ? "shown" : "hidden"}</span>
      </div>
    )
  }

  if (cmd.old_alias || cmd.new_alias) {
    return (
      <div>
        <span className="text-xs font-mono text-primary">{column}</span>
        <span className="text-xs text-muted ml-2">alias: {String(cmd.old_alias || "")} → {String(cmd.new_alias || "")}</span>
      </div>
    )
  }

  if (cmd.synonyms) {
    const syns = Array.isArray(cmd.synonyms) ? cmd.synonyms.join(", ") : String(cmd.synonyms)
    return (
      <div>
        <span className="text-xs font-mono text-primary">{column}</span>
        <span className="text-xs text-muted ml-1">on</span>
        <span className="text-xs font-mono text-muted ml-1">{table.split(".").slice(-1)[0]}</span>
        <FieldRow label="Synonyms">{syns}</FieldRow>
      </div>
    )
  }

  return (
    <div>
      <div className="flex items-center gap-1">
        <span className="text-xs font-mono text-primary">{column}</span>
        <span className="text-xs text-muted">on</span>
        <span className="text-xs font-mono text-muted">{table.split(".").slice(-1)[0]}</span>
      </div>
      {purpose ? (
        <p className="text-xs text-primary mt-1 leading-relaxed">{purpose}</p>
      ) : cmd.new_text ? (
        <p className="text-xs text-primary mt-1 leading-relaxed">{String(cmd.new_text)}</p>
      ) : cmd.value ? (
        <p className="text-xs text-primary mt-1 leading-relaxed">{String(cmd.value)}</p>
      ) : null}
    </div>
  )
}

function ExampleSqlCard({ cmd }: { cmd: Record<string, unknown> }) {
  const question = String(cmd.question || "")
  const sql = String(cmd.sql || cmd.old_sql || cmd.new_sql || "")
  const guidance = cmd.usage_guidance ? String(cmd.usage_guidance) : null
  const params = Array.isArray(cmd.parameters) ? cmd.parameters : null

  return (
    <div>
      {question && (
        <div className="rounded-md border border-accent/20 bg-accent/5 px-3 py-2">
          <p className="text-xs text-primary font-medium">{question}</p>
        </div>
      )}
      {sql && <SqlBlock sql={sql} />}
      {guidance && <FieldRow label="Guidance">{guidance}</FieldRow>}
      {params && params.length > 0 && (
        <FieldRow label="Params">{params.map((p: Record<string, unknown>) => String(p.name || "")).join(", ")}</FieldRow>
      )}
    </div>
  )
}

function InstructionCard({ cmd }: { cmd: Record<string, unknown> }) {
  const [expanded, setExpanded] = useState(false)
  const newText = String(cmd.new_text || "")
  const oldText = String(cmd.old_text || "")
  const op = String(cmd.op || "")
  const TRUNCATE_AT = 300
  const isLong = newText.length > TRUNCATE_AT
  const displayText = isLong && !expanded ? newText.slice(0, TRUNCATE_AT) + "…" : newText
  const showOld = !!(oldText && (op === "update" || op === "rewrite"))

  return (
    <div>
      {newText && (
        <div className="rounded-md border border-default bg-elevated/30 px-3 py-2">
          <p className="text-xs text-primary leading-relaxed whitespace-pre-wrap">{displayText}</p>
          {isLong && (
            <button
              onClick={() => setExpanded(!expanded)}
              className="flex items-center gap-1 mt-1.5 text-[11px] text-accent hover:underline"
            >
              <ChevronDown className={`h-3 w-3 transition-transform ${expanded ? "rotate-180" : ""}`} />
              {expanded ? "Show less" : "Show more"}
            </button>
          )}
        </div>
      )}
      {showOld && (
        <details className="mt-1.5">
          <summary className="text-[11px] text-muted cursor-pointer hover:text-primary">
            Previous instruction
          </summary>
          <div className="mt-1 rounded-md border border-default/50 bg-elevated/10 px-3 py-2">
            <p className="text-[11px] text-muted line-through leading-relaxed whitespace-pre-wrap">
              {oldText.slice(0, 500)}{oldText.length > 500 ? "…" : ""}
            </p>
          </div>
        </details>
      )}
    </div>
  )
}

function JoinSpecCard({ cmd }: { cmd: Record<string, unknown> }) {
  const joinSpec = (cmd.join_spec || {}) as Record<string, unknown>
  const lt = String(cmd.left_table || joinSpec.left_table_identifier || "")
  const rt = String(cmd.right_table || joinSpec.right_table_identifier || "")
  const relationship = String(joinSpec.relationship || "")
  const sqlArr = Array.isArray(joinSpec.sql) ? joinSpec.sql : []
  const condition = sqlArr[0] ? String(sqlArr[0]) : ""

  return (
    <div>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-xs font-mono text-primary">{lt.split(".").slice(-1)[0]}</span>
        <span className="text-xs text-muted">⟷</span>
        <span className="text-xs font-mono text-primary">{rt.split(".").slice(-1)[0]}</span>
        {relationship && (
          <span className="inline-flex items-center rounded-md border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:text-amber-400">
            {relationship}
          </span>
        )}
      </div>
      {condition && (
        <code className="block mt-1.5 text-[11px] font-mono text-muted bg-elevated/50 rounded px-2 py-1">{condition}</code>
      )}
    </div>
  )
}

function SqlSnippetCard({ cmd }: { cmd: Record<string, unknown> }) {
  const snippet = (cmd.snippet || cmd.sql_snippet || {}) as Record<string, unknown>
  const snippetType = String(cmd.snippet_type || snippet.snippet_type || "expression")
  const displayName = String(snippet.display_name || cmd.display_name || "")
  const alias = String(snippet.alias || cmd.alias || "")
  const rawSql = snippet.sql ?? cmd.sql ?? ""
  const sql = Array.isArray(rawSql) ? (rawSql as string[]).join("\n") : String(rawSql)
  const rawInstruction = snippet.instruction ?? ""
  const instruction = Array.isArray(rawInstruction) ? (rawInstruction as string[]).join(" ") : String(rawInstruction)

  const typeColor: Record<string, string> = {
    measures: "border-blue-500/30 bg-blue-500/10 text-blue-700 dark:text-blue-400",
    filters: "border-orange-500/30 bg-orange-500/10 text-orange-700 dark:text-orange-400",
    expressions: "border-teal-500/30 bg-teal-500/10 text-teal-700 dark:text-teal-400",
  }

  return (
    <div>
      <div className="flex items-center gap-2 flex-wrap">
        <span className={`inline-flex items-center rounded-md border px-1.5 py-0.5 text-[10px] font-medium ${typeColor[snippetType] || typeColor.expressions}`}>
          {snippetType}
        </span>
        {(displayName || alias) && (
          <span className="text-xs font-medium text-primary">{displayName || alias}</span>
        )}
      </div>
      {sql && <SqlBlock sql={sql} />}
      {instruction && <FieldRow label="When">{instruction}</FieldRow>}
    </div>
  )
}

function TableCard({ cmd }: { cmd: Record<string, unknown> }) {
  const identifier = String(cmd.identifier || (cmd.asset as Record<string, unknown>)?.identifier || "")
  return (
    <div>
      <Target value={identifier} />
    </div>
  )
}

function FallbackCard({ cmd }: { cmd: Record<string, unknown> }) {
  const display = JSON.stringify(cmd, null, 2)
  return (
    <pre className="text-[11px] font-mono text-muted/80 whitespace-pre-wrap max-h-24 overflow-hidden">
      {display.slice(0, 500)}{display.length > 500 ? "\n…" : ""}
    </pre>
  )
}

function renderContent(cmd: Record<string, unknown>) {
  const section = String(cmd.section || "")

  if (section === "descriptions") return <DescriptionCard cmd={cmd} />
  if (section === "column_configs") return <ColumnConfigCard cmd={cmd} />
  if (section === "example_question_sqls") return <ExampleSqlCard cmd={cmd} />
  if (section === "instructions") return <InstructionCard cmd={cmd} />
  if (section === "join_specs") return <JoinSpecCard cmd={cmd} />
  if (section === "tables") return <TableCard cmd={cmd} />

  if (section.includes("sql_snippet") || cmd.sql_snippet || cmd.snippet_type) {
    return <SqlSnippetCard cmd={cmd} />
  }

  return <FallbackCard cmd={cmd} />
}

const SECTION_LABELS: Record<string, string> = {
  descriptions: "Table Description",
  column_configs: "Column Config",
  example_question_sqls: "Example SQL",
  instructions: "Instruction",
  join_specs: "Join Spec",
  tables: "Table",
}

export function SmartPatchCard({ patch }: SmartPatchCardProps) {
  const cmd = parseJson(patch.command)
  const op = String(cmd.op || "update")
  const section = String(cmd.section || "")
  const sectionLabel = SECTION_LABELS[section] || patch.patchType

  if (!cmd.op && !cmd.section) {
    const patchData = parseJson(patch.patch)
    const src = Object.keys(cmd).length === 0 ? patchData : cmd

    if (patch.patchType.includes("instruction")) {
      const newText = String(
        src.new_text || patchData.new_text || patchData.proposed_value
        || patchData.change_description || ""
      )
      const oldText = String(
        src.old_text || patchData.old_value || patchData.old_text || ""
      )
      const op = patch.patchType.startsWith("rewrite") ? "rewrite"
        : patch.patchType.startsWith("add") ? "add" : "update"
      if (newText || oldText) {
        return (
          <div className="rounded-lg border border-default bg-surface p-3 space-y-2">
            <div className="flex items-center gap-2 flex-wrap">
              <OpBadge op={op} />
              <span className="text-xs font-medium text-muted">Instruction</span>
              {patch.targetObject && <Target value={patch.targetObject} />}
            </div>
            <InstructionCard cmd={{ op, new_text: newText, old_text: oldText }} />
          </div>
        )
      }
    }

    if (Object.keys(patchData).length > 0) {
      return (
        <div className="rounded-lg border border-default bg-surface p-3 space-y-1.5">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="inline-flex items-center rounded-md border border-accent/30 bg-accent/10 px-2 py-0.5 text-xs font-mono font-medium text-accent">
              {patch.patchType}
            </span>
            <span className="text-xs text-muted">scope: {patch.scope}</span>
            <span className="text-xs text-muted">risk: {patch.riskLevel}</span>
          </div>
          {patch.targetObject && (
            <div className="mt-1"><Target value={patch.targetObject} /></div>
          )}
        </div>
      )
    }

    return (
      <div className="rounded-lg border border-default bg-surface p-3 space-y-1.5">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="inline-flex items-center rounded-md border border-accent/30 bg-accent/10 px-2 py-0.5 text-xs font-mono font-medium text-accent">
            {patch.patchType}
          </span>
          <span className="text-xs text-muted">scope: {patch.scope}</span>
          <span className="text-xs text-muted">risk: {patch.riskLevel}</span>
        </div>
        {patch.targetObject && (
          <p className="text-xs text-muted font-mono">target: {patch.targetObject}</p>
        )}
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-default bg-surface p-3 space-y-2">
      <div className="flex items-center gap-2 flex-wrap">
        <OpBadge op={op} />
        <span className="text-xs font-medium text-muted">{sectionLabel}</span>
        {patch.rolledBack && (
          <Badge variant="danger" className="text-[10px] py-0 px-1.5">rolled back</Badge>
        )}
      </div>
      {renderContent(cmd)}
    </div>
  )
}
