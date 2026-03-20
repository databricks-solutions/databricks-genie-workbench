import { useState } from "react"
import {
  ChevronDown,
  ChevronRight,
  Table2,
  FileText,
  Link2,
  Code2,
  ListChecks,
  MessageSquare,
  BarChart3,
  RefreshCw,
  Check,
} from "lucide-react"

interface SpaceOverviewProps {
  spaceData: Record<string, unknown> | null
  isLoading: boolean
}

interface OverviewSection {
  key: string
  label: string
  Icon: typeof Table2
  count: number
  render: () => React.ReactNode
}

function joinStringArray(arr: unknown): string {
  if (Array.isArray(arr)) return arr.join("")
  if (typeof arr === "string") return arr
  return ""
}

function safeArray(val: unknown): unknown[] {
  return Array.isArray(val) ? val : []
}

function safeObj(val: unknown): Record<string, unknown> {
  return val && typeof val === "object" && !Array.isArray(val) ? (val as Record<string, unknown>) : {}
}

function extractOverviewData(spaceData: Record<string, unknown>) {
  const dataSources = safeObj(spaceData.data_sources)
  const tables = safeArray(dataSources.tables)
  const instructions = safeObj(spaceData.instructions)
  const textInstructions = safeArray(instructions.text_instructions)
  const exampleSqls = safeArray(instructions.example_question_sqls)
  const joinSpecs = safeArray(instructions.join_specs)
  const sqlSnippets = safeObj(instructions.sql_snippets)
  const filters = safeArray(sqlSnippets.filters)
  const expressions = safeArray(sqlSnippets.expressions)
  const measures = safeArray(sqlSnippets.measures)
  const config = safeObj(spaceData.config)
  const sampleQuestions = safeArray(config.sample_questions)
  const benchmarksObj = safeObj(spaceData.benchmarks)
  const benchmarkQuestions = safeArray(benchmarksObj.questions)

  const instructionText = textInstructions
    .map((ti) => joinStringArray(safeObj(ti).content))
    .filter(Boolean)
    .join("\n")

  return {
    tables,
    instructionText,
    joinSpecs,
    filters,
    expressions,
    measures,
    exampleSqls,
    sampleQuestions,
    benchmarkQuestions,
  }
}

const TYPE_BADGE: Record<string, { label: string; cls: string }> = {
  measure: { label: "MEASURE", cls: "bg-blue-500/15 text-blue-400" },
  filter: { label: "FILTER", cls: "bg-amber-500/15 text-amber-400" },
  dimension: { label: "DIMENSION", cls: "bg-emerald-500/15 text-emerald-400" },
}

export function SpaceOverview({ spaceData, isLoading }: SpaceOverviewProps) {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set())

  const toggleSection = (key: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  if (isLoading || !spaceData) {
    return (
      <div className="text-center py-12 text-muted">
        {isLoading ? (
          <>
            <RefreshCw className="w-8 h-8 mx-auto mb-3 animate-spin opacity-40" />
            <p>Loading space configuration...</p>
          </>
        ) : (
          <p>Space data unavailable.</p>
        )}
      </div>
    )
  }

  const data = extractOverviewData(spaceData)
  const sqlExpressionCount = data.measures.length + data.filters.length + data.expressions.length

  const sections: OverviewSection[] = [
    {
      key: "tables",
      label: "Tables",
      Icon: Table2,
      count: data.tables.length,
      render: () =>
        data.tables.length === 0 ? (
          <p className="text-muted italic">None configured</p>
        ) : (
          <div className="space-y-2">
            {data.tables.map((t, i) => {
              const table = safeObj(t)
              const identifier = String(table.identifier || "")
              const description = joinStringArray(table.description)
              const columns = safeArray(table.column_configs)
              return (
                <div key={i} className="border border-default rounded-lg p-3 bg-elevated">
                  <code className="text-xs font-mono text-accent">{identifier}</code>
                  {description && <p className="text-xs text-secondary mt-1">{description}</p>}
                  <span className="text-[10px] text-muted mt-1 block">{columns.length} columns</span>
                </div>
              )
            })}
          </div>
        ),
    },
    {
      key: "text_instructions",
      label: "Text Instructions",
      Icon: FileText,
      count: data.instructionText.trim() ? 1 : 0,
      render: () =>
        !data.instructionText.trim() ? (
          <p className="text-muted italic">None configured</p>
        ) : (
          <div className="text-xs text-secondary whitespace-pre-wrap bg-elevated border border-default rounded-lg p-3">
            {data.instructionText}
          </div>
        ),
    },
    {
      key: "joins",
      label: "Joins",
      Icon: Link2,
      count: data.joinSpecs.length,
      render: () =>
        data.joinSpecs.length === 0 ? (
          <p className="text-muted italic">None configured</p>
        ) : (
          <div className="space-y-2">
            {data.joinSpecs.map((j, i) => {
              const join = safeObj(j)
              const left = safeObj(join.left)
              const right = safeObj(join.right)
              const sql = joinStringArray(join.sql)
              return (
                <div key={i} className="border border-default rounded-lg p-3 bg-elevated">
                  <div className="flex items-center gap-2 text-xs">
                    <code className="font-mono text-accent">{String(left.identifier || left.alias || "")}</code>
                    <span className="text-muted">&rarr;</span>
                    <code className="font-mono text-accent">{String(right.identifier || right.alias || "")}</code>
                  </div>
                  {sql && (
                    <pre className="text-[11px] text-muted mt-2 font-mono bg-surface rounded p-2 overflow-x-auto">
                      {sql.replace(/--rt=.*?--/g, "").trim()}
                    </pre>
                  )}
                </div>
              )
            })}
          </div>
        ),
    },
    {
      key: "sql_expressions",
      label: "SQL Expressions",
      Icon: Code2,
      count: sqlExpressionCount,
      render: () => {
        const tagged = [
          ...data.measures.map((m) => ({ obj: safeObj(m), _type: "measure" })),
          ...data.filters.map((f) => ({ obj: safeObj(f), _type: "filter" })),
          ...data.expressions.map((e) => ({ obj: safeObj(e), _type: "dimension" })),
        ]
        return tagged.length === 0 ? (
          <p className="text-muted italic">None configured</p>
        ) : (
          <div className="space-y-1.5">
            {tagged.map((expr, i) => {
              const badge = TYPE_BADGE[expr._type]
              const name = String(expr.obj.display_name || expr.obj.alias || "")
              const sql = joinStringArray(expr.obj.sql)
              return (
                <div key={i} className="flex items-start gap-2 py-1.5 px-2 rounded bg-elevated border border-default">
                  <span className={`text-[10px] font-medium px-1.5 py-0.5 rounded ${badge.cls} flex-shrink-0`}>
                    {badge.label}
                  </span>
                  <div className="min-w-0 flex-1">
                    <span className="text-xs font-medium text-primary">{name}</span>
                    {sql && <pre className="text-[11px] text-muted font-mono mt-0.5 truncate">{sql}</pre>}
                  </div>
                </div>
              )
            })}
          </div>
        )
      },
    },
    {
      key: "example_sqls",
      label: "Example SQL Queries",
      Icon: ListChecks,
      count: data.exampleSqls.length,
      render: () =>
        data.exampleSqls.length === 0 ? (
          <p className="text-muted italic">None configured</p>
        ) : (
          <div className="space-y-2">
            {data.exampleSqls.map((eq, i) => {
              const example = safeObj(eq)
              const question = joinStringArray(example.question)
              const sql = joinStringArray(example.sql)
              return (
                <div key={i} className="border border-default rounded-lg p-3 bg-elevated">
                  <p className="text-xs text-primary font-medium">{question}</p>
                  {sql && (
                    <pre className="text-[11px] text-muted font-mono mt-2 bg-surface rounded p-2 overflow-x-auto whitespace-pre-wrap">
                      {sql}
                    </pre>
                  )}
                </div>
              )
            })}
          </div>
        ),
    },
    {
      key: "sample_questions",
      label: "Sample Questions",
      Icon: MessageSquare,
      count: data.sampleQuestions.length,
      render: () =>
        data.sampleQuestions.length === 0 ? (
          <p className="text-muted italic">None configured</p>
        ) : (
          <div className="space-y-1">
            {data.sampleQuestions.map((q, i) => {
              const question = joinStringArray(safeObj(q).question)
              return (
                <div key={i} className="flex items-start gap-2 py-1 text-xs">
                  <span className="text-muted select-none w-4 text-right flex-shrink-0">{i + 1}.</span>
                  <span className="text-secondary">{question}</span>
                </div>
              )
            })}
          </div>
        ),
    },
    {
      key: "benchmarks",
      label: "Benchmark Questions",
      Icon: BarChart3,
      count: data.benchmarkQuestions.length,
      render: () =>
        data.benchmarkQuestions.length === 0 ? (
          <p className="text-muted italic">None configured</p>
        ) : (
          <div className="space-y-2">
            {data.benchmarkQuestions.map((b, i) => {
              const bm = safeObj(b)
              const question = joinStringArray(bm.question)
              const answers = safeArray(bm.answer)
              const sqlAnswer = answers
                .map((a) => joinStringArray(safeObj(a).content))
                .filter(Boolean)
                .join("\n")
              return (
                <div key={i} className="border border-default rounded-lg p-3 bg-elevated">
                  <p className="text-xs text-primary font-medium">{question}</p>
                  {sqlAnswer && (
                    <pre className="text-[11px] text-muted font-mono mt-2 bg-surface rounded p-2 overflow-x-auto whitespace-pre-wrap">
                      {sqlAnswer}
                    </pre>
                  )}
                </div>
              )
            })}
          </div>
        ),
    },
  ]

  return (
    <div className="bg-surface border border-default rounded-xl overflow-hidden">
      <div className="divide-y divide-default">
        {sections.map((sec) => {
          const isOpen = expandedSections.has(sec.key)
          const { Icon } = sec
          return (
            <div key={sec.key}>
              <button
                onClick={() => toggleSection(sec.key)}
                className="flex items-center gap-3 w-full px-5 py-4 hover:bg-surface-secondary/50 transition-colors text-left"
              >
                {isOpen ? (
                  <ChevronDown className="w-4 h-4 text-muted" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-muted" />
                )}
                <Icon className="w-4 h-4 text-accent" />
                <span className="text-sm font-medium text-primary flex-1">{sec.label}</span>
                {sec.key === "text_instructions" ? (
                  sec.count > 0 ? (
                    <Check className="w-4 h-4 text-green-400 flex-shrink-0" />
                  ) : (
                    <span className="text-xs text-muted flex-shrink-0">empty</span>
                  )
                ) : (
                  <span className="text-xs text-muted bg-surface-secondary px-2 py-0.5 rounded-full flex-shrink-0">
                    {sec.count}
                  </span>
                )}
              </button>
              {isOpen && <div className="px-5 pb-5">{sec.render()}</div>}
            </div>
          )
        })}
      </div>
    </div>
  )
}
