import type { GSOPatchDetail } from "@/types"

interface ProactiveEnrichmentViewProps {
  patches: GSOPatchDetail[]
}

const SECTIONS = [
  {
    key: "descriptions",
    label: "Column & Table Descriptions",
    types: ["proactive_description_enrichment", "proactive_table_description_enrichment"],
  },
  {
    key: "joins",
    label: "Join Discovery",
    types: ["proactive_join_discovery"],
  },
  {
    key: "example_sqls",
    label: "Example SQLs",
    types: ["proactive_example_sql"],
  },
  {
    key: "sql_expressions",
    label: "SQL Expressions",
    types: ["proactive_sql_expression"],
  },
  {
    key: "instructions",
    label: "Instructions",
    types: ["proactive_instruction_seeding"],
  },
  {
    key: "space_metadata",
    label: "Space Metadata",
    types: ["proactive_space_description", "proactive_sample_question"],
  },
] as const

function parsePatchData(raw: Record<string, unknown> | string | null): Record<string, unknown> {
  if (!raw) return {}
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw)
    } catch {
      return {}
    }
  }
  return raw
}

function ExampleSqlsSection({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-default">
            <th className="text-left px-3 py-1.5 text-muted font-medium w-8">#</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Question</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-48">Target</th>
          </tr>
        </thead>
        <tbody>
          {patches.map((p, i) => {
            const patchData = parsePatchData(p.patch)
            const question = String(patchData.question || patchData.example_question || "—")
            return (
              <tr key={i} className="border-b border-default last:border-0">
                <td className="px-3 py-2 text-muted tabular-nums">{i + 1}</td>
                <td className="px-3 py-2 text-primary">{question}</td>
                <td className="px-3 py-2 text-muted font-mono truncate max-w-[200px]" title={p.targetObject ?? undefined}>
                  {p.targetObject || "—"}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function SqlExpressionsSection({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-default">
            <th className="text-left px-3 py-1.5 text-muted font-medium w-8">#</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-24">Type</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Display Name</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">SQL</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-40">Target Table</th>
          </tr>
        </thead>
        <tbody>
          {patches.map((p, i) => {
            const patchData = parsePatchData(p.patch)
            return (
              <tr key={i} className="border-b border-default last:border-0">
                <td className="px-3 py-2 text-muted tabular-nums">{i + 1}</td>
                <td className="px-3 py-2">
                  <span className="inline-flex items-center rounded-md border border-teal-500/30 bg-teal-500/10 px-1.5 py-0.5 text-[10px] font-medium text-teal-700 dark:text-teal-400">
                    {String(patchData.snippet_type || "expression")}
                  </span>
                </td>
                <td className="px-3 py-2 text-primary">{String(patchData.display_name || "—")}</td>
                <td className="px-3 py-2">
                  <code className="text-[11px] bg-elevated/50 rounded px-1.5 py-0.5 text-primary">
                    {String(patchData.sql || "—")}
                  </code>
                </td>
                <td className="px-3 py-2 text-muted font-mono truncate max-w-[160px]" title={p.targetObject ?? undefined}>
                  {p.targetObject || "—"}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function DescriptionsSection({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-default">
            <th className="text-left px-3 py-1.5 text-muted font-medium w-8">#</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium w-48">Target</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Description</th>
          </tr>
        </thead>
        <tbody>
          {patches.map((p, i) => {
            const patchData = parsePatchData(p.patch)
            const cmdData = parsePatchData(p.command)
            const desc = String(patchData.description || cmdData.description || "—")
            return (
              <tr key={i} className="border-b border-default last:border-0">
                <td className="px-3 py-2 text-muted tabular-nums">{i + 1}</td>
                <td className="px-3 py-2 text-primary font-mono truncate max-w-[200px]" title={p.targetObject ?? undefined}>
                  {p.targetObject || "—"}
                </td>
                <td className="px-3 py-2 text-muted">{desc}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function JoinsSection({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-default">
            <th className="text-left px-3 py-1.5 text-muted font-medium w-8">#</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Target</th>
            <th className="text-left px-3 py-1.5 text-muted font-medium">Details</th>
          </tr>
        </thead>
        <tbody>
          {patches.map((p, i) => {
            const cmdData = parsePatchData(p.command)
            const detail = typeof cmdData === "object" && Object.keys(cmdData).length > 0
              ? JSON.stringify(cmdData)
              : "—"
            return (
              <tr key={i} className="border-b border-default last:border-0">
                <td className="px-3 py-2 text-muted tabular-nums">{i + 1}</td>
                <td className="px-3 py-2 text-primary font-mono">{p.targetObject || "—"}</td>
                <td className="px-3 py-2 text-muted font-mono text-[11px] truncate max-w-[400px]" title={detail}>
                  {detail}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function GenericSection({ patches }: { patches: GSOPatchDetail[] }) {
  return (
    <div className="space-y-2">
      {patches.map((p, i) => {
        const patchData = parsePatchData(p.patch)
        const cmdData = parsePatchData(p.command)
        const content = typeof patchData === "object" && Object.keys(patchData).length > 0
          ? JSON.stringify(patchData, null, 2)
          : typeof cmdData === "object" && Object.keys(cmdData).length > 0
            ? JSON.stringify(cmdData, null, 2)
            : null
        return (
          <div key={i} className="rounded-lg border border-default bg-elevated/30 p-3">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-xs font-medium text-primary">{p.patchType}</span>
              {p.targetObject && (
                <span className="text-xs text-muted font-mono">→ {p.targetObject}</span>
              )}
            </div>
            {content && (
              <pre className="text-[11px] font-mono text-muted/80 whitespace-pre-wrap mt-1">{content}</pre>
            )}
          </div>
        )
      })}
    </div>
  )
}

function SectionRenderer({ sectionKey, patches }: { sectionKey: string; patches: GSOPatchDetail[] }) {
  switch (sectionKey) {
    case "example_sqls":
      return <ExampleSqlsSection patches={patches} />
    case "sql_expressions":
      return <SqlExpressionsSection patches={patches} />
    case "descriptions":
      return <DescriptionsSection patches={patches} />
    case "joins":
      return <JoinsSection patches={patches} />
    default:
      return <GenericSection patches={patches} />
  }
}

export function ProactiveEnrichmentView({ patches }: ProactiveEnrichmentViewProps) {
  const grouped = SECTIONS.map((section) => ({
    ...section,
    patches: patches.filter((p) => section.types.includes(p.patchType)),
  })).filter((s) => s.patches.length > 0)

  if (grouped.length === 0) {
    return <p className="text-xs text-muted py-2">No proactive enrichments applied</p>
  }

  return (
    <div className="space-y-4">
      <p className="text-xs font-medium text-muted">Changes</p>
      {grouped.map((section) => (
        <div key={section.key} className="space-y-1.5">
          <div className="flex items-center gap-2">
            <h4 className="text-xs font-semibold text-primary">{section.label}</h4>
            <span className="text-[10px] text-muted tabular-nums">({section.patches.length})</span>
          </div>
          <div className="rounded-lg border border-default overflow-hidden">
            <SectionRenderer sectionKey={section.key} patches={section.patches} />
          </div>
        </div>
      ))}
    </div>
  )
}
