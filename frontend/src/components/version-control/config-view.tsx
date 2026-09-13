import { AccordionItem } from '@/components/ui/accordion'
import { Badge, type BadgeProps } from '@/components/ui/badge'
import { SqlCodeBlock } from '@/components/SqlCodeBlock'

// A structured, human-readable view of a version's serialized_space (Genie schema v2). Each
// schema concept gets its own clearly-labeled section: data_sources split into Tables and
// Metric views; instructions.{text_instructions, join_specs, sql_snippets.{measures,
// expressions, filters}, sql_functions, example_question_sqls}; sample_questions
// (config/root); and benchmarks.questions. The snapshot type is `unknown`, so every read is
// defensive: unknown keys are preserved under "Other", empty sections are omitted, and the
// raw JSON stays available behind a disclosure for power users.

function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {}
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function text(value: unknown): string {
  if (value == null) return ''
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  // Descriptions and similar fields are often string arrays; render them as plain text
  // rather than a JSON dump. Objects still fall back to JSON.
  if (Array.isArray(value)) return value.map(text).filter(Boolean).join(' ')
  return JSON.stringify(value)
}

function firstText(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = text(record[key])
    if (value) return value
  }
  return ''
}

// text_instructions entries may be a plain string or { content: string[] } — the API
// concatenates content elements (each ends with \n) into one instruction.
function instructionText(value: unknown): string {
  if (typeof value === 'string') return value
  const record = asObject(value)
  if (Array.isArray(record.content)) return record.content.map(text).join('')
  return text(value)
}

// "catalog.schema.table" -> { name: "table", qualifier: "catalog.schema" }. Keeps the
// short table name prominent and the fully-qualified prefix available for context/hover.
function splitIdentifier(id: string): { name: string; qualifier: string } {
  const i = id.lastIndexOf('.')
  return i < 0 ? { name: id, qualifier: '' } : { name: id.slice(i + 1), qualifier: id.slice(0, i) }
}

const RELATIONSHIP_LABELS: Record<string, string> = {
  ONE_TO_ONE: 'One-to-one',
  ONE_TO_MANY: 'One-to-many',
  MANY_TO_ONE: 'Many-to-one',
  MANY_TO_MANY: 'Many-to-many',
}

// join_specs[].sql is a two-element array: (1) the boolean condition, (2) a relationship
// marker like "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--". Lift the marker into a friendly
// cardinality label and return the condition on its own (backticks stripped for reading) so
// the raw "--rt=…--" never leaks into the displayed SQL.
function parseJoinSql(sqlArr: unknown[]): { relationship: string | null; condition: string } {
  const parts = sqlArr.map(text).filter(Boolean)
  let relationship: string | null = null
  const conds: string[] = []
  for (const part of parts) {
    const match = part.match(/--rt=(?:FROM_RELATIONSHIP_TYPE_)?([A-Z_]+?)--/)
    if (match) relationship = RELATIONSHIP_LABELS[match[1]] ?? match[1].replace(/_/g, ' ').toLowerCase()
    else conds.push(part)
  }
  return { relationship, condition: conds.join('\n').replace(/`/g, '') }
}

const box = 'rounded-md border border-default bg-surface p-2'

function Section({ id, title, count, children }: { id?: string; title: string; count?: number; children: React.ReactNode }) {
  return (
    <section id={id} className="space-y-2 scroll-mt-2">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-secondary">
        {title}{typeof count === 'number' ? <span className="ml-1 text-muted">({count})</span> : null}
      </h4>
      {children}
    </section>
  )
}

// Attribute badges surface the schema's per-column prompt-matching flags at a glance
// (native title = the explanation; a hover Tooltip would not render in a review/search).
function ColumnBadges({ col }: { col: Record<string, unknown> }) {
  const badge = (label: string, title: string, variant: BadgeProps['variant']) => (
    <Badge variant={variant} className="px-1.5 py-0 text-[10px] font-medium" title={title}>{label}</Badge>
  )
  return (
    <>
      {col.exclude === true && badge('Excluded', 'Hidden from Genie', 'secondary')}
      {col.enable_entity_matching === true && badge('Entity match', 'Matches user terms to actual column values', 'info')}
      {col.enable_format_assistance === true && badge('Format assist', 'Shows representative values to help Genie understand formats', 'info')}
      {col.get_example_values === true && badge('Example values', 'Fetches sample values from the column', 'secondary')}
      {col.build_value_dictionary === true && badge('Value dict', 'Builds a dictionary of distinct values for matching', 'secondary')}
    </>
  )
}

// One table / metric view as a collapsible card: identifier + description + a column-count
// summary in the header; columns (with attribute badges, description, synonyms) revealed on
// expansion. Small tables open by default; large ones collapse to tame the wall of columns.
// AccordionItem keeps the body in the DOM when collapsed, so it stays searchable.
function DataSourceCard({ entry, index }: { entry: unknown; index: number }) {
  const table = asObject(entry)
  const columns = asArray(table.column_configs)
  const identifier = firstText(table, ['identifier', 'table', 'name']) || `Data source ${index + 1}`
  const description = firstText(table, ['description'])
  const excludedCount = columns.filter(c => asObject(c).exclude === true).length
  const title = (
    <div className="min-w-0">
      <p className="font-mono text-xs text-secondary break-all">{identifier}</p>
      {description && <p className="text-xs text-muted break-words font-normal">{description}</p>}
    </div>
  )
  const action = (
    <span className="text-xs text-muted whitespace-nowrap">
      {columns.length} col{columns.length === 1 ? '' : 's'}{excludedCount ? ` · ${excludedCount} excl` : ''}
    </span>
  )
  return (
    <AccordionItem title={title} action={action} defaultOpen={columns.length > 0 && columns.length <= 8}>
      {columns.length > 0 ? (
        <ul className="space-y-1.5">
          {columns.map((column, ci) => {
            const col = asObject(column)
            const name = firstText(col, ['column_name', 'name'])
            const colDesc = firstText(col, ['description'])
            const synonyms = asArray(col.synonyms).map(text).filter(Boolean)
            return (
              <li key={ci} className="border-t border-default/50 pt-1.5 first:border-t-0 first:pt-0">
                <div className="flex items-center gap-1.5 flex-wrap">
                  <span className="font-mono text-xs text-secondary break-all">{name}</span>
                  <ColumnBadges col={col} />
                </div>
                {colDesc && <p className="mt-0.5 text-xs text-muted break-words">{colDesc}</p>}
                {synonyms.length > 0 && <p className="mt-0.5 text-[11px] text-muted break-words">Synonyms: {synonyms.join(', ')}</p>}
              </li>
            )
          })}
        </ul>
      ) : (
        <p className="text-xs text-muted">No column configuration.</p>
      )}
    </AccordionItem>
  )
}

// A sql_snippet (measure / expression / filter): label, SQL, and its optional API metadata
// (synonyms, instruction, comment).
function SqlEntry({ record, index, labelKeys }: { record: Record<string, unknown>; index: number; labelKeys: string[] }) {
  const label = firstText(record, labelKeys) || `#${index + 1}`
  const sql = firstText(record, ['sql', 'answer', 'query'])
  const synonyms = asArray(record.synonyms).map(text).filter(Boolean)
  const instruction = firstText(record, ['instruction'])
  const comment = firstText(record, ['comment'])
  return (
    <div className={`${box} space-y-1`}>
      <p className="text-xs text-secondary break-words">{label}</p>
      {sql ? <SqlCodeBlock code={sql} maxLines={8} format label={label} /> : null}
      {synonyms.length > 0 && <p className="text-xs text-muted break-words">Synonyms: {synonyms.join(', ')}</p>}
      {instruction && <p className="text-xs text-muted break-words">{instruction}</p>}
      {comment && <p className="text-xs text-muted break-words">{comment}</p>}
    </div>
  )
}

// An example_question_sqls entry: question, SQL, bound parameters, and usage guidance.
function ExampleSqlEntry({ record, index }: { record: Record<string, unknown>; index: number }) {
  const question = firstText(record, ['question', 'display_name', 'id']) || `#${index + 1}`
  const sql = firstText(record, ['sql'])
  const parameters = asArray(record.parameters)
  const usage = firstText(record, ['usage_guidance'])
  return (
    <div className={`${box} space-y-1`}>
      <p className="text-xs text-secondary break-words">{question}</p>
      {sql ? <SqlCodeBlock code={sql} maxLines={8} format label="Example SQL" /> : null}
      {parameters.length > 0 && (
        <div className="space-y-0.5">
          <p className="text-[11px] uppercase tracking-wide text-muted">Parameters</p>
          {parameters.map((param, pi) => {
            const p = asObject(param)
            const name = firstText(p, ['name'])
            const typeHint = firstText(p, ['type_hint', 'type'])
            const desc = firstText(p, ['description'])
            const def = firstText(asObject(p.default_value), ['values']) || firstText(p, ['default_value'])
            return (
              <p key={pi} className="text-xs text-muted break-words">
                <span className="font-mono text-secondary break-all">{name}</span>
                {typeHint ? ` · ${typeHint}` : ''}
                {desc ? ` — ${desc}` : ''}
                {def ? ` (default: ${def})` : ''}
              </p>
            )
          })}
        </div>
      )}
      {usage && <p className="text-xs text-muted break-words">Usage: {usage}</p>}
    </div>
  )
}

// A sql_functions entry: UC identifier + description (no SQL body in the config).
function FunctionEntry({ record, index }: { record: Record<string, unknown>; index: number }) {
  const identifier = firstText(record, ['identifier', 'name', 'id']) || `#${index + 1}`
  const description = firstText(record, ['description'])
  return (
    <div className={`${box} space-y-0.5`}>
      <p className="font-mono text-xs text-secondary break-all">{identifier}</p>
      {description && <p className="text-xs text-muted break-words">{description}</p>}
    </div>
  )
}

// A join_specs entry: two tables + cardinality + the ON condition. Shows the short table
// names prominently (full identifier on hover), the shared catalog.schema once, a relationship
// badge lifted from the "--rt=…--" marker, and the condition as a compact inline snippet —
// the one-line predicate does not warrant the full SqlCodeBlock chrome.
function JoinEntry({ record, index }: { record: Record<string, unknown>; index: number }) {
  const left = firstText(asObject(record.left), ['identifier', 'alias'])
  const right = firstText(asObject(record.right), ['identifier', 'alias'])
  const l = splitIdentifier(left)
  const r = splitIdentifier(right)
  const { relationship, condition } = parseJoinSql(asArray(record.sql))
  const note = firstText(record, ['comment', 'instruction'])
  const sharedQualifier = l.qualifier && l.qualifier === r.qualifier ? l.qualifier : ''
  return (
    <div className={`${box} space-y-2 min-w-0`}>
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <span className="font-mono text-xs font-medium text-primary break-all" title={left}>{l.name || `Join ${index + 1}`}</span>
        <span className="text-muted">⋈</span>
        {r.name && <span className="font-mono text-xs font-medium text-primary break-all" title={right}>{r.name}</span>}
        {relationship && <Badge variant="info" className="px-1.5 py-0 text-[10px] font-medium">{relationship}</Badge>}
      </div>
      {sharedQualifier
        ? <p className="text-[11px] text-muted break-all">{sharedQualifier}</p>
        : (l.qualifier || r.qualifier) && <p className="text-[11px] text-muted break-all">{[l.qualifier, r.qualifier].filter(Boolean).join(' · ')}</p>}
      {condition && <code className="block w-fit max-w-full overflow-x-auto rounded bg-surface-secondary px-2 py-1 font-mono text-xs text-secondary">{condition}</code>}
      {note && <p className="text-xs text-muted break-words">{note}</p>}
    </div>
  )
}

// A benchmarks.questions entry: the evaluation question + its expected SQL answer.
function BenchmarkEntry({ record, index }: { record: Record<string, unknown>; index: number }) {
  const question = firstText(record, ['question', 'id']) || `#${index + 1}`
  const answer = asObject(asArray(record.answer)[0])
  const sql = firstText(answer, ['content', 'sql'])
  return (
    <div className={`${box} space-y-1`}>
      <p className="text-xs text-secondary break-words">{question}</p>
      {sql ? <SqlCodeBlock code={sql} maxLines={8} format label="Expected SQL answer" /> : null}
    </div>
  )
}

export function ConfigView({ snapshot }: { snapshot: unknown }) {
  const root = asObject(snapshot)
  const dataSources = root.data_sources
  const tables = Array.isArray(dataSources) ? dataSources : asArray(asObject(dataSources).tables)
  const metricViews = Array.isArray(dataSources) ? [] : asArray(asObject(dataSources).metric_views)

  const instructions = asObject(root.instructions)
  const textInstructions = (asArray(instructions.text_instructions).length
    ? asArray(instructions.text_instructions)
    : asArray(root.text_instructions))
  const exampleSqls = asArray(instructions.example_question_sqls)
  const joins = asArray(instructions.join_specs)
  const snippets = asObject(instructions.sql_snippets)
  const filters = asArray(snippets.filters)
  const expressions = asArray(snippets.expressions)
  const measures = asArray(snippets.measures)
  const sqlFunctions = asArray(instructions.sql_functions)
  const sampleQuestions = (asArray(root.sample_questions).length
    ? asArray(root.sample_questions)
    : asArray(instructions.sample_questions).length
      ? asArray(instructions.sample_questions)
      : asArray(asObject(root.config).sample_questions))
  const benchmarkQuestions = asArray(asObject(root.benchmarks).questions)

  const metadataEntries = Object.entries(root).filter(
    ([key, value]) =>
      !['data_sources', 'instructions', 'text_instructions', 'sample_questions', 'config', 'benchmarks'].includes(key) &&
      (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean'),
  )
  const handled = new Set(['data_sources', 'instructions', 'text_instructions', 'sample_questions', 'config', 'benchmarks', ...metadataEntries.map(([key]) => key)])
  const otherEntries = Object.entries(root).filter(([key]) => !handled.has(key))

  // Declarative section list — the single source of truth for BOTH the jump-nav and the
  // rendered sections, so they can never drift. Only present sections are pushed, in order.
  type SectionDef = { id: string; label: string; count?: number; node: React.ReactNode }
  const defs: SectionDef[] = []
  if (tables.length > 0) defs.push({ id: 'tables', label: 'Tables', count: tables.length, node: (
    <div className="space-y-2">{tables.map((entry, index) => <DataSourceCard key={index} entry={entry} index={index} />)}</div>
  ) })
  if (metricViews.length > 0) defs.push({ id: 'metric-views', label: 'Metric views', count: metricViews.length, node: (
    <div className="space-y-2">{metricViews.map((entry, index) => <DataSourceCard key={index} entry={entry} index={index} />)}</div>
  ) })
  if (textInstructions.length > 0) defs.push({ id: 'instructions', label: 'Instructions', node: (
    <div className="space-y-2">{textInstructions.map((entry, index) => (
      <pre key={index} className={`${box} max-h-64 overflow-auto whitespace-pre-wrap break-words text-xs text-secondary`}>{instructionText(entry)}</pre>
    ))}</div>
  ) })
  if (measures.length > 0) defs.push({ id: 'measures', label: 'Measures', count: measures.length, node: (
    <div className="space-y-2">{measures.map((entry, index) => <SqlEntry key={index} record={asObject(entry)} index={index} labelKeys={['display_name', 'name', 'id']} />)}</div>
  ) })
  if (expressions.length > 0) defs.push({ id: 'expressions', label: 'Expressions', count: expressions.length, node: (
    <div className="space-y-2">{expressions.map((entry, index) => <SqlEntry key={index} record={asObject(entry)} index={index} labelKeys={['display_name', 'name', 'id']} />)}</div>
  ) })
  if (filters.length > 0) defs.push({ id: 'filters', label: 'Filters', count: filters.length, node: (
    <div className="space-y-2">{filters.map((entry, index) => <SqlEntry key={index} record={asObject(entry)} index={index} labelKeys={['display_name', 'id']} />)}</div>
  ) })
  if (joins.length > 0) defs.push({ id: 'joins', label: 'Joins', count: joins.length, node: (
    <div className="space-y-2">{joins.map((entry, index) => <JoinEntry key={index} record={asObject(entry)} index={index} />)}</div>
  ) })
  if (sqlFunctions.length > 0) defs.push({ id: 'sql-functions', label: 'SQL functions', count: sqlFunctions.length, node: (
    <div className="space-y-2">{sqlFunctions.map((entry, index) => <FunctionEntry key={index} record={asObject(entry)} index={index} />)}</div>
  ) })
  if (exampleSqls.length > 0) defs.push({ id: 'example-sql', label: 'Example SQL', count: exampleSqls.length, node: (
    <div className="space-y-2">{exampleSqls.map((entry, index) => <ExampleSqlEntry key={index} record={asObject(entry)} index={index} />)}</div>
  ) })
  if (sampleQuestions.length > 0) defs.push({ id: 'sample-questions', label: 'Sample questions', count: sampleQuestions.length, node: (
    <ul className="space-y-1">{sampleQuestions.map((entry, index) => (
      <li key={index} className="text-xs text-secondary">{firstText(asObject(entry), ['question', 'text']) || text(entry)}</li>
    ))}</ul>
  ) })
  if (benchmarkQuestions.length > 0) defs.push({ id: 'benchmarks', label: 'Benchmarks', count: benchmarkQuestions.length, node: (
    <div className="space-y-2">{benchmarkQuestions.map((entry, index) => <BenchmarkEntry key={index} record={asObject(entry)} index={index} />)}</div>
  ) })
  if (metadataEntries.length > 0) defs.push({ id: 'metadata', label: 'Metadata', node: (
    <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">{metadataEntries.map(([key, value]) => (
      <div key={key} className="contents"><dt className="text-muted">{key}</dt><dd className="text-secondary break-words">{text(value)}</dd></div>
    ))}</dl>
  ) })
  if (otherEntries.length > 0) defs.push({ id: 'other', label: 'Other', node: (
    <pre className={`${box} max-h-64 overflow-auto whitespace-pre-wrap break-words text-xs text-muted`}>{JSON.stringify(Object.fromEntries(otherEntries), null, 2)}</pre>
  ) })

  const showNav = defs.length > 1
  const scrollToSection = (id: string) => {
    document.getElementById(`vc-cfg-${id}`)?.scrollIntoView?.({ behavior: 'smooth', block: 'start' })
  }

  return (
    <div className={showNav ? 'xl:grid xl:grid-cols-[168px_minmax(0,1fr)] xl:gap-4' : undefined}>
      {showNav && (
        <nav aria-label="Config sections" className="mb-3 flex flex-wrap gap-1.5 xl:mb-0 xl:flex-col xl:flex-nowrap xl:sticky xl:top-0 xl:self-start">
          {defs.map(d => (
            <button
              key={d.id}
              type="button"
              onClick={() => scrollToSection(d.id)}
              className="rounded-md border border-default bg-surface px-2 py-1 text-left text-xs text-secondary hover:bg-surface-secondary transition-colors"
            >
              {d.label}{typeof d.count === 'number' ? <span className="ml-1 text-muted">({d.count})</span> : null}
            </button>
          ))}
        </nav>
      )}

      <div className="space-y-4 min-w-0">
        {defs.map(d => (
          <Section key={d.id} id={`vc-cfg-${d.id}`} title={d.label} count={d.count}>{d.node}</Section>
        ))}

        {defs.length === 0 && (
          <p className="text-xs text-muted">No structured configuration in this snapshot.</p>
        )}

        <details className="rounded-lg border border-default bg-surface-secondary/40">
          <summary className="cursor-pointer select-none px-3 py-2 text-xs font-medium text-secondary">
            View raw JSON
          </summary>
          <pre className="max-h-80 overflow-auto px-3 pb-3 text-xs text-muted whitespace-pre-wrap break-words">
            {JSON.stringify(snapshot, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  )
}
