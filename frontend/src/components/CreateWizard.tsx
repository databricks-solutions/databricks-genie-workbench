import { useEffect, useState } from "react"
import { ArrowLeft, ArrowRight, Check, X } from "lucide-react"
import {
  discoverCatalogs,
  discoverSchemas,
  discoverTables,
  validateSpaceConfig,
  createWizardSpace,
} from "@/lib/api"
import type { UcTable, ValidateConfigResponse, CreateWizardSpaceResponse } from "@/types"

const STEPS = ["Requirements", "Data Sources", "Sample Questions", "Instructions", "Review", "Create"]

interface WizardState {
  name: string
  description: string
  audience: string
  tables: UcTable[]
  sampleQuestions: string[]
  textInstructions: string
  sqlExpressions: unknown[]
  sqlQueries: unknown[]
}

const EMPTY: WizardState = {
  name: "",
  description: "",
  audience: "",
  tables: [],
  sampleQuestions: Array(5).fill(""),
  textInstructions: "",
  sqlExpressions: [],
  sqlQueries: [],
}

interface CreateWizardProps {
  onCreated: (spaceId: string, displayName: string) => void
}

export function CreateWizard({ onCreated }: CreateWizardProps) {
  const [step, setStep] = useState(0)
  const [state, setState] = useState<WizardState>(EMPTY)
  const [validation, setValidation] = useState<ValidateConfigResponse | null>(null)
  const [creating, setCreating] = useState(false)
  const [result, setResult] = useState<CreateWizardSpaceResponse | null>(null)

  // UC browser state
  const [catalogs, setCatalogs] = useState<string[]>([])
  const [schemas, setSchemas] = useState<string[]>([])
  const [browseCatalog, setBrowseCatalog] = useState("")
  const [browseSchema, setBrowseSchema] = useState("")
  const [browseTables, setBrowseTables] = useState<UcTable[]>([])
  const [loadingCatalogs, setLoadingCatalogs] = useState(false)
  const [loadingSchemas, setLoadingSchemas] = useState(false)
  const [loadingTables, setLoadingTables] = useState(false)
  const [tableSearch, setTableSearch] = useState("")

  // Load catalogs when entering step 1
  useEffect(() => {
    if (step !== 1 || catalogs.length > 0) return
    setLoadingCatalogs(true)
    discoverCatalogs()
      .then((r) => setCatalogs(r.catalogs.map((c) => c.name)))
      .catch(console.error)
      .finally(() => setLoadingCatalogs(false))
  }, [step, catalogs.length])

  // Load schemas when catalog changes
  useEffect(() => {
    if (!browseCatalog) return
    setBrowseSchema("")
    setBrowseTables([])
    setLoadingSchemas(true)
    discoverSchemas(browseCatalog)
      .then((r) => setSchemas(r.schemas.map((s) => s.name)))
      .catch(console.error)
      .finally(() => setLoadingSchemas(false))
  }, [browseCatalog])

  // Load tables when schema changes
  useEffect(() => {
    if (!browseCatalog || !browseSchema) return
    setBrowseTables([])
    setTableSearch("")
    setLoadingTables(true)
    discoverTables(browseCatalog, browseSchema)
      .then((r) => setBrowseTables(r.tables))
      .catch(console.error)
      .finally(() => setLoadingTables(false))
  }, [browseCatalog, browseSchema])

  const toggleTable = (t: UcTable) => {
    setState((s) => {
      const exists = s.tables.some((x) => x.full_name === t.full_name)
      return {
        ...s,
        tables: exists ? s.tables.filter((x) => x.full_name !== t.full_name) : [...s.tables, t],
      }
    })
  }

  const isSelected = (t: UcTable) => state.tables.some((x) => x.full_name === t.full_name)

  const uuid = () => crypto.randomUUID().replace(/-/g, "")

  const buildConfig = () => ({
    version: 2,
    data_sources: { tables: state.tables.map((t) => ({ identifier: t.full_name })) },
    instructions: {
      text_instructions: state.textInstructions
        ? [{ id: uuid(), content: [state.textInstructions] }]
        : [],
      example_question_sqls: [
        ...state.sampleQuestions.filter(Boolean).map((q) => ({ id: uuid(), question: [q] })),
        ...state.sqlQueries,
      ],
      sql_snippets: { expressions: state.sqlExpressions, measures: [], filters: [] },
    },
  })

  const validate = async () => {
    try {
      const r = await validateSpaceConfig(buildConfig())
      setValidation(r)
      return r.valid
    } catch (e: unknown) {
      alert(`Validation failed: ${e instanceof Error ? e.message : String(e)}`)
      return false
    }
  }

  const create = async () => {
    setCreating(true)
    try {
      const r = await createWizardSpace({
        display_name: state.name,
        serialized_space: buildConfig(),
      })
      setResult(r)
      setStep(5)
    } catch (e: unknown) {
      alert(`Failed: ${e instanceof Error ? e.message : String(e)}`)
    } finally {
      setCreating(false)
    }
  }

  const filteredTables = browseTables.filter((t) =>
    t.name.toLowerCase().includes(tableSearch.toLowerCase())
  )

  return (
    <div className="max-w-2xl mx-auto">
      {/* Step indicator */}
      <div className="flex items-center gap-1 mb-8">
        {STEPS.map((s, i) => (
          <div key={s} className="flex items-center">
            <div
              className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-medium ${
                i < step
                  ? "bg-accent text-white"
                  : i === step
                  ? "bg-accent/10 text-accent border-2 border-accent"
                  : "bg-elevated text-muted"
              }`}
            >
              {i < step ? <Check className="w-3.5 h-3.5" /> : i + 1}
            </div>
            {i < STEPS.length - 1 && (
              <div className={`w-8 h-0.5 ${i < step ? "bg-accent" : "bg-[var(--border-color)]"}`} />
            )}
          </div>
        ))}
      </div>

      <h2 className="text-xl font-bold mb-6 text-primary">{STEPS[step]}</h2>

      {/* Step 0: Requirements */}
      {step === 0 && (
        <div className="space-y-4">
          <div>
            <label className="text-sm font-medium block mb-1 text-secondary">Space Name *</label>
            <input
              value={state.name}
              onChange={(e) => setState((s) => ({ ...s, name: e.target.value }))}
              className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary"
              placeholder="e.g. Sales Analytics"
            />
          </div>
          <div>
            <label className="text-sm font-medium block mb-1 text-secondary">Description</label>
            <textarea
              value={state.description}
              onChange={(e) => setState((s) => ({ ...s, description: e.target.value }))}
              className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary resize-none h-20"
            />
          </div>
          <div>
            <label className="text-sm font-medium block mb-1 text-secondary">Intended Audience</label>
            <input
              value={state.audience}
              onChange={(e) => setState((s) => ({ ...s, audience: e.target.value }))}
              className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary"
              placeholder="e.g. Sales team, Finance analysts"
            />
          </div>
        </div>
      )}

      {/* Step 1: Data Sources — UC table browser */}
      {step === 1 && (
        <div>
          {state.tables.length > 0 && (
            <div className="mb-5">
              <p className="text-sm font-medium mb-2 text-secondary">
                {state.tables.length} table{state.tables.length !== 1 ? "s" : ""} selected
              </p>
              <div className="flex flex-wrap gap-2">
                {state.tables.map((t) => (
                  <span
                    key={t.full_name}
                    className="flex items-center gap-1 pl-2.5 pr-1.5 py-1 bg-accent/10 text-accent text-xs rounded-full font-mono"
                  >
                    {t.full_name}
                    <button onClick={() => toggleTable(t)} className="ml-0.5 hover:opacity-70">
                      <X className="w-3 h-3" />
                    </button>
                  </span>
                ))}
              </div>
            </div>
          )}

          <div className="space-y-3">
            <div>
              <label className="text-xs font-medium text-muted uppercase tracking-wide block mb-1">
                Catalog
              </label>
              <select
                value={browseCatalog}
                onChange={(e) => setBrowseCatalog(e.target.value)}
                className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary"
              >
                <option value="">{loadingCatalogs ? "Loading..." : "Select a catalog"}</option>
                {catalogs.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </div>

            {browseCatalog && (
              <div>
                <label className="text-xs font-medium text-muted uppercase tracking-wide block mb-1">
                  Schema
                </label>
                <select
                  value={browseSchema}
                  onChange={(e) => setBrowseSchema(e.target.value)}
                  className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary"
                >
                  <option value="">{loadingSchemas ? "Loading..." : "Select a schema"}</option>
                  {schemas.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>
            )}

            {browseSchema && (
              <div>
                <label className="text-xs font-medium text-muted uppercase tracking-wide block mb-1">
                  Tables {filteredTables.length > 0 && `(${filteredTables.length})`}
                </label>
                <input
                  value={tableSearch}
                  onChange={(e) => setTableSearch(e.target.value)}
                  placeholder="Filter tables..."
                  className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary mb-2"
                />
                {loadingTables ? (
                  <div className="text-sm text-muted py-6 text-center border border-default rounded-lg">
                    Loading tables...
                  </div>
                ) : filteredTables.length === 0 ? (
                  <div className="text-sm text-muted py-6 text-center border border-default rounded-lg">
                    No tables found
                  </div>
                ) : (
                  <div className="max-h-64 overflow-y-auto border border-default rounded-lg divide-y divide-[var(--border-color)]">
                    {filteredTables.map((t) => (
                      <label
                        key={t.full_name}
                        className="flex items-center gap-3 px-3 py-2.5 hover:bg-elevated cursor-pointer"
                      >
                        <input
                          type="checkbox"
                          checked={isSelected(t)}
                          onChange={() => toggleTable(t)}
                          className="w-4 h-4 shrink-0"
                        />
                        <div className="min-w-0 flex-1">
                          <span className="text-sm font-mono block truncate text-primary">{t.name}</span>
                          {t.comment && (
                            <span className="text-xs text-muted block truncate">{t.comment}</span>
                          )}
                        </div>
                        {t.table_type && (
                          <span className="text-xs text-muted shrink-0">
                            {t.table_type.replace("TableType.", "")}
                          </span>
                        )}
                      </label>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Step 2: Sample Questions */}
      {step === 2 && (
        <div className="space-y-2">
          <p className="text-sm text-muted mb-3">
            Add 5–20 natural language questions this space should answer.
          </p>
          {state.sampleQuestions.map((q, i) => (
            <input
              key={i}
              value={q}
              onChange={(e) =>
                setState((s) => ({
                  ...s,
                  sampleQuestions: s.sampleQuestions.map((v, j) =>
                    j === i ? e.target.value : v
                  ),
                }))
              }
              className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary"
              placeholder={`Question ${i + 1}`}
            />
          ))}
          <button
            onClick={() => setState((s) => ({ ...s, sampleQuestions: [...s.sampleQuestions, ""] }))}
            className="text-sm text-accent hover:underline mt-2"
          >
            + Add question
          </button>
        </div>
      )}

      {/* Step 3: Instructions */}
      {step === 3 && (
        <div className="space-y-4">
          <div>
            <label className="text-sm font-medium block mb-1 text-secondary">
              Text Instructions (≤500 chars)
            </label>
            <textarea
              value={state.textInstructions}
              onChange={(e) => setState((s) => ({ ...s, textInstructions: e.target.value }))}
              className="w-full border border-default rounded-lg px-3 py-2 text-sm bg-surface text-primary resize-none h-28"
              placeholder="Guide Genie on business terminology, preferred metrics, time conventions..."
            />
            <p className="text-xs text-muted mt-1">{state.textInstructions.length}/500 chars</p>
          </div>
        </div>
      )}

      {/* Step 4: Review + validate */}
      {step === 4 && (
        <div>
          <div className="mb-4 text-sm space-y-1">
            <div className="flex justify-between text-muted">
              <span>Space name</span>
              <span className="font-medium text-primary">{state.name}</span>
            </div>
            <div className="flex justify-between text-muted">
              <span>Tables</span>
              <span className="font-medium text-primary">{state.tables.length}</span>
            </div>
            <div className="flex justify-between text-muted">
              <span>Sample questions</span>
              <span className="font-medium text-primary">
                {state.sampleQuestions.filter(Boolean).length}
              </span>
            </div>
          </div>
          {validation && (
            <div className="mb-4 space-y-1.5">
              {validation.errors.map((e) => (
                <div key={e} className="text-red-500 text-sm">
                  ✗ {e}
                </div>
              ))}
              {validation.warnings.map((w) => (
                <div key={w} className="text-yellow-500 text-sm">
                  ⚠ {w}
                </div>
              ))}
              {validation.valid && (
                <div className="text-green-500 text-sm">✓ Configuration looks good</div>
              )}
            </div>
          )}
          <button
            onClick={validate}
            className="px-4 py-2 bg-elevated text-primary border border-default rounded-lg text-sm hover:bg-sunken"
          >
            Validate Config
          </button>
          <details className="mt-4">
            <summary className="text-sm text-muted cursor-pointer">
              Preview serialized_space JSON
            </summary>
            <pre className="text-xs mt-2 bg-elevated text-secondary p-3 rounded overflow-auto max-h-64">
              {JSON.stringify(buildConfig(), null, 2)}
            </pre>
          </details>
        </div>
      )}

      {/* Step 5: Success */}
      {step === 5 && result && (
        <div className="text-center py-8">
          <div className="w-12 h-12 rounded-full bg-green-500/10 flex items-center justify-center mx-auto mb-4">
            <Check className="w-6 h-6 text-green-500" />
          </div>
          <p className="font-semibold text-primary mb-2">Space Created!</p>
          <a
            href={result.space_url}
            target="_blank"
            rel="noreferrer"
            className="text-accent underline text-sm"
          >
            Open in Databricks
          </a>
          <br />
          <button
            onClick={() => onCreated(result.space_id, state.name)}
            className="mt-4 px-4 py-2 bg-accent text-white rounded-lg text-sm font-medium"
          >
            View Space →
          </button>
        </div>
      )}

      {/* Nav buttons */}
      {step < 5 && (
      <div className="flex justify-between mt-8">
        <button
          onClick={() => setStep((s) => Math.max(0, s - 1))}
          disabled={step === 0}
          className="flex items-center gap-1 px-4 py-2 border border-default rounded-lg text-sm text-secondary disabled:opacity-30"
        >
          <ArrowLeft className="w-4 h-4" /> Back
        </button>
        {step < 4 ? (
          <button
            onClick={() => setStep((s) => Math.min(4, s + 1))}
            disabled={(step === 0 && !state.name) || (step === 1 && state.tables.length === 0)}
            className="flex items-center gap-1 px-4 py-2 bg-accent text-white rounded-lg text-sm disabled:opacity-50"
          >
            Next <ArrowRight className="w-4 h-4" />
          </button>
        ) : (
          <button
            onClick={create}
            disabled={creating || validation?.valid === false}
            className="px-4 py-2 bg-accent text-white rounded-lg text-sm font-medium disabled:opacity-50"
          >
            {creating ? "Creating..." : "Create Space"}
          </button>
        )}
      </div>
      )}
    </div>
  )
}
