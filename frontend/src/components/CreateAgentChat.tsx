import { useState, useRef, useEffect, useCallback } from "react"
import {
  Send,
  Bot,
  User,
  Loader2,
  Wrench,
  ChevronDown,
  ChevronRight,
  Check,
  ExternalLink,
  AlertCircle,
  Sparkles,
  Copy,
  CheckCheck,
  Database,
  Table2,
  Server,
  FileText,
  MessageSquare,
  Settings,
  Rocket,
  X,
  Pencil,
  BarChart3,
  Plus,
  RotateCcw,
  Code2,
  Link2,
  ListChecks,
  Search,
  Trash2,
  FastForward,
  Clock,
  GitBranch,
} from "lucide-react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { streamAgentChat } from "@/lib/api"
import type { AgentChatMessage, AgentUIElement, AgentStep } from "@/types"

interface CreateAgentChatProps {
  onCreated: (spaceId: string, displayName: string) => void
}

let msgCounter = 0
const nextId = () => `msg-${++msgCounter}-${Date.now()}`

const TOOL_LABELS: Record<string, string> = {
  discover_catalogs: "Browsing catalogs",
  discover_schemas: "Browsing schemas",
  discover_tables: "Browsing tables",
  describe_table: "Inspecting table",
  assess_data_quality: "Assessing data quality",
  profile_table_usage: "Profiling table usage & lineage",
  profile_columns: "Profiling columns",
  test_sql: "Testing SQL",
  discover_warehouses: "Finding warehouses",
  present_plan: "Preparing plan for review",
  get_config_schema: "Fetching config schema",
  generate_config: "Generating config",
  update_config: "Updating config",
  validate_config: "Validating config",
  create_space: "Creating space",
  update_space: "Updating space",
}

const ELEMENT_ICONS: Record<string, typeof Database> = {
  catalog_selection: Database,
  schema_selection: Database,
  table_selection: Table2,
  warehouse_selection: Server,
}

const STEP_META = [
  { key: "requirements", label: "Requirements" },
  { key: "data_sources", label: "Data sources" },
  { key: "inspection", label: "Inspection" },
  { key: "plan", label: "Plan" },
  { key: "config_create", label: "Config" },
  { key: "post_creation", label: "Create" },
]

const COMBOBOX_THRESHOLD = 15

// ─── Build progress tracking ───────────────────────────────────

interface PlanSummary {
  questions: number
  benchmarks: number
  measures: number
  filters: number
  expressions: number
  exampleSqls: number
  joins: number
  textInstruction: boolean
}

interface BuildProgress {
  title: string
  description: string
  businessContext: string[]
  catalog: string
  schemas: string[]
  tables: string[]
  inspectionDone: boolean
  inspectionSummary: { qualityIssues: number; lineageCount: number; columnsProfiled: number }
  planReady: boolean
  planSummary: PlanSummary
  configReady: boolean
  config: Record<string, unknown> | null
  spaceId: string
  spaceUrl: string
  spaceDisplayName: string
}

const EMPTY_PLAN_SUMMARY: PlanSummary = { questions: 0, benchmarks: 0, measures: 0, filters: 0, expressions: 0, exampleSqls: 0, joins: 0, textInstruction: false }

const EMPTY_PROGRESS: BuildProgress = {
  title: "",
  description: "",
  businessContext: [],
  catalog: "",
  schemas: [],
  tables: [],
  inspectionDone: false,
  inspectionSummary: { qualityIssues: 0, lineageCount: 0, columnsProfiled: 0 },
  planReady: false,
  planSummary: { ...EMPTY_PLAN_SUMMARY },
  configReady: false,
  config: null,
  spaceId: "",
  spaceUrl: "",
  spaceDisplayName: "",
}

const STEPS = [
  { key: "requirements", label: "Requirements", Icon: FileText, backtrackMsg: "Let's go back to the requirements. I want to change the title or purpose." },
  { key: "data", label: "Data Sources", Icon: Database, backtrackMsg: "Let's go back to data selection. I want to change which tables to use." },
  { key: "inspection", label: "Data Inspection", Icon: Search, backtrackMsg: "Let's re-inspect the data. I want to review quality or lineage again." },
  { key: "plan", label: "Plan", Icon: ListChecks, backtrackMsg: "Let's go back to the plan. I want to adjust questions, instructions, or benchmarks." },
  { key: "config", label: "Configuration", Icon: Settings, backtrackMsg: "Let's revisit the configuration before creating the space." },
  { key: "create", label: "Create Space", Icon: Rocket, backtrackMsg: "" },
] as const

function currentStep(p: BuildProgress): number {
  if (p.spaceId) return 5
  if (p.configReady) return 4
  if (p.planReady) return 3
  if (p.inspectionDone) return 2
  if (p.tables.length > 0) return 1
  if (p.catalog || p.schemas.length > 0) return 0
  return 0
}

// ─── Editable plan ─────────────────────────────────────────────

interface EditablePlan {
  sample_questions: string[]
  text_instructions: string
  joins: Record<string, string>[]
  measures: Record<string, string>[]
  filters: Record<string, string>[]
  expressions: Record<string, string>[]
  example_sqls: Record<string, string>[]
  benchmarks: Record<string, string>[]
}

function planFromResult(result: Record<string, unknown>): EditablePlan {
  const s = (result.sections as Record<string, unknown[]>) || {}
  const tiArr = (s.text_instructions as string[]) || []
  return {
    sample_questions: [...((s.sample_questions as string[]) || [])],
    text_instructions: tiArr.join("\n"),
    joins: ((s.joins as Record<string, string>[]) || []).map((j) => ({ ...j })),
    measures: ((s.measures as Record<string, string>[]) || []).map((m) => ({ ...m })),
    filters: ((s.filters as Record<string, string>[]) || []).map((f) => ({ ...f })),
    expressions: ((s.expressions as Record<string, string>[]) || []).map((e) => ({ ...e })),
    example_sqls: ((s.example_sqls as Record<string, string>[]) || []).map((e) => ({ ...e })),
    benchmarks: ((s.benchmarks as Record<string, string>[]) || []).map((b) => ({ ...b })),
  }
}

// ─── Session persistence ───────────────────────────────────────

const STORAGE_KEY = "genie-create-session"

interface PersistedState {
  messages: AgentChatMessage[]
  sessionId: string | null
  progress: BuildProgress
  usedElements: string[]
  panelOpen: boolean
  editedPlan?: EditablePlan | null
}

function saveState(s: PersistedState) {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(s))
  } catch {
    // storage full or unavailable — silently ignore
  }
}

function loadState(): PersistedState | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as PersistedState
    // Migrate old schema (string) -> schemas (string[])
    const p = parsed.progress as any
    if (p && !Array.isArray(p.schemas)) {
      p.schemas = p.schema ? [p.schema] : []
      delete p.schema
    }
    // Migrate old sidebar fields to new BuildProgress shape
    if (p && !("inspectionDone" in p)) {
      p.inspectionDone = p.profilingDone ?? false
      p.inspectionSummary = p.inspectionSummary ?? { qualityIssues: 0, lineageCount: 0, columnsProfiled: 0 }
      p.businessContext = p.businessContext ?? []
      p.planReady = p.planReady ?? (p.sampleQuestions?.length > 0 || p.instructionCounts?.measures > 0)
      p.planSummary = p.planSummary ?? { ...EMPTY_PLAN_SUMMARY }
      delete p.profilingDone
      delete p.sampleQuestions
      delete p.instructionCounts
      delete p.benchmarks
    }
    // Migrate editedPlan: ensure benchmarks array exists
    if (parsed.editedPlan && !Array.isArray(parsed.editedPlan.benchmarks)) {
      parsed.editedPlan.benchmarks = []
    }
    // Migrate text_instructions from string[] to single string
    if (parsed.editedPlan && Array.isArray((parsed.editedPlan as any).text_instructions)) {
      parsed.editedPlan.text_instructions = ((parsed.editedPlan as any).text_instructions as string[]).join("\n")
    }
    // Reconstruct editedPlan from messages if missing
    if (!parsed.editedPlan && parsed.messages) {
      for (let i = parsed.messages.length - 1; i >= 0; i--) {
        const m = parsed.messages[i]
        if (m.role === "tool" && m.tool_name === "present_plan" && m.tool_result && !m.tool_result.error) {
          parsed.editedPlan = planFromResult(m.tool_result as Record<string, unknown>)
          break
        }
      }
    }
    return parsed
  } catch {
    return null
  }
}

// ─── Message grouping ──────────────────────────────────────────

const INSPECTION_TOOLS = new Set(["describe_table", "profile_columns", "assess_data_quality", "profile_table_usage"])

type RenderItem =
  | { type: "message"; msg: AgentChatMessage }
  | { type: "inspection_group"; msgs: AgentChatMessage[]; id: string }

function groupMessages(msgs: AgentChatMessage[]): RenderItem[] {
  const items: RenderItem[] = []
  let group: AgentChatMessage[] = []

  const flushGroup = () => {
    if (group.length >= 2) {
      items.push({ type: "inspection_group", msgs: [...group], id: `grp-${group[0].id}` })
    } else if (group.length === 1) {
      items.push({ type: "message", msg: group[0] })
    }
    group = []
  }

  for (const msg of msgs) {
    if (msg.role === "tool" && INSPECTION_TOOLS.has(msg.tool_name || "")) {
      group.push(msg)
    } else {
      flushGroup()
      items.push({ type: "message", msg })
    }
  }
  flushGroup()
  return items
}

// ─── Component ─────────────────────────────────────────────────

export function CreateAgentChat({ onCreated }: CreateAgentChatProps) {
  const restored = useRef(loadState())

  const [messages, setMessages] = useState<AgentChatMessage[]>(restored.current?.messages ?? [])
  const [input, setInput] = useState("")
  const [sessionId, setSessionId] = useState<string | null>(restored.current?.sessionId ?? null)
  const [isStreaming, setIsStreaming] = useState(false)
  const [expandedTools, setExpandedTools] = useState<Set<string>>(new Set())
  const [copiedConfig, setCopiedConfig] = useState(false)
  const [usedElements, setUsedElements] = useState<Set<string>>(
    new Set(restored.current?.usedElements ?? []),
  )
  const [multiSelections, setMultiSelections] = useState<Record<string, Set<string>>>({})
  const [progress, setProgress] = useState<BuildProgress>(restored.current?.progress ?? EMPTY_PROGRESS)
  const [panelOpen] = useState(true)
  const [editingTitle, setEditingTitle] = useState(false)
  const [titleDraft, setTitleDraft] = useState("")
  const [businessContextDraft, setBusinessContextDraft] = useState("")
  const [expandedPlanSections, setExpandedPlanSections] = useState<Set<string>>(new Set(["sample_questions"]))
  const [agentStatus, setAgentStatus] = useState<string | null>(null)
  const [agentStep, setAgentStep] = useState<AgentStep | null>(null)
  const [editedPlan, setEditedPlan] = useState<EditablePlan | null>(restored.current?.editedPlan ?? null)
  const [editingPlanItem, setEditingPlanItem] = useState<string | null>(null)
  const [autoPilot, setAutoPilot] = useState(false)
  const [elementSearch, setElementSearch] = useState<Record<string, string>>({})
  const [queuedMessage, setQueuedMessage] = useState<string | null>(null)

  const messagesEndRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLTextAreaElement>(null)
  const stopRef = useRef<(() => void) | null>(null)

  // Streaming message state — accumulate tokens in a ref and flush to React
  // state on an animation-frame schedule to keep renders at ~60 fps.
  const streamingContentRef = useRef("")
  const streamingMsgIdRef = useRef<string | null>(null)
  const streamingRafRef = useRef<number | null>(null)

  // Persist key state to sessionStorage on change
  useEffect(() => {
    saveState({
      messages,
      sessionId,
      progress,
      usedElements: Array.from(usedElements),
      panelOpen,
      editedPlan,
    })
  }, [messages, sessionId, progress, usedElements, panelOpen, editedPlan])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages, agentStatus])

  useEffect(() => {
    if (!isStreaming) inputRef.current?.focus()
  }, [isStreaming])

  const toggleTool = (id: string) => {
    setExpandedTools((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  // ─── Send message + SSE streaming ─────────────────────────────

  const sendMessage = useCallback(
    (text: string, selections?: Record<string, unknown>) => {
      if (!text.trim()) return
      if (isStreaming) {
        setQueuedMessage(text.trim())
        setInput("")
        return
      }

      const userMsg: AgentChatMessage = {
        id: nextId(),
        role: "user",
        content: text.trim(),
        timestamp: Date.now(),
      }
      setMessages((prev) => [...prev, userMsg])
      setInput("")
      setIsStreaming(true)

      let pendingToolCalls: AgentChatMessage[] = []

      const getStatusText = (tool: string, args: Record<string, unknown> | undefined): string => {
        const tableName = (args?.table as string)?.split(".").pop()
        const schema = args?.schema as string
        const catalog = args?.catalog as string
        switch (tool) {
          case "discover_catalogs": return "Discovering catalogs..."
          case "discover_schemas": return catalog ? `Browsing schemas in ${catalog}...` : "Discovering schemas..."
          case "discover_tables": return schema ? `Finding tables in ${schema}...` : "Discovering tables..."
          case "describe_table": return tableName ? `Inspecting ${tableName}...` : "Inspecting table..."
          case "assess_data_quality": return "Assessing data quality..."
          case "profile_table_usage": return "Checking table usage & lineage..."
          case "profile_columns": return tableName ? `Profiling ${tableName}...` : "Profiling data..."
          case "test_sql": return "Testing SQL..."
          case "discover_warehouses": return "Finding warehouses..."
          case "present_plan": return "Preparing plan..."
          case "get_config_schema": return "Fetching config schema..."
          case "generate_config": return "Generating configuration..."
          case "update_config": return "Updating configuration..."
          case "validate_config": return "Validating configuration..."
          case "create_space": return "Creating space..."
          case "update_space": return "Updating space..."
          default: return "Working..."
        }
      }

      const flushStreamingContent = () => {
        const id = streamingMsgIdRef.current
        const content = streamingContentRef.current
        if (!id) return
        setMessages((prev) =>
          prev.map((m) => (m.id === id ? { ...m, content } : m)),
        )
      }

      stopRef.current = streamAgentChat(text.trim(), sessionId, selections ?? null, {
        onSession: (sid) => setSessionId(sid),
        onStep: (step, label, index, total) => {
          setAgentStep({ step, label, index, total })
        },
        onThinking: (message, _step, _round) => {
          setAgentStatus(message)
        },
        onToolCall: (tool, args) => {
          setAgentStatus(getStatusText(tool, args))

          // Finalize any in-flight streaming message before showing tool calls
          if (streamingMsgIdRef.current) {
            if (streamingRafRef.current) {
              cancelAnimationFrame(streamingRafRef.current)
              streamingRafRef.current = null
            }
            const id = streamingMsgIdRef.current
            const content = streamingContentRef.current
            setMessages((prev) =>
              prev.map((m) => (m.id === id ? { ...m, content } : m)),
            )
            streamingContentRef.current = ""
            streamingMsgIdRef.current = null
          }

          const toolMsg: AgentChatMessage = {
            id: nextId(),
            role: "tool",
            content: TOOL_LABELS[tool] || tool,
            timestamp: Date.now(),
            tool_name: tool,
            tool_args: args,
          }
          pendingToolCalls.push(toolMsg)
          setMessages((prev) => [...prev, toolMsg])

          // Capture structured data from generate_config args for plan summary
          if (tool === "generate_config" && args) {
            const measures = (args.measures as unknown[])?.length ?? 0
            const filters = (args.filters as unknown[])?.length ?? 0
            const expressions = (args.expressions as unknown[])?.length ?? 0
            const exSqls = (args.example_sqls as unknown[])?.length ?? 0
            const hasText = !!args.text_instruction
            setProgress((p) => ({
              ...p,
              planSummary: { ...p.planSummary, measures, filters, expressions, exampleSqls: exSqls, textInstruction: hasText },
            }))
          }
        },
        onToolResult: (tool, result) => {
          let resolvedId = ""
          setMessages((prev) =>
            prev.map((m) => {
              if (m.role === "tool" && m.tool_name === tool && !m.tool_result) {
                resolvedId = m.id
                return { ...m, tool_result: result }
              }
              return m
            }),
          )

          if ((tool === "describe_table" || tool === "profile_columns" || tool === "assess_data_quality" || tool === "profile_table_usage") && !result.error) {
            setProgress((p) => {
              const updated = { ...p, inspectionDone: true }
              if (tool === "assess_data_quality") {
                const qs = (result as Record<string, unknown>).summary as { total_recommended_excludes?: number; total_recommended_review?: number } | undefined
                updated.inspectionSummary = {
                  ...p.inspectionSummary,
                  qualityIssues: (qs?.total_recommended_excludes ?? 0) + (qs?.total_recommended_review ?? 0),
                }
              }
              if (tool === "profile_table_usage") {
                const us = (result as Record<string, unknown>).summary as { tables_with_lineage?: number } | undefined
                updated.inspectionSummary = { ...updated.inspectionSummary, lineageCount: us?.tables_with_lineage ?? 0 }
              }
              if (tool === "profile_columns") {
                const profiles = (result as Record<string, unknown>).profiles as Record<string, unknown> | undefined
                updated.inspectionSummary = {
                  ...updated.inspectionSummary,
                  columnsProfiled: p.inspectionSummary.columnsProfiled + (profiles ? Object.keys(profiles).length : 0),
                }
              }
              return updated
            })
          }
          if (tool === "present_plan" && !result.error) {
            if (resolvedId) setExpandedTools((et) => new Set(et).add(resolvedId))
            const plan = planFromResult(result as Record<string, unknown>)
            setEditedPlan(plan)
            setEditingPlanItem(null)
            setProgress((p) => ({
              ...p,
              planReady: true,
              planSummary: {
                questions: plan.sample_questions.length,
                benchmarks: plan.benchmarks.length,
                measures: plan.measures.length,
                filters: plan.filters.length,
                expressions: plan.expressions.length,
                exampleSqls: plan.example_sqls.length,
                joins: plan.joins.length,
                textInstruction: plan.text_instructions.trim().length > 0,
              },
            }))
          }

          // Derive catalog/schema/tables from tool results so the
          // progress panel stays accurate even when the user changes
          // their mind via free-text instead of clicking UI buttons.
          if (tool === "describe_table" && result.table && !result.error) {
            const parts = (result.table as string).split(".")
            if (parts.length === 3) {
              const [cat, sch, _tbl] = parts
              const fullName = result.table as string
              setProgress((p) => ({
                ...p,
                catalog: cat,
                schema: sch,
                tables: p.tables.includes(fullName) ? p.tables : [...p.tables, fullName],
              }))
            }
          }

          // Track config generation
          if (tool === "generate_config" && result && "config" in result) {
            setProgress((p) => ({
              ...p,
              configReady: true,
              config: result.config as Record<string, unknown>,
            }))
          }

          setAgentStatus("Thinking...")
        },
        onMessageDelta: (token) => {
          setAgentStatus(null)
          streamingContentRef.current += token

          if (!streamingMsgIdRef.current) {
            const id = nextId()
            streamingMsgIdRef.current = id
            setMessages((prev) => [
              ...prev,
              { id, role: "assistant", content: token, timestamp: Date.now() },
            ])
          } else if (!streamingRafRef.current) {
            streamingRafRef.current = requestAnimationFrame(() => {
              flushStreamingContent()
              streamingRafRef.current = null
            })
          }
        },
        onMessage: (content, uiElements) => {
          pendingToolCalls = []
          setAgentStatus(null)

          if (streamingRafRef.current) {
            cancelAnimationFrame(streamingRafRef.current)
            streamingRafRef.current = null
          }

          const streamId = streamingMsgIdRef.current
          if (streamId) {
            // Finalize the streaming message with full content and ui_elements
            setMessages((prev) =>
              prev.map((m) =>
                m.id === streamId
                  ? { ...m, content: content || streamingContentRef.current, ui_elements: uiElements as AgentUIElement[] | null | undefined }
                  : m,
              ),
            )
            streamingContentRef.current = ""
            streamingMsgIdRef.current = null
          } else {
            // Fallback: no preceding deltas (e.g. reasoning text before tool calls)
            setMessages((prev) => [
              ...prev,
              { id: nextId(), role: "assistant", content, timestamp: Date.now(), ui_elements: uiElements as AgentUIElement[] | null | undefined },
            ])
          }
        },
        onCreated: (spaceId, url, displayName) => {
          setMessages((prev) => [
            ...prev,
            {
              id: nextId(),
              role: "assistant",
              content: "",
              timestamp: Date.now(),
              created_space: { space_id: spaceId, url, display_name: displayName },
            },
          ])
          setProgress((p) => ({
            ...p,
            spaceId,
            spaceUrl: url,
            spaceDisplayName: displayName,
            title: p.title || displayName,
          }))
        },
        onUpdated: (_spaceId, _url) => {
          // Space updated — no special UI handling needed
        },
        onError: (message) => {
          setAgentStatus(null)
          if (streamingRafRef.current) {
            cancelAnimationFrame(streamingRafRef.current)
            streamingRafRef.current = null
          }
          streamingContentRef.current = ""
          streamingMsgIdRef.current = null

          setMessages((prev) => [
            ...prev,
            {
              id: nextId(),
              role: "assistant",
              content: message,
              timestamp: Date.now(),
              is_error: true,
            } as AgentChatMessage,
          ])
        },
        onDone: () => {
          setAgentStatus(null)
          setAgentStep(null)
          setIsStreaming(false)
          // Clean up streaming refs
          if (streamingRafRef.current) {
            cancelAnimationFrame(streamingRafRef.current)
            streamingRafRef.current = null
          }
          streamingContentRef.current = ""
          streamingMsgIdRef.current = null
          pendingToolCalls = []
          setQueuedMessage((queued) => {
            if (queued) {
              setTimeout(() => sendMessage(queued), 100)
            }
            return null
          })
        },
      })
    },
    [sessionId, isStreaming],
  )

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    sendMessage(input)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault()
      sendMessage(input)
    }
  }

  const handleStop = () => {
    stopRef.current?.()
    setIsStreaming(false)
    setAgentStatus(null)
    setAgentStep(null)
    setQueuedMessage(null)
    if (streamingRafRef.current) {
      cancelAnimationFrame(streamingRafRef.current)
      streamingRafRef.current = null
    }
    streamingContentRef.current = ""
    streamingMsgIdRef.current = null
  }

  const [showClearConfirm, setShowClearConfirm] = useState(false)

  const handleClear = () => {
    setShowClearConfirm(true)
  }

  const confirmClear = () => {
    stopRef.current?.()
    setMessages([])
    setSessionId(null)
    setIsStreaming(false)
    setAgentStatus(null)
    setAgentStep(null)
    if (streamingRafRef.current) {
      cancelAnimationFrame(streamingRafRef.current)
      streamingRafRef.current = null
    }
    streamingContentRef.current = ""
    streamingMsgIdRef.current = null
    setExpandedTools(new Set())
    setUsedElements(new Set())
    setMultiSelections({})
    setProgress(EMPTY_PROGRESS)
    setEditingTitle(false)
    setTitleDraft("")
    setBusinessContextDraft("")
    setExpandedPlanSections(new Set(["sample_questions"]))
    setEditedPlan(null)
    setEditingPlanItem(null)
    setAutoPilot(false)
    setQueuedMessage(null)
    setShowClearConfirm(false)
    sessionStorage.removeItem(STORAGE_KEY)
  }

  const handleCopyConfig = (config: Record<string, unknown>) => {
    navigator.clipboard.writeText(JSON.stringify(config, null, 2))
    setCopiedConfig(true)
    setTimeout(() => setCopiedConfig(false), 2000)
  }

  // ─── Interactive selections → update progress ─────────────────

  const handleSingleSelect = (msgId: string, el: AgentUIElement, option: { value: string; label: string }) => {
    const key = `${msgId}:${el.id}`
    if (isStreaming || usedElements.has(key)) return
    setUsedElements((prev) => new Set(prev).add(key))
    const selectionData = { [el.id]: option.value }

    if (el.id === "catalog_selection") {
      setProgress((p) => ({ ...p, catalog: option.value, schemas: [], tables: [] }))
    } else if (el.id === "schema_selection") {
      setProgress((p) => ({
        ...p,
        schemas: p.schemas.includes(option.value) ? p.schemas : [...p.schemas, option.value],
      }))
    }

    sendMessage(`I'll go with ${option.label}`, selectionData)
  }

  const toggleMultiOption = (key: string, value: string) => {
    setMultiSelections((prev) => {
      const current = new Set(prev[key] || [])
      if (current.has(value)) current.delete(value)
      else current.add(value)
      return { ...prev, [key]: current }
    })
  }

  const confirmMultiSelect = (msgId: string, el: AgentUIElement) => {
    const key = `${msgId}:${el.id}`
    const selected = multiSelections[key]
    if (!selected || selected.size === 0 || isStreaming || usedElements.has(key)) return
    setUsedElements((prev) => new Set(prev).add(key))

    const selectedLabels =
      el.options?.filter((o) => selected.has(o.value)).map((o) => o.label) || []
    const selectionData = { [el.id]: Array.from(selected) }

    if (el.id === "table_selection") {
      setProgress((p) => {
        const merged = new Set([...p.tables, ...selected])
        return { ...p, tables: Array.from(merged) }
      })
    }

    sendMessage(
      `I've selected ${selectedLabels.length} table${selectedLabels.length !== 1 ? "s" : ""}: ${selectedLabels.join(", ")}`,
      selectionData,
    )
  }

  // Panel: submit title edit
  const submitTitle = () => {
    if (!titleDraft.trim()) return
    setProgress((p) => ({ ...p, title: titleDraft.trim() }))
    setEditingTitle(false)
    sendMessage(`The space name should be "${titleDraft.trim()}"`)

  }

  // Panel: remove a table
  const removeTable = (t: string) => {
    setProgress((p) => ({ ...p, tables: p.tables.filter((x) => x !== t) }))
    const short = t.split(".").pop() || t
    sendMessage(`Please remove the ${short} table from the selection`)
  }

  // Panel: business context management
  const addBusinessContext = () => {
    if (!businessContextDraft.trim()) return
    const rule = businessContextDraft.trim()
    setProgress((p) => ({ ...p, businessContext: [...p.businessContext, rule] }))
    setBusinessContextDraft("")
    sendMessage(`Business rule to keep in mind: "${rule}"`)
  }
  const removeBusinessContext = (i: number) => {
    const removed = progress.businessContext[i]
    setProgress((p) => ({ ...p, businessContext: p.businessContext.filter((_, j) => j !== i) }))
    if (removed) {
      sendMessage(`Please disregard the previous business rule: "${removed}"`)
    }
  }

  // ─── Render helpers ───────────────────────────────────────────

  const renderColumnBadge = (label: string, colorClass: string, title?: string) => (
    <span
      title={title}
      className={`ml-1.5 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${colorClass}`}
    >
      {label}
    </span>
  )

  const renderColumnBadges = (col: {
    name: string;
    pii_hint?: boolean;
    recommendations?: { action: string; reason: string; detail?: string; confidence?: string }[];
  }) => {
    const badges: React.ReactNode[] = []
    const reasons = new Set<string>()

    if (col.recommendations) {
      for (const rec of col.recommendations) {
        reasons.add(rec.reason)
      }
    }

    if (col.pii_hint || reasons.has("pii"))
      badges.push(renderColumnBadge("PII", "bg-amber-500/15 text-amber-500", "Potentially sensitive column"))
    if (reasons.has("etl_metadata"))
      badges.push(renderColumnBadge("ETL", "bg-zinc-500/15 text-zinc-400", "ETL/metadata column — consider hiding"))
    if (reasons.has("all_null"))
      badges.push(renderColumnBadge("EMPTY", "bg-red-500/15 text-red-400", "100% null values"))
    if (reasons.has("high_null_rate")) {
      const detail = col.recommendations?.find(r => r.reason === "high_null_rate")?.detail
      badges.push(renderColumnBadge(detail || "HIGH NULL", "bg-orange-500/15 text-orange-400", "High null rate"))
    }
    if (reasons.has("constant_value"))
      badges.push(renderColumnBadge("CONSTANT", "bg-zinc-500/15 text-zinc-400", "Single distinct value"))
    if (reasons.has("inconsistent_boolean") || reasons.has("boolean_as_string"))
      badges.push(renderColumnBadge("BOOL STR", "bg-purple-500/15 text-purple-400", "Boolean values stored as strings with mixed casing"))
    if (reasons.has("inconsistent_casing"))
      badges.push(renderColumnBadge("CASING", "bg-purple-500/15 text-purple-400", "Inconsistent casing across values"))

    return badges.length > 0 ? <>{badges}</> : null
  }

  const renderDescribeCard = (result: Record<string, unknown>) => {
    const columns = (result.columns as {
      name: string;
      type: string;
      description?: string;
      pii_hint?: boolean;
      recommendations?: { action: string; reason: string; detail?: string; confidence?: string }[];
    }[]) || []
    const sampleRows = (result.sample_rows as Record<string, unknown>[]) || []
    const ucUrl = result.uc_url as string | undefined
    const tableName = result.table as string | undefined
    const comment = result.comment as string | undefined
    const recommendations = result.recommendations as {
      exclude_pii?: string[];
      exclude_etl?: string[];
    } | undefined
    const colNames = columns.slice(0, 12).map((c) => c.name)

    const excludeCount = (recommendations?.exclude_pii?.length || 0) + (recommendations?.exclude_etl?.length || 0)

    return (
      <div className="mt-2 border border-default rounded-lg overflow-hidden bg-surface text-xs">
        {/* Header */}
        <div className="flex items-center justify-between px-3 py-2 bg-surface-secondary border-b border-default">
          <div className="flex items-center gap-2">
            <Table2 className="w-3.5 h-3.5 text-accent" />
            <span className="font-semibold text-primary">{tableName?.split(".").pop()}</span>
            <span className="text-muted">{columns.length} columns</span>
            {excludeCount > 0 && (
              <span className="text-muted">
                · {excludeCount} recommended to hide
              </span>
            )}
          </div>
          {ucUrl && (
            <a
              href={ucUrl}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1 text-accent hover:underline"
            >
              <ExternalLink className="w-3 h-3" />
              View in UC
            </a>
          )}
        </div>
        {comment && <div className="px-3 py-1.5 text-muted border-b border-default italic">{comment}</div>}

        {/* Column schema */}
        <div className="max-h-48 overflow-auto">
          <table className="w-full text-left">
            <thead>
              <tr className="text-muted border-b border-default">
                <th className="px-3 py-1.5 font-medium">Column</th>
                <th className="px-3 py-1.5 font-medium">Type</th>
                <th className="px-3 py-1.5 font-medium">Description</th>
              </tr>
            </thead>
            <tbody>
              {columns.map((col) => (
                <tr key={col.name} className="border-b border-default last:border-0">
                  <td className="px-3 py-1 font-mono text-primary">
                    {col.name}
                    {renderColumnBadges(col)}
                  </td>
                  <td className="px-3 py-1 text-muted font-mono">{col.type}</td>
                  <td className="px-3 py-1 text-secondary">{col.description || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Sample data */}
        {sampleRows.length > 0 && (
          <>
            <div className="px-3 py-1.5 bg-surface-secondary border-t border-default text-muted font-medium">
              Sample Data ({sampleRows.length} rows)
            </div>
            <div className="overflow-x-auto max-h-40">
              <table className="w-full text-left">
                <thead>
                  <tr className="text-muted border-b border-default">
                    {colNames.map((n) => (
                      <th key={n} className="px-2 py-1 font-medium whitespace-nowrap">{n}</th>
                    ))}
                    {columns.length > 12 && <th className="px-2 py-1 text-muted">...</th>}
                  </tr>
                </thead>
                <tbody>
                  {sampleRows.map((row, ri) => (
                    <tr key={ri} className="border-b border-default last:border-0">
                      {colNames.map((n) => (
                        <td key={n} className="px-2 py-1 font-mono whitespace-nowrap max-w-[200px] truncate text-secondary">
                          {row[n] == null ? <span className="text-muted italic">null</span> : String(row[n])}
                        </td>
                      ))}
                      {columns.length > 12 && <td className="px-2 py-1 text-muted">...</td>}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    )
  }

  const renderQualityCard = (result: Record<string, unknown>) => {
    const tables = (result.tables || {}) as Record<string, {
      total_rows?: number;
      column_quality?: Record<string, {
        null_rate?: number;
        empty_rate?: number;
        distinct_count?: number;
        boolean_as_string?: { normalized: string; variants: string[]; count: number }[];
        inconsistent_casing?: { normalized: string; variants: string[]; variant_count: number }[];
        recommendations?: { action: string; reason: string; detail?: string }[];
        error?: string;
      }>;
      summary?: {
        good_columns?: number;
        sparse_columns?: number;
        empty_columns?: number;
        constant_columns?: number;
        recommended_excludes?: string[];
        recommended_review?: string[];
      };
      error?: string;
    }>
    const globalSummary = result.summary as {
      tables_assessed?: number;
      tables_with_issues?: number;
      total_recommended_excludes?: number;
      total_recommended_review?: number;
    } | undefined

    const reasonBadge = (reason: string) => {
      const map: Record<string, { label: string; color: string }> = {
        all_null: { label: "EMPTY", color: "bg-red-500/15 text-red-400" },
        high_null_rate: { label: "HIGH NULL", color: "bg-orange-500/15 text-orange-400" },
        constant_value: { label: "CONSTANT", color: "bg-zinc-500/15 text-zinc-400" },
        inconsistent_boolean: { label: "BOOL CASING", color: "bg-purple-500/15 text-purple-400" },
        boolean_as_string: { label: "BOOL STR", color: "bg-purple-500/15 text-purple-400" },
        inconsistent_casing: { label: "CASING", color: "bg-purple-500/15 text-purple-400" },
        etl_metadata: { label: "ETL", color: "bg-zinc-500/15 text-zinc-400" },
        pii: { label: "PII", color: "bg-amber-500/15 text-amber-500" },
      }
      const m = map[reason] || { label: reason.toUpperCase(), color: "bg-zinc-500/15 text-zinc-400" }
      return (
        <span key={reason} className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${m.color}`}>
          {m.label}
        </span>
      )
    }

    return (
      <div className="mt-2 border border-default rounded-lg overflow-hidden bg-surface text-xs">
        {/* Header */}
        <div className="px-3 py-2 bg-surface-secondary border-b border-default flex items-center gap-2">
          <Table2 className="w-3.5 h-3.5 text-accent" />
          <span className="font-semibold text-primary">Data Quality Assessment</span>
          {globalSummary && (
            <span className="text-muted">
              {globalSummary.tables_assessed} table{(globalSummary.tables_assessed ?? 0) !== 1 ? "s" : ""}
              {(globalSummary.total_recommended_excludes ?? 0) > 0 && (
                <> · <span className="text-red-400">{globalSummary.total_recommended_excludes} to hide</span></>
              )}
              {(globalSummary.total_recommended_review ?? 0) > 0 && (
                <> · <span className="text-orange-400">{globalSummary.total_recommended_review} to review</span></>
              )}
            </span>
          )}
        </div>

        {/* Per-table results */}
        <div className="max-h-64 overflow-auto divide-y divide-default">
          {Object.entries(tables).map(([tbl, data]) => {
            if (data.error) {
              return (
                <div key={tbl} className="px-3 py-2">
                  <span className="font-mono text-primary">{tbl.split(".").pop()}</span>
                  <span className="ml-2 text-red-400">Error: {data.error}</span>
                </div>
              )
            }
            const quality = data.column_quality || {}
            const flagged = Object.entries(quality).filter(
              ([, m]) => m.recommendations && m.recommendations.length > 0
            )
            if (flagged.length === 0) {
              return (
                <div key={tbl} className="px-3 py-2">
                  <span className="font-mono text-primary">{tbl.split(".").pop()}</span>
                  <span className="ml-2 text-emerald-400">No issues found</span>
                  <span className="text-muted ml-1">({data.total_rows?.toLocaleString()} rows)</span>
                </div>
              )
            }
            return (
              <div key={tbl} className="px-3 py-2">
                <div className="flex items-center gap-2 mb-1.5">
                  <span className="font-mono font-semibold text-primary">{tbl.split(".").pop()}</span>
                  <span className="text-muted">{data.total_rows?.toLocaleString()} rows · {flagged.length} flagged</span>
                </div>
                <div className="space-y-1">
                  {flagged.map(([colName, metrics]) => (
                    <div key={colName} className="flex items-center gap-2 pl-2">
                      <span className="font-mono text-secondary w-40 truncate" title={colName}>{colName}</span>
                      <div className="flex items-center gap-1 flex-wrap">
                        {metrics.recommendations?.map((rec) => reasonBadge(rec.reason))}
                        {metrics.null_rate !== undefined && metrics.null_rate > 0 && (
                          <span className="text-muted">{(metrics.null_rate * 100).toFixed(0)}% null</span>
                        )}
                      </div>
                      {metrics.inconsistent_casing && metrics.inconsistent_casing.length > 0 && (
                        <span className="text-muted truncate" title={metrics.inconsistent_casing.map(c => c.variants.join(", ")).join("; ")}>
                          e.g. {metrics.inconsistent_casing[0].variants.slice(0, 3).join(", ")}
                        </span>
                      )}
                      {metrics.boolean_as_string && metrics.boolean_as_string.length > 0 && !metrics.inconsistent_casing?.length && (
                        <span className="text-muted truncate" title={metrics.boolean_as_string.map(b => b.variants.join(", ")).join("; ")}>
                          e.g. {metrics.boolean_as_string[0].variants.slice(0, 3).join(", ")}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  const renderUsageCard = (result: Record<string, unknown>) => {
    const tables = (result.tables || {}) as Record<string, {
      lineage?: { upstream?: string[]; downstream?: string[]; error?: string };
      recent_queries?: { query_preview?: string; executed_by?: string; duration_ms?: number }[];
    }>
    const summary = result.summary as {
      tables_with_lineage?: number; total_upstream_sources?: number;
      total_downstream_consumers?: number; recent_query_count?: number;
    } | undefined
    const sysAvailable = result.system_tables_available as boolean | undefined

    return (
      <div className="mt-2 border border-default rounded-lg overflow-hidden bg-surface text-xs">
        <div className="flex items-center justify-between px-3 py-2 bg-surface-secondary border-b border-default">
          <div className="flex items-center gap-2">
            <GitBranch className="w-3.5 h-3.5 text-accent" />
            <span className="font-semibold text-primary">Table Usage & Lineage</span>
            {sysAvailable === false && (
              <span className="px-1.5 py-0.5 rounded bg-yellow-500/10 text-yellow-600 text-[10px] font-medium">system tables unavailable</span>
            )}
          </div>
          {summary && (
            <span className="text-muted">
              {summary.total_upstream_sources ?? 0} upstream · {summary.total_downstream_consumers ?? 0} downstream · {summary.recent_query_count ?? 0} queries
            </span>
          )}
        </div>
        <div className="max-h-64 overflow-auto divide-y divide-[var(--border-color)]">
          {Object.entries(tables).map(([tbl, info]) => (
            <div key={tbl} className="px-3 py-2">
              <span className="font-mono font-medium text-primary">{tbl.split(".").pop()}</span>
              {info.lineage?.error ? (
                <span className="ml-2 text-muted italic">lineage unavailable</span>
              ) : (
                <div className="mt-1 flex flex-wrap gap-1">
                  {(info.lineage?.upstream || []).map((u, i) => (
                    <span key={`u-${i}`} className="inline-block px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-400 font-mono text-[10px]">
                      ← {u.split(".").pop()}
                    </span>
                  ))}
                  {(info.lineage?.downstream || []).map((d, i) => (
                    <span key={`d-${i}`} className="inline-block px-1.5 py-0.5 rounded bg-emerald-500/10 text-emerald-400 font-mono text-[10px]">
                      → {d.split(".").pop()}
                    </span>
                  ))}
                  {!(info.lineage?.upstream?.length) && !(info.lineage?.downstream?.length) && (
                    <span className="text-muted italic">no lineage found</span>
                  )}
                </div>
              )}
              {(info.recent_queries?.length ?? 0) > 0 && (
                <div className="mt-1.5 space-y-0.5">
                  {info.recent_queries!.slice(0, 3).map((q, i) => (
                    <div key={i} className="flex items-center gap-2 text-[10px] text-muted">
                      <span className="font-mono truncate max-w-[400px]">{q.query_preview?.slice(0, 120)}</span>
                      {q.duration_ms != null && (
                        <span className="shrink-0 text-secondary/60">{(q.duration_ms / 1000).toFixed(1)}s</span>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    )
  }

  const renderProfileCard = (result: Record<string, unknown>) => {
    const profiles = (result.profiles as Record<string, { distinct_values?: unknown[]; has_more?: boolean; error?: string }>) || {}
    const ucUrl = result.uc_url as string | undefined
    const tableName = result.table as string | undefined
    const profileEntries = Object.entries(profiles)

    return (
      <div className="mt-2 border border-default rounded-lg overflow-hidden bg-surface text-xs">
        <div className="flex items-center justify-between px-3 py-2 bg-surface-secondary border-b border-default">
          <div className="flex items-center gap-2">
            <BarChart3 className="w-3.5 h-3.5 text-accent" />
            <span className="font-semibold text-primary">Column Profile</span>
            <span className="text-muted">{tableName?.split(".").pop()}</span>
          </div>
          {ucUrl && (
            <a href={ucUrl} target="_blank" rel="noreferrer" className="flex items-center gap-1 text-accent hover:underline">
              <ExternalLink className="w-3 h-3" /> View in UC
            </a>
          )}
        </div>
        <div className="max-h-64 overflow-auto divide-y divide-[var(--border-color)]">
          {profileEntries.map(([colName, prof]) => (
            <div key={colName} className="px-3 py-2">
              <span className="font-mono font-medium text-primary">{colName}</span>
              {prof.error ? (
                <span className="ml-2 text-red-400">{prof.error}</span>
              ) : (
                <div className="mt-1 flex flex-wrap gap-1">
                  {(prof.distinct_values || []).slice(0, 15).map((v, i) => (
                    <span key={i} className="inline-block px-1.5 py-0.5 rounded bg-surface-secondary text-secondary font-mono">
                      {String(v)}
                    </span>
                  ))}
                  {prof.has_more && (
                    <span className="inline-block px-1.5 py-0.5 rounded bg-accent/10 text-accent font-medium">
                      20+ values
                    </span>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    )
  }

  const togglePlanSection = (key: string) => {
    setExpandedPlanSections((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  // ─── Plan editing helpers ──────────────────────────────────────

  const updatePlanList = (key: keyof EditablePlan, index: number, value: string) => {
    setEditedPlan((prev) => {
      if (!prev) return prev
      const arr = [...(prev[key] as string[])]
      arr[index] = value
      return { ...prev, [key]: arr }
    })
  }

  const removePlanItem = (key: keyof EditablePlan, index: number) => {
    setEditedPlan((prev) => {
      if (!prev) return prev
      const arr = [...(prev[key] as unknown[])]
      arr.splice(index, 1)
      return { ...prev, [key]: arr }
    })
    setEditingPlanItem(null)
  }

  const addPlanItem = (key: keyof EditablePlan, blank: unknown) => {
    setEditedPlan((prev) => {
      if (!prev) return prev
      return { ...prev, [key]: [...(prev[key] as unknown[]), blank] }
    })
    // Open that section
    setExpandedPlanSections((s) => new Set(s).add(key === "measures" || key === "filters" || key === "expressions" ? "sql_expressions" : key))
  }

  const updatePlanObj = (key: keyof EditablePlan, index: number, field: string, value: string) => {
    setEditedPlan((prev) => {
      if (!prev) return prev
      const arr = [...(prev[key] as Record<string, string>[])]
      arr[index] = { ...arr[index], [field]: value }
      return { ...prev, [key]: arr }
    })
  }

  const approvePlanAndCreate = () => {
    if (!editedPlan) return
    sendMessage("Plan approved — go ahead and create the space.", { edited_plan: editedPlan, action: "create" })
  }

  const requestAIReview = () => {
    if (!editedPlan) return
    sendMessage("Please review the plan and suggest improvements before I approve.", { edited_plan: editedPlan, action: "review" })
  }

  const requestAddMoreTables = () => {
    sendMessage("I want to add more tables from another schema or catalog before creating.")
  }

  // ─── Plan card renderer ──────────────────────────────────────

  const renderPlanCard = (_result: Record<string, unknown>) => {
    const plan = editedPlan
    if (!plan) return null

    const sqlExpressionCount = plan.measures.length + plan.filters.length + plan.expressions.length

    const PLAN_SECTIONS: { key: string; label: string; description: string; Icon: typeof MessageSquare; count: number }[] = [
      { key: "sample_questions", label: "Sample Questions", description: "Click-to-ask suggestions shown to users in the Genie Space UI", Icon: MessageSquare, count: plan.sample_questions.length },
      { key: "text_instructions", label: "Text Instructions", description: "Business rules and domain context that guide how Genie interprets questions", Icon: FileText, count: plan.text_instructions.trim() ? 1 : 0 },
      { key: "joins", label: "Joins", description: "Table relationships so Genie can combine data across tables", Icon: Link2, count: plan.joins.length },
      { key: "sql_expressions", label: "SQL Expressions", description: "Reusable measures, filters, and dimensions for common calculations", Icon: Code2, count: sqlExpressionCount },
      { key: "example_sqls", label: "Example SQL Queries", description: "Question-SQL pairs that teach Genie how to write correct queries", Icon: ListChecks, count: plan.example_sqls.length },
      { key: "benchmarks", label: "Benchmark Questions", description: "Test questions with expected SQL to evaluate Genie accuracy after creation", Icon: BarChart3, count: (plan.benchmarks || []).length },
    ]

    const TYPE_BADGE: Record<string, { label: string; cls: string }> = {
      measure: { label: "MEASURE", cls: "bg-blue-500/15 text-blue-400" },
      filter: { label: "FILTER", cls: "bg-amber-500/15 text-amber-400" },
      dimension: { label: "DIMENSION", cls: "bg-emerald-500/15 text-emerald-400" },
    }

    const taggedExpressions: { _type: string; _key: keyof EditablePlan; _idx: number; display_name?: string; alias?: string; sql?: string }[] = [
      ...plan.measures.map((m, i) => ({ ...m, _type: "measure" as const, _key: "measures" as keyof EditablePlan, _idx: i })),
      ...plan.filters.map((f, i) => ({ ...f, _type: "filter" as const, _key: "filters" as keyof EditablePlan, _idx: i })),
      ...plan.expressions.map((e, i) => ({ ...e, _type: "dimension" as const, _key: "expressions" as keyof EditablePlan, _idx: i })),
    ]

    const totalItems = plan.sample_questions.length + (plan.benchmarks || []).length + (plan.text_instructions.trim() ? 1 : 0) + plan.joins.length + sqlExpressionCount + plan.example_sqls.length

    const isEditing = (itemKey: string) => editingPlanItem === itemKey
    const startEdit = (itemKey: string) => setEditingPlanItem(itemKey)
    const stopEdit = () => setEditingPlanItem(null)

    return (
      <div className="mt-2 border border-default rounded-lg overflow-hidden bg-surface text-xs">
        <div className="flex items-center justify-between px-3 py-2 bg-surface-secondary border-b border-default">
          <div className="flex items-center gap-2">
            <Settings className="w-3.5 h-3.5 text-accent" />
            <span className="font-semibold text-primary">Plan Review</span>
            <span className="text-muted">{totalItems} items</span>
          </div>
          <span className="text-[10px] text-muted">Click any item to edit</span>
        </div>

        <div className="divide-y divide-[var(--border-color)]">
          {PLAN_SECTIONS.map((sec) => {
            const alwaysShow = ["sample_questions", "example_sqls", "benchmarks", "text_instructions"]
            if (sec.count === 0 && !alwaysShow.includes(sec.key)) return null
            const isOpen = expandedPlanSections.has(sec.key)
            const { Icon } = sec
            return (
              <div key={sec.key}>
                <button
                  onClick={() => togglePlanSection(sec.key)}
                  className="flex items-center gap-2 w-full px-3 py-2.5 hover:bg-elevated transition-colors text-left"
                >
                  {isOpen ? <ChevronDown className="w-3 h-3 text-muted" /> : <ChevronRight className="w-3 h-3 text-muted" />}
                  <Icon className="w-3.5 h-3.5 text-accent" />
                  <div className="flex-1 min-w-0">
                    <span className="font-medium text-primary block">{sec.label}</span>
                    {!isOpen && <span className="text-[10px] text-muted block truncate">{sec.description}</span>}
                  </div>
                  {sec.key === "text_instructions" ? (
                    sec.count > 0 ? <Check className="w-3.5 h-3.5 text-green-400 flex-shrink-0" /> : <span className="text-[10px] text-muted flex-shrink-0">empty</span>
                  ) : (
                    <span className="text-[10px] text-muted bg-surface-secondary px-1.5 py-0.5 rounded-full flex-shrink-0">{sec.count}</span>
                  )}
                </button>

                {isOpen && (
                  <div className="px-3 pb-3">
                    {/* Sample Questions */}
                    {sec.key === "sample_questions" && (
                      <div className="space-y-1">
                        {plan.sample_questions.map((q, i) => {
                          const itemKey = `sq-${i}`
                          return isEditing(itemKey) ? (
                            <div key={i} className="flex gap-1.5 items-start">
                              <input
                                autoFocus
                                value={q}
                                onChange={(e) => updatePlanList("sample_questions", i, e.target.value)}
                                onBlur={stopEdit}
                                onKeyDown={(e) => e.key === "Enter" && stopEdit()}
                                className="flex-1 bg-elevated border border-accent/30 rounded px-2 py-1 text-secondary focus:outline-none focus:ring-1 focus:ring-accent/40"
                              />
                              <button onClick={() => removePlanItem("sample_questions", i)} className="p-1 text-red-400 hover:text-red-300 flex-shrink-0">
                                <Trash2 className="w-3 h-3" />
                              </button>
                            </div>
                          ) : (
                            <div key={i} className="flex items-start gap-2 group/item py-1 cursor-pointer hover:bg-elevated rounded px-1 -mx-1" onClick={() => startEdit(itemKey)}>
                              <span className="text-muted select-none w-4 text-right flex-shrink-0">{i + 1}.</span>
                              <span className="text-secondary flex-1">{q}</span>
                              <Pencil className="w-3 h-3 text-muted opacity-0 group-hover/item:opacity-100 flex-shrink-0 mt-0.5" />
                            </div>
                          )
                        })}
                        <button
                          onClick={() => addPlanItem("sample_questions", "")}
                          className="flex items-center gap-1 text-accent hover:underline mt-1"
                        >
                          <Plus className="w-3 h-3" /> Add question
                        </button>
                      </div>
                    )}

                    {/* Benchmark Questions */}
                    {sec.key === "benchmarks" && (
                      <div className="space-y-2">
                        {plan.benchmarks.map((b, i) => {
                          const itemKey = `bm-${i}`
                          return isEditing(itemKey) ? (
                            <div key={i} className="border border-accent/20 rounded-lg overflow-hidden bg-elevated">
                              <div className="flex items-center gap-1.5 px-3 py-2">
                                <input
                                  autoFocus
                                  value={b.question}
                                  onChange={(e) => updatePlanObj("benchmarks", i, "question", e.target.value)}
                                  placeholder="Benchmark question"
                                  className="flex-1 bg-surface border border-default rounded px-2 py-1 text-primary focus:outline-none focus:ring-1 focus:ring-accent/40"
                                />
                                <button onClick={() => removePlanItem("benchmarks", i)} className="p-1 text-red-400 hover:text-red-300"><Trash2 className="w-3 h-3" /></button>
                              </div>
                              <textarea
                                value={b.expected_sql}
                                onChange={(e) => updatePlanObj("benchmarks", i, "expected_sql", e.target.value)}
                                rows={3}
                                className="w-full px-3 py-2 font-mono text-secondary bg-surface resize-none focus:outline-none focus:ring-1 focus:ring-accent/40"
                                placeholder="SELECT ... (expected SQL answer)"
                                onBlur={stopEdit}
                              />
                            </div>
                          ) : (
                            <div key={i} className="group/item cursor-pointer hover:bg-elevated rounded-lg px-2 py-1.5 -mx-1" onClick={() => startEdit(itemKey)}>
                              <div className="flex items-start gap-2">
                                <span className="text-muted select-none w-4 text-right flex-shrink-0">{i + 1}.</span>
                                <div className="flex-1 min-w-0">
                                  <span className="text-secondary block">{b.question}</span>
                                  <span className="text-muted font-mono text-[10px] block truncate mt-0.5">{b.expected_sql}</span>
                                </div>
                                <Pencil className="w-3 h-3 text-muted opacity-0 group-hover/item:opacity-100 flex-shrink-0 mt-0.5" />
                              </div>
                            </div>
                          )
                        })}
                        <button
                          onClick={() => addPlanItem("benchmarks", { question: "", expected_sql: "" })}
                          className="flex items-center gap-1 text-accent hover:underline mt-1"
                        >
                          <Plus className="w-3 h-3" /> Add benchmark
                        </button>
                      </div>
                    )}

                    {/* Text Instructions — single editable block */}
                    {sec.key === "text_instructions" && (
                      <div>
                        <textarea
                          value={plan.text_instructions}
                          onChange={(e) => setEditedPlan((prev) => prev ? { ...prev, text_instructions: e.target.value } : prev)}
                          rows={Math.max(8, plan.text_instructions.split("\n").length + 2)}
                          placeholder="Business rules, terminology, default assumptions, data quality warnings..."
                          className="w-full bg-elevated border border-default rounded px-3 py-2 text-secondary text-sm font-mono leading-relaxed resize-y focus:outline-none focus:ring-1 focus:ring-accent/40 focus:border-accent/30"
                        />
                        <p className="text-[10px] text-muted mt-1">
                          Use ## headers to organize (Terminology, Default Assumptions, Data Quality, etc.)
                        </p>
                      </div>
                    )}

                    {/* Joins (read-only — complex structure) */}
                    {sec.key === "joins" && (
                      <div className="overflow-x-auto">
                        <table className="w-full text-left">
                          <thead>
                            <tr className="text-muted border-b border-default">
                              <th className="px-2 py-1.5 font-medium">Left Table</th>
                              <th className="px-2 py-1.5 font-medium">Relationship</th>
                              <th className="px-2 py-1.5 font-medium">Right Table</th>
                              <th className="px-2 py-1.5 font-medium">Condition</th>
                            </tr>
                          </thead>
                          <tbody>
                            {plan.joins.map((j, i) => {
                              const leftShort = (j.left_table || "").split(".").pop() || j.left_table
                              const rightShort = (j.right_table || "").split(".").pop() || j.right_table
                              const rel = j.relationship || "—"
                              return (
                                <tr key={i} className="border-b border-default last:border-0">
                                  <td className="px-2 py-1.5 font-mono text-primary">{leftShort}</td>
                                  <td className="px-2 py-1.5">
                                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-purple-500/10 text-purple-400 whitespace-nowrap">{rel}</span>
                                  </td>
                                  <td className="px-2 py-1.5 font-mono text-primary">{rightShort}</td>
                                  <td className="px-2 py-1.5 font-mono text-secondary">
                                    {leftShort}.{j.left_column} = {rightShort}.{j.right_column}
                                  </td>
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}

                    {/* SQL Expressions */}
                    {sec.key === "sql_expressions" && (
                      <div className="space-y-1">
                        {taggedExpressions.map((expr, i) => {
                          const badge = TYPE_BADGE[expr._type] || TYPE_BADGE.dimension
                          const itemKey = `expr-${expr._key}-${expr._idx}`
                          const name = expr.display_name || expr.alias || ""
                          const sql = expr.sql || ""
                          return isEditing(itemKey) ? (
                            <div key={i} className="flex gap-1.5 items-start border border-accent/20 rounded p-2 bg-elevated">
                              <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold ${badge.cls} flex-shrink-0 mt-0.5`}>
                                {badge.label}
                              </span>
                              <div className="flex-1 space-y-1">
                                <input
                                  autoFocus
                                  value={name}
                                  onChange={(e) => updatePlanObj(expr._key, expr._idx, expr.display_name !== undefined ? "display_name" : "alias", e.target.value)}
                                  placeholder="Name"
                                  className="w-full bg-surface border border-default rounded px-2 py-1 text-primary focus:outline-none focus:ring-1 focus:ring-accent/40"
                                />
                                <input
                                  value={sql}
                                  onChange={(e) => updatePlanObj(expr._key, expr._idx, "sql", e.target.value)}
                                  placeholder="SQL expression"
                                  className="w-full bg-surface border border-default rounded px-2 py-1 font-mono text-secondary focus:outline-none focus:ring-1 focus:ring-accent/40"
                                />
                              </div>
                              <div className="flex flex-col gap-1 flex-shrink-0">
                                <button onClick={stopEdit} className="p-1 text-accent hover:text-accent/80"><Check className="w-3 h-3" /></button>
                                <button onClick={() => removePlanItem(expr._key, expr._idx)} className="p-1 text-red-400 hover:text-red-300"><Trash2 className="w-3 h-3" /></button>
                              </div>
                            </div>
                          ) : (
                            <div key={i} className="flex items-center gap-2 group/item py-1 cursor-pointer hover:bg-elevated rounded px-1 -mx-1" onClick={() => startEdit(itemKey)}>
                              <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold ${badge.cls} flex-shrink-0`}>
                                {badge.label}
                              </span>
                              <span className="text-primary font-medium">{name || "—"}</span>
                              <span className="font-mono text-secondary flex-1 truncate" title={sql}>{sql}</span>
                              <Pencil className="w-3 h-3 text-muted opacity-0 group-hover/item:opacity-100 flex-shrink-0" />
                            </div>
                          )
                        })}
                        <div className="flex gap-2 mt-1">
                          <button onClick={() => addPlanItem("measures", { display_name: "", sql: "" })} className="flex items-center gap-1 text-accent hover:underline">
                            <Plus className="w-3 h-3" /> Measure
                          </button>
                          <button onClick={() => addPlanItem("filters", { display_name: "", sql: "" })} className="flex items-center gap-1 text-accent hover:underline">
                            <Plus className="w-3 h-3" /> Filter
                          </button>
                          <button onClick={() => addPlanItem("expressions", { display_name: "", sql: "" })} className="flex items-center gap-1 text-accent hover:underline">
                            <Plus className="w-3 h-3" /> Dimension
                          </button>
                        </div>
                      </div>
                    )}

                    {/* Example SQL Queries */}
                    {sec.key === "example_sqls" && (
                      <div className="space-y-2">
                        {plan.example_sqls.map((ex, i) => {
                          const itemKey = `exsql-${i}`
                          return isEditing(itemKey) ? (
                            <div key={i} className="border border-accent/20 rounded-lg overflow-hidden bg-elevated">
                              <div className="px-3 py-2 flex gap-1.5 items-center border-b border-default">
                                <span className="text-muted text-[10px] flex-shrink-0">{i + 1}.</span>
                                <input
                                  autoFocus
                                  value={ex.question}
                                  onChange={(e) => updatePlanObj("example_sqls", i, "question", e.target.value)}
                                  placeholder="Question"
                                  className="flex-1 bg-surface border border-default rounded px-2 py-1 text-primary focus:outline-none focus:ring-1 focus:ring-accent/40"
                                />
                                <button onClick={stopEdit} className="p-1 text-accent hover:text-accent/80"><Check className="w-3 h-3" /></button>
                                <button onClick={() => removePlanItem("example_sqls", i)} className="p-1 text-red-400 hover:text-red-300"><Trash2 className="w-3 h-3" /></button>
                              </div>
                              <textarea
                                value={ex.sql}
                                onChange={(e) => updatePlanObj("example_sqls", i, "sql", e.target.value)}
                                rows={4}
                                className="w-full px-3 py-2 font-mono text-secondary bg-surface resize-none focus:outline-none focus:ring-1 focus:ring-accent/40"
                                placeholder="SELECT ..."
                              />
                            </div>
                          ) : (
                            <div key={i} className="border border-default rounded-lg overflow-hidden group/item cursor-pointer hover:border-accent/30 transition-colors" onClick={() => startEdit(itemKey)}>
                              <div className="px-3 py-2 bg-surface-secondary text-primary font-medium flex items-center">
                                <span className="flex-1">{i + 1}. {ex.question}</span>
                                <Pencil className="w-3 h-3 text-muted opacity-0 group-hover/item:opacity-100 flex-shrink-0" />
                              </div>
                              <pre className="px-3 py-2 font-mono text-secondary whitespace-pre-wrap break-words bg-surface max-h-32 overflow-auto">
                                {ex.sql}
                              </pre>
                            </div>
                          )
                        })}
                        <button
                          onClick={() => addPlanItem("example_sqls", { question: "", sql: "" })}
                          className="flex items-center gap-1 text-accent hover:underline mt-1"
                        >
                          <Plus className="w-3 h-3" /> Add example SQL
                        </button>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          })}
        </div>

        {/* Action buttons footer */}
        <div className="px-3 py-2.5 bg-surface-secondary border-t border-default flex flex-wrap items-center gap-2">
          <button
            onClick={approvePlanAndCreate}
            disabled={isStreaming}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-emerald-600 text-white rounded-md text-[11px] font-semibold hover:bg-emerald-500 transition-colors disabled:opacity-40"
          >
            <Rocket className="w-3 h-3" />
            Approve &amp; Create
          </button>
          <button
            onClick={requestAIReview}
            disabled={isStreaming}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-surface border border-default text-secondary rounded-md text-[11px] font-medium hover:bg-elevated transition-colors disabled:opacity-40"
          >
            <Sparkles className="w-3 h-3 text-accent" />
            AI Review &amp; Suggest
          </button>
          <button
            onClick={requestAddMoreTables}
            disabled={isStreaming}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-surface border border-default text-secondary rounded-md text-[11px] font-medium hover:bg-elevated transition-colors disabled:opacity-40"
          >
            <Plus className="w-3 h-3 text-muted" />
            Add More Tables
          </button>
        </div>
      </div>
    )
  }

  const renderTestSqlCard = (result: Record<string, unknown>) => {
    const sql = result.sql as string || ""
    const rawCols = result.columns as (string | { name: string })[] || []
    const colNames = rawCols.map((c) => (typeof c === "string" ? c : c?.name ?? ""))
    const sampleRows = result.sample_rows as unknown[][] || []
    const rowCount = result.row_count as number || 0

    return (
      <div className="ml-5 mt-1 text-xs border border-default rounded-lg overflow-hidden bg-surface">
        <div className="px-3 py-2 bg-surface-secondary border-b border-default">
          <pre className="font-mono text-[11px] text-secondary whitespace-pre-wrap break-words">{sql}</pre>
        </div>
        {colNames.length > 0 && sampleRows.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead>
                <tr className="border-b border-default bg-elevated/50">
                  {colNames.map((c, ci) => (
                    <th key={ci} className="px-2 py-1 text-left font-semibold text-primary whitespace-nowrap">{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sampleRows.slice(0, 3).map((row, ri) => (
                  <tr key={ri} className="border-b border-default last:border-0">
                    {(row as unknown[]).map((cell, ci) => (
                      <td key={ci} className="px-2 py-1 text-muted whitespace-nowrap max-w-[200px] truncate">
                        {cell == null ? <span className="italic text-muted/50">null</span> : String(cell)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="px-3 py-1.5 text-[10px] text-muted border-t border-default">
          {rowCount} row{rowCount !== 1 ? "s" : ""} returned · {colNames.length} column{colNames.length !== 1 ? "s" : ""}
        </div>
      </div>
    )
  }

  const hasRichCard = (msg: AgentChatMessage): boolean =>
    !!msg.tool_result && !msg.tool_result.error && (msg.tool_name === "describe_table" || msg.tool_name === "profile_columns" || msg.tool_name === "present_plan" || msg.tool_name === "test_sql" || msg.tool_name === "assess_data_quality" || msg.tool_name === "profile_table_usage")

  const getToolSummary = (msg: AgentChatMessage): string | null => {
    if (!msg.tool_result || msg.tool_result.error) return null
    const r = msg.tool_result as Record<string, unknown>
    if (msg.tool_name === "describe_table") {
      const table = (r.table as string)?.split(".").pop() || ""
      const cols = (r.columns as unknown[])?.length ?? 0
      return `${table} — ${cols} col${cols !== 1 ? "s" : ""}`
    }
    if (msg.tool_name === "profile_columns") {
      const table = (r.table as string)?.split(".").pop() || ""
      const profiles = r.profiles as Record<string, unknown> | undefined
      const profiled = profiles ? Object.keys(profiles).length : 0
      return `${table} — ${profiled} profiled`
    }
    if (msg.tool_name === "test_sql") {
      const cols = (r.columns as unknown[])?.length ?? 0
      const rows = (r.row_count as number) ?? 0
      return `${cols} col${cols !== 1 ? "s" : ""} · ${rows} row${rows !== 1 ? "s" : ""}`
    }
    if (msg.tool_name === "assess_data_quality") {
      const summary = r.summary as { tables_assessed?: number; total_recommended_excludes?: number; total_recommended_review?: number } | undefined
      if (summary) {
        const parts: string[] = [`${summary.tables_assessed ?? 0} table${(summary.tables_assessed ?? 0) !== 1 ? "s" : ""}`]
        if (summary.total_recommended_excludes) parts.push(`${summary.total_recommended_excludes} to hide`)
        if (summary.total_recommended_review) parts.push(`${summary.total_recommended_review} to review`)
        return parts.join(" · ")
      }
    }
    if (msg.tool_name === "profile_table_usage") {
      const usageSummary = r.summary as { tables_with_lineage?: number; recent_query_count?: number } | undefined
      if (usageSummary) {
        const parts: string[] = []
        if (usageSummary.tables_with_lineage) parts.push(`${usageSummary.tables_with_lineage} with lineage`)
        if (usageSummary.recent_query_count) parts.push(`${usageSummary.recent_query_count} recent queries`)
        return parts.length ? parts.join(" · ") : "no usage data"
      }
      if (r.system_tables_available === false) return "system tables unavailable"
    }
    return null
  }

  const renderToolCall = (msg: AgentChatMessage) => {
    if (msg.tool_name === "present_plan" && msg.tool_result && !msg.tool_result.error) {
      return <div key={msg.id} className="mx-4 my-2">{renderPlanCard(msg.tool_result)}</div>
    }
    const isExpanded = expandedTools.has(msg.id)
    const isDone = !!msg.tool_result
    const hasError = isDone && !!msg.tool_result?.error
    const rich = hasRichCard(msg)
    const summary = !isExpanded && isDone ? getToolSummary(msg) : null

    return (
      <div key={msg.id} className="mx-12 my-1">
        <button
          onClick={() => toggleTool(msg.id)}
          className="flex items-center gap-2 text-xs text-muted hover:text-secondary transition-colors group w-full"
        >
          {isExpanded ? (
            <ChevronDown className="w-3 h-3" />
          ) : (
            <ChevronRight className="w-3 h-3" />
          )}
          <Wrench className="w-3 h-3" />
          <span className="font-medium">{msg.content}</span>
          {summary && <span className="text-secondary/60">· {summary}</span>}
          {!isDone && <Loader2 className="w-3 h-3 animate-spin ml-1" />}
          {isDone && !hasError && <Check className="w-3 h-3 text-emerald-500 ml-1" />}
          {hasError && <AlertCircle className="w-3 h-3 text-red-400 ml-1" />}
          {rich && !isExpanded && isDone && !summary && (
            <span className="ml-1 text-accent text-[10px]">click to view</span>
          )}
        </button>
        {isExpanded && msg.tool_result && (
          rich ? (
            msg.tool_name === "describe_table"
              ? renderDescribeCard(msg.tool_result)
              : msg.tool_name === "present_plan"
                ? renderPlanCard(msg.tool_result)
                : msg.tool_name === "test_sql"
                  ? renderTestSqlCard(msg.tool_result)
                  : msg.tool_name === "assess_data_quality"
                    ? renderQualityCard(msg.tool_result)
                    : msg.tool_name === "profile_table_usage"
                      ? renderUsageCard(msg.tool_result)
                      : renderProfileCard(msg.tool_result)
          ) : (
            <div className="ml-5 mt-1 text-xs bg-surface-secondary rounded-lg p-3 max-h-48 overflow-auto">
              <pre className="font-mono text-secondary whitespace-pre-wrap break-words">
                {JSON.stringify(msg.tool_result, null, 2)}
              </pre>
            </div>
          )
        )}
      </div>
    )
  }

  const renderInspectionGroup = (group: { msgs: AgentChatMessage[]; id: string }) => {
    const isExpanded = expandedTools.has(group.id)
    const allDone = group.msgs.every((m) => !!m.tool_result)
    const anyError = group.msgs.some((m) => !!m.tool_result?.error)

    const tables = new Set<string>()
    let totalCols = 0
    let totalProfiled = 0
    let qualityIssues = 0
    let lineageCount = 0

    for (const m of group.msgs) {
      if (!m.tool_result) continue
      const r = m.tool_result as Record<string, unknown>
      const table = (r.table as string)?.split(".").pop() || ""
      if (table) tables.add(table)
      if (m.tool_name === "describe_table") totalCols += (r.columns as unknown[])?.length ?? 0
      if (m.tool_name === "profile_columns") {
        const profiles = r.profiles as Record<string, unknown> | undefined
        totalProfiled += profiles ? Object.keys(profiles).length : 0
      }
      if (m.tool_name === "assess_data_quality") {
        const qs = r.summary as { total_recommended_excludes?: number; total_recommended_review?: number } | undefined
        qualityIssues += (qs?.total_recommended_excludes ?? 0) + (qs?.total_recommended_review ?? 0)
      }
      if (m.tool_name === "profile_table_usage") {
        const us = r.summary as { tables_with_lineage?: number } | undefined
        if (us?.tables_with_lineage) lineageCount += us.tables_with_lineage
      }
    }

    const summaryParts: string[] = []
    summaryParts.push(`${tables.size} table${tables.size !== 1 ? "s" : ""}`)
    if (totalCols) summaryParts.push(`${totalCols} cols`)
    if (totalProfiled) summaryParts.push(`${totalProfiled} profiled`)
    if (qualityIssues) summaryParts.push(`${qualityIssues} quality issue${qualityIssues !== 1 ? "s" : ""}`)
    if (lineageCount) summaryParts.push(`${lineageCount} with lineage`)
    const summary = allDone ? summaryParts.join(" · ") : `${group.msgs.length} operations`

    return (
      <div key={group.id} className="mx-12 my-1">
        <button
          onClick={() => toggleTool(group.id)}
          className="flex items-center gap-2 text-xs text-muted hover:text-secondary transition-colors group w-full"
        >
          {isExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
          <Search className="w-3 h-3" />
          <span className="font-medium">Data inspection</span>
          <span className="text-secondary/60">· {summary}</span>
          {!allDone && <Loader2 className="w-3 h-3 animate-spin ml-1" />}
          {allDone && !anyError && <Check className="w-3 h-3 text-emerald-500 ml-1" />}
          {anyError && <AlertCircle className="w-3 h-3 text-red-400 ml-1" />}
        </button>
        {isExpanded && (
          <div className="ml-2 mt-1 border-l border-default pl-2 space-y-0.5">
            {group.msgs.map((m) => renderToolCall(m))}
          </div>
        )}
      </div>
    )
  }

  const isElementSuperseded = (elId: string): boolean => {
    if (elId === "catalog_selection") return progress.schemas.length > 0
    if (elId === "schema_selection") return progress.tables.length > 0
    if (elId === "table_selection") return progress.inspectionDone
    if (elId === "warehouse_selection") return progress.configReady
    return false
  }

  const isLatestAssistantMsg = (msgId: string): boolean => {
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === "assistant" && !messages[i].is_thinking) {
        return messages[i].id === msgId
      }
    }
    return false
  }

  const renderUIElements = (msgId: string, elements: AgentUIElement[]) => {
    const isLatest = isLatestAssistantMsg(msgId)
    return elements.map((el) => {
      if (!isLatest && isElementSuperseded(el.id)) return null
      const key = `${msgId}:${el.id}`
      const isUsed = usedElements.has(key)
      const Icon = ELEMENT_ICONS[el.id] || Database

      if (el.type === "single_select" && el.options && el.options.length > 0) {
        const useLargeList = el.options.length > COMBOBOX_THRESHOLD && !isUsed
        const searchTerm = elementSearch[key] || ""
        const filtered = useLargeList
          ? el.options.filter((o) => {
              const q = searchTerm.toLowerCase()
              return o.label.toLowerCase().includes(q) || (o.description || "").toLowerCase().includes(q)
            })
          : el.options

        return (
          <div key={el.id} className="mt-3">
            <div className="flex items-center gap-1.5 mb-2">
              <Icon className="w-3.5 h-3.5 text-muted" />
              <span className="text-xs font-medium text-muted uppercase tracking-wide">
                {el.label || "Select one"}
              </span>
              {useLargeList && (
                <span className="text-[10px] text-muted ml-auto">
                  {searchTerm ? `${filtered.length} of ` : ""}{el.options.length} items
                </span>
              )}
              {isUsed && <Check className="w-3 h-3 text-emerald-500 ml-1" />}
            </div>

            {useLargeList ? (
              <div className="border border-default rounded-lg overflow-hidden">
                <div className="flex items-center gap-2 px-3 py-2 border-b border-default bg-surface-secondary">
                  <Search className="w-3.5 h-3.5 text-muted flex-shrink-0" />
                  <input
                    type="text"
                    value={searchTerm}
                    onChange={(e) => setElementSearch((prev) => ({ ...prev, [key]: e.target.value }))}
                    placeholder={`Search ${el.label?.toLowerCase() || "options"}...`}
                    className="flex-1 text-xs bg-transparent text-primary placeholder:text-muted focus:outline-none"
                    autoFocus
                  />
                  {searchTerm && (
                    <button
                      onClick={() => setElementSearch((prev) => ({ ...prev, [key]: "" }))}
                      className="text-muted hover:text-secondary"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  )}
                </div>
                <div className="max-h-48 overflow-y-auto divide-y divide-[var(--border-color)]">
                  {filtered.length === 0 ? (
                    <div className="px-3 py-4 text-xs text-muted text-center">
                      No matches for &ldquo;{searchTerm}&rdquo;
                    </div>
                  ) : (
                    filtered.map((opt) => (
                      <button
                        key={opt.value}
                        onClick={() => handleSingleSelect(msgId, el, opt)}
                        disabled={isStreaming}
                        className="w-full text-left px-3 py-2 text-xs hover:bg-elevated transition-colors"
                      >
                        <span className="font-mono text-primary">{opt.label}</span>
                        {opt.description && (
                          <span className="text-[10px] text-muted block truncate mt-0.5">
                            {opt.description}
                          </span>
                        )}
                      </button>
                    ))
                  )}
                </div>
              </div>
            ) : (
              <div className="flex flex-wrap gap-1.5">
                {el.options.map((opt) => (
                  <button
                    key={opt.value}
                    onClick={() => handleSingleSelect(msgId, el, opt)}
                    disabled={isUsed || isStreaming}
                    className={`group relative px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                      isUsed
                        ? "border-default bg-surface-secondary text-muted cursor-default opacity-60"
                        : "border-accent/30 bg-accent/5 text-accent hover:bg-accent/15 hover:border-accent/50 cursor-pointer"
                    }`}
                  >
                    {opt.label}
                    {opt.description && (
                      <span className="absolute left-1/2 -translate-x-1/2 bottom-full mb-1.5 px-2 py-1 text-[10px] text-white bg-gray-900 rounded-md whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-10">
                        {opt.description.slice(0, 80)}
                        {opt.description.length > 80 ? "..." : ""}
                      </span>
                    )}
                  </button>
                ))}
              </div>
            )}

            {!isUsed && (
              <div className="mt-2">
                <button
                  onClick={() => {
                    setUsedElements((prev) => new Set(prev).add(key))
                    sendMessage("I'm not sure — please explore and recommend the best option for me.")
                  }}
                  disabled={isStreaming}
                  className="px-3 py-1.5 rounded-lg text-xs font-medium border border-dashed border-default text-muted hover:text-secondary hover:border-accent/30 transition-all cursor-pointer"
                >
                  Not sure? Let AI explore
                </button>
              </div>
            )}
          </div>
        )
      }

      if (el.type === "multi_select" && el.options && el.options.length > 0) {
        const selected = multiSelections[key] || new Set<string>()
        const useLargeMulti = el.options.length > COMBOBOX_THRESHOLD
        const multiSearch = elementSearch[key] || ""
        const filteredOpts = useLargeMulti && multiSearch
          ? el.options.filter((o) => {
              const q = multiSearch.toLowerCase()
              return o.label.toLowerCase().includes(q) || (o.description || "").toLowerCase().includes(q)
            })
          : el.options
        const filteredValues = filteredOpts.map((o) => o.value)
        const allFilteredSelected = filteredValues.length > 0 && filteredValues.every((v) => selected.has(v))

        const toggleAll = () => {
          setMultiSelections((prev) => {
            if (allFilteredSelected) {
              const next = new Set(selected)
              filteredValues.forEach((v) => next.delete(v))
              return { ...prev, [key]: next }
            }
            return { ...prev, [key]: new Set([...selected, ...filteredValues]) }
          })
        }

        return (
          <div key={el.id} className="mt-3">
            <div className="flex items-center gap-1.5 mb-2">
              <Icon className="w-3.5 h-3.5 text-muted" />
              <span className="text-xs font-medium text-muted uppercase tracking-wide">
                {el.label || "Select tables"}
              </span>
              {!isUsed && selected.size > 0 && (
                <span className="text-xs text-accent ml-1">{selected.size} selected</span>
              )}
              {isUsed && <Check className="w-3 h-3 text-emerald-500 ml-1" />}
              {!isUsed && filteredOpts.length > 2 && (
                <button
                  onClick={toggleAll}
                  disabled={isStreaming}
                  className="ml-auto text-[10px] text-accent hover:underline disabled:opacity-40"
                >
                  {allFilteredSelected ? "Deselect all" : `Select all${multiSearch ? " filtered" : ""}`}
                </button>
              )}
            </div>

            {/* Selected chips (large lists only) */}
            {useLargeMulti && !isUsed && selected.size > 0 && (
              <div className="flex flex-wrap gap-1 mb-2">
                {Array.from(selected).map((v) => {
                  const opt = el.options!.find((o) => o.value === v)
                  return (
                    <span
                      key={v}
                      className="inline-flex items-center gap-0.5 pl-1.5 pr-1 py-0.5 bg-accent/10 text-accent text-[10px] rounded font-mono"
                    >
                      {opt?.label || v.split(".").pop()}
                      <button onClick={() => toggleMultiOption(key, v)} disabled={isStreaming}>
                        <X className="w-2.5 h-2.5" />
                      </button>
                    </span>
                  )
                })}
              </div>
            )}

            <div className={`border border-default rounded-lg overflow-hidden ${isUsed ? "opacity-60" : ""}`}>
              {/* Search input for large lists */}
              {useLargeMulti && !isUsed && (
                <div className="flex items-center gap-2 px-3 py-2 border-b border-default bg-surface-secondary">
                  <Search className="w-3.5 h-3.5 text-muted flex-shrink-0" />
                  <input
                    type="text"
                    value={multiSearch}
                    onChange={(e) => setElementSearch((prev) => ({ ...prev, [key]: e.target.value }))}
                    placeholder={`Search ${el.options.length} tables...`}
                    className="flex-1 text-xs bg-transparent text-primary placeholder:text-muted focus:outline-none"
                  />
                  {multiSearch && (
                    <button
                      onClick={() => setElementSearch((prev) => ({ ...prev, [key]: "" }))}
                      className="text-muted hover:text-secondary"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  )}
                  <span className="text-[10px] text-muted flex-shrink-0">
                    {multiSearch ? `${filteredOpts.length} of ${el.options.length}` : `${el.options.length} tables`}
                  </span>
                </div>
              )}

              <div className="divide-y divide-[var(--border-color)] max-h-52 overflow-y-auto">
                {filteredOpts.length === 0 ? (
                  <div className="px-3 py-4 text-xs text-muted text-center">
                    No matches for &ldquo;{multiSearch}&rdquo;
                  </div>
                ) : (
                  filteredOpts.map((opt) => {
                    const checked = selected.has(opt.value)
                    return (
                      <label
                        key={opt.value}
                        className={`flex items-center gap-3 px-3 py-2 text-xs transition-colors ${
                          isUsed ? "cursor-default" : "cursor-pointer hover:bg-elevated"
                        }`}
                      >
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => toggleMultiOption(key, opt.value)}
                          disabled={isUsed || isStreaming}
                          className="w-3.5 h-3.5 shrink-0 accent-[var(--color-accent)]"
                        />
                        <div className="min-w-0 flex-1">
                          <span
                            className={`font-mono block truncate ${checked ? "text-primary font-medium" : "text-secondary"}`}
                          >
                            {opt.label}
                          </span>
                          {opt.description && (
                            <span className="text-[10px] text-muted block truncate">
                              {opt.description}
                            </span>
                          )}
                        </div>
                      </label>
                    )
                  })
                )}
              </div>
            </div>
            {!isUsed && (
              <div className="mt-2 flex items-center gap-2">
                <button
                  onClick={() => confirmMultiSelect(msgId, el)}
                  disabled={selected.size === 0 || isStreaming}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-accent text-white rounded-lg text-xs font-medium disabled:opacity-40 hover:bg-accent/90 transition-colors"
                >
                  <Check className="w-3 h-3" />
                  Confirm Selection ({selected.size})
                </button>
                <button
                  onClick={() => {
                    setUsedElements((prev) => new Set(prev).add(key))
                    sendMessage("I'm not sure which tables to pick — please recommend the best ones for my use case.")
                  }}
                  disabled={isStreaming}
                  className="px-3 py-1.5 rounded-lg text-xs font-medium border border-dashed border-default text-muted hover:text-secondary hover:border-accent/30 transition-colors"
                >
                  Not sure? Let AI pick
                </button>
              </div>
            )}
          </div>
        )
      }

      if (el.type === "config_preview" && el.config) {
        return (
          <div key={el.id} className="mt-3 border border-default rounded-lg overflow-hidden">
            <div className="flex items-center justify-between px-3 py-2 bg-surface-secondary border-b border-default">
              <span className="text-xs font-medium text-secondary">
                {el.label || "Configuration Preview"}
              </span>
              <button
                onClick={() => handleCopyConfig(el.config!)}
                className="flex items-center gap-1 text-xs text-muted hover:text-secondary transition-colors"
              >
                {copiedConfig ? (
                  <>
                    <CheckCheck className="w-3 h-3 text-emerald-500" /> Copied
                  </>
                ) : (
                  <>
                    <Copy className="w-3 h-3" /> Copy JSON
                  </>
                )}
              </button>
            </div>
            <pre className="text-xs font-mono p-3 max-h-64 overflow-auto text-secondary">
              {JSON.stringify(el.config, null, 2)}
            </pre>
          </div>
        )
      }

      return null
    })
  }

  const renderCreatedBanner = (space: {
    space_id: string
    url: string
    display_name: string
  }) => (
    <div className="mx-4 my-4">
      <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-xl p-4">
        <div className="flex items-center gap-3 mb-3">
          <div className="w-8 h-8 rounded-full bg-emerald-500/20 flex items-center justify-center flex-shrink-0">
            <Check className="w-4 h-4 text-emerald-500" />
          </div>
          <div>
            <p className="text-sm font-semibold text-primary">Space Created</p>
            <p className="text-xs text-muted">{space.display_name}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <a
            href={space.url}
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-1.5 px-3 py-1.5 bg-accent text-white rounded-lg text-xs font-medium hover:bg-accent/90 transition-colors"
          >
            <ExternalLink className="w-3 h-3" />
            Open Genie Space
          </a>
          <button
            onClick={() => onCreated(space.space_id, space.display_name)}
            className="flex items-center gap-1.5 px-3 py-1.5 border border-default text-secondary rounded-lg text-xs font-medium hover:bg-elevated transition-colors"
          >
            Diagnose Space
          </button>
        </div>
      </div>
    </div>
  )

  const renderMessage = (msg: AgentChatMessage) => {
    if (msg.role === "tool") return renderToolCall(msg)
    if (msg.created_space) return renderCreatedBanner(msg.created_space)
    if (msg.role === "user") {
      return (
        <div key={msg.id} className="flex items-start gap-3 mx-4 my-3 justify-end">
          <div className="max-w-[80%] bg-accent/10 rounded-xl rounded-tr-sm px-4 py-2.5">
            <p className="text-sm text-primary whitespace-pre-wrap">{msg.content}</p>
          </div>
          <div className="w-7 h-7 rounded-lg bg-surface-secondary flex items-center justify-center flex-shrink-0 mt-0.5">
            <User className="w-4 h-4 text-muted" />
          </div>
        </div>
      )
    }

    if (msg.is_error) {
      return (
        <div key={msg.id} className="mx-4 my-3">
          <div className="flex items-start gap-2.5 px-3.5 py-2.5 bg-red-500/10 border border-red-500/25 rounded-xl">
            <AlertCircle className="w-4 h-4 text-red-400 flex-shrink-0 mt-0.5" />
            <div className="min-w-0">
              <p className="text-xs font-medium text-red-400 mb-0.5">Something went wrong</p>
              <p className="text-xs text-red-300/80 break-words">{msg.content}</p>
            </div>
          </div>
        </div>
      )
    }

    let isLastAssistant = false
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === "assistant" && !messages[i].is_thinking) {
        isLastAssistant = messages[i] === msg
        break
      }
    }
    const showSuggestions = isLastAssistant && !isStreaming && !msg.ui_elements?.length

    return (
      <div key={msg.id} className="flex items-start gap-3 mx-4 my-3">
        <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center flex-shrink-0 mt-0.5">
          <Bot className="w-4 h-4 text-accent" />
        </div>
        <div className="max-w-[85%] min-w-0">
          <div className="prose prose-chat max-w-none">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
          </div>
          {msg.ui_elements && msg.ui_elements.length > 0 && renderUIElements(msg.id, msg.ui_elements)}
          {showSuggestions && getSuggestions().length > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-3">
              {getSuggestions().map((s) => (
                <button
                  key={s}
                  onClick={() => sendMessage(s)}
                  className="px-2.5 py-1 text-[11px] text-accent bg-accent/5 border border-accent/20 rounded-full hover:bg-accent/15 transition-colors"
                >
                  {s}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
    )
  }

  const getSuggestions = (): string[] => {
    if (isStreaming || messages.length === 0) return []
    const p = progress
    if (p.spaceId) return ["Add a business rule", "Update sample questions", "Diagnose the space"]
    if (p.configReady) return ["Create the space now", "Add a business rule first", "Show me the config"]
    if (p.planReady) return ["Looks good — proceed", "Add more sample questions", "Add a business rule"]
    if (p.inspectionDone) return ["Build the plan", "Inspect more tables", "Add a business rule"]
    if (p.tables.length > 0) return ["Inspect these tables", "Add more tables", "Skip inspection — build plan"]
    if (p.catalog) return []
    return []
  }

  // ─── Progress panel ───────────────────────────────────────────

  const step = currentStep(progress)

  const renderPanel = () => (
    <aside className="w-72 xl:w-80 flex-shrink-0 border border-default rounded-xl bg-surface overflow-hidden flex flex-col">
      {/* Panel header */}
      <div className="px-4 py-3 border-b border-default">
        <div className="flex items-center justify-between">
          <span className="text-xs font-semibold text-primary uppercase tracking-wide">
            Build Progress
          </span>
          {messages.length > 0 && (
            <button
              onClick={handleClear}
              disabled={isStreaming}
              className="flex items-center gap-1 text-[10px] text-muted hover:text-red-400 transition-colors disabled:opacity-40"
              title="Start over — clears everything"
            >
              <RotateCcw className="w-3 h-3" />
              Start over
            </button>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-1">
        {/* Steps */}
        {STEPS.map((s, i) => {
          const done = i < step
          const active = i === step
          const canBacktrack = done && !isStreaming && !!s.backtrackMsg
          const { Icon } = s

          return (
            <div key={s.key} className="flex gap-3">
              {/* Vertical line + dot */}
              <div className="flex flex-col items-center">
                <div
                  onClick={canBacktrack ? () => sendMessage(s.backtrackMsg) : undefined}
                  className={`w-6 h-6 rounded-full flex items-center justify-center flex-shrink-0 ${
                    done
                      ? "bg-emerald-500/20 text-emerald-500"
                      : active
                        ? "bg-accent/15 text-accent ring-2 ring-accent/40"
                        : "bg-elevated text-muted"
                  } ${canBacktrack ? "cursor-pointer hover:ring-2 hover:ring-emerald-500/40 transition-all" : ""}`}
                >
                  {done ? <Check className="w-3 h-3" /> : <Icon className="w-3 h-3" />}
                </div>
                {i < STEPS.length - 1 && (
                  <div
                    className={`w-px flex-1 min-h-4 my-0.5 ${done ? "bg-emerald-500/40" : "bg-[var(--border-color)]"}`}
                  />
                )}
              </div>

              {/* Step content */}
              <div className="pb-3 flex-1 min-w-0">
                <div className="flex items-center gap-1.5">
                  <span
                    onClick={canBacktrack ? () => sendMessage(s.backtrackMsg) : undefined}
                    className={`text-xs font-medium ${
                      done
                        ? "text-emerald-500"
                        : active
                          ? "text-accent"
                          : "text-muted"
                    } ${canBacktrack ? "cursor-pointer hover:underline" : ""}`}
                    title={canBacktrack ? "Click to go back to this step" : undefined}
                  >
                    {s.label}
                  </span>
                  {active && !autoPilot && !isStreaming && s.key !== "create" && (
                    <button
                      onClick={() =>
                        sendMessage(`Skip ${s.label} — let AI decide`, { skip_step: s.key })
                      }
                      className="flex items-center gap-1 ml-auto px-2 py-0.5 rounded-full bg-accent/10 text-accent text-[10px] font-medium hover:bg-accent/20 transition-colors"
                      title={`Let AI handle ${s.label} automatically`}
                    >
                      <FastForward className="w-2.5 h-2.5" />
                      skip — let AI decide
                    </button>
                  )}
                </div>

                {/* Step-specific details */}
                {s.key === "requirements" && (
                  <div className="mt-1 space-y-1.5">
                    {editingTitle ? (
                      <div className="flex gap-1">
                        <input
                          value={titleDraft}
                          onChange={(e) => setTitleDraft(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") submitTitle()
                            if (e.key === "Escape") setEditingTitle(false)
                          }}
                          autoFocus
                          placeholder="Space name"
                          className="flex-1 text-xs border border-accent/40 rounded px-2 py-1 bg-surface text-primary focus:outline-none"
                        />
                        <button
                          onClick={submitTitle}
                          disabled={!titleDraft.trim() || isStreaming}
                          className="px-1.5 text-accent disabled:opacity-40"
                        >
                          <Check className="w-3 h-3" />
                        </button>
                      </div>
                    ) : progress.title ? (
                      <button
                        onClick={() => {
                          setTitleDraft(progress.title)
                          setEditingTitle(true)
                        }}
                        className="group flex items-center gap-1 text-xs text-secondary hover:text-primary transition-colors"
                      >
                        <span className="truncate">{progress.title}</span>
                        <Pencil className="w-2.5 h-2.5 text-muted opacity-0 group-hover:opacity-100 transition-opacity" />
                      </button>
                    ) : active ? (
                      <button
                        onClick={() => setEditingTitle(true)}
                        className="text-[10px] text-accent hover:underline mt-0.5"
                      >
                        + Set name
                      </button>
                    ) : null}
                    {progress.businessContext.length > 0 && (
                      <div className="space-y-0.5">
                        {progress.businessContext.map((ctx, ci) => (
                          <div key={ci} className="group flex items-start gap-1">
                            <p className="text-[10px] text-muted flex-1 truncate" title={ctx}>
                              &bull; {ctx}
                            </p>
                            <button
                              onClick={() => removeBusinessContext(ci)}
                              className="opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
                            >
                              <X className="w-2.5 h-2.5 text-muted hover:text-red-400" />
                            </button>
                          </div>
                        ))}
                      </div>
                    )}
                    {(active || done) && (
                      <div className="flex gap-1">
                        <input
                          value={businessContextDraft}
                          onChange={(e) => setBusinessContextDraft(e.target.value)}
                          onKeyDown={(e) => { if (e.key === "Enter") addBusinessContext() }}
                          placeholder="Add a business rule..."
                          className="flex-1 text-[10px] border border-default rounded px-2 py-1 bg-surface text-primary focus:outline-none focus:border-accent/40"
                        />
                        <button
                          onClick={addBusinessContext}
                          disabled={!businessContextDraft.trim()}
                          className="px-1.5 text-accent disabled:opacity-30"
                        >
                          <Plus className="w-3 h-3" />
                        </button>
                      </div>
                    )}
                  </div>
                )}

                {s.key === "data" && (progress.catalog || progress.tables.length > 0) && (
                  <div className="mt-1 space-y-1">
                    {progress.catalog && (
                      <span className="text-[10px] text-muted font-mono block truncate">
                        {progress.schemas.length > 0
                          ? progress.schemas.map((s) => s.includes(".") ? s : `${progress.catalog}.${s}`).join(", ")
                          : progress.catalog}
                      </span>
                    )}
                    {progress.tables.length > 0 && (
                      <div className="flex flex-wrap gap-1">
                        {progress.tables.map((t) => {
                          const short = t.split(".").pop() || t
                          return (
                            <span
                              key={t}
                              className="group inline-flex items-center gap-0.5 pl-1.5 pr-1 py-0.5 bg-accent/10 text-accent text-[10px] rounded font-mono"
                            >
                              {short}
                              <button
                                onClick={() => removeTable(t)}
                                disabled={isStreaming}
                                className="opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-0"
                              >
                                <X className="w-2.5 h-2.5" />
                              </button>
                            </span>
                          )
                        })}
                      </div>
                    )}
                  </div>
                )}

                {s.key === "inspection" && progress.inspectionDone && (
                  <div className="mt-1">
                    {(() => {
                      const is = progress.inspectionSummary
                      const parts: string[] = []
                      if (is.columnsProfiled > 0) parts.push(`${is.columnsProfiled} columns profiled`)
                      if (is.qualityIssues > 0) parts.push(`${is.qualityIssues} quality issue${is.qualityIssues !== 1 ? "s" : ""}`)
                      if (is.lineageCount > 0) parts.push(`${is.lineageCount} table${is.lineageCount !== 1 ? "s" : ""} with lineage`)
                      return parts.length > 0 ? (
                        <p className="text-[10px] text-muted">{parts.join(" · ")}</p>
                      ) : (
                        <p className="text-[10px] text-muted">Inspection complete</p>
                      )
                    })()}
                  </div>
                )}

                {s.key === "plan" && progress.planReady && (
                  <div className="mt-1">
                    {(() => {
                      const ps = progress.planSummary
                      const parts: string[] = []
                      if (ps.questions > 0) parts.push(`${ps.questions} question${ps.questions !== 1 ? "s" : ""}`)
                      if (ps.benchmarks > 0) parts.push(`${ps.benchmarks} benchmark${ps.benchmarks !== 1 ? "s" : ""}`)
                      if (ps.measures > 0) parts.push(`${ps.measures} measure${ps.measures !== 1 ? "s" : ""}`)
                      if (ps.joins > 0) parts.push(`${ps.joins} join${ps.joins !== 1 ? "s" : ""}`)
                      if (ps.exampleSqls > 0) parts.push(`${ps.exampleSqls} SQL example${ps.exampleSqls !== 1 ? "s" : ""}`)
                      if (ps.filters > 0) parts.push(`${ps.filters} filter${ps.filters !== 1 ? "s" : ""}`)
                      if (ps.textInstruction) parts.push("text instruction")
                      return parts.length > 0 ? (
                        <p className="text-[10px] text-muted">{parts.join(", ")}</p>
                      ) : null
                    })()}
                  </div>
                )}

                {s.key === "config" && progress.configReady && progress.config && (
                  <div className="mt-1">
                    <button
                      onClick={() => handleCopyConfig(progress.config!)}
                      className="flex items-center gap-1 text-[10px] text-accent hover:underline"
                    >
                      <Copy className="w-2.5 h-2.5" />
                      Copy JSON
                    </button>
                  </div>
                )}

                {s.key === "create" && progress.spaceId && (
                  <div className="mt-1">
                    <a
                      href={progress.spaceUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="flex items-center gap-1 text-[10px] text-accent hover:underline"
                    >
                      <ExternalLink className="w-2.5 h-2.5" />
                      Open space
                    </a>
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>

      {/* Panel footer — space links */}
      {progress.spaceId && progress.spaceUrl && (
        <div className="border-t border-default px-4 py-3 flex gap-2">
          <a
            href={progress.spaceUrl}
            target="_blank"
            rel="noreferrer"
            className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 text-xs font-medium text-accent bg-accent/5 border border-accent/20 rounded-lg hover:bg-accent/10 transition-colors"
          >
            <ExternalLink className="w-3 h-3" />
            Open Space
          </a>
          <button
            onClick={() => onCreated(progress.spaceId, progress.spaceDisplayName)}
            className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 text-xs font-medium text-secondary border border-default rounded-lg hover:bg-elevated transition-colors"
          >
            Diagnose Space
          </button>
        </div>
      )}
    </aside>
  )

  // ─── Main layout ──────────────────────────────────────────────

  return (
    <div className="flex gap-4 h-[calc(100vh-13rem)]">
      {/* Chat column */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Chat area */}
        <div className="flex-1 overflow-y-auto border border-default rounded-xl bg-surface">
          {messages.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-center px-8">
              <div className="w-12 h-12 rounded-2xl bg-accent/10 flex items-center justify-center mb-4">
                <Sparkles className="w-6 h-6 text-accent" />
              </div>
              <h3 className="text-lg font-semibold text-primary mb-2">
                Create a Genie Space
              </h3>
              <p className="text-sm text-muted max-w-md mb-6">
                Describe what you want to build and the AI agent will guide you through — or
                use the progress panel to fill in details directly.
              </p>
              <div className="flex flex-wrap gap-2 justify-center">
                {[
                  "Build a space for NYC taxi trip analysis using samples.nyctaxi",
                  "Create a sales analytics space from samples.tpch",
                  "Explore retail data with samples.tpcds",
                ].map((q) => (
                  <button
                    key={q}
                    onClick={() => sendMessage(q)}
                    className="px-3 py-1.5 text-xs text-muted bg-surface-secondary hover:bg-elevated rounded-full border border-default transition-colors"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div className="py-4">
              {groupMessages(messages).map((item) =>
                item.type === "inspection_group"
                  ? renderInspectionGroup(item)
                  : renderMessage(item.msg),
              )}
              {agentStatus && (
                <div className="mx-4 my-2 py-1.5 space-y-1.5">
                  <div className="flex items-center gap-2.5">
                    <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center flex-shrink-0">
                      <Loader2 className="w-4 h-4 text-accent animate-spin" />
                    </div>
                    <span className="text-xs font-medium text-muted">{agentStatus}</span>
                  </div>
                  {agentStep && (
                    <div className="ml-9 flex items-center gap-1.5">
                      {STEP_META.map((s, i) => (
                        <div
                          key={s.key}
                          className={`h-1 rounded-full transition-all duration-300 ${
                            i < agentStep.index
                              ? "w-5 bg-accent/40"
                              : i === agentStep.index
                                ? "w-7 bg-accent"
                                : "w-5 bg-border"
                          }`}
                          title={s.label}
                        />
                      ))}
                      <span className="ml-1.5 text-[10px] text-muted/60">{agentStep.label}</span>
                    </div>
                  )}
                </div>
              )}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        {/* "Continue" nudge — shown when agent is idle, it's the user's turn, and no suggestion chips are visible */}
        {messages.length > 0 && !isStreaming && !queuedMessage && !input.trim() && getSuggestions().length === 0 && (() => {
          const lastMsg = messages[messages.length - 1]
          const isAgentTurn = lastMsg?.role === "assistant" || lastMsg?.role === "tool"
          if (!isAgentTurn) return null
          const p = progress
          let nudge = "Continue"
          if (p.spaceId) nudge = "What else can you help with?"
          else if (p.configReady) nudge = "Let's create the space"
          else if (editedPlan) nudge = "I've reviewed the plan — let's proceed"
          else if (p.inspectionDone) nudge = "Continue to build the plan"
          else if (p.tables.length > 0) nudge = "Continue with data inspection"
          else if (p.schemas.length > 0) nudge = "Continue with table selection"
          else if (p.catalog) nudge = "Continue with schema selection"
          else nudge = "Let's get started"
          return (
            <button
              onClick={() => sendMessage(nudge)}
              className="mt-1.5 self-start flex items-center gap-1.5 px-3 py-1.5 text-xs text-accent bg-accent/5 border border-accent/20 rounded-full hover:bg-accent/10 transition-colors"
            >
              <Sparkles className="w-3 h-3" />
              {nudge}
            </button>
          )
        })()}

        {/* Queued message indicator */}
        {queuedMessage && (
          <div className="mt-1.5 flex items-center gap-2 px-3 py-1.5 bg-amber-500/10 border border-amber-500/30 rounded-lg">
            <Clock className="w-3.5 h-3.5 text-amber-500 flex-shrink-0" />
            <span className="text-xs text-amber-600 dark:text-amber-400 flex-1 truncate">
              Queued: &ldquo;{queuedMessage}&rdquo;
            </span>
            <button
              onClick={() => setQueuedMessage(null)}
              className="text-amber-500 hover:text-amber-600 flex-shrink-0"
            >
              <X className="w-3 h-3" />
            </button>
          </div>
        )}

        {/* Auto-pilot + Input area */}
        {messages.length > 0 && (
          <div className="mt-2 flex items-center justify-between">
            <label
              className={`flex items-center gap-2 cursor-pointer select-none ${isStreaming ? "opacity-40 pointer-events-none" : ""}`}
            >
              <button
                type="button"
                role="switch"
                aria-checked={autoPilot}
                onClick={() => {
                  const next = !autoPilot
                  setAutoPilot(next)
                  sendMessage(
                    next
                      ? "Switch to auto-pilot — handle everything from here, pause only at the plan review step."
                      : "Switch back to guided mode — pause at each step for my input.",
                    { auto_pilot: next },
                  )
                }}
                className={`relative inline-flex h-4 w-7 flex-shrink-0 items-center rounded-full transition-colors ${
                  autoPilot ? "bg-accent" : "bg-[var(--border-color)]"
                }`}
              >
                <span
                  className={`inline-block h-3 w-3 rounded-full bg-white shadow-sm transition-transform ${
                    autoPilot ? "translate-x-3.5" : "translate-x-0.5"
                  }`}
                />
              </button>
              <span className={`text-[11px] ${autoPilot ? "text-accent font-medium" : "text-muted"}`}>
                {autoPilot ? "Auto-Pilot ON" : "Auto-Pilot"}
              </span>
            </label>
          </div>
        )}
        <form onSubmit={handleSubmit} className={`${messages.length > 0 ? "mt-1" : "mt-2"} relative`}>
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={
              isStreaming
                ? queuedMessage
                  ? "Edit your queued message or type a new one..."
                  : "Type to queue a message for when the agent finishes..."
                : "Describe your Genie space or answer a question..."
            }
            rows={1}
            className="w-full border border-default rounded-xl pl-4 pr-11 py-2.5 text-sm bg-surface text-primary resize-none focus:outline-none focus:ring-2 focus:ring-accent/30 focus:border-accent/50 transition-all"
            style={{ minHeight: "40px", maxHeight: "120px" }}
            onInput={(e) => {
              const target = e.target as HTMLTextAreaElement
              target.style.height = "auto"
              target.style.height = Math.min(target.scrollHeight, 120) + "px"
            }}
          />
          {isStreaming ? (
            <button
              type="button"
              onClick={handleStop}
              className="absolute right-1.5 top-1/2 -translate-y-1/2 flex items-center justify-center w-7 h-7 rounded-lg border border-red-500/30 text-red-400 hover:bg-red-500/10 transition-colors"
            >
              <div className="w-2.5 h-2.5 rounded-sm bg-red-400" />
            </button>
          ) : (
            <button
              type="submit"
              disabled={!input.trim()}
              className="absolute right-1.5 top-1/2 -translate-y-1/2 flex items-center justify-center w-7 h-7 rounded-lg bg-accent text-white disabled:opacity-30 hover:bg-accent/90 transition-colors"
            >
              <Send className="w-3.5 h-3.5" />
            </button>
          )}
        </form>
      </div>

      {/* Progress panel */}
      {panelOpen && renderPanel()}

      {/* Clear confirmation dialog */}
      {showClearConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-surface border border-default rounded-xl shadow-xl p-5 max-w-sm mx-4">
            <h3 className="text-sm font-semibold text-primary mb-2">Start over?</h3>
            <p className="text-xs text-muted mb-4">
              This will clear the entire conversation, all progress, and any unsaved plan. This cannot be undone.
            </p>
            <div className="flex items-center justify-end gap-2">
              <button
                onClick={() => setShowClearConfirm(false)}
                className="px-3 py-1.5 text-xs font-medium text-secondary border border-default rounded-md hover:bg-elevated transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={confirmClear}
                className="px-3 py-1.5 text-xs font-medium text-white bg-red-600 rounded-md hover:bg-red-500 transition-colors"
              >
                Clear everything
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
