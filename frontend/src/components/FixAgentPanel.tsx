/**
 * FixAgentPanel - Streaming AI fix agent UI with diff view.
 */
import { useState, useRef, useEffect } from "react"
import { X, Zap, CheckCircle, AlertCircle, Code2, ChevronDown, ChevronRight } from "lucide-react"
import { streamFixAgent } from "@/lib/api"
import type { FixAgentEvent, FixPatch } from "@/types"

interface FixAgentPanelProps {
  spaceId: string
  findings: string[]
  spaceConfig: Record<string, unknown>
  onClose: () => void
}

export function FixAgentPanel({ spaceId, findings, spaceConfig, onClose }: FixAgentPanelProps) {
  const [events, setEvents] = useState<FixAgentEvent[]>([])
  const [isRunning, setIsRunning] = useState(false)
  const [completed, setCompleted] = useState(false)
  const [patches, setPatches] = useState<FixPatch[]>([])
  const [expandedPatch, setExpandedPatch] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const stopRef = useRef<(() => void) | null>(null)
  const logRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight
    }
  }, [events])

  const handleRun = () => {
    setEvents([])
    setPatches([])
    setCompleted(false)
    setError(null)
    setIsRunning(true)

    stopRef.current = streamFixAgent(
      spaceId,
      findings,
      spaceConfig,
      (event) => {
        setEvents(prev => [...prev, event])
        if (event.status === "complete") {
          setCompleted(true)
          setIsRunning(false)
          if (event.diff?.patches) setPatches(event.diff.patches)
        } else if (event.status === "error") {
          setError(event.message || "Fix agent failed")
          setIsRunning(false)
        }
      },
      (err) => {
        setError(err.message)
        setIsRunning(false)
      }
    )
  }

  const handleStop = () => {
    stopRef.current?.()
    setIsRunning(false)
  }

  return (
    <div className="bg-surface border border-accent/30 rounded-xl overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-3 border-b border-default bg-accent/5">
        <div className="flex items-center gap-2">
          <Zap className="w-4 h-4 text-accent" />
          <span className="text-sm font-semibold text-primary">AI Fix Agent</span>
          {isRunning && (
            <span className="flex items-center gap-1 text-xs text-accent">
              <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />
              Running
            </span>
          )}
        </div>
        <button onClick={onClose} className="text-muted hover:text-secondary transition-colors">
          <X className="w-4 h-4" />
        </button>
      </div>

      <div className="p-5 space-y-4">
        {/* Findings summary */}
        <div>
          <p className="text-sm text-muted mb-2">Findings to fix ({findings.length}):</p>
          <ul className="space-y-1">
            {findings.map((f, i) => (
              <li key={i} className="text-xs text-secondary flex items-start gap-2">
                <span className="w-1 h-1 rounded-full bg-amber-400 mt-1.5 flex-shrink-0" />
                {f}
              </li>
            ))}
          </ul>
        </div>

        {/* Controls */}
        <div className="flex items-center gap-2">
          {!isRunning && !completed && (
            <button
              onClick={handleRun}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-accent text-white text-sm hover:bg-accent/90 transition-colors"
            >
              <Zap className="w-4 h-4" />
              Run Fix Agent
            </button>
          )}
          {isRunning && (
            <button
              onClick={handleStop}
              className="flex items-center gap-2 px-4 py-2 rounded-lg border border-red-500/30 text-red-400 text-sm hover:bg-red-500/10 transition-colors"
            >
              Stop
            </button>
          )}
          {completed && (
            <button
              onClick={handleRun}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-default text-muted text-sm hover:bg-surface-secondary transition-colors"
            >
              Run Again
            </button>
          )}
        </div>

        {/* Event log */}
        {events.length > 0 && (
          <div
            ref={logRef}
            className="bg-surface-secondary rounded-lg p-3 space-y-1.5 max-h-48 overflow-y-auto text-xs font-mono"
          >
            {events.map((event, i) => {
              if (event.status === "thinking") return (
                <div key={i} className="text-muted">› {event.message}</div>
              )
              if (event.status === "patch") return (
                <div key={i} className="text-blue-400">
                  ✎ {event.field_path}
                </div>
              )
              if (event.status === "applying") return (
                <div key={i} className="text-amber-400">⚡ {event.message}</div>
              )
              if (event.status === "complete") return (
                <div key={i} className="text-emerald-400">✓ {event.summary}</div>
              )
              if (event.status === "error") return (
                <div key={i} className="text-red-400">✗ {event.message}</div>
              )
              return null
            })}
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="flex items-center gap-2 text-sm text-red-400 p-3 bg-red-500/10 rounded-lg">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            {error}
          </div>
        )}

        {/* Patches applied */}
        {completed && patches.length > 0 && (
          <div>
            <div className="flex items-center gap-2 text-sm text-emerald-400 mb-3">
              <CheckCircle className="w-4 h-4" />
              Applied {patches.length} patch{patches.length !== 1 ? "es" : ""}
            </div>
            <div className="space-y-2">
              {patches.map((patch, i) => (
                <div key={i} className="border border-default rounded-lg overflow-hidden">
                  <button
                    onClick={() => setExpandedPatch(expandedPatch === i ? null : i)}
                    className="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-surface-secondary transition-colors"
                  >
                    {expandedPatch === i ? <ChevronDown className="w-3.5 h-3.5 text-muted" /> : <ChevronRight className="w-3.5 h-3.5 text-muted" />}
                    <Code2 className="w-3.5 h-3.5 text-blue-400" />
                    <span className="text-xs font-mono text-secondary flex-1 truncate">{patch.field_path}</span>
                  </button>
                  {expandedPatch === i && (
                    <div className="px-3 pb-3 space-y-2 bg-surface-secondary/50 text-xs">
                      <div>
                        <span className="text-red-400">- </span>
                        <span className="text-muted font-mono">{JSON.stringify(patch.old_value)}</span>
                      </div>
                      <div>
                        <span className="text-emerald-400">+ </span>
                        <span className="text-secondary font-mono">{JSON.stringify(patch.new_value)}</span>
                      </div>
                      <p className="text-muted italic">{patch.rationale}</p>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {completed && patches.length === 0 && !error && (
          <div className="flex items-center gap-2 text-sm text-muted">
            <CheckCircle className="w-4 h-4 text-emerald-400" />
            No patches needed — space configuration looks good!
          </div>
        )}
      </div>
    </div>
  )
}
