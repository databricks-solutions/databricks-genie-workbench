import { useEffect, useMemo, useRef, useState } from "react"
import type { ReactNode } from "react"
import { AlertTriangle, Bot, Check, ChevronDown, Loader2 } from "lucide-react"
import { getModels } from "@/lib/api"
import type { LLMModelInfo } from "@/types"
import { cn } from "@/lib/utils"

interface ModelPickerProps {
  value: string | null
  onChange: (model: string | null) => void
  disabled?: boolean
  label?: string
  helper?: ReactNode
  className?: string
}

interface ChatModelMenuProps {
  value: string | null
  onChange: (model: string | null) => void
  disabled?: boolean
  className?: string
}

function useModelCatalog() {
  const [models, setModels] = useState<LLMModelInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    getModels()
      .then((items) => {
        if (cancelled) return
        setModels(items)
        setError(null)
      })
      .catch((err) => {
        if (cancelled) return
        setModels([])
        setError(err instanceof Error ? err.message : "Model list unavailable")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const defaultModel = useMemo(
    () => models.find((model) => model.isDefault) ?? null,
    [models],
  )

  return { models, loading, error, defaultModel }
}

export function ModelPicker({
  value,
  onChange,
  disabled = false,
  label = "Model",
  helper,
  className,
}: ModelPickerProps) {
  const { models, loading, error, defaultModel } = useModelCatalog()
  const selectedValue = value ?? defaultModel?.name ?? ""
  const isDisabled = disabled || loading || models.length === 0

  return (
    <div className={cn("min-w-[15rem] space-y-1.5", className)}>
      <label className="flex items-center gap-1.5 text-xs font-medium text-muted">
        {loading ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin text-accent" />
        ) : error ? (
          <AlertTriangle className="h-3.5 w-3.5 text-warning-foreground" />
        ) : (
          <Bot className="h-3.5 w-3.5 text-accent" />
        )}
        {label}
      </label>
      <div className="relative">
        <select
          value={selectedValue}
          disabled={isDisabled}
          onChange={(event) => onChange(event.target.value || null)}
          className={cn(
            "h-9 w-full appearance-none rounded-lg border border-default bg-surface px-3 pr-8 text-sm text-primary shadow-sm",
            "focus:outline-none focus:ring-2 focus:ring-accent/20 focus:border-accent",
            "disabled:cursor-not-allowed disabled:opacity-60",
          )}
        >
          {loading ? (
            <option value="">Loading models...</option>
          ) : models.length === 0 ? (
            <option value="">No chat models found</option>
          ) : (
            models.map((model) => (
              <option key={model.name} value={model.name}>
                {model.displayName}{model.isDefault ? " (default)" : ""}
              </option>
            ))
          )}
        </select>
        <ChevronDown className="pointer-events-none absolute right-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted" />
      </div>
      {helper && <div className="text-xs text-muted leading-relaxed">{helper}</div>}
      {error && <div className="text-xs text-warning-foreground">{error}</div>}
    </div>
  )
}

export function ChatModelMenu({
  value,
  onChange,
  disabled = false,
  className,
}: ChatModelMenuProps) {
  const { models, loading, error, defaultModel } = useModelCatalog()
  const [open, setOpen] = useState(false)
  const rootRef = useRef<HTMLDivElement>(null)

  const selectedValue = value ?? defaultModel?.name ?? ""
  const selectedModel = models.find((model) => model.name === selectedValue) ?? defaultModel
  const isDisabled = disabled || loading
  const menuOpen = open && !isDisabled

  useEffect(() => {
    if (!menuOpen) return

    function handlePointerDown(event: MouseEvent) {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false)
      }
    }

    document.addEventListener("mousedown", handlePointerDown)
    return () => document.removeEventListener("mousedown", handlePointerDown)
  }, [menuOpen])

  const buttonLabel = loading
    ? "Models"
    : error
      ? "Models unavailable"
      : selectedModel?.displayName ?? "No models"

  return (
    <div ref={rootRef} className={cn("relative", className)}>
      <button
        type="button"
        disabled={isDisabled}
        aria-haspopup="listbox"
        aria-expanded={menuOpen}
        onClick={() => setOpen((next) => !next)}
        className={cn(
          "flex h-8 min-w-0 max-w-[13rem] items-center gap-1.5 rounded-lg border border-default bg-surface-secondary px-2.5 text-xs font-medium text-secondary shadow-sm transition-colors",
          "hover:border-accent/40 hover:bg-elevated hover:text-primary",
          "focus:outline-none focus:ring-2 focus:ring-accent/25",
          "disabled:cursor-not-allowed disabled:opacity-55",
        )}
      >
        {loading ? (
          <Loader2 className="h-3.5 w-3.5 flex-shrink-0 animate-spin text-accent" />
        ) : error ? (
          <AlertTriangle className="h-3.5 w-3.5 flex-shrink-0 text-warning-foreground" />
        ) : (
          <Bot className="h-3.5 w-3.5 flex-shrink-0 text-accent" />
        )}
        <span className="truncate">{buttonLabel}</span>
        <ChevronDown className={cn("h-3.5 w-3.5 flex-shrink-0 text-muted transition-transform", menuOpen && "rotate-180")} />
      </button>

      {menuOpen && (
        <div
          role="listbox"
          className="absolute bottom-full right-0 z-50 mb-2 w-72 overflow-hidden rounded-xl border border-default bg-surface p-1.5 shadow-xl"
        >
          <div className="px-2.5 py-2 text-xs font-medium text-muted">Model</div>
          {error ? (
            <div className="px-2.5 pb-2 text-xs text-warning-foreground">{error}</div>
          ) : models.length === 0 ? (
            <div className="px-2.5 pb-2 text-xs text-muted">No chat models found</div>
          ) : (
            <div className="max-h-72 overflow-y-auto">
              {models.map((model) => {
                const selected = model.name === selectedValue
                return (
                  <button
                    key={model.name}
                    type="button"
                    role="option"
                    aria-selected={selected}
                    onClick={() => {
                      onChange(model.name)
                      setOpen(false)
                    }}
                    className={cn(
                      "flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-left text-sm transition-colors",
                      selected
                        ? "bg-accent/10 text-primary"
                        : "text-secondary hover:bg-elevated hover:text-primary",
                    )}
                  >
                    <span className="flex h-4 w-4 flex-shrink-0 items-center justify-center">
                      {selected && <Check className="h-3.5 w-3.5 text-accent" />}
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="block truncate font-medium">{model.displayName}</span>
                      {model.isDefault && <span className="block text-xs text-muted">Default</span>}
                    </span>
                  </button>
                )
              })}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
