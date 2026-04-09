import type { ReactNode } from "react"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"

export interface PipelineStep {
  icon: ReactNode
  label: string
  description?: string
  color: string
}

interface PipelineDiagramProps {
  steps: PipelineStep[]
  className?: string
}

export function PipelineDiagram({ steps, className }: PipelineDiagramProps) {
  return (
    <div className={cn("", className)}>
      {/* Mobile: vertical stack */}
      <div className="flex flex-col gap-3 sm:hidden">
        {steps.map((step, i) => (
          <div key={i} className="flex items-start gap-3">
            <div
              className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border-2"
              style={{ borderColor: step.color, background: `${step.color}15` }}
            >
              <div style={{ color: step.color }}>{step.icon}</div>
            </div>
            <div className="pt-0.5">
              <span className="text-xs font-semibold text-primary leading-tight block">{step.label}</span>
              {step.description && (
                <span className="text-[11px] text-muted leading-snug mt-0.5 block">{step.description}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Desktop: two-row layout — icons+arrows row, labels row */}
      <div className="hidden sm:block">
        {/* Row 1: icons interleaved with chevron arrows */}
        <div className="flex items-center justify-center">
          {steps.map((step, i) => (
            <div key={i} className="flex items-center">
              <div
                className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border-2"
                style={{ borderColor: step.color, background: `${step.color}15` }}
              >
                <div style={{ color: step.color }}>{step.icon}</div>
              </div>
              {i < steps.length - 1 && (
                <ChevronRight className="h-5 w-5 text-muted mx-2 shrink-0" />
              )}
            </div>
          ))}
        </div>

        {/* Row 2: labels aligned under each icon */}
        <div className="flex justify-center mt-2.5">
          {steps.map((step, i) => (
            <div key={i} className="flex items-start">
              <div className="w-11 text-center shrink-0">
                <span className="text-xs font-semibold text-primary leading-tight block">{step.label}</span>
                {step.description && (
                  <span className="text-[11px] text-muted leading-snug mt-0.5 block">{step.description}</span>
                )}
              </div>
              {/* Spacer matching the chevron+margin width */}
              {i < steps.length - 1 && <div className="w-9 shrink-0" />}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
