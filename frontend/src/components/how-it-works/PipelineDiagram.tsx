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
    <div className={cn("flex flex-col gap-3 sm:flex-row sm:items-start sm:gap-0", className)}>
      {steps.map((step, i) => (
        <div key={i} className="flex items-start sm:flex-1 sm:flex-col sm:items-center gap-3 sm:gap-0">
          <div className="flex items-center gap-2 sm:gap-0 sm:flex-col sm:w-full">
            {/* Step node */}
            <div className="flex flex-col items-center">
              <div
                className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border-2 transition-colors"
                style={{
                  borderColor: step.color,
                  background: `${step.color}15`,
                }}
              >
                <div style={{ color: step.color }}>{step.icon}</div>
              </div>
            </div>

            {/* Connector arrow (between nodes, not after last) */}
            {i < steps.length - 1 && (
              <ChevronRight
                className="hidden sm:block h-5 w-5 text-muted self-center shrink-0 mx-auto"
                style={{ marginTop: 0 }}
              />
            )}
          </div>

          {/* Label + description */}
          <div className="sm:mt-2.5 sm:text-center sm:px-1">
            <span className="text-xs font-semibold text-primary leading-tight block">{step.label}</span>
            {step.description && (
              <span className="text-[11px] text-muted leading-snug mt-0.5 block">{step.description}</span>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}
