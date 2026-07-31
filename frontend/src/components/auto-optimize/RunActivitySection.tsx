import { useId, type ReactNode } from "react"
import type { LucideIcon } from "lucide-react"

interface RunActivitySectionProps {
  title: string
  description: string
  icon: LucideIcon
  children: ReactNode
}

/**
 * A lightweight phase divider for the run view. The modules inside each phase
 * already use cards, so this keeps the workflow hierarchy visible without
 * adding another layer of nested borders.
 */
export function RunActivitySection({
  title,
  description,
  icon: Icon,
  children,
}: RunActivitySectionProps) {
  const headingId = useId()

  return (
    <section aria-labelledby={headingId} className="space-y-4">
      <div className="border-t border-default pt-4">
        <div className="flex items-start gap-2">
          <Icon aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0 text-muted" />
          <div className="min-w-0">
            <h2
              id={headingId}
              className="text-xs font-semibold uppercase tracking-wide text-muted"
            >
              {title}
            </h2>
            <p className="mt-1 text-xs text-muted">{description}</p>
          </div>
        </div>
      </div>
      <div className="space-y-4">{children}</div>
    </section>
  )
}
