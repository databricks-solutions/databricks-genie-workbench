import type { ReactNode } from "react"
import { cn } from "@/lib/utils"

interface StageCardProps {
  title: string
  subtitle?: string
  icon?: ReactNode
  children: ReactNode
  className?: string
}

export function StageCard({ title, subtitle, icon, children, className }: StageCardProps) {
  return (
    <div
      className={cn(
        "rounded-xl border border-default bg-surface overflow-hidden",
        "dark:card-glow",
        className,
      )}
    >
      <div className="border-b border-default bg-elevated/50 px-6 py-4">
        <div className="flex items-center gap-3">
          {icon && (
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-accent/10 text-accent">
              {icon}
            </div>
          )}
          <div>
            <h3 className="font-display font-bold text-primary">{title}</h3>
            {subtitle && <p className="text-sm text-muted mt-0.5">{subtitle}</p>}
          </div>
        </div>
      </div>
      <div className="p-6">{children}</div>
    </div>
  )
}
