import type { ReactNode } from "react"
import { cn } from "@/lib/utils"

interface FeatureCardProps {
  icon: ReactNode
  title: string
  description: string
  accentColor: string
  glowColor: string
  className?: string
}

export function FeatureCard({ icon, title, description, accentColor, glowColor, className }: FeatureCardProps) {
  return (
    <div
      className={cn(
        "group relative rounded-xl border border-default bg-surface p-6",
        "transition-all duration-300 hover:scale-[1.02] hover:shadow-lg",
        "dark:card-glow",
        className,
      )}
    >
      <div
        className="absolute inset-0 rounded-xl opacity-0 transition-opacity duration-300 group-hover:opacity-100"
        style={{ background: `radial-gradient(ellipse at 50% 0%, ${glowColor}, transparent 70%)` }}
      />

      <div className="relative">
        <div
          className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg"
          style={{ background: `linear-gradient(135deg, ${accentColor}20, ${accentColor}40)` }}
        >
          <div style={{ color: accentColor }}>{icon}</div>
        </div>

        <h3 className="mb-2 text-lg font-display font-bold text-primary">{title}</h3>
        <p className="text-sm leading-relaxed text-secondary">{description}</p>
      </div>
    </div>
  )
}
