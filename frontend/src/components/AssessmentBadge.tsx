/**
 * Hero badge component displaying the qualitative assessment category.
 * Replaces the numeric ScoreGauge with an improvement-focused presentation.
 */

import { CheckCircle2, Sparkles, Wrench } from "lucide-react"
import { cn } from "@/lib/utils"
import type { AssessmentCategory } from "@/types"

interface AssessmentBadgeProps {
  assessment: AssessmentCategory
  rationale: string
  className?: string
}

const assessmentConfig: Record<
  AssessmentCategory,
  {
    label: string
    description: string
    icon: typeof CheckCircle2
    gradient: string
    glow: string
    iconColor: string
    bgColor: string
  }
> = {
  good_to_go: {
    label: "Good to Go",
    description: "Well-configured for its style",
    icon: CheckCircle2,
    gradient: "from-success to-success-light",
    glow: "rgba(16, 185, 129, 0.3)",
    iconColor: "text-success",
    bgColor: "bg-success/10 dark:bg-success/15",
  },
  quick_wins: {
    label: "Quick Wins Available",
    description: "Opportunities to improve",
    icon: Sparkles,
    gradient: "from-warning to-warning-light",
    glow: "rgba(245, 158, 11, 0.3)",
    iconColor: "text-warning",
    bgColor: "bg-warning/10 dark:bg-warning/15",
  },
  foundation_needed: {
    label: "Foundation Needed",
    description: "Key improvements to make",
    icon: Wrench,
    gradient: "from-info to-info-light",
    glow: "rgba(59, 130, 246, 0.3)",
    iconColor: "text-info",
    bgColor: "bg-info/10 dark:bg-info/15",
  },
}

export function AssessmentBadge({
  assessment,
  rationale,
  className,
}: AssessmentBadgeProps) {
  const config = assessmentConfig[assessment]
  const Icon = config.icon

  return (
    <div
      className={cn(
        "relative flex flex-col items-center text-center animate-scale-in",
        className
      )}
    >
      {/* Badge container */}
      <div
        className={cn(
          "relative flex items-center justify-center w-28 h-28 rounded-full",
          config.bgColor
        )}
        style={{
          boxShadow: `0 0 40px ${config.glow}`,
        }}
      >
        {/* Icon */}
        <Icon className={cn("w-14 h-14", config.iconColor)} strokeWidth={1.5} />

        {/* Decorative ring */}
        <div
          className="absolute inset-0 rounded-full opacity-50"
          style={{
            background: `conic-gradient(from 0deg, transparent 0%, ${config.glow} 25%, transparent 50%)`,
          }}
        />
      </div>

      {/* Label */}
      <h3 className="mt-4 text-xl font-display font-bold text-primary">
        {config.label}
      </h3>

      {/* Description */}
      <p className="mt-1 text-sm text-muted">{config.description}</p>

      {/* Rationale */}
      {rationale && (
        <p className="mt-3 text-sm text-secondary max-w-md leading-relaxed">
          {rationale}
        </p>
      )}
    </div>
  )
}
