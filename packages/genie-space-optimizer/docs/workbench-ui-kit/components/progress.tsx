import * as React from "react"
import { cn } from "@/lib/utils"

interface ProgressProps extends React.HTMLAttributes<HTMLDivElement> {
  value?: number
  max?: number
  variant?: "default" | "success" | "warning" | "danger"
  /** Show glow effect in dark mode */
  glow?: boolean
}

const Progress = React.forwardRef<HTMLDivElement, ProgressProps>(
  ({ className, value = 0, max = 100, variant = "default", glow = false, ...props }, ref) => {
    const percentage = Math.min(Math.max((value / max) * 100, 0), 100)

    const variantStyles = {
      default: "bg-accent",
      success: "bg-success",
      warning: "bg-warning",
      danger: "bg-danger",
    }

    const glowStyles = {
      default: "dark:shadow-[0_0_8px_rgba(79,70,229,0.5)]",
      success: "dark:shadow-[0_0_8px_rgba(16,185,129,0.5)]",
      warning: "dark:shadow-[0_0_8px_rgba(245,158,11,0.5)]",
      danger: "dark:shadow-[0_0_8px_rgba(239,68,68,0.5)]",
    }

    return (
      <div
        ref={ref}
        className={cn(
          "relative h-2.5 w-full overflow-hidden rounded-full bg-elevated",
          className
        )}
        {...props}
      >
        <div
          className={cn(
            "h-full transition-all duration-500 ease-out rounded-full",
            variantStyles[variant],
            glow && glowStyles[variant]
          )}
          style={{ width: `${percentage}%` }}
        />
      </div>
    )
  }
)
Progress.displayName = "Progress"

export { Progress }
