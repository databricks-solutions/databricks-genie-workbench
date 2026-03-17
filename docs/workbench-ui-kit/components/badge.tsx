import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"
import { cn } from "@/lib/utils"

const badgeVariants = cva(
  "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold transition-colors",
  {
    variants: {
      variant: {
        default: "bg-accent/10 text-accent dark:bg-accent/20",
        success: "bg-success/10 text-success dark:bg-success/20",
        warning: "bg-warning/10 text-warning dark:bg-warning/20",
        danger: "bg-danger/10 text-danger dark:bg-danger/20",
        info: "bg-info/10 text-info dark:bg-info/20",
        secondary: "bg-elevated text-muted border border-default",
        high: "bg-danger/15 text-danger dark:bg-danger/25",
        medium: "bg-warning/15 text-warning dark:bg-warning/25",
        low: "bg-info/15 text-info dark:bg-info/25",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
)

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  )
}

export { Badge, badgeVariants }
