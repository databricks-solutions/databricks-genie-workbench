import * as React from "react"
import { cn } from "@/lib/utils"

const Input = React.forwardRef<HTMLInputElement, React.ComponentProps<"input">>(
  ({ className, type, ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          "flex h-11 w-full rounded-lg border bg-surface px-4 py-2 text-base shadow-sm transition-all",
          "border-default text-primary placeholder:text-muted",
          "file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-primary",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/20 focus-visible:border-accent",
          "dark:focus-visible:ring-accent/30 dark:focus-visible:glow-accent",
          "disabled:cursor-not-allowed disabled:opacity-50",
          className
        )}
        ref={ref}
        {...props}
      />
    )
  }
)
Input.displayName = "Input"

export { Input }
