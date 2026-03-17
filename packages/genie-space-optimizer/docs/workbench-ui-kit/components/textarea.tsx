import * as React from "react"
import { cn } from "@/lib/utils"

const Textarea = React.forwardRef<
  HTMLTextAreaElement,
  React.ComponentProps<"textarea">
>(({ className, ...props }, ref) => {
  return (
    <textarea
      className={cn(
        "flex min-h-[120px] w-full rounded-lg border bg-surface px-4 py-3 text-base shadow-sm transition-all resize-none",
        "border-default text-primary placeholder:text-muted",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/20 focus-visible:border-accent",
        "dark:focus-visible:ring-accent/30",
        "disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      ref={ref}
      {...props}
    />
  )
})
Textarea.displayName = "Textarea"

export { Textarea }
