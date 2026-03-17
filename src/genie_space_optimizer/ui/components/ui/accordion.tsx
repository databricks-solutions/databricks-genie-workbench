import * as React from "react"
import { ChevronDown } from "lucide-react"
import { cn } from "@/lib/utils"

interface AccordionItemProps {
  title: React.ReactNode
  children: React.ReactNode
  defaultOpen?: boolean
  /** Controlled open state */
  open?: boolean
  /** Callback when open state changes */
  onOpenChange?: (open: boolean) => void
  className?: string
  icon?: React.ReactNode
  action?: React.ReactNode
  disabled?: boolean
}

function AccordionItem({
  title,
  children,
  defaultOpen = false,
  open: controlledOpen,
  onOpenChange,
  className,
  icon,
  action,
  disabled,
}: AccordionItemProps) {
  const [internalOpen, setInternalOpen] = React.useState(defaultOpen)
  const isOpen = controlledOpen ?? internalOpen

  const toggle = () => {
    if (disabled) return
    const next = !isOpen
    if (controlledOpen === undefined) {
      setInternalOpen(next)
    }
    onOpenChange?.(next)
  }

  return (
    <div
      className={cn(
        "border rounded-lg overflow-hidden transition-shadow",
        "border-default",
        "dark:card-glow",
        disabled && "opacity-50",
        className
      )}
    >
      <button
        type="button"
        onClick={toggle}
        disabled={disabled}
        className={cn(
          "flex w-full items-center justify-between px-4 py-3 text-left transition-colors",
          "bg-surface hover:bg-elevated",
          disabled && "cursor-not-allowed hover:bg-surface"
        )}
      >
        <div className="flex items-center gap-2 font-medium text-primary">
          {icon}
          {title}
        </div>
        <div className="flex items-center gap-2">
          {action}
          <ChevronDown
            className={cn(
              "h-5 w-5 text-muted transition-transform duration-200",
              isOpen && "rotate-180"
            )}
          />
        </div>
      </button>
      <div
        className={cn(
          "grid transition-all duration-200 ease-in-out",
          isOpen ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0"
        )}
      >
        <div className="overflow-hidden">
          <div className="px-4 py-3 bg-elevated border-t border-default">
            {children}
          </div>
        </div>
      </div>
    </div>
  )
}

export { AccordionItem }
