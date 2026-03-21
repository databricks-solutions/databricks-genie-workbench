import * as React from "react"
import { cn } from "@/lib/utils"

interface CollapsibleProps extends React.HTMLAttributes<HTMLDivElement> {
  open?: boolean
  onOpenChange?: (open: boolean) => void
}

function Collapsible({ open, onOpenChange, children, className, ...props }: CollapsibleProps) {
  const [isOpen, setIsOpen] = React.useState(open ?? false)

  React.useEffect(() => {
    if (open !== undefined) setIsOpen(open)
  }, [open])

  const toggle = () => {
    const next = !isOpen
    setIsOpen(next)
    onOpenChange?.(next)
  }

  return (
    <div className={cn(className)} data-state={isOpen ? "open" : "closed"} {...props}>
      {React.Children.map(children, (child) => {
        if (React.isValidElement<CollapsibleTriggerProps>(child) && child.type === CollapsibleTrigger) {
          return React.cloneElement(child, { onClick: toggle })
        }
        if (React.isValidElement(child) && child.type === CollapsibleContent) {
          return isOpen ? child : null
        }
        return child
      })}
    </div>
  )
}

type CollapsibleTriggerProps = React.ButtonHTMLAttributes<HTMLButtonElement>

function CollapsibleTrigger({ children, className, ...props }: CollapsibleTriggerProps) {
  return (
    <button type="button" className={cn(className)} {...props}>
      {children}
    </button>
  )
}

function CollapsibleContent({ children, className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn(className)} {...props}>
      {children}
    </div>
  )
}

export { Collapsible, CollapsibleTrigger, CollapsibleContent }
