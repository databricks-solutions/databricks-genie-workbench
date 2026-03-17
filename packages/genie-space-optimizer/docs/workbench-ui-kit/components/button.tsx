import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"
import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "inline-flex items-center justify-center whitespace-nowrap rounded-lg text-sm font-semibold transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/50 focus-visible:ring-offset-2 focus-visible:ring-offset-surface disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        default:
          "gradient-accent text-white shadow-lg shadow-accent/25 hover:shadow-xl hover:shadow-accent/30 hover:-translate-y-0.5 dark:shadow-accent/20 dark:hover:shadow-accent/30 dark:hover:glow-accent",
        secondary:
          "bg-elevated text-primary border border-default hover:bg-sunken dark:hover:border-strong",
        outline:
          "border-2 border-default bg-transparent hover:bg-elevated text-primary dark:border-strong dark:hover:bg-elevated",
        ghost:
          "hover:bg-elevated text-secondary hover:text-primary",
        link:
          "text-accent underline-offset-4 hover:underline",
        success:
          "gradient-success text-white shadow-lg shadow-success/25 hover:shadow-xl hover:shadow-success/30 hover:-translate-y-0.5",
        danger:
          "gradient-danger text-white shadow-lg shadow-danger/25 hover:shadow-xl hover:shadow-danger/30 hover:-translate-y-0.5",
      },
      size: {
        default: "h-10 px-5 py-2",
        sm: "h-8 px-3 text-xs",
        lg: "h-12 px-8 text-base",
        icon: "h-10 w-10",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
)

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, ...props }, ref) => {
    return (
      <button
        className={cn(buttonVariants({ variant, size, className }))}
        ref={ref}
        {...props}
      />
    )
  }
)
Button.displayName = "Button"

export { Button, buttonVariants }
