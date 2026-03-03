/**
 * Theme toggle button component.
 * Displays sun/moon icon based on current theme with smooth transition.
 */

import { Sun, Moon, Monitor } from "lucide-react"
import { useTheme, type Theme } from "@/hooks/useTheme"
import { cn } from "@/lib/utils"

interface ThemeToggleProps {
  /** Show three-way toggle (light/dark/system) vs simple two-way (light/dark) */
  showSystemOption?: boolean
  className?: string
}

export function ThemeToggle({
  showSystemOption = false,
  className,
}: ThemeToggleProps) {
  const { theme, resolvedTheme, toggleLightDark, setTheme } = useTheme()

  if (showSystemOption) {
    // Three-way segmented control
    return (
      <div
        className={cn(
          "flex items-center gap-1 p-1 rounded-lg bg-elevated border border-default",
          className
        )}
      >
        {(["light", "system", "dark"] as Theme[]).map((option) => (
          <button
            key={option}
            type="button"
            onClick={() => setTheme(option)}
            className={cn(
              "p-2 rounded-md transition-all duration-200",
              theme === option
                ? "bg-surface text-accent shadow-sm"
                : "text-muted hover:text-secondary"
            )}
            aria-label={`Set theme to ${option}`}
          >
            {option === "light" && <Sun className="w-4 h-4" />}
            {option === "system" && <Monitor className="w-4 h-4" />}
            {option === "dark" && <Moon className="w-4 h-4" />}
          </button>
        ))}
      </div>
    )
  }

  // Simple two-way toggle button
  return (
    <button
      type="button"
      onClick={toggleLightDark}
      className={cn(
        "relative p-2 rounded-lg transition-all duration-200",
        "bg-elevated hover:bg-sunken border border-default",
        "text-muted hover:text-primary",
        "focus-ring",
        className
      )}
      aria-label={`Switch to ${resolvedTheme === "light" ? "dark" : "light"} mode`}
    >
      <div className="relative w-5 h-5">
        {/* Sun icon */}
        <Sun
          className={cn(
            "absolute inset-0 w-5 h-5 transition-all duration-300",
            resolvedTheme === "light"
              ? "opacity-100 rotate-0 scale-100"
              : "opacity-0 rotate-90 scale-75"
          )}
        />
        {/* Moon icon */}
        <Moon
          className={cn(
            "absolute inset-0 w-5 h-5 transition-all duration-300",
            resolvedTheme === "dark"
              ? "opacity-100 rotate-0 scale-100"
              : "opacity-0 -rotate-90 scale-75"
          )}
        />
      </div>
    </button>
  )
}
