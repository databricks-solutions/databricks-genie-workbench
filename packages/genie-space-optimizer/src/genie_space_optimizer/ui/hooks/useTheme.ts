/**
 * Theme management hook for dark mode support.
 * - Detects system preference via prefers-color-scheme
 * - Persists user override in localStorage
 * - Applies 'dark' class to <html> element
 */

import { useState, useEffect, useCallback, useMemo } from "react"

type Theme = "light" | "dark" | "system"

const STORAGE_KEY = "genierx-theme"

function getSystemTheme(): "light" | "dark" {
  if (typeof window === "undefined") return "light"
  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light"
}

function getStoredTheme(): Theme {
  if (typeof window === "undefined") return "system"
  const stored = localStorage.getItem(STORAGE_KEY)
  if (stored === "light" || stored === "dark" || stored === "system") {
    return stored
  }
  return "system"
}

function applyTheme(theme: Theme) {
  const root = document.documentElement
  const effectiveTheme = theme === "system" ? getSystemTheme() : theme

  if (effectiveTheme === "dark") {
    root.classList.add("dark")
  } else {
    root.classList.remove("dark")
  }
}

export function useTheme() {
  // Initialize state lazily to avoid hydration issues
  const [theme, setThemeState] = useState<Theme>(() => getStoredTheme())

  const resolvedTheme = useMemo(
    () => (theme === "system" ? getSystemTheme() : theme),
    [theme]
  )

  // Apply theme on mount and when theme changes
  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  // Listen for system theme changes
  useEffect(() => {
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)")

    const handleChange = () => {
      if (theme === "system") {
        applyTheme("system")
        // Force re-render to update resolvedTheme
        setThemeState((prev) => prev)
      }
    }

    mediaQuery.addEventListener("change", handleChange)
    return () => mediaQuery.removeEventListener("change", handleChange)
  }, [theme])

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme)
    localStorage.setItem(STORAGE_KEY, newTheme)
    applyTheme(newTheme)
  }, [])

  const toggleTheme = useCallback(() => {
    // Cycle through: system -> light -> dark -> system
    setThemeState((prev) => {
      const next: Theme =
        prev === "system" ? "light" : prev === "light" ? "dark" : "system"
      localStorage.setItem(STORAGE_KEY, next)
      applyTheme(next)
      return next
    })
  }, [])

  const toggleLightDark = useCallback(() => {
    // Simple light/dark toggle (ignores system, just flips current resolved theme)
    const currentResolved = theme === "system" ? getSystemTheme() : theme
    const next = currentResolved === "light" ? "dark" : "light"
    setThemeState(next)
    localStorage.setItem(STORAGE_KEY, next)
    applyTheme(next)
  }, [theme])

  return {
    theme, // Current setting: "light" | "dark" | "system"
    resolvedTheme, // Actual applied theme: "light" | "dark"
    setTheme, // Set to specific theme
    toggleTheme, // Cycle through all three options
    toggleLightDark, // Simple light/dark flip
    isDark: resolvedTheme === "dark",
    isLight: resolvedTheme === "light",
    isSystem: theme === "system",
  }
}

export type { Theme }
