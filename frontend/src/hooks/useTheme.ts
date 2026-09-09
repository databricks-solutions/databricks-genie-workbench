/**
 * Theme management hook for dark mode support.
 * - Detects system preference via prefers-color-scheme
 * - Persists user override in localStorage
 * - Applies 'dark' class to <html> element
 */

import { useState, useEffect, useCallback } from "react"

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

/**
 * The theme ACTUALLY applied to the document, read from `<html>.dark`. This is the shared
 * source of truth every `useTheme()` instance agrees on: the app restyles off that class, and
 * components that pick colours from `resolvedTheme` in React (e.g. the Ontology Map's
 * `graphTokens`) must track it live. SSR / no-DOM falls back to the stored/system setting so
 * `renderToStaticMarkup` and the node test env stay light-by-default (unchanged behaviour).
 */
export function readAppliedTheme(): "light" | "dark" {
  if (typeof document !== "undefined" && document.documentElement) {
    return document.documentElement.classList.contains("dark") ? "dark" : "light"
  }
  return getStoredTheme() === "dark" ? "dark" : "light"
}

export function useTheme() {
  // Initialize state lazily to avoid hydration issues
  const [theme, setThemeState] = useState<Theme>(() => getStoredTheme())

  // `resolvedTheme` mirrors the LIVE applied class rather than this instance's `theme` copy, so
  // a flip from any consumer (ThemeToggle) propagates to every `useTheme()` — the fix for the
  // Ontology Map palette going stale on toggle.
  const [resolvedTheme, setResolvedTheme] = useState<"light" | "dark">(() => readAppliedTheme())

  // Apply theme on mount and when theme changes
  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  // React to the applied theme changing anywhere: observe `<html>`'s class attribute (any
  // instance's toggle flips it) and cross-tab `storage` writes. Both recompute `resolvedTheme`
  // so all instances stay in agreement.
  useEffect(() => {
    if (typeof document === "undefined") return
    const sync = () => setResolvedTheme(readAppliedTheme())
    sync() // reconcile in case the class was applied before this instance mounted

    const observer = new MutationObserver(sync)
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] })

    const onStorage = (e: StorageEvent) => {
      if (e.key !== STORAGE_KEY) return
      const stored = getStoredTheme()
      setThemeState(stored) // adopt the cross-tab setting (re-applies via the effect above)
      applyTheme(stored) // apply now so the class + resolvedTheme converge immediately
      sync()
    }
    window.addEventListener("storage", onStorage)

    return () => {
      observer.disconnect()
      window.removeEventListener("storage", onStorage)
    }
  }, [])

  // Listen for system theme changes
  useEffect(() => {
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)")

    const handleChange = () => {
      if (theme === "system") {
        // Re-apply; the MutationObserver above picks up the class change and updates
        // resolvedTheme for every instance.
        applyTheme("system")
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
