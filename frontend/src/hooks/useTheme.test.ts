import { afterEach, describe, expect, it, vi } from "vitest"
import { readAppliedTheme } from "@/hooks/useTheme"

// `readAppliedTheme` is the shared source of truth that makes `resolvedTheme` reactive: every
// useTheme() instance derives its theme from the live `<html>.dark` class, so a flip from one
// consumer (ThemeToggle) is seen by all — the fix for the Ontology Map palette going stale.
// (The MutationObserver/storage wiring that pushes re-renders needs a DOM env; the node test
// suite has no jsdom, so we cover the pure read here and the rendered outcome in
// EstateGraph.test.tsx, which asserts the correct-theme token hexes off the applied class.)
function stubHtmlDark(hasDark: boolean) {
  vi.stubGlobal("document", {
    documentElement: { classList: { contains: (c: string) => c === "dark" && hasDark } },
  })
}

afterEach(() => vi.unstubAllGlobals())

describe("readAppliedTheme", () => {
  it("returns 'dark' when <html> carries the dark class", () => {
    stubHtmlDark(true)
    expect(readAppliedTheme()).toBe("dark")
  })

  it("returns 'light' when <html> has no dark class", () => {
    stubHtmlDark(false)
    expect(readAppliedTheme()).toBe("light")
  })

  it("falls back to 'light' with no DOM (SSR / node env)", () => {
    // No document stubbed → the SSR/no-DOM branch, matching the app's light default.
    expect(readAppliedTheme()).toBe("light")
  })
})
