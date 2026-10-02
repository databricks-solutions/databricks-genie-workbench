// @vitest-environment jsdom
// m7e2-a on a client mount: React warns about duplicate keys only when it
// reconciles, never in renderToStaticMarkup, and only a mount can take a click.
import { act } from "react"
import { createRoot, type Root } from "react-dom/client"
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import { ModelTabOneCalculationTwoTablesFrame } from "./MvM7e2FidelityFrames"

let host: HTMLDivElement
let root: Root
let consoleError: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  consoleError = vi.spyOn(console, "error").mockImplementation(() => {})
  host = document.createElement("div")
  root = createRoot(host)
})
afterEach(async () => {
  await act(async () => root.unmount())
  consoleError.mockRestore()
})

const chips = () => [...host.querySelectorAll<SVGGElement>('g[data-chip="measure"]')]
const opacity = (chip: SVGGElement) => chip.querySelector("rect")?.getAttribute("fill-opacity")

it("mounts with no React duplicate-key warning", async () => {
  await act(async () => root.render(<ModelTabOneCalculationTwoTablesFrame />))
  expect(chips()).toHaveLength(2)
  const keyWarnings = consoleError.mock.calls.filter((args) => args.some((a) => String(a).includes("same key")))
  expect(keyWarnings).toEqual([])
})

it("clicking the second chip selects it and not the first", async () => {
  await act(async () => root.render(<ModelTabOneCalculationTwoTablesFrame />))
  const [first, second] = chips()
  expect(new Set(chips().map((c) => c.getAttribute("data-chip-id"))).size).toBe(2)
  await act(async () => second.dispatchEvent(new MouseEvent("click", { bubbles: true })))
  const [firstAfter, secondAfter] = chips()
  expect(opacity(secondAfter)).toBe("0.3")
  expect(opacity(firstAfter)).toBe("0.14")
  expect(first.getAttribute("data-chip-id")).toBe(firstAfter.getAttribute("data-chip-id"))
})
