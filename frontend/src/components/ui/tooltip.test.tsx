// @vitest-environment jsdom
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { act } from "react"
import { createRoot, type Root } from "react-dom/client"
import { renderToStaticMarkup } from "react-dom/server"
import { afterEach, beforeEach, expect, it } from "vitest"
import { Tooltip } from "./tooltip"

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  host = document.createElement("div")
  document.body.appendChild(host)
  root = createRoot(host)
})
afterEach(async () => {
  await act(async () => root.unmount())
  host.remove()
})

const trigger = () => host.querySelector<HTMLElement>("[data-trigger]")!
const bubble = () => host.querySelector<HTMLElement>('[role="tooltip"]')
const fire = (event: Event) => act(async () => { trigger().dispatchEvent(event) })

async function mount(element: React.ReactElement) {
  await act(async () => root.render(element))
}

it("shows on hover and hides when the pointer leaves", async () => {
  await mount(<Tooltip content="Explains it"><span data-trigger tabIndex={0}>i</span></Tooltip>)
  expect(bubble()).toBeNull()
  await fire(new MouseEvent("mouseover", { bubbles: true, relatedTarget: document.body }))
  expect(bubble()?.textContent).toBe("Explains it")
  await fire(new MouseEvent("mouseout", { bubbles: true, relatedTarget: document.body }))
  expect(bubble()).toBeNull()
})

it("shows on keyboard focus and hides when focus leaves", async () => {
  await mount(<Tooltip content="Explains it"><span data-trigger tabIndex={0}>i</span></Tooltip>)
  await fire(new FocusEvent("focusin", { bubbles: true }))
  expect(bubble()?.textContent).toBe("Explains it")
  await fire(new FocusEvent("focusout", { bubbles: true }))
  expect(bubble()).toBeNull()
})

it("sizes the bubble to its text on a visible surface, not to the trigger", () => {
  const html = renderToStaticMarkup(<Tooltip content="Explains it" defaultOpen><span>i</span></Tooltip>)
  for (const cls of ["w-max", "max-w-xs", "whitespace-normal", "bg-surface", "border-default", "text-secondary"]) {
    expect(html).toContain(cls)
  }
})

it("lets a caller's width replace the default", () => {
  const html = renderToStaticMarkup(<Tooltip content="Explains it" className="w-80" defaultOpen><span>i</span></Tooltip>)
  expect(html).toContain("w-80")
  expect(html).not.toContain("w-max")
})

it("renders closed unless defaultOpen is set", () => {
  expect(renderToStaticMarkup(<Tooltip content="Explains it"><span>i</span></Tooltip>)).not.toContain('role="tooltip"')
  expect(renderToStaticMarkup(<Tooltip content="Explains it" defaultOpen><span>i</span></Tooltip>)).toContain('role="tooltip"')
})

it("uses no colour class the theme leaves undefined", () => {
  const source = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "tooltip.tsx"), "utf8")
  expect(source).not.toMatch(/\bbg-primary\b|primary-foreground/)
})
