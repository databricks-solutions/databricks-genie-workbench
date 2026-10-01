// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { SpaceDetailLoadError } from './SpaceAccessChrome'
import { RETURN_TO_AGENTS, SPACE_NO_ACCESS_TITLE } from '@/lib/space-access'

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => {
  await act(async () => root.unmount())
})

const text = () => host.textContent ?? ''

async function mount(status: number | undefined, message: string) {
  const onBack = vi.fn()
  await act(async () => root.render(<SpaceDetailLoadError status={status} message={message} onBack={onBack} />))
  return onBack
}

function clickReturn() {
  const button = [...host.querySelectorAll('button')].find((b) => b.textContent === RETURN_TO_AGENTS)
  expect(button).toBeDefined()
  act(() => button!.click())
}

it.each([403, 404])('a %s is the no-access state, and Return goes back', async (status) => {
  const onBack = await mount(status, 'You need Can View permission on this Genie Agent.')
  expect(text()).toContain(SPACE_NO_ACCESS_TITLE)
  expect(text()).toContain('You need Can View permission on this Genie Agent.')
  clickReturn()
  expect(onBack).toHaveBeenCalledTimes(1)
})

it.each([403, 404])('a %s centres Return beneath the no-access card', async (status) => {
  await mount(status, 'You need Can View permission on this Genie Agent.')
  const wrapper = host.firstElementChild as HTMLElement
  expect(wrapper.classList.contains('text-center')).toBe(true)
  expect(wrapper.querySelector('button')?.textContent).toBe(RETURN_TO_AGENTS)
})

it.each([500, undefined])('a failed load (%s) shows its message, not the no-access state', async (status) => {
  const onBack = await mount(status, 'Failed to load this Agent')
  expect(text()).toContain('Failed to load this Agent')
  expect(text()).not.toContain(SPACE_NO_ACCESS_TITLE)
  clickReturn()
  expect(onBack).toHaveBeenCalledTimes(1)
})
