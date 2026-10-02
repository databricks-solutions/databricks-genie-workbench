// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { ApiError } from '@/lib/api'

const api = vi.hoisted(() => ({ getSpaceDetail: vi.fn() }))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

// Only the deep-link loader is under test; the views and the theme are stubbed.
vi.mock('@/pages/SpaceList', () => ({ SpaceList: () => <div>space-list</div> }))
vi.mock('@/pages/SpaceDetail', () => ({ SpaceDetail: () => <div>space-detail</div> }))
vi.mock('@/pages/AdminDashboard', () => ({ AdminDashboard: () => <div>admin</div> }))
vi.mock('@/pages/HowItWorks', () => ({ HowItWorks: () => <div>how-it-works</div> }))
vi.mock('@/components/CreateAgentChat', () => ({ CreateAgentChat: () => <div>create-agent</div> }))
vi.mock('@/components/ThemeToggle', () => ({ ThemeToggle: () => null }))
vi.mock('@/hooks/useTheme', () => ({ useTheme: () => undefined }))

import App from './App'
import { RETURN_TO_AGENTS, SPACE_NO_ACCESS_TITLE } from '@/lib/space-access'

let host: HTMLDivElement
let root: Root
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  vi.clearAllMocks()
  window.history.replaceState(null, '', '/?view=detail&space=s1')
  host = document.createElement('div')
  root = createRoot(host)
})
afterEach(async () => {
  await act(async () => root.unmount())
  window.history.replaceState(null, '', '/')
})

const text = () => host.textContent ?? ''

async function mountRejecting(error: Error) {
  api.getSpaceDetail.mockRejectedValue(error)
  await act(async () => root.render(<App />))
  expect(api.getSpaceDetail).toHaveBeenCalledWith('s1')
}

it('a deep link answered 403 is the no-access state with the way back', async () => {
  await mountRejecting(new ApiError('You need Can View permission on this Genie Agent.', 403))
  expect(text()).toContain(SPACE_NO_ACCESS_TITLE)
  expect(text()).toContain('You need Can View permission on this Genie Agent.')
  expect(text()).toContain(RETURN_TO_AGENTS)
  expect(text()).not.toContain('space-detail')
})

it('a deep link that fails with a 500 shows its message, not the no-access state', async () => {
  await mountRejecting(new ApiError('Failed to get agent detail', 500))
  expect(text()).toContain('Failed to get agent detail')
  expect(text()).toContain(RETURN_TO_AGENTS)
  expect(text()).not.toContain(SPACE_NO_ACCESS_TITLE)
})
