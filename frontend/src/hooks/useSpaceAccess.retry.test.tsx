// @vitest-environment jsdom
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { ApiError } from '@/lib/api'
import type { SpaceAccessLevel } from '@/types'

const api = vi.hoisted(() => ({ getSpaceAccess: vi.fn() }))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

import { NOT_FOUND_RETRY_MS } from '@/lib/space-access'
import { useSpaceAccess } from './useSpaceAccess'

function Probe({ spaceId, retryNotFound }: { spaceId: string; retryNotFound?: boolean }) {
  const { access } = useSpaceAccess(spaceId, { retryNotFound })
  return <span>{access}</span>
}

const answers = (level: SpaceAccessLevel) => async (spaceId: string) => ({ space_id: spaceId, level })
const refuses = (status: number) => async () => {
  throw new ApiError(status === 404 ? 'Genie Agent not found, or you cannot see it.' : 'You need Can View permission on this Genie Agent.', status)
}

let host: HTMLDivElement
let root: Root
let mounted: boolean
beforeEach(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  vi.useFakeTimers()
  api.getSpaceAccess.mockReset()
  host = document.createElement('div')
  root = createRoot(host)
  mounted = true
})
afterEach(async () => {
  if (mounted) await act(async () => root.unmount())
  vi.useRealTimers()
})

const render = (spaceId: string, retryNotFound?: boolean) =>
  act(async () => root.render(<Probe spaceId={spaceId} retryNotFound={retryNotFound} />))
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms) })
const text = () => host.textContent ?? ''

it('with retryNotFound, a 404 waits and a second answer of edit is taken', async () => {
  api.getSpaceAccess.mockImplementationOnce(refuses(404)).mockImplementationOnce(answers('edit'))
  await render('s1', true)
  expect(text()).toBe('checking')
  await advance(NOT_FOUND_RETRY_MS - 1)
  expect(text()).toBe('checking')
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(1)
  await advance(1)
  expect(text()).toBe('edit')
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(2)
})

it('with retryNotFound, a second 404 is none and no third request is sent', async () => {
  api.getSpaceAccess.mockImplementationOnce(refuses(404)).mockImplementationOnce(refuses(404))
  await render('s1', true)
  await advance(NOT_FOUND_RETRY_MS)
  expect(text()).toBe('none')
  await advance(NOT_FOUND_RETRY_MS * 3)
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(2)
})

it('without retryNotFound, a 404 is none at once', async () => {
  api.getSpaceAccess.mockImplementationOnce(refuses(404)).mockImplementationOnce(answers('edit'))
  await render('s1')
  expect(text()).toBe('none')
  await advance(NOT_FOUND_RETRY_MS)
  expect(text()).toBe('none')
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(1)
})

it('with retryNotFound, a 403 is none at once', async () => {
  api.getSpaceAccess.mockImplementationOnce(refuses(403)).mockImplementationOnce(answers('edit'))
  await render('s1', true)
  expect(text()).toBe('none')
  await advance(NOT_FOUND_RETRY_MS)
  expect(text()).toBe('none')
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(1)
})

it('unmounting during the wait sends no second request', async () => {
  api.getSpaceAccess.mockImplementationOnce(refuses(404)).mockImplementationOnce(answers('edit'))
  await render('s1', true)
  await act(async () => root.unmount())
  mounted = false
  await advance(NOT_FOUND_RETRY_MS)
  expect(api.getSpaceAccess).toHaveBeenCalledTimes(1)
})

it('switching space during the wait sends no retry for the previous space', async () => {
  api.getSpaceAccess.mockImplementationOnce(refuses(404)).mockImplementationOnce(answers('view'))
  await render('a', true)
  await render('b', true)
  expect(text()).toBe('view')
  await advance(NOT_FOUND_RETRY_MS)
  expect(api.getSpaceAccess.mock.calls).toEqual([['a'], ['b']])
  expect(text()).toBe('view')
})
