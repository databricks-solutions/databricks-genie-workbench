// @vitest-environment jsdom
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { expect, it, vi } from 'vitest'

const pending = vi.hoisted(() => new Map<string, (level: 'view' | 'edit') => void>())
vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  getSpaceAccess: vi.fn((spaceId: string) => new Promise(resolve => {
    pending.set(spaceId, level => resolve({ space_id: spaceId, level }))
  })),
}))

import { useSpaceAccess } from './useSpaceAccess'

function Probe({ spaceId }: { spaceId: string }) {
  const { access } = useSpaceAccess(spaceId)
  return <span>{access}</span>
}

it('an answer for the previous space grants nothing after a switch', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  const host = document.createElement('div')
  const root = createRoot(host)
  try {
    await act(async () => root.render(<Probe spaceId="a" />))
    expect(host.textContent).toBe('checking')
    await act(async () => root.render(<Probe spaceId="b" />))
    await act(async () => pending.get('a')!('edit'))
    expect(host.textContent).toBe('checking')
    await act(async () => pending.get('b')!('view'))
    expect(host.textContent).toBe('view')
  } finally {
    await act(async () => root.unmount())
  }
})
