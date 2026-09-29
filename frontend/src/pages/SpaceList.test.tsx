// @vitest-environment jsdom
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { renderToStaticMarkup } from 'react-dom/server'
import { expect, it, vi } from 'vitest'
import { ApiError } from '@/lib/api'
import type { SpaceListItem } from '@/types'

const space = { space_id: 's1', display_name: 'Sales', is_starred: false, score: null, maturity: null, optimization_accuracy: null, last_scanned: null, space_url: null } as unknown as SpaceListItem
const api = vi.hoisted(() => ({
  listSpaces: vi.fn(),
  scanSpace: vi.fn(async () => { throw new ApiError('You need Can Edit permission on this Genie Agent.', 403, { code: 'space_access_denied' }) }),
  toggleStar: vi.fn(),
}))
vi.mock('@/lib/api', async (importOriginal) => ({ ...(await importOriginal<typeof import('@/lib/api')>()), ...api }))

import { SpaceCard, SpaceList } from './SpaceList'

it('a refused scan tells the user it needs Can Edit, and a retry clears the note', async () => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
  // listSpaces returns SpaceListItem[] (SpaceList.tsx loadSpaces → setSpaces(data))
  api.listSpaces.mockResolvedValue([space])
  const host = document.createElement('div')
  const root = createRoot(host)
  try {
    await act(async () => root.render(<SpaceList onSelectSpace={() => {}} onCreateSpace={() => {}} />))
    const scan = () => [...host.querySelectorAll('button')].find(b => b.textContent?.includes('Scan'))!
    await act(async () => scan().click())
    expect(host.textContent).toContain('Scanning needs Can Edit on this agent')
    api.scanSpace.mockImplementationOnce(() => new Promise(() => {}))
    await act(async () => scan().click())
    expect(host.textContent).not.toContain('Scanning needs Can Edit on this agent')
  } finally {
    await act(async () => root.unmount())
  }
})

it('a card with no refusal shows no note', () => {
  const html = renderToStaticMarkup(<SpaceCard space={space} scanning={false} scanError={null} onSelect={() => {}} onToggleStar={() => {}} onScan={() => {}} />)
  expect(html).not.toContain('role="alert"')
})
