import { afterEach, expect, it, vi } from 'vitest'
import { getSpaceDetail, getSpaceHistory, scanSpace, toggleStar } from './api'

afterEach(() => { vi.unstubAllGlobals() })

it('encodes the space id in every /spaces/ path', async () => {
  const urls: string[] = []
  vi.stubGlobal('fetch', vi.fn(async (url: string) => {
    urls.push(url)
    return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } })
  }))
  await getSpaceDetail('a/b')
  await scanSpace('a/b')
  await getSpaceHistory('a/b')
  await toggleStar('a/b', true)
  expect(urls).toHaveLength(4)
  for (const url of urls) expect(url).toContain('/spaces/a%2Fb')
})
