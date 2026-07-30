import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import type { SpaceListItem } from '@/watch/types/api'
import { buildSpacesCsv, filterSpaces } from '@/watch/lib/spaces'
import { ManagerSummary } from './SpacesList'
import { Overview } from './SpaceDetail'

const item: SpaceListItem = {
  space_id: 'space-1', title: 'Sales', owner_email: null, description: null,
  permissions: [
    { principal: 'alice@example.com', permission_level: 'CAN_MANAGE', principal_type: 'user', inherited: false },
    { principal: 'data-admins', permission_level: 'CAN_MANAGE', principal_type: 'group', inherited: true },
  ],
  last_seen_at: null, queries_7d: 2, cost_7d_usd: 1.5,
  feedback_pos_7d: 1, feedback_neg_7d: 0, last_query_at: null,
}

describe('manager presentation', () => {
  it('renders an em dash for no managers', () => {
    expect(renderToStaticMarkup(<ManagerSummary permissions={[]} />)).toContain('—')
  })

  it('renders one manager compactly', () => {
    const html = renderToStaticMarkup(<ManagerSummary permissions={[item.permissions[0]]} />)
    expect(html).toContain('alice@example.com')
    expect(html).not.toContain('+1')
  })

  it('renders the first manager and count for multiple managers', () => {
    const html = renderToStaticMarkup(<ManagerSummary permissions={item.permissions} />)
    expect(html).toContain('alice@example.com')
    expect(html).toContain('+1')
    expect(html).toContain('data-admins (group, inherited)')
  })

  it('searches every manager and exports them in the managers column', () => {
    expect(filterSpaces([item], 'DATA-ADMINS')).toEqual([item])
    const csv = buildSpacesCsv([item])
    expect(csv).toContain('"managers"')
    expect(csv).toContain('"alice@example.com; data-admins"')
    expect(csv).not.toContain('owner_email')
  })

  it('renders every manager with type and inheritance in detail', () => {
    const html = renderToStaticMarkup(<Overview space={item} />)
    expect(html).toContain('alice@example.com')
    expect(html).toContain('data-admins')
    expect(html).toContain('group · inherited')
  })
})
