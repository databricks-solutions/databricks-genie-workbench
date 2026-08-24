import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import type { SpaceListItem } from '@/watch/types/api'
import { buildSpacesCsv, filterSpaces } from '@/watch/lib/spaces'
import { ManagerSummary, SpaceIdLink } from './SpacesList'
import { Overview } from './SpaceDetail'

const item: SpaceListItem = {
  space_id: 'space-1', title: 'Sales', owner_email: null, description: null,
  permissions: [
    { principal: 'alice@example.com', permission_level: 'CAN_MANAGE', principal_type: 'user', inherited: false },
    { principal: 'data-admins', permission_level: 'CAN_MANAGE', principal_type: 'group', inherited: true },
    { principal: '0013c5df-38cd-4316-bebe-e733247677a2', permission_level: 'CAN_MANAGE', principal_type: 'service_principal', inherited: false },
  ],
  last_seen_at: null, queries_7d: 2, cost_7d_usd: 1.5,
  feedback_pos_7d: 1, feedback_neg_7d: 0, last_query_at: null,
}

describe('space id navigation', () => {
  it('renders the complete space id as a link that opens Genie in a new tab', () => {
    const spaceId = '01f186aef02e1001a23456789abcdef0'
    const html = renderToStaticMarkup(
      <SpaceIdLink spaceId={spaceId} workspaceHost="https://example.cloud.databricks.com" />,
    )

    expect(html).toContain(`>${spaceId}</a>`)
    expect(html).toContain(`href="https://example.cloud.databricks.com/genie/rooms/${spaceId}"`)
    expect(html).toContain('target="_blank"')
    expect(html).not.toContain('…')
  })
})

describe('manager presentation', () => {
  it('renders an em dash for no managers', () => {
    expect(renderToStaticMarkup(<ManagerSummary permissions={[]} />)).toContain('—')
  })

  it('renders one manager inline', () => {
    const html = renderToStaticMarkup(<ManagerSummary permissions={[item.permissions[0]]} />)
    expect(html).toContain('alice@example.com')
    expect(html).toContain('user')
  })

  it('renders every manager inline without hover-only content', () => {
    const html = renderToStaticMarkup(<ManagerSummary permissions={item.permissions} />)
    expect(html).toContain('alice@example.com')
    expect(html).toContain('data-admins')
    expect(html).toContain('group · inherited')
    expect(html).toContain('0013c5df-38cd-4316-bebe-e733247677a2')
    expect(html).toContain('service principal')
    expect(html).toContain('aria-label="3 managers"')
    expect(html).not.toContain('+1')
    expect(html).not.toContain('title=')
    expect(html).not.toContain('role="tooltip"')
  })

  it('searches every manager and exports them in the managers column', () => {
    expect(filterSpaces([item], 'DATA-ADMINS')).toEqual([item])
    const csv = buildSpacesCsv([item])
    expect(csv).toContain('"managers"')
    expect(csv).toContain('"alice@example.com; data-admins; 0013c5df-38cd-4316-bebe-e733247677a2"')
    expect(csv).not.toContain('owner_email')
  })

  it('renders every manager with type and inheritance in detail', () => {
    const html = renderToStaticMarkup(<Overview space={item} />)
    expect(html).toContain('alice@example.com')
    expect(html).toContain('data-admins')
    expect(html).toContain('group · inherited')
  })
})
