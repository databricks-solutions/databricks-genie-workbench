import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import { History } from './history'
import { versionFixture } from './fixtures'

it('history_shows_origin_actor_lineage_and_optimizer_drillthrough', () => {
  const html = renderToStaticMarkup(<History page={{ items: [{ ...versionFixture, origin: 'optimizer', optimizer_run_id: 'run/42', champion_id: 'champion-7', restored_from_version_id: 'original-1' }], next_cursor: 'next' }} onNext={vi.fn()} onSelect={vi.fn()} onCompare={vi.fn()} />)
  for (const text of ['Optimizer', versionFixture.observed_by, 'approved-2', 'original-1', 'champion-7', 'run%2F42', 'Compare versions', 'Load more', 'external-3']) expect(html).toContain(text)
  expect(html).not.toContain('Version 3')
})

it('history_checkbox_mode_renders_selection_and_hides_dropdown', () => {
  const items = [versionFixture, { ...versionFixture, version_id: 'external-2' }]
  const html = renderToStaticMarkup(
    <History page={{ items, next_cursor: null }} onNext={vi.fn()} onSelect={vi.fn()} compareIds={['external-2']} onToggleCompare={vi.fn()} />,
  )
  expect(html).toContain('type="checkbox"')
  expect(html).toContain('1 selected — pick one more to compare')
  // The legacy dropdown compare is retired in checkbox mode.
  expect(html).not.toContain('Compare versions')
})

it('history_renders_empty_and_loading_states', () => {
  const empty = renderToStaticMarkup(<History page={{ items: [], next_cursor: null }} onNext={vi.fn()} onSelect={vi.fn()} onCompare={vi.fn()} />)
  expect(empty).toContain('No versions captured yet')
  const loading = renderToStaticMarkup(<History page={{ items: [], next_cursor: null }} loading onNext={vi.fn()} onSelect={vi.fn()} onCompare={vi.fn()} />)
  expect(loading).toContain('animate-pulse')
  expect(loading).not.toContain('No versions captured yet')
})

describe('History tags', () => {
  it('renders the tag label in the rail when a tag exists for the version', () => {
    const html = renderToStaticMarkup(
      <History
        page={{ items: [versionFixture], next_cursor: null }}
        onNext={vi.fn()}
        onSelect={vi.fn()}
        tags={{ [versionFixture.version_id]: { label: 'Golden', note: null } }}
      />,
    )
    expect(html).toContain('Golden')
  })

  it('omits the tag badge when the version has no tag', () => {
    const html = renderToStaticMarkup(
      <History page={{ items: [versionFixture], next_cursor: null }} onNext={vi.fn()} onSelect={vi.fn()} tags={{}} />,
    )
    expect(html).not.toContain('Golden')
  })
})

describe('History benchmark-changed chip', () => {
  const parent = {
    ...versionFixture, version_id: 'parent-1', parent_version_id: null,
    fingerprints: { ...versionFixture.fingerprints, benchmark: 'BM1' },
  }
  const changedChild = {
    ...versionFixture, version_id: 'child-changed', parent_version_id: 'parent-1',
    fingerprints: { ...versionFixture.fingerprints, benchmark: 'BM2' },
  }
  const unchangedChild = {
    ...versionFixture, version_id: 'child-same', parent_version_id: 'parent-1',
    fingerprints: { ...versionFixture.fingerprints, benchmark: 'BM1' },
  }

  it('renders the chip when a row benchmark differs from its parent', () => {
    const html = renderToStaticMarkup(
      <History page={{ items: [changedChild, parent], next_cursor: null }} onNext={vi.fn()} onSelect={vi.fn()} />,
    )
    expect(html).toContain('Benchmarks changed')
  })

  it('omits the chip when the benchmark matches the parent', () => {
    const html = renderToStaticMarkup(
      <History page={{ items: [unchangedChild, parent], next_cursor: null }} onNext={vi.fn()} onSelect={vi.fn()} />,
    )
    expect(html).not.toContain('Benchmarks changed')
  })
})
