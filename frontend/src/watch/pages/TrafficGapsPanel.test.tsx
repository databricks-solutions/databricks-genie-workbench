import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import type { TrafficGapAnalysis } from '@/watch/types/api'
import { TrafficGapsPanel } from './SpaceDetail'

const analysis: TrafficGapAnalysis = {
  scanned_message_count: 18,
  family_count: 10,
  covered_family_count: 4,
  candidates: [
    {
      candidate_id: 'candidate-1',
      occurrence_count: 3,
      distinct_user_count: 2,
      failed_count: 1,
      negative_feedback_count: 1,
      signals: ['negative_feedback', 'failed', 'cross_user_repeat'],
      conversation_urls: ['https://workspace.example/genie/rooms/s/chats/c'],
      first_seen_at: '2026-08-01T00:00:00Z',
      last_seen_at: '2026-08-02T00:00:00Z',
    },
  ],
}

describe('TrafficGapsPanel', () => {
  it('labels findings as review candidates and links to supporting conversations', () => {
    const html = renderToStaticMarkup(<TrafficGapsPanel data={analysis} />)

    expect(html).toContain('Candidate benchmark gaps')
    expect(html).toContain('review candidates')
    expect(html).toContain('candidate-1')
    expect(html).toContain('Negative feedback')
    expect(html).toContain('Failed answer')
    expect(html).toContain('Repeated across users')
    expect(html).toContain('href="https://workspace.example/genie/rooms/s/chats/c"')
    expect(html).toContain('target="_blank"')
    expect(html).not.toContain('automatically')
  })

  it('shows a clear empty state without claiming complete semantic coverage', () => {
    const html = renderToStaticMarkup(
      <TrafficGapsPanel data={{ ...analysis, candidates: [] }} />,
    )

    expect(html).toContain('No actionable candidate gaps')
    expect(html).toContain('exact normalized matching')
    expect(html).not.toContain('fully covered')
  })
})
