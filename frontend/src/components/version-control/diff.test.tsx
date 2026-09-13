import { describe, it, expect } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { SemanticDiffView } from './diff'
import type { SemanticDiff } from '@/types/version-control'

const added: SemanticDiff = { comparison: 'different', items: [
  { category: 'questions', path: 'benchmarks.questions[0]', change: 'added', before: null, after: { id: 'q' }, review_required: false },
]}
const modified: SemanticDiff = { comparison: 'different', items: [
  { category: 'instructions', path: 'instructions', change: 'modified', before: 'a', after: 'b', review_required: false },
]}

describe('SemanticDiffView toggle', () => {
  it('hides the split toggle when there are no modified items', () => {
    expect(renderToStaticMarkup(<SemanticDiffView diff={added} />)).not.toContain('Switch to unified diff')
  })
  it('shows the split toggle when a modified item is present', () => {
    expect(renderToStaticMarkup(<SemanticDiffView diff={modified} />)).toContain('Switch to unified diff')
  })
})
