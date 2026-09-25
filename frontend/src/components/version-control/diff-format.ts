import type { DiffItem } from '@/types/version-control'

type Category = DiffItem['category']

// Friendly labels + a fixed display order for the 11 backend diff categories
// (backend/services/version_control/canonical_diff.py). Shared by the diff view and,
// conceptually, the config view so inspect and compare speak one vocabulary.
const LABELS: Record<Category, string> = {
  sources: 'Data sources',
  columns: 'Columns',
  instructions: 'Instructions',
  joins: 'Joins',
  filters: 'Filters',
  parameters: 'Parameters',
  sql: 'Sample SQL',
  questions: 'Sample questions',
  benchmarks: 'Benchmarks',
  metadata: 'Metadata',
  bindings: 'Bindings',
}

const ORDER: Category[] = [
  'sources', 'columns', 'instructions', 'joins', 'filters',
  'parameters', 'sql', 'questions', 'benchmarks', 'metadata', 'bindings',
]

export function categoryLabel(category: Category): string {
  return LABELS[category] ?? category
}

export function categoryOrder(category: Category): number {
  const index = ORDER.indexOf(category)
  return index === -1 ? ORDER.length : index
}

// A JSON pointer (e.g. "/data_sources/tables/orders/description") rendered as a readable
// breadcrumb. Stable-id segments name the entity, so they render as-is. RFC-6901 escapes
// are decoded; the raw pointer stays available (in a title attribute) for exact reference.
export function friendlyPath(path: string): string {
  const segments = path
    .split('/')
    .filter(Boolean)
    .map(segment => segment.replace(/~1/g, '/').replace(/~0/g, '~'))
  return segments.length ? segments.join(' › ') : '(root)'
}
