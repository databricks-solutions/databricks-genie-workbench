import { renderToStaticMarkup } from 'react-dom/server'
import { expect, it } from 'vitest'
import { ConfigView } from './config-view'

const snapshot = {
  data_sources: {
    tables: [
      { identifier: 'main.sales.orders', description: 'Order facts', column_configs: [
        { column_name: 'region', description: 'Sales region', synonyms: ['area'], enable_entity_matching: true, enable_format_assistance: true },
        { column_name: 'etl_ts', exclude: true },
      ] },
    ],
    metric_views: [
      { identifier: 'main.sales.revenue_metrics', description: 'Revenue by region', column_configs: [{ column_name: 'period', description: 'Time period' }] },
    ],
  },
  instructions: {
    text_instructions: [{ content: ['Revenue = SUM(amount).\n'] }],
    join_specs: [{ id: 'j1', left: { identifier: 'main.sales.orders' }, right: { identifier: 'main.sales.customers' }, sql: ['`orders`.`cid` = `customers`.`id`', '--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--'] }],
    sql_snippets: {
      filters: [{ id: 'f1', display_name: 'Active only', sql: 'status = 1', synonyms: ['live'] }],
      measures: [{ id: 'm1', display_name: 'Total Revenue', sql: 'SUM(amount)', comment: 'non-cancelled only' }],
      expressions: [{ id: 'x1', display_name: 'Order Year', sql: 'YEAR(order_date)' }],
    },
    sql_functions: [{ id: 'fn1', identifier: 'main.sales.fiscal_quarter', description: 'Fiscal quarter from a date' }],
    example_question_sqls: [{
      id: 'q1', question: 'Top regions?', sql: 'select region from orders',
      parameters: [{ name: 'region_name', type_hint: 'STRING', description: 'Region to filter', default_value: { values: ['North America'] } }],
      usage_guidance: 'Use for region filters',
    }],
  },
  benchmarks: { questions: [{ id: 'b1', question: 'Average order value?', answer: [{ format: 'SQL', content: ['SELECT AVG(order_amount) FROM orders'] }] }] },
  config: { sample_questions: [{ id: 's1', question: 'What were total sales last month?' }] },
  title: 'Sales agent',
  weird_unmapped_field: { keep: 'me' },
}

it('config_view_renders_friendly_sections_and_preserves_unknowns', () => {
  const html = renderToStaticMarkup(<ConfigView snapshot={snapshot} />)
  for (const text of [
    'Tables', 'main.sales.orders', 'region',
    'Entity match', 'Format assist', 'Excluded', 'etl_ts', 'Synonyms: area',
    'Metric views', 'main.sales.revenue_metrics', 'period',
    'Instructions', 'Revenue = SUM(amount).',
    'Joins', 'orders', 'customers', 'Many-to-one', 'main.sales', 'orders.cid = customers.id',
    'Measures', 'Total Revenue', 'non-cancelled only',
    'Expressions', 'Order Year',
    'Filters', 'Active only', 'Synonyms: live',
    'SQL functions', 'main.sales.fiscal_quarter', 'Fiscal quarter from a date',
    'Example SQL', 'Top regions?', 'Parameters', 'region_name', 'North America', 'Usage: Use for region filters',
    'Sample questions', 'What were total sales last month?',
    'Benchmarks', 'Average order value?',
    'Metadata', 'Sales agent',
    'Other', 'weird_unmapped_field',
    'View raw JSON',
  ]) {
    expect(html).toContain(text)
  }
  // The retired conflated bucket is gone.
  expect(html.split('View raw JSON')[0]).not.toContain('Sample SQL')
  // The relationship marker is lifted into a badge, never rendered raw in the join surface.
  expect(html.split('View raw JSON')[0]).not.toContain('--rt=')
  expect(html.split('View raw JSON')[0]).not.toContain('FROM_RELATIONSHIP_TYPE')
  // Jump-nav is present with anchored sections when there is more than one section.
  expect(html).toContain('aria-label="Config sections"')
  expect(html).toContain('id="vc-cfg-tables"')
  expect(html).toContain('id="vc-cfg-benchmarks"')
})

it('config_view_renders_array_descriptions_as_plain_text', () => {
  const arraySnapshot = {
    data_sources: { tables: [{ identifier: 'cat.sch.tbl', description: ['Demo source orders for VC promotion gate'] }] },
  }
  const html = renderToStaticMarkup(<ConfigView snapshot={arraySnapshot} />)
  // The structured section shows plain text (no JSON brackets); brackets only remain in
  // the raw-JSON disclosure at the bottom.
  expect(html).toContain('Demo source orders for VC promotion gate')
  expect(html.split('View raw JSON')[0]).not.toContain('["Demo source orders')
})

it('config_view_handles_empty_snapshot_gracefully', () => {
  const html = renderToStaticMarkup(<ConfigView snapshot={{}} />)
  expect(html).toContain('No structured configuration in this snapshot.')
  expect(html).toContain('View raw JSON')
})
