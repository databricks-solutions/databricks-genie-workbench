// GenieWatch wire types. Mirrors backend/watch/models.py — keep in sync.

export interface SpacePermission {
  principal: string | null
  permission_level: string | null
}

export interface SpaceSummary {
  space_id: string
  title: string | null
  owner_email: string | null
  description: string | null
  permissions: SpacePermission[]
  last_seen_at: string | null
}

export interface SpaceListItem extends SpaceSummary {
  queries_7d: number
  cost_7d_usd: number
  feedback_pos_7d: number
  feedback_neg_7d: number
  last_query_at: string | null
}

export interface CostPoint {
  day: string
  warehouse_id: string | null
  query_count: number
  approx_dbus: number | null
  approx_usd: number | null
}

export interface CostRollup {
  space_id: string
  days: number
  total_query_count: number
  total_approx_usd: number | null
  total_approx_dbus: number | null
  by_warehouse: CostPoint[]
  time_series: CostPoint[]
  apportionment: string
}

export interface CostTopSpender {
  space_id: string
  workspace_id: string | null
  workspace_name: string | null
  query_count: number
  approx_usd: number | null
}

export interface UsagePoint {
  day: string
  queries: number
  p50_ms: number | null
  p95_ms: number | null
  errors: number
  distinct_users: number
}

export interface FeedbackEvent {
  event_time: string
  user_email: string | null
  rating: string | null
  comment: string | null
  message_id: string | null
  conversation_id: string | null
}

export interface FeedbackSummary {
  positive: number
  negative: number
  total: number
  sample: FeedbackEvent[]
}

export interface Conversation {
  conversation_id: string
  user_email: string | null
  created_at: string | null
  message_count: number
  last_message_at: string | null
}

export interface UsageRollup {
  space_id: string
  days: number
  total_queries: number
  total_errors: number
  distinct_users: number
  time_series: UsagePoint[]
  feedback: FeedbackSummary
  conversations: Conversation[]
}

export interface TopQuery {
  statement_id: string
  executed_by: string | null
  start_time: string
  total_duration_ms: number | null
  execution_status: string | null
  statement_text: string | null
}

export interface CostPerConversation {
  conversation_id: string
  user_email: string | null
  first_query_at: string | null
  last_query_at: string | null
  query_count: number
  approx_usd: number | null
}

export interface ResourceUsage {
  full_name: string
  kind: 'table' | 'view' | 'metric_view'
  source: 'configured' | 'executed' | 'both'
  query_count: number
  last_used: string | null
  owner: string | null
  comment: string | null
}

export interface ResourceRollupItem {
  full_name: string
  space_count: number
  query_count_total: number
  last_used: string | null
}

export interface ResourceGraphEdge {
  space_id: string
  full_name: string
  query_count: number
  last_used: string | null
}

export interface ResourceGraphSpaceNode {
  space_id: string
  title: string | null
  workspace_id: string | null
  workspace_name: string | null
}

export interface ResourceGraph {
  edges: ResourceGraphEdge[]
  spaces: ResourceGraphSpaceNode[]
  days: number
  truncated: boolean
}

export interface EvalRun {
  run_id: string
  run_name: string | null
  status: string | null
  start_time: number | null
  end_time: number | null
  user_id: string | null
  metrics: Record<string, number>
  params: Record<string, string>
  tags: Record<string, string>
}

export interface EvalSummary {
  space_id: string
  experiment_id: string | null
  experiment_name: string | null
  runs: EvalRun[]
  permission_denied: boolean
}

export interface EvalExperimentMapping {
  space_id: string
  experiment_id: string
  created_by: string
  created_at: string | null
  updated_at: string | null
}

export interface HealthStatus {
  lakebase_available: boolean
  obo_active: boolean
  warehouse_id: string | null
  dashboard_cost_id: string | null
  workspace_host: string | null
}
