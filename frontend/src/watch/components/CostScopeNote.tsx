import { Info } from 'lucide-react'

import { Card } from '@/components/ui/card'

/**
 * Clarifies that GenieWatch cost figures cover SQL warehouse compute only, and
 * that Genie's separate LLM charge (new with the Jul 2026 Paygo pricing, on the
 * Serverless Realtime Inference SKU) is not reflected here. Rendered wherever a
 * cost figure is shown: the CostExplorer overview and the SpaceDetail Cost tab.
 */
export function CostScopeNote() {
  return (
    <Card className="border-blue-500/30 bg-blue-500/10 p-3 text-xs text-fg">
      <Info className="mr-1 inline align-text-bottom text-blue-500" size={14} />
      <span className="font-medium">
        All Genie costs shown on this page reflect estimated SQL warehouse compute consumption only.
      </span>{' '}
      Genie Agents also incur LLM charges — billed under the Genie / Serverless
      Realtime Inference SKU — <span className="font-medium">that are not reflected on this page.</span> As of this
      update (July 2026), Genie LLM pricing is subject to promotional and
      discount periods, which makes long-term cost tracking challenging. For that
      reason, please check the Databricks documentation or contact your account
      team for the latest on Genie LLM pricing.
    </Card>
  )
}
