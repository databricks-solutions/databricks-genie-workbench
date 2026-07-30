import { Info } from 'lucide-react'

import { Card } from '@/components/ui/card'

/**
 * Clarifies the scope of GenieWatch cost figures: they cover Genie Agents only
 * (not Genie One / Genie Code), reflect SQL warehouse compute only, and exclude
 * Genie's separate LLM charge (new with the Jul 2026 Paygo pricing, on the
 * Serverless Realtime Inference SKU). Rendered wherever a cost figure is shown:
 * the CostExplorer overview and the SpaceDetail Cost tab.
 */
export function CostScopeNote() {
  return (
    <Card className="border-blue-500/30 bg-blue-500/10 p-3 text-xs text-fg">
      <Info className="mr-1 inline align-text-bottom text-blue-500" size={14} />
      <span className="font-medium">
        All Genie costs shown on this page reflect estimated SQL warehouse compute consumption
        associated with Genie Agents only.
      </span>{' '}
      Usage or consumption from other Genie surfaces (Genie One, Genie Code) is not included. Per the{' '}
      <a
        href="https://docs.databricks.com/aws/en/release-notes/product/2026/july#genie-products-now-use-pay-as-you-go-pricing"
        target="_blank"
        rel="noreferrer"
        className="text-accent hover:underline"
      >
        July 8, 2026 announcement
      </a>
      , Genie Agents also incur <span className="font-medium">LLM charges</span> — billed under the
      Genie / Serverless Realtime Inference SKU — but because Genie LLM pricing is currently subject
      to promotional and discount periods, an accurate estimate of those charges is difficult to
      produce reliably. To avoid presenting misleading numbers,{' '}
      <span className="font-medium">this page intentionally excludes all Genie LLM charges</span> and
      shows SQL warehouse compute only. For the current state of Genie LLM pricing, check the
      Databricks documentation or contact your account team.
    </Card>
  )
}
