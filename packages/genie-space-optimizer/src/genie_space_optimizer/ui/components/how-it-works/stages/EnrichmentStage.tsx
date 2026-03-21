"use client";

import type { ReactNode } from "react";
import { StageScreen } from "../StageScreen";
import { BeforeAfterSplit } from "../shared/BeforeAfterSplit";
import { ENRICHMENT_SUBSTAGES } from "../data";

const REVENUE_ANALYTICS_BEFORE = {
  title: "Before",
  items: [
    "total_revenue_usd — (no description)",
    "category — (no description)",
    "product_id — (no description)",
  ],
};

const REVENUE_ANALYTICS_AFTER = {
  title: "After",
  items: [
    "DEFINITION: Total gross revenue in USD\nSYNONYMS: revenue, total_sales, gross_revenue",
    "DEFINITION: Product classification\nVALUES: Electronics, Clothing, Home & Garden, Sports, Books",
    "DEFINITION: Foreign key linking to dim_product\nJOIN: dim_product.product_id",
  ],
};

const SUMMARY_ITEMS = [
  { label: "Columns enriched", value: "~15-30" },
  { label: "Join specs discovered", value: "3-8" },
  { label: "Instructions seeded", value: "yes/no" },
  { label: "Example SQLs added", value: "2-5" },
];

export function EnrichmentStage() {
  const learnMore: { id: string; title: string; content: ReactNode }[] = [
    ...ENRICHMENT_SUBSTAGES.map((substage) => ({
      id: substage.id,
      title: substage.title,
      content: (
        <ul className="list-inside list-disc space-y-1">
          {substage.details.map((d, i) => (
            <li key={i}>{d}</li>
          ))}
        </ul>
      ),
    })),
    {
      id: "enrichment-summary",
      title: "Enrichment Summary",
      content: (
        <div className="grid gap-3 sm:grid-cols-2">
          {SUMMARY_ITEMS.map(({ label, value }) => (
            <div
              key={label}
              className="flex justify-between rounded border border-db-gray-border px-3 py-2"
            >
              <span className="text-sm text-muted">{label}:</span>
              <span className="text-sm font-medium">{value}</span>
            </div>
          ))}
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Enrichment"
      subtitle="Proactively fill metadata gaps before optimization"
      pipelineGroup="enrichment"
      visual={
        <div className="space-y-4">
          <p className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-200">
            If baseline already meets all quality thresholds, enrichment is
            skipped entirely.
          </p>
          <BeforeAfterSplit
            before={REVENUE_ANALYTICS_BEFORE}
            after={REVENUE_ANALYTICS_AFTER}
          />
        </div>
      }
      explanation={
        <>
          Before the optimization loop begins, the system proactively fills
          metadata gaps — blank column descriptions, missing join
          specifications, and empty instructions. This &quot;low-hanging
          fruit&quot; pass ensures the lever loop can focus on harder problems.
        </>
      }
      learnMore={learnMore}
    />
  );
}
