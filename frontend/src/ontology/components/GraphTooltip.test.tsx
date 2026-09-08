import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import { graphTokens } from "@/ontology/graphTokens"
import { GraphEdgeTooltip, GraphTooltip, assembleEdgeTooltip, assembleTooltip } from "./GraphTooltip"

describe("assembleTooltip — hover-snippet content (MV-D85 / R21)", () => {
  it("prefers the real description over the generic fallback", () => {
    const t = assembleTooltip({
      typeLabel: "Metric View",
      name: "net sales",
      description: "Net sales after returns and discounts.",
      meta: null,
      fallbackDescription: "A curated set of business measures.",
    })
    expect(t.description).toBe("Net sales after returns and discounts.")
  })

  it("falls back to the generic copy when the description is null or blank (R13)", () => {
    expect(
      assembleTooltip({ typeLabel: "Table", name: "t", description: null, meta: null, fallbackDescription: "A data table." }).description,
    ).toBe("A data table.")
    expect(
      assembleTooltip({ typeLabel: "Table", name: "t", description: "   ", meta: null, fallbackDescription: "A data table." }).description,
    ).toBe("A data table.")
  })

  it("lifts a case-insensitive Expression key into its own mono line and out of meta", () => {
    const t = assembleTooltip({
      typeLabel: "Measure",
      name: "net_sales",
      description: null,
      meta: { expression: "SUM(x) - SUM(y)", Format: "USD" },
      fallbackDescription: "A number a metric view reports.",
    })
    expect(t.expression).toBe("SUM(x) - SUM(y)")
    expect(t.meta.map(([k]) => k)).not.toContain("expression")
    expect(t.meta).toContainEqual(["Format", "USD"])
  })

  it("caps meta at ~6 entries and drops empty values", () => {
    const meta: Record<string, string> = {}
    for (let i = 0; i < 10; i++) meta[`k${i}`] = `v${i}`
    meta.blank = ""
    const t = assembleTooltip({
      typeLabel: "Table",
      name: "t",
      description: "d",
      meta,
      fallbackDescription: "A data table.",
    })
    expect(t.meta.length).toBe(6)
    expect(t.meta.map(([k]) => k)).not.toContain("blank")
  })

  it("returns nothing to render when data is null", () => {
    const html = renderToStaticMarkup(<GraphTooltip data={null} x={0} y={0} tokens={graphTokens("dark")} />)
    expect(html).toBe("")
  })

  it("renders type, name, description, meta KV and the mono expression", () => {
    const data = assembleTooltip({
      typeLabel: "Measure",
      name: "net_sales",
      description: "Gross sales less returns.",
      meta: { Expression: "SUM(gross) - SUM(ret)", Format: "USD" },
      fallbackDescription: "A number a metric view reports.",
    })
    const html = renderToStaticMarkup(<GraphTooltip data={data} x={10} y={10} tokens={graphTokens("light")} />)
    expect(html).toContain("Measure")
    expect(html).toContain("net_sales")
    expect(html).toContain("Gross sales less returns.")
    expect(html).toContain("USD")
    expect(html).toContain("SUM(gross) - SUM(ret)")
    expect(html).toContain("monospace")
    // pointer-events:none so the snippet never eats the hover it describes (R21).
    expect(html).toContain("pointer-events:none")
  })
})

describe("assembleEdgeTooltip — relationship hover content (MV-D87 / R25)", () => {
  it("carries endpoints, verb, and the within-domain class label", () => {
    const t = assembleEdgeTooltip({ fromName: "fact_sales", toName: "ref_calendar", verb: "joins calendar", xdom: false })
    expect(t.fromName).toBe("fact_sales")
    expect(t.toName).toBe("ref_calendar")
    expect(t.verb).toBe("joins calendar")
    expect(t.classLabel).toBe("Within business area")
    expect(t.xdom).toBe(false)
  })

  it("labels a cross-domain edge and folds in the pre-seed evidence bag", () => {
    const t = assembleEdgeTooltip({
      fromName: "a",
      toName: "b",
      verb: "also queried with",
      xdom: true,
      detail: { "Co-queried": "42 sessions", Shares: "customer_id" },
    })
    expect(t.classLabel).toBe("Across business areas")
    expect(t.detail).toContainEqual(["Co-queried", "42 sessions"])
    expect(t.detail).toContainEqual(["Shares", "customer_id"])
  })

  it("DEGRADES to verb + endpoints + class when detail is null/empty (never requires Lane E, R13)", () => {
    expect(assembleEdgeTooltip({ fromName: "a", toName: "b", verb: "relates to", xdom: false, detail: null }).detail).toEqual([])
    expect(assembleEdgeTooltip({ fromName: "a", toName: "b", verb: "relates to", xdom: false }).detail).toEqual([])
    // Blank values are dropped.
    expect(
      assembleEdgeTooltip({ fromName: "a", toName: "b", verb: "x", xdom: false, detail: { Shares: "   " } }).detail,
    ).toEqual([])
  })

  it("caps the evidence bag at ~6 entries", () => {
    const detail: Record<string, string> = {}
    for (let i = 0; i < 10; i++) detail[`k${i}`] = `v${i}`
    expect(assembleEdgeTooltip({ fromName: "a", toName: "b", verb: "x", xdom: false, detail }).detail.length).toBe(6)
  })

  it("renders From → To, verb, class, and evidence; nothing when null", () => {
    expect(renderToStaticMarkup(<GraphEdgeTooltip data={null} x={0} y={0} tokens={graphTokens("dark")} />)).toBe("")
    const data = assembleEdgeTooltip({
      fromName: "fact_sales",
      toName: "ref_calendar",
      verb: "joins calendar",
      xdom: false,
      detail: { Shares: "date_key" },
    })
    const html = renderToStaticMarkup(<GraphEdgeTooltip data={data} x={10} y={10} tokens={graphTokens("light")} />)
    expect(html).toContain("Relationship")
    expect(html).toContain("fact_sales")
    expect(html).toContain("ref_calendar")
    expect(html).toContain("joins calendar")
    expect(html).toContain("Within business area")
    expect(html).toContain("date_key")
    expect(html).toContain("pointer-events:none")
  })
})
