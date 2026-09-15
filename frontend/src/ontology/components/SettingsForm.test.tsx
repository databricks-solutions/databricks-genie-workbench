import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { OntologySettings, SourceStatus } from "@/ontology/types"
import { SettingsForm } from "./SettingsForm"

const noop = () => {}

function settings(overrides: Partial<OntologySettings> = {}): OntologySettings {
  return { company_name: null, catalog_allowlist: ["finance"], ...overrides }
}

function source(id: string, label: string): SourceStatus {
  return {
    id,
    label,
    klass: "external",
    provenance_tier: "T3",
    influence: "naming, synonyms",
    execute_status: "ok",
    grant_line: null,
    reason: "",
  }
}

// The default OFF contract (MV-D44): a settings row with no external_context reads as
// disabled — the toggle renders unchecked and, since it is off, no per-source checkboxes
// show even when the preflight would report sources.
describe("SettingsForm — external context toggle (Stage C, MV-D23/D44)", () => {
  it("shows the plain-language toggle, defaulted OFF, with no per-source checkboxes", () => {
    const html = renderToStaticMarkup(
      <SettingsForm
        settings={settings()}
        onSaved={noop}
        sources={[source("web_search", "Web search (AI Gateway)")]}
      />,
    )
    expect(html).toContain("Use industry context to improve naming")
    // Off ⇒ the sub-list of sources does not render.
    expect(html).not.toContain("Sources to draw from")
    expect(html).not.toContain("Web search (AI Gateway)")
    // Zero-burden copy (MV-D23): the Stage C surface never leaks pack / provenance /
    // MCP jargon. (Scoped to tokens the pre-existing form does not use.)
    const lower = html.toLowerCase()
    for (const token of ["context pack", "provenance", "managed-mcp", "mcp service"]) {
      expect(lower).not.toContain(token)
    }
  })

  it("surfaces the per-source checkboxes only once the toggle is on", () => {
    const html = renderToStaticMarkup(
      <SettingsForm
        settings={settings({ external_context: { enabled: true, sources: { youcom: false } } })}
        onSaved={noop}
        sources={[
          source("web_search", "Web search (AI Gateway)"),
          source("youcom", "You.com"),
        ]}
      />,
    )
    expect(html).toContain("Sources to draw from")
    expect(html).toContain("Web search (AI Gateway)")
    expect(html).toContain("You.com")
  })

  it("does not render checkboxes when on but the preflight reports no sources", () => {
    const html = renderToStaticMarkup(
      <SettingsForm
        settings={settings({ external_context: { enabled: true, sources: {} } })}
        onSaved={noop}
        sources={[]}
      />,
    )
    expect(html).toContain("Use industry context to improve naming")
    expect(html).not.toContain("Sources to draw from")
  })
})
