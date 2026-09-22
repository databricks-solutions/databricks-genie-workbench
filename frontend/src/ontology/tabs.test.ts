import { describe, expect, it } from "vitest"
import { ONTOLOGY_TABS } from "./tabs"

describe("ONTOLOGY_TABS (MV-D107 IA)", () => {
  it("is ordered land → orient → act: Overview · Review · Map · Estate · Settings", () => {
    expect(ONTOLOGY_TABS.map((t) => t.id)).toEqual([
      "overview",
      "review",
      "map",
      "estate",
      "settings",
    ])
    expect(ONTOLOGY_TABS.map((t) => t.label)).toEqual([
      "Overview",
      "Review",
      "Map",
      "Estate",
      "Settings",
    ])
  })
})
