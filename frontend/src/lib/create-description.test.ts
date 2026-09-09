import { describe, expect, it } from "vitest"
import {
  EMPTY_DESCRIPTION, descriptionEdit, descriptionSelections, generatedDescription, restoreDescription,
} from "./create-description"

describe("Create Agent descriptions", () => {
  it("sends a trimmed edit as structured selections and carries it into chat approval", () => {
    const edit = descriptionEdit("  Sales by region\n ")!
    expect(edit.selections).toEqual({ description: "Sales by region" })
    expect(edit.text).toBe("The agent description should be: Sales by region")
    expect(descriptionSelections(edit.state)).toEqual(edit.selections)
    expect(descriptionSelections(edit.state, { action: "create", edited_plan: {} }))
      .toEqual({ description: "Sales by region", action: "create", edited_plan: {} })
  })

  it("uses the submitted edit even before React has committed its state", () => {
    const previous = descriptionEdit("Old edit")!
    const next = descriptionEdit("New edit")!
    expect(descriptionSelections(previous.state, next.selections)).toEqual({ description: "New edit" })
  })

  it("refreshes generated descriptions and never sends them as user overrides", () => {
    const first = generatedDescription(EMPTY_DESCRIPTION, "Original tables")
    const next = generatedDescription(first, "New tables")
    expect(next.description).toBe("New tables")
    expect(descriptionSelections(next, { action: "create" })).toEqual({ action: "create" })
    expect(generatedDescription(next)).toEqual(next)
  })

  it("returns only description state when a plan omits its suggestion", () => {
    const progress = {
      description: "Existing description",
      descriptionEdited: false,
      planReady: false,
      title: "Old title",
    }
    expect(generatedDescription(progress)).toEqual({
      description: "Existing description",
      descriptionEdited: false,
    })
  })

  it("preserves intentional edits across regeneration and JSON session restoration", () => {
    const edit = descriptionEdit("My wording")!
    const saved = JSON.parse(JSON.stringify(edit.state))
    const restored = restoreDescription(saved)
    expect(generatedDescription(restored, "New suggestion")).toEqual(edit.state)
    expect(descriptionSelections(restored)).toEqual(edit.selections)
  })

  it("keeps generated descriptions refreshable after restoration", () => {
    const saved = JSON.parse(JSON.stringify(generatedDescription(EMPTY_DESCRIPTION, "First")))
    expect(generatedDescription(restoreDescription(saved), "Second").description).toBe("Second")
  })

  it("defaults missing fields on restoration", () => {
    expect(restoreDescription({})).toEqual(EMPTY_DESCRIPTION)
    expect(restoreDescription({ description: "Saved" })).toEqual({ description: "Saved", descriptionEdited: false })
  })

  it("a new session accepts suggestions without inheriting the previous edit", () => {
    const edited = descriptionEdit("Previous session")!
    expect(edited.state.descriptionEdited).toBe(true)
    expect(descriptionSelections(EMPTY_DESCRIPTION)).toBeUndefined()
    expect(generatedDescription(EMPTY_DESCRIPTION, "New session"))
      .toEqual({ description: "New session", descriptionEdited: false })
  })

  it("ignores blank edits", () => {
    expect(descriptionEdit(" \n ")).toBeNull()
  })
})
