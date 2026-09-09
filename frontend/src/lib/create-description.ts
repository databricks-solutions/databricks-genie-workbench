export interface DescriptionState {
  description: string
  descriptionEdited: boolean
}

export const EMPTY_DESCRIPTION: DescriptionState = { description: "", descriptionEdited: false }

const EDIT_PREFIX = "The agent description should be: "

export function descriptionEdit(draft: string) {
  const description = draft.trim()
  if (!description) return null
  return {
    state: { description, descriptionEdited: true },
    text: `${EDIT_PREFIX}${description}`,
    selections: { description },
  }
}

export function generatedDescription(state: DescriptionState, suggestion?: string): DescriptionState {
  if (!state.descriptionEdited && suggestion !== undefined) {
    return { description: suggestion, descriptionEdited: false }
  }
  return { description: state.description, descriptionEdited: state.descriptionEdited }
}

export function descriptionSelections(state: DescriptionState, selections?: Record<string, unknown>) {
  return state.descriptionEdited
    ? { description: state.description, ...selections }
    : selections
}

// description and descriptionEdited are always persisted together, so a restored session
// carries both fields — this just normalizes a possibly-partial saved object.
export function restoreDescription(saved: Partial<DescriptionState>): DescriptionState {
  return {
    description: saved.description ?? "",
    descriptionEdited: saved.descriptionEdited ?? false,
  }
}
