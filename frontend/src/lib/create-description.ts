import type { AgentChatMessage } from "@/types"

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

export function restoreDescription(
  saved: Partial<DescriptionState>, messages: AgentChatMessage[],
): DescriptionState {
  const description = saved.description ?? ""
  if (typeof saved.descriptionEdited === "boolean") {
    return { description, descriptionEdited: saved.descriptionEdited }
  }
  // Older sessions stored edits as prose only. Explicit edit messages take
  // precedence even when the user intentionally saved the suggested wording.
  const edited = messages.some(m => m.role === "user" && m.content === `${EDIT_PREFIX}${description}`)
  const generated = messages.some(m =>
    m.role === "tool" && !m.tool_result?.error &&
    (m.tool_name === "generate_plan" || m.tool_name === "present_plan") &&
    m.tool_result?.suggested_description === description,
  )
  // Preserve unknown nonempty values when older history is incomplete.
  return { description, descriptionEdited: !!description && (edited || !generated) }
}
