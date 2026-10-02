import { VersionControlError } from '@/lib/version-control-api'
import type { VersionControlApi } from '@/lib/version-control-api'
import type { Operation, RestoreCommand } from '@/types/version-control'

// Non-component helpers live in a sibling module so the .tsx files export only
// components (react-refresh/only-export-components).

export async function pollOperation(api: VersionControlApi, operationId: string, onUpdate?: (operation: Operation) => void) {
  for (let attempt = 0; attempt < 30; attempt += 1) {
    const operation = await api.operation(operationId)
    onUpdate?.(operation)
    if (!['requested', 'preimage_captured', 'apply_attempted', 'compensation_attempted'].includes(operation.status) || attempt === 29) return operation
    await new Promise(resolve => setTimeout(resolve, 1000))
  }
  throw new Error('Operation polling exhausted')
}

export async function submitRestore(api: VersionControlApi, bindingId: string, command: RestoreCommand, confirmed: boolean, key: string, onUpdate?: (operation: Operation) => void) {
  if (!confirmed || !command.approval_id.trim()) throw new Error('Confirm the reviewed version and supply a fresh approval reference.')
  try {
    const handle = await api.restore(bindingId, command, key)
    return await pollOperation(api, handle.operation_id, onUpdate)
  } catch (error) {
    if (error instanceof VersionControlError && error.operation_id) return pollOperation(api, error.operation_id, onUpdate)
    throw error
  }
}
