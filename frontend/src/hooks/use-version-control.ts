import { useEffect, useMemo, useSyncExternalStore } from 'react'
import type { VersionControlApi } from '@/lib/version-control-api'
import { VersionControlError } from '@/lib/version-control-api'
import type { BindingStatus, VersionPage } from '@/types/version-control'
import { demoApi } from '@/components/version-control/demo-api'

export interface VersionControlState {
  bindingId: string; loading: boolean; captured: boolean; busy: boolean; stale: boolean
  captureFailure: { kind: 'conflict' | 'unresolved' | 'unavailable'; message: string; operationId?: string } | null
  status: BindingStatus | null; history: VersionPage; error: string
}
const initialState = (): VersionControlState => ({ bindingId: '', loading: false, captured: false, busy: false, stale: false, captureFailure: null, status: null, history: { items: [], next_cursor: null }, error: '' })
export function canMutate(state: VersionControlState, action: string) {
  return Boolean(state.captured && !state.loading && !state.busy && !state.stale && !state.captureFailure && state.status && !state.status.stale && !state.status.quarantined && !state.status.unresolved_operation_id && !['unknown', 'unreachable', 'applied_unverified', 'conflicted'].includes(state.status.drift) && state.status.allowed_actions.includes(action))
}
export function createVersionControlStore(api: VersionControlApi) {
  let state = initialState()
  let generation = 0
  let controller = new AbortController()
  const listeners = new Set<() => void>()
  const update = (changes: Partial<VersionControlState>) => { state = { ...state, ...changes }; listeners.forEach(listener => listener()) }
  const open = async (bindingId: string, reason: 'open' | 'history' | 'refresh' | 'return' = 'open') => {
    generation += 1
    const current = generation
    controller.abort()
    controller = new AbortController()
    const signal = controller.signal
    const updateCurrent = (changes: Partial<VersionControlState>) => { if (current === generation && !signal.aborted) update(changes) }
    if (state.bindingId !== bindingId) state = initialState()
    updateCurrent({ bindingId, loading: true, captured: false, captureFailure: null, error: '' })
    try {
      const observation = await api.observe(bindingId, reason, crypto.randomUUID())
      updateCurrent({ status: observation.status, captured: !observation.busy && !observation.status.stale, busy: observation.busy, stale: observation.status.stale })
    } catch (error) {
      const kind = error instanceof VersionControlError && error.status === 409 ? 'conflict' : error instanceof VersionControlError && error.status === 423 ? 'unresolved' : 'unavailable'
      const message = error instanceof Error ? error.message : 'Capture unavailable'
      updateCurrent({ captured: false, stale: true, error: message, captureFailure: { kind, message, operationId: error instanceof VersionControlError ? error.operation_id : undefined } })
    }
    if (current !== generation || signal.aborted) return
    try { updateCurrent({ history: await api.versions(bindingId, undefined, signal) }) }
    catch (error) { updateCurrent({ stale: true, captured: false, error: `${state.error} History unavailable: ${String(error)}` }) }
    finally { updateCurrent({ loading: false }) }
  }
  return {
    getSnapshot: () => state,
    subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener) } },
    async nextPage() {
      if (!state.history.next_cursor || state.loading) return
      const current = generation
      const signal = controller.signal
      update({ loading: true })
      try {
        const page = await api.versions(state.bindingId, state.history.next_cursor, signal)
        if (current === generation && !signal.aborted) update({ history: { items: [...state.history.items, ...page.items], next_cursor: page.next_cursor } })
      } catch (error) { if (current === generation && !signal.aborted) update({ error: String(error), stale: true }) }
      finally { if (current === generation && !signal.aborted) update({ loading: false }) }
    },
    open,
    invalidate: () => open(state.bindingId, 'refresh'),
    dispose: () => { generation += 1; controller.abort() },
  }
}
export function useVersionControl(bindingId: string, api: VersionControlApi = demoApi) {
  const store = useMemo(() => createVersionControlStore(api), [api])
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot)
  useEffect(() => { void store.open(bindingId); return () => store.dispose() }, [store, bindingId])
  useEffect(() => {
    const onReturn = () => { void store.open(bindingId, 'return') }
    window.addEventListener('focus', onReturn)
    return () => window.removeEventListener('focus', onReturn)
  }, [store, bindingId])
  return { ...state, nextPage: store.nextPage, refresh: (reason: 'history' | 'refresh' = 'refresh') => store.open(bindingId, reason) }
}
