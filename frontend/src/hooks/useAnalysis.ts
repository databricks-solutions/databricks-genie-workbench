/**
 * Custom hook for managing the Genie Space workspace state.
 * Handles space data loading.
 */

import { useState, useCallback } from "react"
import type { FetchSpaceResponse } from "@/types"
import { fetchSpace, parseSpaceJson } from "@/lib/api"

export interface AnalysisState {
  genieSpaceId: string
  spaceData: Record<string, unknown> | null
  isLoading: boolean
  error: string | null
}

const initialState: AnalysisState = {
  genieSpaceId: "",
  spaceData: null,
  isLoading: false,
  error: null,
}

export function useAnalysis() {
  const [state, setState] = useState<AnalysisState>(initialState)

  const setError = useCallback((error: string | null) => {
    setState((prev) => ({ ...prev, error, isLoading: false }))
  }, [])

  const setLoading = useCallback((isLoading: boolean) => {
    setState((prev) => ({ ...prev, isLoading }))
  }, [])

  const handleFetchSpace = useCallback(async (spaceId: string) => {
    setState((prev) => ({ ...prev, isLoading: true, error: null }))

    try {
      const response: FetchSpaceResponse = await fetchSpace(spaceId)
      setState((prev) => ({
        ...prev,
        genieSpaceId: response.genie_space_id,
        spaceData: response.space_data,
        isLoading: false,
      }))
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Failed to fetch space",
        isLoading: false,
      }))
    }
  }, [])

  const handleParseJson = useCallback(async (jsonContent: string) => {
    setState((prev) => ({ ...prev, isLoading: true, error: null }))

    try {
      const response: FetchSpaceResponse = await parseSpaceJson(jsonContent)
      setState((prev) => ({
        ...prev,
        genieSpaceId: response.genie_space_id,
        spaceData: response.space_data,
        isLoading: false,
      }))
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Failed to parse JSON",
        isLoading: false,
      }))
    }
  }, [])

  return {
    state,
    actions: {
      setError,
      setLoading,
      handleFetchSpace,
      handleParseJson,
    },
  }
}
