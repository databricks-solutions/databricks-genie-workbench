import { createContext, useContext } from "react"

import type { CheckResult, Mode } from "@/lib/update-check"
import type { AppVersionInfo } from "@/types"

export interface UpdateStatus {
  info: AppVersionInfo | null
  isAdmin: boolean
  mode: Mode
  locked: boolean
  result: CheckResult | null
  checking: boolean
  updateAvailable: boolean
  showUpdateBanner: boolean
  saveError: string | null
  checkNow: () => Promise<void>
  setConsent: (enabled: boolean) => Promise<void>
  dismissUpdate: () => void
}

export const SAVE_FAILED_MESSAGE = "Couldn't save, try again."

export const EMPTY_STATUS: UpdateStatus = {
  info: null,
  isAdmin: false,
  mode: "off",
  locked: false,
  result: null,
  checking: false,
  updateAvailable: false,
  showUpdateBanner: false,
  saveError: null,
  checkNow: async () => {},
  setConsent: async () => {},
  dismissUpdate: () => {},
}

export const UpdateStatusContext = createContext<UpdateStatus>(EMPTY_STATUS)

/** Works without a provider (returns EMPTY_STATUS), so pages render in isolation. */
export function useUpdateStatus(): UpdateStatus {
  return useContext(UpdateStatusContext)
}
