import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type { ReactNode } from "react"

import { getAppVersion, getCurrentUser, setUpdateCheck } from "@/lib/api"
import {
  effectiveMode,
  fetchLatestRelease,
  isCheckDue,
  isUpdateAvailable,
  loadCheckResult,
  loadDismissedTag,
  loadLocalConsent,
  nextResult,
  saveCheckResult,
  saveDismissedTag,
  saveLocalConsent,
} from "@/lib/update-check"
import type { CheckResult, Consent } from "@/lib/update-check"
import type { AppVersionInfo } from "@/types"

import { SAVE_FAILED_MESSAGE, UpdateStatusContext } from "./update-status-context"
import type { UpdateStatus } from "./update-status-context"

interface UpdateStatusProviderProps {
  children: ReactNode
  fetchImpl?: typeof fetch
  now?: () => number
}

export function UpdateStatusProvider({ children, fetchImpl, now = Date.now }: UpdateStatusProviderProps) {
  const [info, setInfo] = useState<AppVersionInfo | null>(null)
  const [isAdmin, setIsAdmin] = useState(false)
  const [localConsent, setLocalConsent] = useState<Consent | null>(() => loadLocalConsent())
  const [result, setResult] = useState<CheckResult | null>(() => loadCheckResult())
  const [dismissed, setDismissed] = useState<string | null>(() => loadDismissedTag())
  const [checking, setChecking] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const inFlight = useRef<Promise<void> | null>(null)

  // Checks start only from callbacks (initial load, tab refocus, consent, Check now),
  // never synchronously from an effect body. Callers decide eligibility.
  const startCheck = useCallback(
    async (target: AppVersionInfo, force: boolean): Promise<void> => {
      if (inFlight.current) return inFlight.current
      const prev = loadCheckResult()
      if (!force && !isCheckDue(prev, now())) return
      const task = (async () => {
        setChecking(true)
        let next: CheckResult
        try {
          const latest = await fetchLatestRelease(target.releases_repo, fetchImpl)
          next = nextResult(prev, now(), { latest })
        } catch (e) {
          next = nextResult(prev, now(), { error: e instanceof Error ? e.message : String(e) })
        }
        saveCheckResult(next)
        setResult(next)
        setChecking(false)
      })()
      inFlight.current = task.finally(() => {
        inFlight.current = null
      })
      return inFlight.current
    },
    [fetchImpl, now],
  )

  useEffect(() => {
    let cancelled = false
    Promise.all([getCurrentUser().catch(() => null), getAppVersion().catch(() => null)]).then(([user, version]) => {
      if (cancelled) return
      const admin = Boolean(user?.is_admin)
      setIsAdmin(admin)
      setInfo(version)
      if (admin && version && effectiveMode(version.update_check, loadLocalConsent()) === "on") {
        void startCheck(version, false)
      }
    })
    return () => {
      cancelled = true
    }
  }, [startCheck])

  const mode = info ? effectiveMode(info.update_check, localConsent) : "off"
  const canCheck = isAdmin && mode === "on" && info !== null

  useEffect(() => {
    if (!canCheck || !info) return
    const target = info
    const onVisible = () => {
      if (document.visibilityState === "visible") void startCheck(target, false)
    }
    document.addEventListener("visibilitychange", onVisible)
    return () => document.removeEventListener("visibilitychange", onVisible)
  }, [canCheck, info, startCheck])

  const setConsent = useCallback(
    async (enabled: boolean) => {
      if (!info || info.update_check.locked) return
      setSaveError(null)
      try {
        const state = await setUpdateCheck(enabled)
        const nextInfo = { ...info, update_check: state }
        setInfo(nextInfo)
        let nextLocal = localConsent
        if (!state.persisted) {
          const value: Consent = enabled ? "on" : "off"
          saveLocalConsent(value)
          setLocalConsent(value)
          nextLocal = value
        }
        if (isAdmin && effectiveMode(state, nextLocal) === "on") void startCheck(nextInfo, false)
      } catch {
        setSaveError(SAVE_FAILED_MESSAGE)
      }
    },
    [info, isAdmin, localConsent, startCheck],
  )

  const latestTag = result?.latest?.tag ?? null
  const dismissUpdate = useCallback(() => {
    if (!latestTag) return
    saveDismissedTag(latestTag)
    setDismissed(latestTag)
  }, [latestTag])

  const checkNow = useCallback(async () => {
    if (canCheck && info) await startCheck(info, true)
  }, [canCheck, info, startCheck])

  const updateAvailable = Boolean(canCheck && info && latestTag && isUpdateAvailable(latestTag, info.version) === true)
  const showUpdateBanner = updateAvailable && dismissed !== latestTag

  const value = useMemo<UpdateStatus>(
    () => ({
      info,
      isAdmin,
      mode,
      locked: info?.update_check.locked ?? false,
      result,
      checking,
      updateAvailable,
      showUpdateBanner,
      saveError,
      checkNow,
      setConsent,
      dismissUpdate,
    }),
    [info, isAdmin, mode, result, checking, updateAvailable, showUpdateBanner, saveError, checkNow, setConsent, dismissUpdate],
  )

  return <UpdateStatusContext.Provider value={value}>{children}</UpdateStatusContext.Provider>
}
