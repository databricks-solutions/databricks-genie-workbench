import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type { ReactNode } from "react"

import { getAppVersion, setUpdateCheck } from "@/lib/api"
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
  // How many Admin pages are open; the GitHub check runs only while one is (spec 12).
  const [adminPages, setAdminPages] = useState(0)
  const adminPagesOpen = useRef(0)
  const [localConsent, setLocalConsent] = useState<Consent | null>(() => loadLocalConsent())
  const [result, setResult] = useState<CheckResult | null>(() => loadCheckResult())
  const [dismissed, setDismissed] = useState<string | null>(() => loadDismissedTag())
  const [checking, setChecking] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const inFlight = useRef<Promise<void> | null>(null)

  // Checks start only from callbacks (initial load, opening an Admin page, tab refocus,
  // consent, Check now), never synchronously from an effect body. Callers decide eligibility.
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
    getAppVersion()
      .catch(() => null)
      .then((version) => {
        if (cancelled) return
        setInfo(version)
        if (adminPagesOpen.current > 0 && version && effectiveMode(version.update_check, loadLocalConsent()) === "on") {
          void startCheck(version, false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [startCheck])

  const mode = info ? effectiveMode(info.update_check, localConsent) : "off"
  const canCheck = adminPages > 0 && mode === "on" && info !== null

  const enterAdminPage = useCallback(() => {
    adminPagesOpen.current += 1
    setAdminPages((n) => n + 1)
    if (info && effectiveMode(info.update_check, localConsent) === "on") void startCheck(info, false)
    return () => {
      adminPagesOpen.current -= 1
      setAdminPages((n) => n - 1)
    }
  }, [info, localConsent, startCheck])

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
        if (effectiveMode(state, nextLocal) === "on") void startCheck(nextInfo, false)
      } catch {
        setSaveError(SAVE_FAILED_MESSAGE)
      }
    },
    [info, localConsent, startCheck],
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

  // From this browser's cached result, so the Admin tab dot also shows on other pages.
  const updateAvailable = Boolean(mode === "on" && info && latestTag && isUpdateAvailable(latestTag, info.version) === true)
  const showUpdateBanner = updateAvailable && dismissed !== latestTag

  const value = useMemo<UpdateStatus>(
    () => ({
      info,
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
      enterAdminPage,
    }),
    [info, mode, result, checking, updateAvailable, showUpdateBanner, saveError, checkNow, setConsent, dismissUpdate, enterAdminPage],
  )

  return <UpdateStatusContext.Provider value={value}>{children}</UpdateStatusContext.Provider>
}
