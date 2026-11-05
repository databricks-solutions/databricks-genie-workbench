const DOCS_BASE = "https://databricks-solutions.github.io/databricks-genie-workbench/docs/getting-started/deployment-guide/"

export const DOCS_RELEASE_CHECK_URL = `${DOCS_BASE}#release-update-check`
export const DOCS_UPGRADE_URL = `${DOCS_BASE}#upgrading-an-existing-install`

export const COPY = {
  consentText:
    "New: Genie Workbench can check GitHub once a day for new releases. The check runs in your browser and sends no workspace data.",
  consentOn: "Turn on",
  consentOff: "No thanks",
  learnMore: "Learn more",
  updateAvailable: (latest: string, installed: string) =>
    `Genie Workbench ${latest} is available (you're on ${installed}).`,
  releaseNotes: "Release notes",
  howToUpgrade: "How to upgrade",
  dismiss: "Dismiss",
  upToDate: "Up to date",
  checkOff: "Release check is off",
  askPending: "Not decided yet. Use the banner at the top of the page, or turn it on here.",
  cantReach: "Couldn't reach GitHub",
  networksBlock: "Some networks block api.github.com.",
  settingError: "Couldn't load the release-check setting.",
  lockedHint:
    "Set at install. Change GENIE_UPDATE_CHECK in .env.deploy or the release_check notebook widget and redeploy.",
  checkNow: "Check now",
  checking: "Checking…",
  turnOn: "Turn on",
  turnOff: "Turn off",
  notRecorded: "Not recorded",
  navDotLabel: "Update available",
} as const

export function installMethodLabel(method: string | null): string {
  if (method === "cli") return "CLI install"
  if (method === "notebook") return "Notebook install"
  return COPY.notRecorded
}

export function formatAgo(ms: number, now: number): string {
  const minutes = Math.round((now - ms) / 60_000)
  if (minutes < 1) return "just now"
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.round(minutes / 60)
  if (hours < 48) return `${hours}h ago`
  return `${Math.round(hours / 24)}d ago`
}

export function formatDay(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ""
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" })
}
