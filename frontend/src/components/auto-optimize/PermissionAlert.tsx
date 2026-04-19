import { useState } from "react"
import { ShieldAlert, Check, Copy, CheckCheck, RefreshCw, AlertTriangle } from "lucide-react"
import type {
  GSOPermissionCheck,
  GSOPromptRegistryActionableBy,
  GSOPromptRegistryReasonCode,
} from "@/types"

interface PermissionAlertProps {
  permissions: GSOPermissionCheck
  loading: boolean
  onRefresh?: () => void
}

function CopyableText({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  function handleCopy() {
    navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <button
      onClick={handleCopy}
      className="inline-flex items-center gap-1 rounded bg-amber-100 px-1.5 py-0.5 text-xs font-mono text-amber-900 hover:bg-amber-200 transition-colors"
      title="Click to copy"
    >
      {text}
      {copied ? <CheckCheck className="w-3 h-3" /> : <Copy className="w-3 h-3 opacity-50" />}
    </button>
  )
}

function CopyableCodeBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false)

  function handleCopy() {
    navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="relative mt-2">
      <pre className="rounded-md bg-slate-800 text-slate-100 text-xs p-3 overflow-x-auto whitespace-pre-wrap">
        {code}
      </pre>
      <button
        onClick={handleCopy}
        className="absolute top-2 right-2 p-1 rounded bg-slate-700 hover:bg-slate-600 text-slate-300 transition-colors"
        title="Copy to clipboard"
      >
        {copied ? <CheckCheck className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
      </button>
    </div>
  )
}

function PermissionStep({
  step,
  title,
  description,
  granted,
  code,
}: {
  step: number
  title: string
  description: React.ReactNode
  granted: boolean
  code?: string
}) {
  return (
    <div className={`rounded-lg border p-3 ${granted ? "border-green-300 bg-green-50" : "border-amber-300 bg-amber-50"}`}>
      <div className="flex items-start gap-2">
        {granted ? (
          <span className="flex-shrink-0 mt-0.5 flex items-center justify-center w-5 h-5 rounded-full bg-green-500 text-white">
            <Check className="w-3 h-3" />
          </span>
        ) : (
          <span className="flex-shrink-0 mt-0.5 flex items-center justify-center w-5 h-5 rounded-full bg-amber-500 text-white text-xs font-bold">
            {step}
          </span>
        )}
        <div className="flex-1 min-w-0">
          <p className={`text-sm font-medium ${granted ? "text-green-800" : "text-amber-900"}`}>
            {title}
            {granted && <span className="ml-1.5 text-xs font-normal text-green-600">(granted)</span>}
          </p>
          {!granted && (
            <div className="mt-1 text-xs text-amber-800">{description}</div>
          )}
          {!granted && code && <CopyableCodeBlock code={code} />}
        </div>
      </div>
    </div>
  )
}

export function PermissionAlert({ permissions, loading, onRefresh }: PermissionAlertProps) {
  if (loading) return null
  if (permissions.can_start) return null

  const missingSchemas = permissions.schemas.filter((s) => !s.read_granted)
  const allGrantSql = missingSchemas
    .map((s) => s.grant_sql)
    .filter(Boolean)
    .join("\n\n")

  return (
    <div className="rounded-lg border border-amber-300 bg-amber-50 p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <ShieldAlert className="w-4 h-4 text-amber-600 flex-shrink-0" />
          <h4 className="text-sm font-semibold text-amber-900">Missing permissions</h4>
        </div>
        {onRefresh && (
          <button
            onClick={onRefresh}
            className="flex items-center gap-1 text-xs text-amber-700 hover:text-amber-900 transition-colors"
          >
            <RefreshCw className="w-3 h-3" />
            Re-check
          </button>
        )}
      </div>

      <div className="space-y-2">
        <PermissionStep
          step={1}
          title="Grant Genie Space access"
          description={
            <>
              Open the Genie Space sharing dialog and add{" "}
              <CopyableText text={permissions.sp_display_name || "<service-principal>"} />{" "}
              with <strong>CAN_MANAGE</strong> permission.
            </>
          }
          granted={permissions.sp_has_manage}
        />

        {permissions.schemas.length > 0 && (
          <PermissionStep
            step={2}
            title="Grant data access"
            description={
              <>
                The service principal needs <strong>SELECT</strong> and <strong>EXECUTE</strong> on
                the underlying schemas. Run the following in a SQL editor:
              </>
            }
            granted={missingSchemas.length === 0}
            code={allGrantSql || undefined}
          />
        )}

        <PromptRegistryStep
          permissions={permissions}
        />
      </div>
    </div>
  )
}

function PromptRegistryStep({ permissions }: { permissions: GSOPermissionCheck }) {
  const granted = permissions.prompt_registry_available !== false
  const code = permissions.prompt_registry_reason_code
  const actionable = permissions.prompt_registry_actionable_by
  const vendorCode = permissions.prompt_registry_error_code
  const rawError = permissions.prompt_registry_error

  // Platform-actionable failures render with a distinct visual treatment
  // (slate "notice" rather than amber "blocker") so customers know they
  // are NOT being asked to fix a permission — the Genie Workbench team is.
  if (!granted && actionable === "platform") {
    return (
      <div className="rounded-lg border border-slate-300 bg-slate-50 p-3">
        <div className="flex items-start gap-2">
          <span className="flex-shrink-0 mt-0.5 flex items-center justify-center w-5 h-5 rounded-full bg-slate-500 text-white">
            <AlertTriangle className="w-3 h-3" />
          </span>
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium text-slate-800">
              {promptRegistryTitle(code, actionable)}
              <span className="ml-1.5 text-xs font-normal text-slate-500">
                (platform issue — not an admin task)
              </span>
            </p>
            <div className="mt-1 text-xs text-slate-700">
              {promptRegistryDescription(code, rawError, vendorCode)}
            </div>
            {vendorCode && (
              <div className="mt-2 font-mono text-xs text-slate-600">
                error_code:{" "}
                <span className="rounded bg-slate-200 px-1 py-0.5">{vendorCode}</span>
              </div>
            )}
          </div>
        </div>
      </div>
    )
  }

  return (
    <PermissionStep
      step={3}
      title={promptRegistryTitle(code, actionable)}
      description={
        <>
          {promptRegistryDescription(code, rawError, vendorCode)}
          {!granted && vendorCode && (
            <div className="mt-2 font-mono text-xs">
              error_code:{" "}
              <span className="rounded bg-amber-100 px-1 py-0.5">{vendorCode}</span>
            </div>
          )}
        </>
      }
      granted={granted}
    />
  )
}

function promptRegistryTitle(
  code: GSOPromptRegistryReasonCode | null | undefined,
  actionable?: GSOPromptRegistryActionableBy | null,
): string {
  switch (code) {
    case "feature_not_enabled":
      return "Enable MLflow Prompt Registry"
    case "missing_uc_permissions":
      return "Grant MLflow Prompt Registry permissions"
    case "registry_path_not_found":
      return "Verify MLflow Prompt Registry schema"
    case "missing_sp_scope":
      return "Redeploy Genie Workbench to refresh SP scope"
    case "vendor_bug":
      return "MLflow Prompt Registry platform error"
    case "probe_error":
      return "Prompt Registry check could not run"
    default:
      // Don't guess: if we don't recognize the code and the backend marks
      // it platform-actionable, say so explicitly rather than falling back
      // to "admin go enable the toggle".
      return actionable === "platform"
        ? "MLflow Prompt Registry platform error"
        : "Enable MLflow Prompt Registry"
  }
}

function promptRegistryDescription(
  code: GSOPromptRegistryReasonCode | null | undefined,
  rawError: string | null,
  vendorCode: string | null | undefined,
): React.ReactNode {
  switch (code) {
    case "feature_not_enabled":
      return (
        <>
          The MLflow Prompt Registry is not enabled on this workspace. Contact your{" "}
          <strong>workspace admin</strong> to enable the GenAI preview in workspace
          settings.
        </>
      )
    case "missing_uc_permissions":
      return (
        <>
          The service principal is missing Unity Catalog privileges required to use
          the Prompt Registry. Ask a UC admin to grant <strong>CREATE FUNCTION</strong>,{" "}
          <strong>EXECUTE</strong>, and <strong>MANAGE</strong> on the target schema,
          then click <em>Re-check</em>.
        </>
      )
    case "registry_path_not_found":
      return (
        <>
          The target catalog/schema for the Prompt Registry could not be found.
          Verify that the GSO catalog exists and the service principal has{" "}
          <strong>USE CATALOG</strong> / <strong>USE SCHEMA</strong> on it.
        </>
      )
    case "missing_sp_scope":
      return (
        <>
          The app's service principal token is missing the Prompt Registry OAuth
          scope. Redeploy the Genie Workbench app so its token picks up the
          current workspace preview scopes, or ask a UC admin to re-grant access
          to the service principal.
        </>
      )
    case "vendor_bug":
      return (
        <>
          MLflow Prompt Registry returned a platform-side error that is not
          something you can fix from the workspace. The error has been logged
          server-side. Click <em>Re-check</em> to retry; if the problem
          persists, contact Databricks FE support with the run id and the{" "}
          <code>error_code</code> below.
        </>
      )
    case "probe_error":
      return (
        <>
          The Prompt Registry probe could not run. Reload the page; if the problem
          persists, contact support with the error below:
          {rawError && <div className="mt-1 font-mono">{rawError}</div>}
        </>
      )
    default:
      return (
        <>
          MLflow Prompt Registry is unavailable for an unrecognized reason.
          Contact support with the <code>error_code</code>
          {vendorCode ? " below" : ""} and the raw error:
          {rawError && <div className="mt-1 font-mono">{rawError}</div>}
        </>
      )
  }
}
