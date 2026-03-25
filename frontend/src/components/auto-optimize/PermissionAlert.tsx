import { useState } from "react"
import { ShieldAlert, Check, Copy, CheckCheck, RefreshCw } from "lucide-react"
import type { GSOPermissionCheck } from "@/types"

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

        <PermissionStep
          step={3}
          title="Enable MLflow Prompt Registry"
          description={
            <>
              The MLflow Prompt Registry must be enabled on your workspace for judge prompt
              traceability. Contact your <strong>workspace admin</strong> to enable this feature
              in the workspace settings.
            </>
          }
          granted={permissions.prompt_registry_available !== false}
        />
      </div>
    </div>
  )
}
