// Empty/gate states as onboarding (MV-D107 Phase 2). One shape everywhere: an outcome
// HEADING, a muted GHOST preview of the surface that will populate, ONE primary action, and
// an optional escape hatch. Shared by the Ontology gate notices and the cold Review state so
// a blocked/empty surface still tells the user what to do next (not just what's missing).
import type { ReactNode } from "react"
import { Button } from "@/components/ui/button"

// A decorative skeleton of the populated surface — rows of muted bars. Purely visual, so it's
// hidden from assistive tech.
export function GhostPreview() {
  return (
    <div aria-hidden="true" className="mt-5 space-y-2 opacity-50">
      {[0, 1, 2].map((i) => (
        <div
          key={i}
          className="flex items-center gap-3 rounded-lg border border-default bg-elevated px-3 py-3"
        >
          <div className="h-4 w-4 rounded bg-sunken" />
          <div className="h-3 flex-1 rounded bg-sunken" />
          <div className="h-3 w-14 rounded bg-sunken" />
        </div>
      ))}
    </div>
  )
}

export interface OnboardingAction {
  label: string
  onClick: () => void
  disabled?: boolean
}

export function OnboardingState({
  icon,
  heading,
  body,
  primary,
  escape,
  ghost = <GhostPreview />,
}: {
  icon: ReactNode
  heading: string
  body: ReactNode
  primary: OnboardingAction
  escape?: OnboardingAction
  /** Override the default ghost skeleton, or pass null to omit it. */
  ghost?: ReactNode
}) {
  return (
    <div className="rounded-xl border border-default bg-surface px-5 py-6">
      <div className="flex items-start gap-3">
        <span className="mt-0.5 shrink-0 text-accent" aria-hidden="true">
          {icon}
        </span>
        <div className="min-w-0">
          <h3 className="text-base font-semibold text-primary">{heading}</h3>
          <p className="mt-1 max-w-prose text-sm text-secondary">{body}</p>
          <div className="mt-4 flex flex-wrap items-center gap-4">
            <Button onClick={primary.onClick} disabled={primary.disabled}>
              {primary.label}
            </Button>
            {escape && (
              <button
                type="button"
                onClick={escape.onClick}
                disabled={escape.disabled}
                className="text-sm text-muted underline underline-offset-2 hover:text-secondary disabled:opacity-50"
              >
                {escape.label}
              </button>
            )}
          </div>
        </div>
      </div>
      {ghost}
    </div>
  )
}
