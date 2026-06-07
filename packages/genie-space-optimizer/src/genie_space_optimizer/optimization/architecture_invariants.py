"""Trial 29 W29.5 — decomposed ArchitectureInvariants typed model.

Splits the monolithic ``architecture_invariants_held: bool`` into
per-domain sub-invariants so progress is visible per domain.

Sub-invariants:

* ``rca_invariants_held`` — RCA canonicaliser (Trial 26 W26.1, Trial 28
  W28.1), kit-for-RCA validator (Trial 24), kit-map coverage (Trial 26
  W26.2). True today after W28.1 deploy.
* ``lever_lattice_invariants_held`` — Stage 3 prompt fits cap (Trial 27
  W27.1), lever loop runs when needed (Trial 27 W27.3 force override),
  inert kit-forced patches re-route to a different structural mechanism
  (Trial 29 W29.1). Becomes true after W29.1 deploys + a live re-route
  is observed.
* ``bundle_completeness_invariants_held`` — postmortem evidence bundle
  is complete (every kit-forced acceptance has a behaviour-delta
  record, every inert re-route has a Trial29InertPatchDiagnostic,
  persistence / handoff hops succeed end-to-end). Tracked as a
  separate sub-invariant so an orthogonal infra gap does not mask
  RCA / lever-lattice progress.

``all_held`` is the conjunction (preserving the legacy single-bool
contract for harness reads). :func:`legacy_architecture_invariants_held`
is a free-function alias for the same conjunction so the postmortem
serialiser does not need to reach into the typed model.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ArchitectureInvariants(BaseModel):
    """Per-domain typed invariant view.

    Replaces the monolithic ``architecture_invariants_held: bool``
    field at the postmortem boundary. Existing callers that need the
    single-bool aggregate use :attr:`all_held` (or the free-function
    :func:`legacy_architecture_invariants_held`); new callers can
    consume each sub-invariant directly so progress in one domain is
    not masked by an orthogonal gap in another.
    """

    model_config = ConfigDict(frozen=True)

    rca_invariants_held: bool
    lever_lattice_invariants_held: bool
    bundle_completeness_invariants_held: bool

    @property
    def all_held(self) -> bool:
        """Conjunction across every sub-invariant. The single-bool
        backwards-compat alias for the legacy
        ``architecture_invariants_held`` field.
        """
        return (
            self.rca_invariants_held
            and self.lever_lattice_invariants_held
            and self.bundle_completeness_invariants_held
        )


def legacy_architecture_invariants_held(inv: ArchitectureInvariants) -> bool:
    """Backwards-compat helper for postmortem serialisers that still
    write the single ``architecture_invariants_held: bool`` field.
    """
    return inv.all_held


def render_postmortem_section(inv: ArchitectureInvariants) -> str:
    """Render the per-domain sub-invariants + the backwards-compat
    aggregate into the postmortem markdown.

    Output format matches the existing ``architectural
    self-assessment`` section vocabulary so the /goal harness parser
    keeps working: each line is ``<name> = <true|false>`` and the
    final line carries the legacy aggregate.
    """
    return (
        f"rca_invariants_held = {str(inv.rca_invariants_held).lower()}\n"
        f"lever_lattice_invariants_held = "
        f"{str(inv.lever_lattice_invariants_held).lower()}\n"
        f"bundle_completeness_invariants_held = "
        f"{str(inv.bundle_completeness_invariants_held).lower()}\n"
        f"architecture_invariants_held = {str(inv.all_held).lower()}"
    )


__all__ = [
    "ArchitectureInvariants",
    "legacy_architecture_invariants_held",
    "render_postmortem_section",
]
