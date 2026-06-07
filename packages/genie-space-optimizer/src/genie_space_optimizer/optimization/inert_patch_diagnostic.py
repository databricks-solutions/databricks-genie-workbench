"""Trial 29 W29.1 — typed Trial29InertPatchDiagnostic record + JSONL
postmortem persistence.

When the acceptance gate fires the ``kit_forced_inert_reroute`` lane,
the harness builds a Trial29InertPatchDiagnostic with the full
forensic context (RCA, rejected mechanism, patch body, pre/post
arbiter scores, signature, iteration, trial). The record is appended
to the postmortem evidence bundle as JSONL so the next postmortem can
prove or refute "the re-route worked / a different mechanism is also
inert / the RCA is mislabeled".

Pure module: no env reads, no global state, no side effects beyond
the explicit file write in :func:`persist_inert_patch_diagnostic`.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


_BUNDLE_FILENAME = "trial29_inert_patch_diagnostics.jsonl"


class Trial29InertPatchDiagnostic(BaseModel):
    """One inert-patch event captured for postmortem analysis."""

    model_config = ConfigDict(frozen=True)

    qid: str
    rca_kind: str  # canonical key
    rejected_mechanism: str
    patch_json: dict[str, Any] = Field(default_factory=dict)
    pre_arbiter_score: float
    post_arbiter_score: float
    behavioral_diff: str  # always "unchanged" in this lane; recorded for completeness
    signature: str
    iteration: int
    trial: str  # e.g. "trial29"


def persist_inert_patch_diagnostic(
    diagnostic: Trial29InertPatchDiagnostic,
    *,
    bundle_dir: Path,
) -> Path:
    """Append a single diagnostic to the bundle's JSONL file.

    Creates ``bundle_dir`` if missing. Returns the path of the JSONL
    file (so callers can log it). Each call appends one line; the
    file accumulates across iterations and is read by the postmortem
    summariser via :func:`load_inert_patch_diagnostics`.
    """
    bundle_dir.mkdir(parents=True, exist_ok=True)
    target = bundle_dir / _BUNDLE_FILENAME
    payload = diagnostic.model_dump()
    with target.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
    return target


def load_inert_patch_diagnostics(
    bundle_dir: Path,
) -> tuple[Trial29InertPatchDiagnostic, ...]:
    """Read every diagnostic in the bundle's JSONL file in order.

    Returns an empty tuple when the file doesn't exist (no inert
    re-routes happened, byte-stable on green replays).
    """
    target = Path(bundle_dir) / _BUNDLE_FILENAME
    if not target.exists():
        return ()
    out: list[Trial29InertPatchDiagnostic] = []
    with target.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            out.append(Trial29InertPatchDiagnostic.model_validate_json(line))
    return tuple(out)


__all__ = [
    "Trial29InertPatchDiagnostic",
    "persist_inert_patch_diagnostic",
    "load_inert_patch_diagnostics",
]
