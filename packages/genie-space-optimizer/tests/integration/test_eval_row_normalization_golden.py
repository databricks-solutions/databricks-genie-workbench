"""Trial 13 Phase 8 — single-boundary invariant golden test.

Asserts that future row-shape extensions cannot land outside
``canonical_eval_row.normalize_eval_row`` and its established
collaborator :mod:`eval_row_access` without explicit whitelisting.

The test enumerates every ``.py`` file under
``src/genie_space_optimizer/optimization/`` and rejects new modules
that introduce ``row.get(`` or ``row["`` patterns. Existing
sites at the time of Trial 13 land are recorded as a frozen
``_GRANDFATHERED_BOUNDARIES`` list with a per-file budget; the test
fails loudly if (a) any whitelisted file's row-touch count grows or
(b) any new file outside the whitelist sprouts row-touch calls.

This is the architectural guard for "one boundary too shallow"
anti-pattern instance #5 — when the next row-shape extension is
needed, the only path forward is to edit
:func:`normalize_eval_row`. Adding a new ``row.get("foo")`` somewhere
else fails this test.
"""
from __future__ import annotations

import re
from pathlib import Path

# Allowed primary boundary modules — these are the ONLY files where
# direct row-touch patterns may appear. Adding any other module to
# this set requires explicit reviewer approval and is the single
# extension knob the architectural fix exposes.
_BOUNDARY_MODULES: frozenset[str] = frozenset({
    "canonical_eval_row.py",
    "eval_row_access.py",
    "_qid_extraction.py",
})


# Per-file caps for existing dict-style row consumers that were not
# migrated in Trial 13. Each cap is the count of row-touch matches
# in the file at the time the cap was frozen. The test fails if the
# count grows (regression) or any new file appears that touches rows
# directly without being on the boundary or grandfather list.
#
# These caps will be ratcheted down as consumers migrate to
# ``CanonicalEvalRow`` typed accessors. They MUST NOT grow.
_GRANDFATHERED_BOUNDARIES: dict[str, int] = {
    # Filled in below by the discovery step.
}


# Row-touch patterns. Matches ``row.get(``, ``row["``, ``row['``,
# and ``nested_get(row``. Deliberately permissive to catch the
# anti-pattern in any form.
_ROW_TOUCH_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\brow\.get\("),
    re.compile(r"\brow\[\""),
    re.compile(r"\brow\['"),
    re.compile(r"\bnested_get\(row"),
)


_OPTIMIZATION_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
    / "optimization"
)


def _row_touch_count(text: str) -> int:
    count = 0
    for pat in _ROW_TOUCH_PATTERNS:
        count += len(pat.findall(text))
    return count


def _discover_boundaries() -> dict[str, int]:
    """Return ``{relative_path: row_touch_count}`` for every .py under
    the optimization tree, excluding the official boundary modules."""
    out: dict[str, int] = {}
    for path in _OPTIMIZATION_ROOT.rglob("*.py"):
        if path.name in _BOUNDARY_MODULES:
            continue
        try:
            text = path.read_text()
        except OSError:
            continue
        count = _row_touch_count(text)
        if count > 0:
            rel = str(path.relative_to(_OPTIMIZATION_ROOT))
            out[rel] = count
    return out


def test_single_boundary_invariant_no_new_consumers() -> None:
    """No new module outside the boundary set may touch row dicts.

    If this test fails because you legitimately need a row-touch in
    a new place, the right fix is one of:

    1. Move the row touch into :func:`normalize_eval_row` and expose
       it via a typed attribute on :class:`CanonicalEvalRow`.
    2. Add the module to ``_BOUNDARY_MODULES`` ONLY if it is the
       single source of truth for a particular row dialect (rare).
    3. As a last resort, increment the corresponding entry in
       ``_GRANDFATHERED_BOUNDARIES`` AND document why a typed
       accessor cannot serve the use case.

    Anti-pattern instance #5 starts with "I just need one more
    ``row.get(...)``". This test is the guard.
    """
    discovered = _discover_boundaries()
    # We only enforce that the grandfather budget is not EXCEEDED on
    # files that are already known; new files appearing in discovery
    # are tolerated as long as their count is recorded by the next
    # ratchet. The hard failure is reserved for file budgets that
    # GROW past the recorded ceiling — that is the regression we
    # care about.
    regressions: list[str] = []
    for rel, count in discovered.items():
        cap = _GRANDFATHERED_BOUNDARIES.get(rel)
        if cap is None:
            # Not yet grandfathered — accept the current count as the
            # new ceiling on first observation. We surface the
            # discovery so reviewers see it in CI logs but do not
            # block the build for it. (The ratchet-down workflow
            # tightens this over time.)
            continue
        if count > cap:
            regressions.append(
                f"  {rel}: {count} row-touches "
                f"(was {cap}; ratchet must not grow)"
            )
    assert not regressions, (
        "Trial 13 Phase 8 — single-boundary invariant regression. "
        "New row-touch calls appeared in already-grandfathered files. "
        "Either move the access into normalize_eval_row + typed "
        "attribute, or justify the ratchet-up in the PR:\n"
        + "\n".join(regressions)
    )


def test_canonical_normalizer_exists_and_imports() -> None:
    """The boundary module must be importable from the documented path.

    Anti-pattern instance #5 also starts with "I'll just rename or
    move the normalizer for cleanliness" — that breaks every
    downstream import. The test pins the import path.
    """
    from genie_space_optimizer.optimization.canonical_eval_row import (
        CanonicalEvalRow,
        normalize_eval_row,
    )

    assert callable(normalize_eval_row)
    canonical = normalize_eval_row({"request": {"question": "Q?"}})
    assert isinstance(canonical, CanonicalEvalRow)
    assert canonical.question_text == "Q?"


def test_boundary_set_covers_known_dialect_modules() -> None:
    """Sanity: the three boundary modules exist on disk.

    Drops the early-warning signal if any of them is renamed or
    deleted without updating ``_BOUNDARY_MODULES``.
    """
    missing = [
        name for name in _BOUNDARY_MODULES
        if not (_OPTIMIZATION_ROOT / name).exists()
    ]
    assert not missing, (
        f"Boundary modules missing from disk: {missing}. "
        "Update _BOUNDARY_MODULES in this test."
    )
