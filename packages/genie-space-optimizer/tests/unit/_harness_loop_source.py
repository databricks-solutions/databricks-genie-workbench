"""Shared helper — the *effective* ``_run_lever_loop`` source.

The lever loop was split during the state-machine cutover: the public
``_run_lever_loop`` is now a thin dispatcher that routes to
``_run_lever_loop_sm_first`` or ``_run_lever_loop_legacy`` (which carries
the bulk of the historical loop body). Source-introspection tests that
assert on call sites, emitted markers, or local variable names must
search the UNION of all three functions so they stay meaningful after
the refactor instead of breaking on the dispatcher's empty body.

Use :func:`lever_loop_source` anywhere a test previously did
``inspect.getsource(harness._run_lever_loop)``.
"""
from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness

_LOOP_FN_NAMES = (
    "_run_lever_loop",
    "_run_lever_loop_sm_first",
    "_run_lever_loop_legacy",
)


def lever_loop_source() -> str:
    """Return the concatenated source of the dispatcher + both loop
    implementations. Functions that do not exist (e.g. on a build where
    the legacy arm was removed) are skipped."""
    parts: list[str] = []
    for name in _LOOP_FN_NAMES:
        fn = getattr(harness, name, None)
        if fn is not None:
            parts.append(inspect.getsource(fn))
    return "\n".join(parts)
