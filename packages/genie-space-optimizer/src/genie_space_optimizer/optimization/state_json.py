"""Phase 2 Hotfix (2026-05-17) — canonical JSON encoder for every
``json.dumps`` boundary in ``optimization/state.py``.

Background: Phase 2 (2026-05-16) made every non-accepted
reflection-buffer entry carry a ``TerminalSignature`` NamedTuple in
``entry["terminal_signature"]``. The NamedTuple contains two
``frozenset`` fields (``lever_set``, ``target_qids``). ``json.dumps``
cannot serialize frozensets, so the end-of-run ``write_stage`` call
at ``harness.py:~31346`` crashed with ``TypeError: Object of type
frozenset is not JSON serializable``.

This module is the single shared encoder used at every state-
serialization boundary. The encoder is intentionally minimal:

* ``frozenset`` / ``set`` → canonically sorted list (matches
  ``terminal_signature.to_jsonable``'s Section 4.4 contract)
* ``TerminalSignature`` → its canonical ``to_jsonable`` shape
* Any ``NamedTuple`` (detected via ``_asdict``) → its ``_asdict()``
  dict (then encoded recursively, so nested frozensets are caught)
* Anything else → ``super().default(obj)`` (raises TypeError, by
  design — silent passthrough would mask future regressions)

The in-memory consumers of ``entry["terminal_signature"]`` (notably
``compute_retired_signatures`` in ``forbidden_ag_set_v2.py``, which
does ``isinstance(sig, TerminalSignature)``) are NOT affected: the
encoder runs only at the serialization boundary, not at entry-build
time.
"""
from __future__ import annotations

import json
from typing import Any


def _terminal_signature_jsonable(obj: Any) -> dict[str, Any] | None:
    """Return Section 4.4 JSON shape for ``TerminalSignature``, else None."""
    try:
        from genie_space_optimizer.optimization.terminal_signature import (
            TerminalSignature,
            to_jsonable,
        )
        if isinstance(obj, TerminalSignature):
            return to_jsonable(obj)
    except Exception:
        # Import-time cycles should not mask the real serialization error.
        pass
    return None


def _canonicalize(obj: Any) -> Any:
    """Recursively convert frozensets/sets to sorted lists and
    typed signature carriers to dicts so the standard ``json.dumps``
    machinery can handle them.

    NamedTuples are subclasses of tuple, so the stock encoder
    serializes them as JSON arrays via its built-in tuple path —
    that path is reached BEFORE ``JSONEncoder.default()`` fires. A
    custom ``default()`` is therefore not enough; we pre-walk the
    tree and replace problem nodes before json sees them.
    """
    # TerminalSignature moved from NamedTuple to frozen dataclass in
    # P2.5. Pin its JSON surface to ``to_jsonable`` (Section 4.4).
    ts_json = _terminal_signature_jsonable(obj)
    if ts_json is not None:
        return ts_json

    # NamedTuple — check first because it's also an instance of tuple.
    if hasattr(obj, "_asdict") and isinstance(obj, tuple):
        return {k: _canonicalize(v) for k, v in obj._asdict().items()}
    if isinstance(obj, (frozenset, set)):
        try:
            ordered = sorted(obj)
        except TypeError:
            ordered = sorted(obj, key=repr)
        return [_canonicalize(v) for v in ordered]
    if isinstance(obj, dict):
        return {k: _canonicalize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_canonicalize(v) for v in obj]
    return obj


class GsoJsonEncoder(json.JSONEncoder):
    """Canonical JSON encoder for GSO state writes.

    The encoder pre-walks its input via :func:`_canonicalize` (so
    NamedTuples and frozensets are handled even on the tuple
    fast-path) and then delegates to the stock JSONEncoder. The
    ``default()`` method retains the same shape as a safety net for
    any direct ``cls=GsoJsonEncoder`` call that bypasses
    :func:`dumps_state_json`.

    Use via :func:`dumps_state_json` (preferred) or by passing
    ``cls=GsoJsonEncoder`` to ``json.dumps`` directly — but note the
    latter requires the caller to canonicalize first OR rely on
    ``default()`` for top-level non-tuple values only.
    """

    def encode(self, obj: Any) -> str:
        return super().encode(_canonicalize(obj))

    def iterencode(self, obj: Any, _one_shot: bool = False):
        return super().iterencode(_canonicalize(obj), _one_shot=_one_shot)

    def default(self, obj: Any) -> Any:
        # Safety net for direct ``cls=`` use that didn't go through
        # encode()/iterencode(). Frozenset and NamedTuple cases are
        # normally caught by the pre-pass.
        ts_json = _terminal_signature_jsonable(obj)
        if ts_json is not None:
            return ts_json
        if isinstance(obj, (frozenset, set)):
            try:
                return sorted(obj)
            except TypeError:
                return sorted(obj, key=repr)
        if hasattr(obj, "_asdict") and isinstance(obj, tuple):
            return obj._asdict()
        return super().default(obj)


def dumps_state_json(obj: Any) -> str:
    """Serialize ``obj`` to a JSON string using :class:`GsoJsonEncoder`.

    Use this at every ``state.py`` JSON-serialization boundary.
    Equivalent to ``json.dumps(obj, cls=GsoJsonEncoder)``.
    """
    return json.dumps(obj, cls=GsoJsonEncoder)
