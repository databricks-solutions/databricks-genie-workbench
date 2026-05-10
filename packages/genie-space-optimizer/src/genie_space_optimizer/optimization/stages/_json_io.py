"""JSON round-trip + pretty-print mixin for stage I/O dataclasses.

Every stage's ``INPUT_CLASS`` / ``OUTPUT_CLASS`` mixes in
``JsonRoundTrip`` so:

  * the per-stage capture decorator (Phase H) can serialize I/O to
    MLflow without runtime introspection;
  * the operator transcript renders one ``─ Input`` and one
    ``─ Output`` block per stage by calling ``inp.to_pretty()`` /
    ``out.to_pretty()``;
  * boundary-fixture replay tests round-trip through the same path
    a real run would (no synthetic-shape escape hatches).

The mixin operates on dataclass fields only — fields whose types are
non-JSON-native (e.g. ``set[str]`` ⇄ list, ``StrEnum`` ⇄ str,
nested dataclasses ⇄ dict) are converted via the per-class
``__json_field_codec__`` registry. Default behaviour handles
``tuple/list/dict/str/int/float/bool/None`` plus nested
JsonRoundTrip dataclasses.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, ClassVar


class JsonRoundTrip:
    """Mixin: ``to_json``, ``from_json``, ``to_pretty``.

    Subclasses MUST be ``@dataclass(frozen=True, slots=True)``. The
    mixin does NOT itself decorate; the subclass declares its own
    decorator so that ``slots=True`` interacts correctly with the
    multiple-base-class rule (slots base allowed).
    """

    __json_field_codec__: ClassVar[dict[str, tuple[Any, Any]]] = {}

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for f in fields(self):  # type: ignore[arg-type]
            val = getattr(self, f.name)
            out[f.name] = _to_json_value(val)
        return out

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "JsonRoundTrip":
        kwargs: dict[str, Any] = {}
        for f in fields(cls):  # type: ignore[arg-type]
            if f.name not in payload:
                continue
            kwargs[f.name] = _from_json_value(payload[f.name], f.type)
        return cls(**kwargs)  # type: ignore[call-arg]

    def to_pretty(self, *, width: int = 72) -> str:
        rows: list[str] = []
        for f in fields(self):  # type: ignore[arg-type]
            val = getattr(self, f.name)
            rows.append(_pretty_field(f.name, val, width=width))
        return "\n".join(rows)


def _to_json_value(val: Any) -> Any:
    if isinstance(val, JsonRoundTrip):
        return val.to_json()
    if is_dataclass(val):
        return {f.name: _to_json_value(getattr(val, f.name)) for f in fields(val)}
    if isinstance(val, Enum):
        return val.value
    if isinstance(val, (list, tuple)):
        return [_to_json_value(v) for v in val]
    if isinstance(val, set):
        return sorted(_to_json_value(v) for v in val)
    if isinstance(val, dict):
        return {str(k): _to_json_value(v) for k, v in val.items()}
    return val


def _from_json_value(val: Any, type_hint: Any) -> Any:
    # Type hints arrive as strings under PEP 563, or as actual type
    # objects when annotations are not stringified (no future import).
    # The codec registry in __json_field_codec__ overrides. Default
    # behaviour handles the most common non-native types by inspecting
    # the type hint. Stage Input/Output overrides __json_field_codec__
    # for fields whose types do not round-trip natively (StrEnum,
    # nested dataclass, set, frozenset).
    if isinstance(val, list):
        # Check string-form type hints (PEP 563 / from __future__ import annotations)
        if isinstance(type_hint, str):
            hint = type_hint.strip()
            if hint.startswith("tuple"):
                return tuple(val)
            if hint.startswith("frozenset"):
                return frozenset(val)
            if hint.startswith("set["):
                return set(val)
        else:
            # Check actual type objects (GenericAlias, type, etc.)
            origin = getattr(type_hint, "__origin__", None)
            if origin is tuple:
                return tuple(val)
            if origin is frozenset:
                return frozenset(val)
            if origin is set:
                return set(val)
    return val


def _pretty_field(name: str, val: Any, *, width: int) -> str:
    label = f"{name:<24}"
    rendered = _pretty_value(val, width=width - 26)
    return f"{label}: {rendered}"


def _pretty_value(val: Any, *, width: int) -> str:
    if val is None:
        return "(none)"
    if isinstance(val, JsonRoundTrip):
        return f"<{type(val).__name__}>"
    if isinstance(val, (list, tuple, set, frozenset)):
        items = list(val) if not isinstance(val, list) else val
        if not items:
            return "()"
        joined = ", ".join(str(v) for v in items[:6])
        if len(items) > 6:
            joined += f", ... ({len(items) - 6} more)"
        return f"({joined})"
    if isinstance(val, dict):
        if not val:
            return "{}"
        keys = list(val.keys())[:4]
        joined = ", ".join(f"{k}={val[k]!r}" for k in keys)
        if len(val) > 4:
            joined += f", ... ({len(val) - 4} more)"
        return f"{{{joined}}}"
    return repr(val) if isinstance(val, str) else str(val)


def pretty_block(label: str, body: str, *, width: int = 72) -> str:
    """Render a labelled block: ``─ <label> ─...`` then indented body."""
    rule = "─" * max(0, width - len(label) - 2)
    return f"─ {label} {rule}\n{body}"
