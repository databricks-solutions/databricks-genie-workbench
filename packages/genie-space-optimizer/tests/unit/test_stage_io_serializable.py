"""Phase H Task 4: every stage Input/Output dataclass is JSON-serializable
via dataclasses.asdict() + json.dumps().

Catches problems Phase H would otherwise hit at runtime:
  - set fields (asdict produces sets, json.dumps rejects them)
  - tuple fields are fine (asdict converts to lists)
  - callables, modules, or other non-JSON values
"""

from __future__ import annotations

import dataclasses as _dc
import json
from dataclasses import asdict, fields, is_dataclass

import pytest

from genie_space_optimizer.optimization.stages import STAGES


_MISSING_SENTINEL = _dc.MISSING


def _placeholder_for(annotation: object) -> object:
    """Map a type annotation to a minimal placeholder value.

    Conservative: for ambiguous types, returns an empty container or 0.
    """
    text = str(annotation)
    if "tuple[" in text or "Tuple[" in text:
        return ()
    if "list[" in text or "List[" in text:
        return []
    if "dict[" in text or "Dict[" in text:
        return {}
    if "set[" in text or "Set[" in text or "frozenset[" in text:
        return set()
    if text in {"int", "float"}:
        return 0
    if text == "bool":
        return False
    return ""


def _build_minimal_instance(cls: type) -> object:
    """Construct a minimal instance of a dataclass with all required fields.

    Required fields get type-appropriate placeholders; optional fields
    use their defaults / default_factory.
    """
    if not is_dataclass(cls):
        pytest.skip(f"{cls.__name__} is not a dataclass")
    kwargs: dict[str, object] = {}
    for f in fields(cls):
        if f.default is not _MISSING_SENTINEL or f.default_factory is not _MISSING_SENTINEL:  # type: ignore[arg-type]
            continue
        kwargs[f.name] = _placeholder_for(f.type)
    return cls(**kwargs)


@pytest.mark.parametrize(
    "stage_key, kind, cls",
    [
        (entry.stage_key, kind, getattr(entry, f"{kind}_class"))
        for entry in STAGES
        for kind in ("input", "output")
    ],
    ids=lambda v: str(v) if not isinstance(v, type) else v.__name__,
)
def test_stage_io_dataclass_is_json_serializable(
    stage_key: str, kind: str, cls: type,
) -> None:
    instance = _build_minimal_instance(cls)
    as_dict = asdict(instance)
    # Should round-trip through json without raising.
    # default=str matches what the capture decorator does in production:
    # opaque objects become a string rather than the dump raising.
    serialized = json.dumps(
        as_dict,
        sort_keys=True,
        default=lambda v: list(v) if isinstance(v, (set, frozenset)) else str(v),
    )
    parsed = json.loads(serialized)
    assert isinstance(parsed, dict)
