"""Trial 14 — typed ASI ``blame_set`` entries with a closed ``kind``
vocabulary.

Background
----------
Through Trial 13k the ASI judges emitted ``blame_set`` as a free-text
``list[str]`` — sometimes bare schema identifiers
(``"DEST_AIRPORT_CD"``), sometimes Python-repr / JSON-quoted lists
(``'["zone_name"]'``), sometimes SQL fragments
(``"LIMIT 10 vs RANK() <= 10"``), sometimes English prose
(``"PAYMENT_CURRENCY_CD = 'USD' filter incorrectly added"``).
Downstream the ``_normalize_seeds_to_fqn`` resolver could only act on
bare identifiers / FQNs; everything else was dropped, producing
``seeds_unnormalizable`` declines (Trial 13k canary).

Trial 14 promotes ``blame_set`` to a list of typed
:class:`BlameEntry` objects with five closed ``kind`` values and an
optional free-text ``description``. The FQN resolver acts on
``kind in {column, table, join}`` alone; prose is routed to
``kind in {filter, instruction}`` and surfaces as a separate, precise
postmortem signal instead of being lumped under "blame_set_empty".

The dataclass is intentionally minimal — three fields — so it
round-trips cleanly through MLflow assessment metadata (which
serializes everything to JSON) and through the workbench v2 capture
bundle without a custom codec. The :class:`JsonRoundTrip` mixin is the
same one used by :class:`PerQidRcaEvidence` so stage I/O writers and
the per-stage capture decorator pick up the new type for free.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Iterable, Literal

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


# Closed vocabulary of blame kinds. Anything an LLM judge returns that
# is not in this set is coerced to ``"instruction"`` (the catch-all
# textual kind) by :func:`coerce_blame_entries`, with the original
# token preserved in ``description`` so postmortems can recover it.
BLAME_KINDS: frozenset[str] = frozenset(
    {"column", "table", "join", "filter", "instruction"}
)

# Subset of kinds whose ``ref`` is expected to resolve to a 4-part
# schema FQN via ``_normalize_seeds_to_fqn``. Stage 1 seeds its
# reasoning from these kinds only.
SCHEMA_RESOLVABLE_KINDS: frozenset[str] = frozenset({"column", "table", "join"})

BlameKind = Literal["column", "table", "join", "filter", "instruction"]


@dataclass(frozen=True, slots=True)
class BlameEntry(JsonRoundTrip):
    """One element of a structured ``blame_set`` payload.

    Field semantics
    ---------------
    * ``kind`` — closed vocabulary; see :data:`BLAME_KINDS`. Kinds
      ``column`` / ``table`` / ``join`` are schema-resolvable;
      ``filter`` / ``instruction`` carry prose only.
    * ``ref`` — schema reference (FQN or bare identifier) for
      schema-resolvable kinds, the literal expression text for
      ``filter`` (e.g. ``"PAYMENT_CURRENCY_CD = 'USD'"``), the rule
      or instruction string for ``instruction``. May be ``None`` when
      the upstream judge emitted only prose.
    * ``description`` — optional one-line human rationale. Free-form;
      not parsed downstream.

    Construction is permissive — :func:`coerce_blame_entries` is the
    canonical entry point and handles the production-wild shapes
    catalogued in Trial 13k (legacy ``list[str]``, JSON arrays of
    dicts, mixed lists). Direct construction is reserved for trusted
    internal callers (tests, the deterministic ``asset_routing``
    code-judge).
    """

    kind: BlameKind
    ref: str | None
    description: str | None = None

    def __post_init__(self) -> None:
        # Defensive guard: unknown kinds *must not* slip past
        # construction even from internal callers. ``coerce_blame_entries``
        # is responsible for collapsing unknown tokens onto
        # ``instruction`` before reaching here.
        if self.kind not in BLAME_KINDS:
            raise ValueError(
                f"BlameEntry.kind={self.kind!r} not in BLAME_KINDS={sorted(BLAME_KINDS)!r}"
            )
        # Schema-resolvable kinds must carry a non-empty ref —
        # otherwise the entry is useless to the FQN resolver and
        # should have been classified as ``filter`` / ``instruction``
        # by the coercer.
        if self.kind in SCHEMA_RESOLVABLE_KINDS:
            ref_text = (self.ref or "").strip()
            if not ref_text:
                raise ValueError(
                    f"BlameEntry.kind={self.kind!r} requires a non-empty ref"
                )

    def is_schema_resolvable(self) -> bool:
        """True for column/table/join kinds — eligible for FQN normalization."""
        return self.kind in SCHEMA_RESOLVABLE_KINDS

    def to_dict(self) -> dict[str, Any]:
        """Return a plain-dict representation suitable for nested
        ``<judge>/metadata.blame_set_structured`` storage and for
        JSON-encoded flat ``metadata/<judge>/blame_set_structured``
        keys.

        Note: ``to_json`` (from :class:`JsonRoundTrip`) returns the
        same shape but with the field-iteration of the mixin.
        ``to_dict`` is kept as a stable, no-frills synonym for the
        writer paths in ``build_asi_metadata`` and the workbench
        capture serializer.
        """
        return {
            "kind": self.kind,
            "ref": self.ref,
            "description": self.description,
        }


# ── Coercion ──────────────────────────────────────────────────────────
#
# The LLM judges drift in practice. ``coerce_blame_entries`` is the
# single funnel that maps any of the following shapes onto a clean
# ``list[BlameEntry]``:
#
#   * structured ``list[dict]`` with valid kinds (trust path)
#   * legacy ``list[str]`` (Trial 13k production-wild shapes)
#   * mixed list (some dicts, some strings)
#   * JSON-encoded string of either shape (flat metadata key path)
#
# Tokens that don't classify as schema-resolvable land on ``filter``
# (predicate-shaped text) or ``instruction`` (everything else,
# including raw prose); the original token is preserved in
# ``description`` so postmortems can recover the upstream signal.

_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FQN_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Heuristics for routing free-text tokens onto kinds. The order is
# significant — predicate shapes are checked before the bare/FQN paths
# so things like "col = 'val'" do not get misclassified as columns.
_FILTER_HINT_CHARS = frozenset("=<>()'\"")
_FILTER_KEYWORDS = (
    " vs ",
    " v.s. ",
    " limit ",
    " rank ",
    " over ",
    " between ",
    " in (",
    " not in",
    " group by",
    " order by",
    " having ",
)


def _looks_like_filter_expression(token: str) -> bool:
    """True for tokens that look like SQL predicates or fragments."""
    if not token:
        return False
    if any(ch in _FILTER_HINT_CHARS for ch in token):
        return True
    lowered = f" {token.lower().strip()} "
    return any(kw in lowered for kw in _FILTER_KEYWORDS)


def _classify_fqn(token: str) -> tuple[str, str] | None:
    """Map a dotted identifier onto ``(kind, ref)`` when it parses as
    a 3-part (table) or 4-part (column) FQN. Returns ``None`` otherwise.
    """
    parts = token.split(".")
    if len(parts) not in (3, 4):
        return None
    for part in parts:
        if not _FQN_PART_RE.fullmatch(part.strip()):
            return None
    kind = "column" if len(parts) == 4 else "table"
    return kind, token.strip()


def _entry_from_token(token: str) -> BlameEntry:
    """Heuristically classify a single free-text blame token.

    Routing table:

    * ``"a.b.c.d"`` (4-part) -> ``kind=column``, ``ref=token``
    * ``"a.b.c"`` (3-part)   -> ``kind=table``,  ``ref=token``
    * bare identifier        -> ``kind=column``, ``ref=token`` (FQN
      resolver will rescue via ``_normalize_seeds_to_fqn``)
    * predicate-shaped text  -> ``kind=filter``, ``ref=None``,
      ``description=token``
    * everything else        -> ``kind=instruction``, ``ref=None``,
      ``description=token``

    The classifier is intentionally permissive: the worst case is
    misrouting prose to ``filter`` or ``instruction``, both of which
    Stage 1 already handles as non-schema blame (Trial 14's
    seeds_all_filter_kind contract arm).
    """
    text = (token or "").strip()
    if not text:
        # Caller is responsible for filtering empties; defensively
        # return an instruction entry so we never raise here.
        return BlameEntry(kind="instruction", ref=None, description="")
    fqn = _classify_fqn(text)
    if fqn is not None:
        kind, ref = fqn
        return BlameEntry(kind=kind, ref=ref, description=None)  # type: ignore[arg-type]
    if _BARE_IDENT_RE.fullmatch(text):
        return BlameEntry(kind="column", ref=text, description=None)
    if _looks_like_filter_expression(text):
        return BlameEntry(kind="filter", ref=None, description=text)
    return BlameEntry(kind="instruction", ref=None, description=text)


def _entry_from_dict(raw: dict[str, Any]) -> BlameEntry | None:
    """Coerce a single ``{kind, ref, description?}`` dict.

    Unknown / missing ``kind`` collapses onto ``instruction`` so we
    never reject a payload that an LLM judge worked to produce; the
    original kind survives in the description so postmortems can
    surface vocabulary drift.
    """
    kind_raw = raw.get("kind")
    ref_raw = raw.get("ref")
    desc_raw = raw.get("description")
    kind_text = str(kind_raw or "").strip().lower()
    ref_text = str(ref_raw or "").strip() if ref_raw is not None else None
    desc_text = (
        str(desc_raw).strip() if desc_raw is not None and str(desc_raw).strip() else None
    )

    if kind_text in BLAME_KINDS:
        if kind_text in SCHEMA_RESOLVABLE_KINDS:
            if not ref_text:
                # Schema-resolvable kind without a ref — preserve the
                # description (so we don't lose the signal) but
                # demote the kind to ``instruction``.
                if not desc_text:
                    return None
                return BlameEntry(
                    kind="instruction",
                    ref=None,
                    description=desc_text or kind_text,
                )
            return BlameEntry(kind=kind_text, ref=ref_text, description=desc_text)  # type: ignore[arg-type]
        # filter / instruction — ref optional
        return BlameEntry(kind=kind_text, ref=ref_text or None, description=desc_text)  # type: ignore[arg-type]

    # Unknown kind — collapse onto instruction with the original
    # token preserved as description so the upstream signal survives.
    fallback_desc = desc_text or ref_text or (kind_raw if kind_raw else None)
    if fallback_desc is None and ref_text is None:
        return None
    return BlameEntry(
        kind="instruction",
        ref=None,
        description=str(fallback_desc) if fallback_desc else None,
    )


def _normalize_raw_structured(raw: Any) -> list[Any]:
    """Lift ``raw`` (str/list/None) into a list of candidate entries.

    Accepts the production-wild shapes:

    * ``None`` / ``""`` -> empty list
    * ``list``          -> returned as-is (each element classified by
      :func:`_entry_from_dict` / :func:`_entry_from_token`)
    * JSON string of a list -> parsed via ``json.loads``
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
        return []
    return []


def coerce_blame_entries(
    raw_structured: Any = None,
    legacy_strings: Iterable[Any] | None = None,
) -> list[BlameEntry]:
    """Canonical funnel: any input shape -> ``list[BlameEntry]``.

    Priority:

    1. If *raw_structured* yields candidates, use those (trusting the
       judge that emitted them); fall back to ``_entry_from_token``
       for non-dict elements in a mixed list.
    2. Otherwise, classify each *legacy_strings* token via
       ``_entry_from_token``.
    3. De-duplicate on ``(kind, ref, description)`` preserving first
       occurrence so downstream rendering is stable.

    Empty / whitespace tokens are dropped. The result is a fresh
    ``list[BlameEntry]`` ready for serialization via ``to_dict()``.
    """
    entries: list[BlameEntry] = []
    seen: set[tuple[str, str | None, str | None]] = set()

    def _push(entry: BlameEntry | None) -> None:
        if entry is None:
            return
        key = (entry.kind, entry.ref, entry.description)
        if key in seen:
            return
        seen.add(key)
        entries.append(entry)

    raw_list = _normalize_raw_structured(raw_structured)
    if raw_list:
        for element in raw_list:
            if isinstance(element, dict):
                _push(_entry_from_dict(element))
            elif isinstance(element, str):
                text = element.strip()
                if text:
                    _push(_entry_from_token(text))
            # silently skip anything else (ints, None, etc.)
        return entries

    for token in legacy_strings or ():
        text = str(token or "").strip()
        if not text:
            continue
        _push(_entry_from_token(text))
    return entries


def legacy_blame_set_from_entries(entries: list[BlameEntry]) -> list[str]:
    """Project schema-resolvable entries back to a legacy
    ``list[str]`` mirror.

    Only ``kind in {column, table, join}`` with a non-empty ``ref``
    contribute — filter/instruction kinds are deliberately excluded
    from the legacy field so existing FQN-only readers (clustering,
    AFS, RCA card builder) do not start seeing SQL fragments or prose.
    Order-preserving and de-duplicated.
    """
    seen: set[str] = set()
    out: list[str] = []
    for entry in entries:
        if entry.kind not in SCHEMA_RESOLVABLE_KINDS:
            continue
        ref = (entry.ref or "").strip()
        if not ref or ref in seen:
            continue
        seen.add(ref)
        out.append(ref)
    return out


def kinds_distribution(entries: list[BlameEntry] | tuple[BlameEntry, ...]) -> dict[str, int]:
    """Return a ``{kind: count}`` histogram for *entries*.

    Used by the Stage 1 observability marker
    (:func:`plan11_stage1_input_quality_marker`) to publish a
    per-kind frequency map alongside the seed-normalization verdict.
    Returns an empty dict for an empty input so callers can use it
    directly as a marker payload field without ``or {}`` boilerplate.
    """
    out: dict[str, int] = {}
    for entry in entries or ():
        out[entry.kind] = out.get(entry.kind, 0) + 1
    return out
