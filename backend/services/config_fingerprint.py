"""Canonical fingerprints for Genie Agent config and benchmark state.

Powers the Auto-Optimize "current version" check. Configuration and benchmarks
are fingerprinted independently because a scoped revert can intentionally mix
the config from one historical version with the benchmarks from another. A
component non-match proves external drift only when all expected versions have
an authoritative observation; submitted-only legacy history is inconclusive.

Fingerprint contract (all three steps matter equally):

1. **Unwrap** — stored/live configs arrive in three historical shapes (full
   Genie GET-response wrapper, ``_parsed_space`` wrapper, bare parsed
   serialized_space). All reduce to the parsed serialized_space object. The
   pristine ``serialized_space`` payload is preferred over ``_parsed_space``:
   GSO's preflight mutates the latter in place (e.g. injecting
   ``_data_profile``) before persisting the snapshot, so the parsed copy can
   diverge from what the Genie API round-trips. Internal ``_``-prefixed
   top-level keys are stripped for hashing regardless of source; VC snapshots
   retain the exact, unstripped payload for restoration.
2. **Canonicalize** — configuration hashing drops the top-level ``benchmarks``
   block, while benchmark hashing isolates that block and treats an omitted
   block as an empty question set. ``content`` and ``sql`` fragments are
   concatenated; boundary quotes added by Genie are ignored for configuration
   hashing but preserved for benchmark ground-truth SQL; object keys are
   sorted. The legacy ``_collection_keys=None`` path retains blanket sorting
   of string-id arrays for persisted-hash compatibility. The VC path sorts
   only declared id-addressed collections (t4), retaining other array order.
   Duplicate-id arrays retain their order only in the VC path. VC data sources
   merge tables and metric views by identifier: read-back loses that split,
   so the hash must ignore it and its METRIC_VIEW type markers.
3. **Hash** — SHA-256 over the compact JSON serialization.

Pure functions only — no I/O, fully unit-testable offline.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any

from backend.services.version_control.contracts import (
    Comparison,
    DiffItem,
    Fingerprints,
    Snapshot,
    canonical_json_hash,
    to_wire,
)

# Recognized as a bare serialized_space object when any of these keys is present.
_SERIALIZED_SPACE_KEYS = ("data_sources", "instructions", "config", "benchmarks")

# Genie represents these logical strings as ``list[str]`` fragments and may
# split the same value at different boundaries on subsequent GETs. Their
# contract is concatenation, unlike fields such as ``synonyms`` and
# ``default_value.values`` where each array element is independently
# meaningful.
_FRAGMENT_ARRAY_KEYS = frozenset({"content", "sql"})

_VC_COLLECTION_KEYS = {
    "sources": "identifier",
    "tables": "identifier",
    "metric_views": "identifier",
    "column_configs": "column_name",
    **dict.fromkeys(
        (
            "text_instructions",
            "example_question_sqls",
            "join_specs",
            "joins",
            "filters",
            "expressions",
            "measures",
            "parameters",
            "questions",
            "sample_questions",
            "sql_functions",
        ),
        "id",
    ),
}

# Genie also canonicalizes unquoted literals/phrases by adding boundary single
# quotes (for example ``Segment = Enterprise`` -> ``Segment = 'Enterprise'``
# and ``Highest churn risk`` -> ``'Highest churn risk'``). Ignore only quotes
# that are not between two alphanumeric characters. Apostrophes inside words
# such as ``customer's`` and ``O'Reilly`` remain significant.
_BOUNDARY_SINGLE_QUOTE_RE = re.compile(r"(?<![A-Za-z0-9])'|'(?![A-Za-z0-9])")


class Canonicalizer:
    """VC/1.0 adapter extending the legacy fingerprint entry point."""

    def __init__(self, *, dual_compute_version: str | None = None) -> None:
        self._versions = {"vc-c14n/1": _canonical_state_v1}
        if (
            dual_compute_version is not None
            and dual_compute_version not in self._versions
        ):
            raise ValueError("Unsupported dual-compute canonicalizer version")
        self._dual_compute_version = dual_compute_version

    def observe(self, envelope: dict, version: str = "vc-c14n/1") -> Snapshot:
        if version not in self._versions:
            raise ValueError(f"Unsupported canonicalizer version: {version}")
        if not isinstance(envelope, dict):
            raise ValueError("envelope must be a JSON object")
        response = deepcopy(envelope)
        serialized = _extract_restorable_space(response)
        metadata = {}
        if "description" in response:
            metadata["description"] = response["description"]
        if (
            "description" in metadata
            and metadata["description"] is not None
            and not isinstance(metadata["description"], str)
        ):
            raise ValueError("description must be a string or null")
        canonical = self._versions[version](serialized, metadata)
        fingerprints = Fingerprints(
            config=canonical_json_hash(
                "vc-config/1", {"value": canonical["config"]}
            ),
            benchmark=canonical_json_hash(
                "vc-benchmark/1", {"value": canonical["benchmark"]}
            ),
            metadata=canonical_json_hash(
                "vc-metadata/1", {"value": canonical["metadata"]}
            ),
            canonicalizer_version=version,
        )
        return Snapshot(
            response_envelope=response,
            serialized_space=serialized,
            restorable_metadata=metadata,
            response_envelope_digest=canonical_json_hash(
                "vc-envelope/1", {"envelope": response}
            ),
            raw_state_digest=canonical_json_hash(
                "vc-raw-state/1",
                {"serialized_space": serialized, "metadata": metadata},
            ),
            canonical_state=canonical,
            fingerprints=fingerprints,
            state_digest=fingerprints.state_digest,
        )

    def compare(self, left: Snapshot, right: Snapshot) -> Comparison:
        pair = self._comparable_pair(left, right)
        if pair is None:
            return Comparison.UNKNOWN
        left, right = pair
        return (
            Comparison.EQUAL
            if left.fingerprints == right.fingerprints
            else Comparison.DIFFERENT
        )

    def semantic_diff(self, left: Snapshot, right: Snapshot) -> list[DiffItem]:
        # Local import breaks the config_fingerprint ↔ canonical_diff cycle.
        from backend.services.version_control.canonical_diff import semantic_diff

        pair = self._comparable_pair(left, right)
        if pair is None:
            raise ValueError(
                "Cannot diff incompatible canonicalizer versions without dual compute"
            )
        return semantic_diff(*pair)

    def _comparable_pair(
        self, left: Snapshot, right: Snapshot
    ) -> tuple[Snapshot, Snapshot] | None:
        left_version = left.fingerprints.canonicalizer_version
        right_version = right.fingerprints.canonicalizer_version
        if left_version == right_version and left_version in self._versions:
            return left, right
        if self._dual_compute_version is None:
            return None
        try:
            for snapshot in (left, right):
                if (
                    canonical_json_hash(
                        "vc-envelope/1", {"envelope": snapshot.response_envelope}
                    )
                    != snapshot.response_envelope_digest
                ):
                    return None
            return (
                self.observe(
                    to_wire(left.response_envelope), self._dual_compute_version
                ),
                self.observe(
                    to_wire(right.response_envelope), self._dual_compute_version
                ),
            )
        except (ValueError, TypeError):
            return None


def _canonical_state_v1(serialized: dict, metadata: dict) -> dict:
    return {
        "config": _canonical_config(serialized),
        "benchmark": _canonical_benchmark(serialized),
        "metadata": metadata,
    }


def _extract_restorable_space(response: dict) -> dict:
    """Validate the payload without stripping keys needed for exact restoration.

    Prefer serialized_space over the optimizer-mutated _parsed_space copy;
    internal keys are removed only later, when computing the config hash.
    """
    serialized = response.get(
        "serialized_space", response.get("_parsed_space", response)
    )
    if isinstance(serialized, str):
        try:
            serialized = json.loads(serialized)
        except ValueError as error:
            raise ValueError("serialized_space must contain valid JSON") from error
    if not isinstance(serialized, dict) or not any(
        key in serialized for key in _SERIALIZED_SPACE_KEYS
    ):
        raise ValueError(
            "serialized_space must contain a recognized payload section"
        )
    if "version" in serialized and (
        type(serialized["version"]) is not int or serialized["version"] < 1
    ):
        raise ValueError("serialized_space version must be a positive integer")
    for key in _SERIALIZED_SPACE_KEYS:
        if key in serialized and not isinstance(serialized[key], dict):
            raise ValueError(f"serialized_space {key} must be an object")
    _validate_collections(serialized)
    return serialized


def _validate_collections(node: Any) -> None:
    """Reject malformed addressed collections rather than hashing false empties."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key in _VC_COLLECTION_KEYS:
                if not isinstance(value, list) or not all(
                    isinstance(entry, dict) for entry in value
                ):
                    raise ValueError(
                        f"serialized_space {key} must be an array of objects"
                    )
            _validate_collections(value)
    elif isinstance(node, list):
        for value in node:
            _validate_collections(value)


def _canonical_benchmark(serialized: dict) -> dict:
    """Unify empty benchmark shapes while preserving ground-truth SQL quotes."""
    benchmarks = serialized.get("benchmarks") or {}
    return canonicalize(
        {**benchmarks, "questions": benchmarks.get("questions") or []},
        normalize_boundary_quotes=False,
        _collection_keys=_VC_COLLECTION_KEYS,
    )


def _canonical_config(serialized: dict) -> dict:
    """Hash only recoverable config semantics, leaving restoration data intact.

    Genie flattens metric views into tables without preserving their kind.
    Always emit one sources collection, even when empty, without guessing
    kind from identifier prefixes. Drop METRIC_VIEW markers on either input.
    Accept sources itself so canonical observations remain idempotent.

    Duplicate identifiers merge fields, with later entries winning conflicts
    (sources, then tables, then metric_views); unaddressed entries are retained.
    """
    config = _strip_internal_keys(deepcopy(_with_default_version(serialized)))
    sources = config.setdefault("data_sources", {})
    entries = [
        entry
        for key in ("sources", "tables", "metric_views")
        for entry in sources.pop(key, [])
    ]
    by_identifier: dict[str, dict] = {}
    unaddressed = []
    for entry in entries:
        entry = {
            key: value
            for key, value in entry.items()
            if not (
                key in ("table_type", "type", "object_type")
                and "METRIC_VIEW" in str(value).upper()
            )
        }
        identifier = entry.get("identifier")
        if isinstance(identifier, str):
            by_identifier.setdefault(identifier, {}).update(entry)
        else:
            unaddressed.append(entry)
    sources["sources"] = [
        *(by_identifier[key] for key in sorted(by_identifier)),
        *unaddressed,
    ]
    return canonicalize(config, _collection_keys=_VC_COLLECTION_KEYS)


def unwrap_serialized_space(config: Any) -> dict | None:
    """Reduce any stored/live config shape to the parsed serialized_space object.

    Shapes seen in the wild:

    * full Genie GET response — ``{"serialized_space": "<json string>" | {...}}``;
    * ``{"_parsed_space": {...}}`` — GSO's fetch stashes a parsed copy under
      this key (``common/genie_client.py``);
    * bare serialized_space dict — champion submitted/observed JSON rows and
      already-parsed live configs.

    The pristine ``serialized_space`` payload is preferred over
    ``_parsed_space`` when both exist: preflight mutates the parsed copy in
    place (injecting ``_data_profile``) before persisting the snapshot, so the
    parsed copy can diverge from what the Genie API round-trips. Internal
    ``_``-prefixed top-level keys are stripped from every source, so snapshots
    that only retain the mutated copy still match.

    Legacy projected rows that dropped the required top-level ``version`` get
    the current documented default (mirrors GSO's revert path so a legacy
    snapshot can still match the live config it produced). Returns ``None``
    when no recognizable serialized_space object is present — callers treat
    that as "not matchable", never as drift.
    """
    if not isinstance(config, dict) or not config:
        return None

    serialized = config.get("serialized_space")
    if isinstance(serialized, str) and serialized.strip():
        try:
            serialized = json.loads(serialized)
        except (json.JSONDecodeError, TypeError):
            # Legacy SQL-corrupted snapshots (unescaped nested JSON) — fall
            # through to _parsed_space if present; otherwise not matchable.
            # Fail-open either way: no false badge, no false drift.
            serialized = None
    if isinstance(serialized, dict) and serialized:
        return _strip_internal_keys(_with_default_version(serialized))

    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict) and parsed:
        return _strip_internal_keys(_with_default_version(parsed))

    if any(key in config for key in _SERIALIZED_SPACE_KEYS):
        return _strip_internal_keys(_with_default_version(config))
    return None


def _strip_internal_keys(space: dict) -> dict:
    """Drop internal ``_``-prefixed top-level keys injected by the optimizer.

    Preflight persists snapshots after in-place injections such as
    ``_data_profile`` (and similar bookkeeping fields). Those keys never
    appear in the live Genie config, so hashing them would report drift on an
    unchanged agent. The Genie ``serialized_space`` schema has no
    underscore-prefixed top-level fields, so this is semantics-preserving.
    """
    return {key: value for key, value in space.items() if not key.startswith("_")}


def _with_default_version(config: dict) -> dict:
    """Backfill ``version`` for legacy projected rows (same rule as GSO revert)."""
    if "version" in config:
        return config
    return {"version": 2, **config}


def _normalize_representation_string(value: str) -> str:
    """Remove quote punctuation Genie may add during API normalization."""
    return _BOUNDARY_SINGLE_QUOTE_RE.sub("", value)


def canonicalize(
    node: Any,
    *,
    normalize_boundary_quotes: bool = True,
    _depth: int = 0,
    _parent_key: str | None = None,
    _collection_keys: dict[str, str] | None = None,
) -> Any:
    """Recursively canonicalize a parsed serialized_space object.

    * top-level ``benchmarks`` dropped (evaluation state, not configuration —
      see module docstring);
    * dict keys sorted (``json.dumps(sort_keys=True)`` would do this too, but
      explicit sorting keeps the structure inspectable for debugging);
    * ``content`` / ``sql`` fragment arrays are concatenated;
    * Genie-added boundary single quotes are optionally ignored while
      apostrophes inside words remain significant; benchmark ground-truth SQL
      disables this because SQL quote punctuation changes semantics;
    * the legacy ``_collection_keys=None`` path retains blanket string-id
      array sorting, frozen for compatibility with persisted fingerprints;
    * the VC path sorts only collections declared in ``_collection_keys`` by
      their identity field (t4), never arbitrary arrays containing id fields;
    * the VC uniqueness guard preserves duplicate-id array input order rather
      than assuming stable addressing; legacy sorting remains unchanged;
    * everything else (scalars and non-id array order) is preserved — those
      are meaningful configuration.
    """
    if isinstance(node, dict):
        items = {
            key: canonicalize(
                value,
                normalize_boundary_quotes=normalize_boundary_quotes,
                _depth=_depth + 1,
                _parent_key=key,
                _collection_keys=_collection_keys,
            )
            for key, value in node.items()
            if not (_depth == 0 and key == "benchmarks")
        }
        return dict(sorted(items.items()))
    if isinstance(node, list):
        if (
            node
            and _parent_key in _FRAGMENT_ARRAY_KEYS
            and all(isinstance(value, str) for value in node)
        ):
            joined = "".join(node)
            return (
                _normalize_representation_string(joined)
                if normalize_boundary_quotes
                else joined
            )
        items = [
            canonicalize(
                value,
                normalize_boundary_quotes=normalize_boundary_quotes,
                _depth=_depth + 1,
                _parent_key=_parent_key,
                _collection_keys=_collection_keys,
            )
            for value in node
        ]
        identity_key = (
            "id" if _collection_keys is None else _collection_keys.get(_parent_key)
        )
        if (
            identity_key
            and items
            and all(
                isinstance(value, dict)
                and isinstance(value.get(identity_key), str)
                for value in items
            )
            and (
                _collection_keys is None
                or len({value[identity_key] for value in items}) == len(items)
            )
        ):
            items.sort(key=lambda value: value[identity_key])
        return items
    if isinstance(node, str):
        return (
            _normalize_representation_string(node)
            if normalize_boundary_quotes
            else node
        )
    return node


def config_fingerprint(config: Any) -> str | None:
    """SHA-256 fingerprint of a stored/live Genie Agent config.

    Accepts any of the shapes handled by :func:`unwrap_serialized_space`
    (including raw JSON strings returned by Delta reads). Returns ``None``
    when the value cannot be reduced to a serialized_space object.
    """
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            return None
    space = unwrap_serialized_space(config)
    if space is None:
        return None
    return _hash_canonical(canonicalize(space))


def benchmark_fingerprint(config: Any) -> str | None:
    """SHA-256 fingerprint of a stored/live Genie benchmark block.

    The official ``serialized_space`` schema makes ``benchmarks`` optional, so
    an omitted block, an empty block, and ``questions=[]`` all represent the
    same empty benchmark state. Malformed benchmark shapes are not matchable and
    return ``None`` so callers fail open instead of reporting false drift.
    """
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            return None
    space = unwrap_serialized_space(config)
    if space is None:
        return None

    benchmarks = space.get("benchmarks")
    if benchmarks is None:
        benchmarks = {}
    if not isinstance(benchmarks, dict):
        return None
    questions = benchmarks.get("questions")
    if questions is None:
        questions = []
    if not isinstance(questions, list):
        return None

    canonical = canonicalize(
        {**benchmarks, "questions": questions},
        normalize_boundary_quotes=False,
    )
    return _hash_canonical(canonical)


def _hash_canonical(canonical: Any) -> str:
    """Hash one already-canonicalized config component."""
    payload = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
