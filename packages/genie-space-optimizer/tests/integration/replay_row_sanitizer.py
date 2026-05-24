"""Sanitization helpers + audit constants for the production replay corpus.

The committed corpus in ``tests/integration/fixtures/production_replay/``
must never contain raw customer literals (table names, customer-domain
strings, internal identifiers). This module is the single source of
truth for the substitution rules used when authoring a new case and the
greppable token list the sanitization audit test enforces.

It does NOT mutate any committed case at test time — sanitization is a
one-shot performed when a new case is extracted from
``docs/runid_analysis/<run_id>/evidence/``. The helpers here exist so
that one-shot is deterministic and reviewable.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

# ── Substitution rules ────────────────────────────────────────────────
#
# Add new pairs here whenever a new customer domain enters the corpus.
# These rules apply consistently across qid, question_id, blame_set,
# target_qid, and SQL column references.

DOMAIN_PREFIX_SUBSTITUTIONS: Mapping[str, str] = {
    "airline_ticketing_and_fare_analysis_": "domain_a_",
    "7now_delivery_analytics_space_": "domain_b_",
}


# ── Greppable forbidden-tokens audit ──────────────────────────────────
#
# A committed case MUST NOT contain any of these substrings. The
# audit test ``test_production_replay_corpus_sanitization.py`` greps
# every fixture file for these and fails loudly on a hit. Update this
# list whenever a new customer domain is sanitized.

FORBIDDEN_LITERALS: tuple[str, ...] = (
    "airline_ticketing_and_fare_analysis",
    "7now_delivery_analytics_space",
    "PAYMENT_CURRENCY_CD",
)


def apply_domain_substitutions(text: str) -> str:
    """Apply the registered substitutions to a single string in order.

    Pure / idempotent. Used by both the (offline) author flow and the
    sanitization audit's drift detector.
    """
    out = text
    for needle, replacement in DOMAIN_PREFIX_SUBSTITUTIONS.items():
        out = out.replace(needle, replacement)
    return out


def deep_sanitize(payload: Any) -> Any:
    """Walk a JSON-shaped object applying :func:`apply_domain_substitutions`
    to every string leaf.

    Used by the case-authoring helper script and by the audit drift
    detector. Lists become lists, dicts become dicts; non-string scalars
    pass through unchanged.
    """
    if isinstance(payload, str):
        return apply_domain_substitutions(payload)
    if isinstance(payload, dict):
        return {k: deep_sanitize(v) for k, v in payload.items()}
    if isinstance(payload, list):
        return [deep_sanitize(v) for v in payload]
    if isinstance(payload, tuple):
        return tuple(deep_sanitize(v) for v in payload)
    return payload


_FORBIDDEN_RE = re.compile(
    "|".join(re.escape(token) for token in FORBIDDEN_LITERALS)
)


# Paths whose subtree is exempt from the forbidden-literal audit.
# ``_provenance`` intentionally retains the original run id, source QID,
# and sanitization-notes — postmortem-to-fixture handoff (see SCHEMA.md
# "How to add a new case") relies on engineers being able to trace a
# fixture back to its source artefacts. The audit therefore exempts the
# whole subtree; every other branch of a case file is sanitized.
PROVENANCE_EXEMPT_PREFIXES: frozenset[str] = frozenset({"_provenance"})


def find_forbidden_literals(
    payload: Any,
    *,
    exempt_prefixes: frozenset[str] = PROVENANCE_EXEMPT_PREFIXES,
    _prefix: str = "",
) -> list[tuple[str, str]]:
    """Return every ``(json_pointer_like_path, literal)`` pair where a
    forbidden token is present in a string leaf.

    The path uses dotted notation (``provenance.source_run_id``) to make
    failures actionable in the audit test output. Empty list ⇒ clean.
    Subtrees whose top-level key matches any entry in
    ``exempt_prefixes`` are skipped — by default, ``_provenance`` is
    exempt so source-artifact pointers can name the unsanitized
    run id / QID without tripping the audit.
    """
    findings: list[tuple[str, str]] = []
    if isinstance(payload, str):
        for match in _FORBIDDEN_RE.finditer(payload):
            findings.append((_prefix or "<root>", match.group(0)))
        return findings
    if isinstance(payload, dict):
        for key, value in payload.items():
            child_prefix = f"{_prefix}.{key}" if _prefix else str(key)
            top_level = child_prefix.split(".", 1)[0]
            if top_level in exempt_prefixes:
                continue
            findings.extend(
                find_forbidden_literals(
                    value, exempt_prefixes=exempt_prefixes, _prefix=child_prefix
                )
            )
        return findings
    if isinstance(payload, (list, tuple)):
        for idx, value in enumerate(payload):
            child_prefix = f"{_prefix}[{idx}]"
            findings.extend(
                find_forbidden_literals(
                    value, exempt_prefixes=exempt_prefixes, _prefix=child_prefix
                )
            )
    return findings


def load_case_payload(path: Path) -> dict:
    """Read a committed case file as raw JSON.

    Helper used by the audit test so it can iterate the corpus without
    duplicating path arithmetic.
    """
    return json.loads(path.read_text())


__all__ = [
    "DOMAIN_PREFIX_SUBSTITUTIONS",
    "FORBIDDEN_LITERALS",
    "PROVENANCE_EXEMPT_PREFIXES",
    "apply_domain_substitutions",
    "deep_sanitize",
    "find_forbidden_literals",
    "load_case_payload",
]
