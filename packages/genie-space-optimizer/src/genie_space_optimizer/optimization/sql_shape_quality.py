"""SQL-shape patch quality classifiers — Track 5 (Phase A burn-down).

Three predicates plus one promotion helper. The predicates take a
patch dict and return ``True`` when the patch matches the anti-pattern.
``proposal_grounding`` calls them to demote weak patches before the
cap budget consumes them.

Each predicate is intentionally narrow — patches that the predicate
does not match pass through unchanged. The justification fields on a
patch (``justification``, ``metric_native_currency``,
``question_requested_currency``, ``question_requests_exact_top_n``)
allow a producer to opt out by stating its intent explicitly.
"""

from __future__ import annotations

import re
from typing import Any

# Tautological predicates: ``X IS NOT NULL OR X IS NULL`` always
# evaluates to TRUE except when the underlying value is itself NULL,
# which IS-NOT-NULL excluded. The combined form is a no-op filter.
_TAUTOLOGY_RE = re.compile(
    r"\b(\w+)\s+IS\s+NOT\s+NULL\s+OR\s+\1\s+IS\s+NULL\b",
    re.IGNORECASE,
)
_BARE_IS_NOT_NULL_RE = re.compile(
    r"\b\w+\s+IS\s+NOT\s+NULL\b",
    re.IGNORECASE,
)
_RANK_FN_RE = re.compile(r"\bRANK\s*\(\s*\)", re.IGNORECASE)
_ROW_NUMBER_FN_RE = re.compile(r"\bROW_NUMBER\s*\(\s*\)", re.IGNORECASE)
_LIMIT_N_RE = re.compile(r"\bLIMIT\s+\d+\b", re.IGNORECASE)


def _snippet_text(patch: dict[str, Any]) -> str:
    return str(
        patch.get("snippet")
        or patch.get("new_text")
        or patch.get("value")
        or ""
    )


def is_unrequested_is_not_null_filter(patch: dict[str, Any]) -> bool:
    """Return True when the patch adds an unrequested IS-NOT-NULL filter.

    Two cases trigger:

      1. Tautological ``X IS NOT NULL OR X IS NULL`` — always weak.
      2. Bare ``X IS NOT NULL`` with no ``justification`` field that
         explains why the question's intent demands excluding NULLs.

    Patches with a non-empty ``justification`` field naming the
    question intent pass through unchanged.
    """
    text = _snippet_text(patch)
    if not text:
        return False
    if _TAUTOLOGY_RE.search(text):
        return True
    if _BARE_IS_NOT_NULL_RE.search(text):
        justification = str(patch.get("justification") or "").strip()
        if not justification:
            return True
    return False


def is_unrequested_currency_filter(patch: dict[str, Any]) -> bool:
    """Return True when the patch filters on currency that already
    matches the question's requested currency.

    Producers must stamp ``metric_native_currency`` and
    ``question_requested_currency`` on the patch when proposing
    currency-related fixes; without those fields, the predicate
    returns ``False`` (it cannot prove the filter is unrequested).
    """
    text = _snippet_text(patch).upper()
    if "CURRENCY" not in text:
        return False
    native = str(patch.get("metric_native_currency") or "").strip().upper()
    requested = str(patch.get("question_requested_currency") or "").strip().upper()
    if not native or not requested:
        return False
    return native == requested


def is_rank_when_limit_n_required(patch: dict[str, Any]) -> bool:
    """Return True when the patch uses ``RANK()`` for exact-top-N
    semantics where the canonical shape is ``LIMIT N`` or
    ``ROW_NUMBER`` + ``WHERE rn <= N``.

    Triggers only when the patch explicitly stamps
    ``question_requests_exact_top_n=True`` (the producer asserted the
    question's intent). The fallback for ambiguous cases is to keep
    the patch — Track 5's quality bar is conservative.
    """
    if not patch.get("question_requests_exact_top_n"):
        return False
    text = _snippet_text(patch)
    if not text:
        return False
    if _RANK_FN_RE.search(text):
        # If the patch uses RANK() AND already has LIMIT N or
        # ROW_NUMBER, treat it as compound — the RANK call is harmless
        # extra information. Only flag pure-RANK shapes.
        if _LIMIT_N_RE.search(text) or _ROW_NUMBER_FN_RE.search(text):
            return False
        return True
    return False


def prefer_scoped_instruction_over_weak_snippet(
    snippet_patch: dict[str, Any],
    candidate_instruction_patches: list[dict[str, Any]],
) -> bool:
    """Return True when a scoped instruction patch should replace a
    weak SQL snippet.

    A snippet is "weak" when at least one quality predicate flags it.
    A scoped instruction patch in ``candidate_instruction_patches`` is
    a viable replacement when:

      * its ``patch_type`` is ``add_instruction`` or
        ``update_instruction_section``,
      * its ``root_cause`` matches the snippet's ``root_cause``,
      * AND its ``target_qids`` cover the snippet's ``target_qids``.

    Callers (proposal_grounding) demote the snippet only when the
    function returns ``True`` — i.e., a real replacement exists.
    """
    if not (
        is_unrequested_is_not_null_filter(snippet_patch)
        or is_unrequested_currency_filter(snippet_patch)
        or is_rank_when_limit_n_required(snippet_patch)
    ):
        return False

    snippet_root = str(snippet_patch.get("root_cause") or "").strip()
    snippet_qids = {
        str(q) for q in (snippet_patch.get("target_qids") or []) if str(q)
    }

    for ip in candidate_instruction_patches or []:
        ip_type = str(ip.get("type") or ip.get("patch_type") or "")
        if ip_type not in {"add_instruction", "update_instruction_section"}:
            continue
        if str(ip.get("root_cause") or "").strip() != snippet_root:
            continue
        ip_qids = {str(q) for q in (ip.get("target_qids") or []) if str(q)}
        if snippet_qids and snippet_qids <= ip_qids:
            return True
    return False
