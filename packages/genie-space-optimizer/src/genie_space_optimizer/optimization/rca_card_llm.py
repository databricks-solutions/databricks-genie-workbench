"""Phase 1 Action 1.1 — optional LLM-based rationale normalization
for the deterministic RCA card builder.

The normalizer takes a fully-built deterministic :class:`RCACard` and
optionally rewrites *only* its ``rationale`` field into more readable
English. The contract is intentionally narrow:

* All other fields (``root_cause``, ``grounding_terms``,
  ``allowed_patch_families``, ``forbidden_patch_families``,
  ``intended_patch_shape``, ``card_id``, ``cluster_id``, ``qids``)
  are NEVER modified by this layer.
* On any failure (timeout, exception, empty response, response that
  exceeds ``max_chars``), the deterministic rationale is preserved
  and ``NormalizationOutcome.skipped=True`` is returned with a typed
  ``skip_reason``.
* The caller (``optimization.rca.build_rca_card``) emits the
  ``RCA_CARD_LLM_SKIPPED`` decision record when ``skipped=True``.

The ``llm_caller`` argument is dependency-injected so unit tests can
exercise every branch without the real LLM. Production wires
``optimization.llm_client.call_llm``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable

from genie_space_optimizer.optimization.rca import RCACard


_DEFAULT_MAX_CHARS = 1024


@dataclass(frozen=True)
class NormalizationOutcome:
    """Result of attempting to normalize a card's rationale."""

    card: RCACard
    skipped: bool
    skip_reason: str | None = None


def normalize_card_rationale(
    *,
    card: RCACard,
    llm_caller: Callable[[str], str],
    max_chars: int = _DEFAULT_MAX_CHARS,
) -> NormalizationOutcome:
    """Attempt to rewrite ``card.rationale`` into readable English.

    The prompt is built deterministically from the card; the
    ``llm_caller`` receives a single string prompt and must return a
    string response. Any deviation from contract triggers a typed
    skip:

      * raises any exception → skipped, ``llm_call_failed``
      * returns empty/whitespace → skipped, ``empty_response``
      * returns > ``max_chars`` characters → skipped, ``response_too_long``

    On success, returns a new ``RCACard`` with only ``rationale``
    replaced; all other fields are preserved verbatim.
    """
    prompt = _build_prompt(card)

    try:
        response = llm_caller(prompt)
    except Exception:  # noqa: BLE001 — broad on purpose; any LLM failure is a skip
        return NormalizationOutcome(card=card, skipped=True, skip_reason="llm_call_failed")

    if not isinstance(response, str) or not response.strip():
        return NormalizationOutcome(card=card, skipped=True, skip_reason="empty_response")

    cleaned = response.strip()
    if len(cleaned) > int(max_chars):
        return NormalizationOutcome(card=card, skipped=True, skip_reason="response_too_long")

    new_card = replace(card, rationale=cleaned)
    return NormalizationOutcome(card=new_card, skipped=False)


def _build_prompt(card: RCACard) -> str:
    """Build the deterministic LLM prompt — pure function of the card."""
    return (
        "You are rewriting a one-paragraph technical rationale for an "
        "RCA card.\n"
        f"Root cause: {card.root_cause.value}\n"
        f"Cluster QIDs: {', '.join(card.qids) or '(none)'}\n"
        f"Grounding terms: {', '.join(sorted(card.grounding_terms)) or '(none)'}\n"
        f"Intended patch shape: {card.intended_patch_shape}\n"
        f"Deterministic rationale: {card.rationale}\n\n"
        "Rewrite the rationale in 2 sentences max, plain English, no "
        "marketing tone, no new facts. Output the rationale text only."
    )
