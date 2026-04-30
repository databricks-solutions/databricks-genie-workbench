"""Regression guard for customer-name leaks in LLM prompts.

The optimizer has historically baked customer-specific identifiers
(``7NOW``, ``ESR``, ``PSD``, ``same_store_7now``, ``mv_7now_*``,
``mv_esr_*``) into prompt templates as canonical examples. The LLM
literally reads these and pattern-matches against them, biasing
strategist / arbiter / synthesis decisions toward retail same-store
reasoning.

This test scans every module-level string constant in the prompt-
authoring modules and asserts none of those tokens appear. If you
need to add a *new* example to a prompt, choose a placeholder
(``<domain>``, ``mv_<domain>_<entity>``) or a pair of contrasting
industry examples (e.g. retail orders + financial-services claims) so
the LLM does not anchor on a single industry.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

# Tokens that must not appear anywhere in module-level prompt strings.
# Case-insensitive substring match. Each entry is the literal token to
# search for; a match means the prompt has reverted to a customer-
# specific example and should be rewritten to a placeholder.
_BANNED_TOKENS: tuple[str, ...] = (
    "7now",
    "same_store_7now",
    "mv_7now",
    "mv_esr",
)

# Stricter whole-word tokens (case-sensitive): these are short enough
# that a substring match would be noisy (e.g. ``ESR`` lives inside
# ``USERS``), so we use a word-boundary regex instead.
_BANNED_WORD_TOKENS: tuple[str, ...] = (
    "ESR",
    "PSD",
)


def _iter_string_constants(module) -> Iterable[tuple[str, str]]:
    """Yield ``(attribute_name, value)`` for every public string-typed
    module-level attribute. Skips dunder names and non-strings.
    """
    for name in dir(module):
        if name.startswith("_"):
            continue
        try:
            value = getattr(module, name)
        except Exception:
            continue
        if isinstance(value, str):
            yield name, value


def _check_module_for_banned_tokens(module) -> list[str]:
    """Return a list of human-readable violations found in ``module``."""
    violations: list[str] = []
    word_patterns = {
        token: re.compile(rf"\b{re.escape(token)}\b")
        for token in _BANNED_WORD_TOKENS
    }
    for attr, value in _iter_string_constants(module):
        lowered = value.lower()
        for token in _BANNED_TOKENS:
            if token in lowered:
                violations.append(
                    f"{module.__name__}.{attr}: contains banned token "
                    f"{token!r}"
                )
        for token, pattern in word_patterns.items():
            if pattern.search(value):
                violations.append(
                    f"{module.__name__}.{attr}: contains banned word "
                    f"{token!r}"
                )
    return violations


def test_common_config_prompts_have_no_customer_tokens() -> None:
    """``common/config.py`` is the largest prompt registry — it must be
    customer-agnostic.
    """
    from genie_space_optimizer.common import config

    violations = _check_module_for_banned_tokens(config)
    assert violations == [], (
        "Customer-specific tokens leaked back into common/config.py "
        "prompt strings. Replace with placeholders (<domain>) or "
        "paired-industry examples. Violations:\n" + "\n".join(violations)
    )


def test_arbiter_module_has_no_customer_tokens() -> None:
    """The arbiter judge sees instruction context plus its own prompt
    template — both must avoid customer-specific examples.
    """
    from genie_space_optimizer.optimization.scorers import arbiter

    violations = _check_module_for_banned_tokens(arbiter)
    assert violations == [], (
        "Customer-specific tokens leaked back into the arbiter scorer. "
        "Violations:\n" + "\n".join(violations)
    )
