"""Output contract for the reference smoke-test skill.

Trivial 1-field contract. Plans 3-7 copy this file's shape (Pydantic
BaseModel subclassing prompt_io.LLMOutputContract) for their own
output schemas.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class ReferenceSmokeTestOutput(LLMOutputContract):
    """Echoes the input value back. Used only to pin the framework
    layout — never invoked by real optimizer code paths."""

    echoed: str
