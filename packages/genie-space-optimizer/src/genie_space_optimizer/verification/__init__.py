"""Verification tools — postmortem parsers and chain-invariant checks.

Public API:
    AnchorChainVerifier — orchestrates postmortem + transcript parsing.
    VerifierResult     — per-anchor + global verdicts; serializable.
    LifecyclePath      — A | B | C | UNKNOWN.
    verify_runid_dir   — convenience entry point used by the CLI.
"""
from genie_space_optimizer.verification.anchor_chain import (
    AnchorChainVerifier,
    AnchorVerdict,
    LifecyclePath,
    VerifierResult,
    verify_runid_dir,
)

__all__ = [
    "AnchorChainVerifier",
    "AnchorVerdict",
    "LifecyclePath",
    "VerifierResult",
    "verify_runid_dir",
]
