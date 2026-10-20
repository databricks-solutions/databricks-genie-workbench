"""Verify Unity Gateway (AI Gateway v3) is usable for the configured LLM model.

Sends a tiny chat request (and an embeddings request) as the deploying user.
Exits non-zero if the default chat model cannot be queried.

Usage:
    python scripts/check_ai_gateway.py --profile DEFAULT --model system.ai.claude-sonnet-4-6
"""

import argparse
import os
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_GSO_SRC = os.path.join(_SCRIPT_DIR, os.pardir, "packages", "genie-space-optimizer", "src")
if os.path.isdir(_GSO_SRC) and _GSO_SRC not in sys.path:
    sys.path.insert(0, os.path.abspath(_GSO_SRC))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from databricks.sdk import WorkspaceClient  # noqa: E402

from deploy_lib.ai_gateway import run_ai_gateway_preflight  # noqa: E402
from genie_space_optimizer.common.ai_gateway import DEFAULT_EMBEDDING_MODEL  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    args = parser.parse_args()

    report = run_ai_gateway_preflight(
        WorkspaceClient(profile=args.profile), args.model, args.embedding_model,
    )
    if report.chat.ok:
        print(f"  ✓ Unity Gateway chat probe passed ({report.chat.model})")
    if report.embedding.ok:
        print(f"  ✓ Unity Gateway embedding probe passed ({report.embedding.model})")
    for warning in report.warnings:
        print(f"  ⚠ {warning}")
    for error in report.errors:
        print(f"  ✗ {error}", file=sys.stderr)
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
