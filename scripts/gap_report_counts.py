"""Generate the gap report's package-layout block and audit its ``(N L)`` claims.

Four of nine hand-maintained line counts in ``docs/design/mv-advisor-gap-report.md``
drifted during a single prompt, and MV-D9 turns a stale count into a hard stop. The
counts are mechanical, so they should not be typed by hand; the *curation* — which
modules are worth naming, in which grouping, in what order — is editorial and stays
in the ``LAYOUT`` template below.

So this script fills numbers into a template rather than inventing the block:

    python scripts/gap_report_counts.py            # print the generated block
    python scripts/gap_report_counts.py --check     # non-zero if the report is stale
    python scripts/gap_report_counts.py --write     # rewrite the block in place

``--check`` also audits every ``(N L)`` claim *outside* the generated block, since
those live in prose and tables the template does not own. It does not replace the
MV-D9 review — byte-matching fenced quotes and re-reading anchors is still manual,
because a quote can go stale by position with its content unchanged. It removes the
arithmetic, which is the part a human was never adding value on.

Counts are ``len(text.splitlines())``, matching ``wc -l`` for the newline-terminated
files in this repository.
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GAP_REPORT = REPO_ROOT / "docs" / "design" / "mv-advisor-gap-report.md"

BEGIN_MARKER = "<!-- BEGIN GENERATED: package-layout (scripts/gap_report_counts.py) -->"
END_MARKER = "<!-- END GENERATED: package-layout -->"

GSO = "packages/genie-space-optimizer/src/genie_space_optimizer"

# The curated block. ``{path}`` is replaced by that file's live line count as
# ``N L``; everything else is emitted verbatim, so the shape of the block stays an
# editorial decision. Adding a module here is how it starts being counted.
LAYOUT = f"""```text
src/genie_space_optimizer/
  _telemetry.py, _version.py, _workspace_client.py
  backend/        job_launcher.py, utils.py          # shared with Workbench
  common/         config.py ({{{GSO}/common/config.py}}), genie_client.py, metric_view_catalog.py,
                  asset_semantics.py, delta_helpers.py, warehouse.py, uc_metadata.py, ...
  integration/    trigger.py, apply.py, discard.py, revert.py, levers.py, types.py
  iq_scan/        scoring.py, context.py, rls_audit.py
  jobs/           the four notebooks + _helpers.py
  optimization/   applier.py ({{{GSO}/optimization/applier.py}}), \
benchmarking.py ({{{GSO}/optimization/benchmarking.py}}), \
unified_loop.py ({{{GSO}/optimization/unified_loop.py}}),
                  preflight.py ({{{GSO}/optimization/preflight.py}}), \
state.py ({{{GSO}/optimization/state.py}}), publish.py, ddl.py,
                  eval_runner.py, leakage.py, models.py, champion.py,
                  wide_schema*.py, genie_eval_taxonomy.py,
                  mv_fingerprint.py ({{{GSO}/optimization/mv_fingerprint.py}}), \
mv_scoring.py ({{{GSO}/optimization/mv_scoring.py}}),
                  mv_state.py ({{{GSO}/optimization/mv_state.py}}), \
mv_yaml.py ({{{GSO}/optimization/mv_yaml.py}}), ...
```"""

_PLACEHOLDER_RE = re.compile(r"\{([\w./-]+\.py)\}")
_CLAIM_RE = re.compile(r"`?([\w/]+\.py)`?\s*\((\d+) L\)")


def line_count(rel_path: str) -> int:
    path = REPO_ROOT / rel_path
    if not path.is_file():
        raise SystemExit(f"gap_report_counts: no such file: {rel_path}")
    return len(path.read_text(encoding="utf-8").splitlines())


def render_block() -> str:
    return _PLACEHOLDER_RE.sub(lambda m: f"{line_count(m.group(1))} L", LAYOUT)


def _split_report(text: str) -> tuple[str, str, str]:
    if BEGIN_MARKER not in text or END_MARKER not in text:
        raise SystemExit(
            "gap_report_counts: the generated-block markers are missing from "
            f"{GAP_REPORT.relative_to(REPO_ROOT)}. Add:\n{BEGIN_MARKER}\n...\n{END_MARKER}"
        )
    head, rest = text.split(BEGIN_MARKER, 1)
    body, tail = rest.split(END_MARKER, 1)
    return head, body, tail


def audit_other_claims(text: str) -> list[str]:
    """Check ``(N L)`` claims outside the generated block against live files.

    Resolves a bare filename only when exactly one tracked file matches it, so an
    ambiguous name is reported rather than silently checked against the wrong file.
    """
    by_name: dict[str, list[Path]] = {}
    for path in REPO_ROOT.rglob("*.py"):
        if any(part in {".venv", "node_modules", "build", "dist", ".git"} for part in path.parts):
            continue
        by_name.setdefault(path.name, []).append(path)

    findings: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for name, claim in _CLAIM_RE.findall(line):
            candidates = [
                p for p in by_name.get(name.rsplit("/", 1)[-1], [])
                if str(p.relative_to(REPO_ROOT)).endswith(name)
            ]
            if len(candidates) != 1:
                findings.append(
                    f"line {lineno}: '{name}' resolves to {len(candidates)} files — "
                    "qualify the path in the report"
                )
                continue
            live = len(candidates[0].read_text(encoding="utf-8").splitlines())
            if live != int(claim):
                rel = candidates[0].relative_to(REPO_ROOT)
                findings.append(f"line {lineno}: {rel} claims {claim} L, live is {live} L")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="fail if the report is stale")
    group.add_argument("--write", action="store_true", help="rewrite the block in place")
    args = parser.parse_args()

    block = render_block()

    if not (args.check or args.write):
        print(block)
        return 0

    text = GAP_REPORT.read_text(encoding="utf-8")
    head, body, tail = _split_report(text)
    current = body.strip("\n")

    if args.write:
        GAP_REPORT.write_text(
            f"{head}{BEGIN_MARKER}\n{block}\n{END_MARKER}{tail}", encoding="utf-8"
        )
        print(f"wrote {GAP_REPORT.relative_to(REPO_ROOT)}")
        return 0

    problems: list[str] = []
    if current != block:
        diff = difflib.unified_diff(
            current.splitlines(), block.splitlines(),
            fromfile="gap report (committed)", tofile="live counts", lineterm="",
        )
        problems.append(
            "the package-layout block is stale — run "
            "`python scripts/gap_report_counts.py --write`:\n" + "\n".join(diff)
        )

    outside = f"{head}{tail}"
    problems.extend(audit_other_claims(outside))

    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    print("gap report line counts are current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
