"""The two rules copies must stay byte-identical.

``.cursor/rules/mv-advisor.mdc`` is the operative copy Cursor loads; the fenced
block in ``docs/design/mv-advisor-playbook.md`` is its documented mirror. The
rules file itself states "RULES COPIES: exactly two exist ... and they are kept
byte-identical", and nothing enforced it — so the two drifted (the playbook block
lagged three rules). This pins them.

Failure here is a copy-paste fix: re-sync the playbook's fenced FEATURE RULES
block to ``.cursor/rules/mv-advisor.mdc`` (the ``.mdc`` is authoritative),
prefixing every line with three spaces. Do NOT weaken either copy to pass.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
MDC = REPO_ROOT / ".cursor" / "rules" / "mv-advisor.mdc"
PLAYBOOK = REPO_ROOT / "docs" / "design" / "mv-advisor-playbook.md"

# The fenced block indents the whole rules copy by exactly three spaces.
INDENT = "   "


def _fenced_rules_block(markdown: str) -> str:
    """Return the single fenced block containing 'FEATURE RULES', de-indented.

    A fence boundary is any line whose stripped form starts with ``` so a
    language-tagged opener (```` ```bash ````) pairs with its bare ``` close.
    The block of interest is the only one whose body mentions FEATURE RULES.
    The uniform three-space indent is stripped so the result can be compared
    byte-for-byte against the ``.mdc``.
    """
    blocks: list[list[str]] = []
    body: list[str] | None = None
    for line in markdown.splitlines():
        if line.strip().startswith("```"):
            if body is None:
                body = []
            else:
                blocks.append(body)
                body = None
            continue
        if body is not None:
            body.append(line)

    matches = [b for b in blocks if any("FEATURE RULES" in ln for ln in b)]
    assert len(matches) == 1, (
        "expected exactly one fenced block containing 'FEATURE RULES' in "
        f"{PLAYBOOK.name}, found {len(matches)}"
    )
    dedented = [ln[len(INDENT):] if ln.startswith(INDENT) else ln for ln in matches[0]]
    return "\n".join(dedented) + "\n"


def test_playbook_fenced_rules_block_matches_the_operative_mdc() -> None:
    mdc = MDC.read_text(encoding="utf-8")
    block = _fenced_rules_block(PLAYBOOK.read_text(encoding="utf-8"))
    assert block == mdc, (
        "the two rules copies have drifted. `.cursor/rules/mv-advisor.mdc` is the "
        "operative copy Cursor loads; re-sync the fenced FEATURE RULES block in "
        "`docs/design/mv-advisor-playbook.md` to match it byte-for-byte (prefix "
        "every line with three spaces). Do NOT weaken either copy to make this pass."
    )
