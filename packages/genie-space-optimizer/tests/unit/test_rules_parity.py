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


def _rules_fence(markdown: str) -> tuple[str, str, list[str]]:
    """Return ``(opener, closer, body)`` of the one fence containing 'FEATURE RULES'.

    A fence boundary is any line whose stripped form starts with ``` so a
    language-tagged opener (```` ```bash ````) pairs with its bare ``` close.
    The block of interest is the only one whose body mentions FEATURE RULES.
    """
    blocks: list[tuple[str, str, list[str]]] = []
    opener: str | None = None
    body: list[str] = []
    for line in markdown.splitlines():
        if line.strip().startswith("```"):
            if opener is None:
                opener, body = line, []
            else:
                blocks.append((opener, line, body))
                opener = None
            continue
        if opener is not None:
            body.append(line)

    matches = [b for b in blocks if any("FEATURE RULES" in ln for ln in b[2])]
    assert len(matches) == 1, (
        "expected exactly one fenced block containing 'FEATURE RULES' in "
        f"{PLAYBOOK.name}, found {len(matches)}"
    )
    return matches[0]


def _leading_whitespace(line: str) -> str:
    return line[: len(line) - len(line.lstrip())]


def _fenced_rules_block(markdown: str) -> str:
    """The rules fence body with its uniform three-space indent stripped, so it
    can be compared byte-for-byte against the ``.mdc``."""
    _opener, _closer, body = _rules_fence(markdown)
    dedented = [ln[len(INDENT):] if ln.startswith(INDENT) else ln for ln in body]
    return "\n".join(dedented) + "\n"


def test_the_rules_fence_closes_at_its_opening_indent() -> None:
    """A closing fence at a different indent than its opener leaves the list
    item, so CommonMark never closes the block and swallows the rest of the
    playbook."""
    opener, closer, _body = _rules_fence(PLAYBOOK.read_text(encoding="utf-8"))
    assert _leading_whitespace(closer) == _leading_whitespace(opener), (
        f"closing fence indent {_leading_whitespace(closer)!r} != opening fence "
        f"indent {_leading_whitespace(opener)!r} in {PLAYBOOK.name}"
    )


def test_playbook_fenced_rules_block_matches_the_operative_mdc() -> None:
    mdc = MDC.read_text(encoding="utf-8")
    block = _fenced_rules_block(PLAYBOOK.read_text(encoding="utf-8"))
    assert block == mdc, (
        "the two rules copies have drifted. `.cursor/rules/mv-advisor.mdc` is the "
        "operative copy Cursor loads; re-sync the fenced FEATURE RULES block in "
        "`docs/design/mv-advisor-playbook.md` to match it byte-for-byte (prefix "
        "every line with three spaces). Do NOT weaken either copy to make this pass."
    )
