#!/usr/bin/env python3
"""Scan for hand-rolled question_id extraction in *code* (not comments/strings).

Backs the ``HAND_ROLLED_QID_EXTRACTION`` invariant in ``check_invariants.sh``.
The previous implementation was a flat ``rg`` for the literal text
``row.get("question_id")`` / ``row["question_id"]``, which also tripped on the
same text appearing inside comments and docstrings (e.g. a docstring that
*documents* the forbidden pattern). That produced false positives that could
never be made green without deleting otherwise-correct prose.

This scanner tokenizes each file with :mod:`tokenize` and matches the pattern
only against real ``NAME``/``OP``/``STRING`` token sequences. Because comment
text is a ``COMMENT`` token and docstring/string *contents* live inside a
single ``STRING`` token, the same words inside a comment or docstring are
structurally invisible to the matcher — they cannot trip the rule.

Forbidden patterns (matched on the variable literally named ``row``, mirroring
the original rule's intent):

* ``row.get("question_id")``
* ``row["question_id"]``

Usage::

    _qid_hardroll_scan.py <path-or-dir> [<path-or-dir> ...]

Directory arguments are expanded to ``*.py`` files, excluding the same paths
the original ``rg`` globs excluded: ``_legacy/``, ``_qid_extraction.py``,
``test_*.py`` and ``*_test.py``. Prints one ``path:lineno: snippet`` line per
violation to stdout and exits 1 if any are found, else exits 0.
"""

from __future__ import annotations

import ast
import io
import sys
import tokenize
from pathlib import Path
from typing import List, Tuple

Violation = Tuple[str, int, str]

_TARGET = "question_id"
# Token types that carry meaning for sequence matching. NL/NEWLINE/INDENT/
# DEDENT/COMMENT/ENCODING are intentionally dropped, so comments and layout
# never participate in a match.
_SIG_TYPES = frozenset({tokenize.NAME, tokenize.OP, tokenize.STRING, tokenize.NUMBER})


def _excluded(path: Path) -> bool:
    """Mirror the original rg --glob exclusions."""
    if "_legacy" in path.parts:
        return True
    name = path.name
    return (
        name == "_qid_extraction.py"
        or name.startswith("test_")
        or name.endswith("_test.py")
    )


def _string_value(tok_str: str) -> str | None:
    """Return the literal value of a STRING token, or None if not parseable.

    f-strings and other exotic prefixes simply return None (not a plain
    ``"question_id"`` literal, so not a match).
    """
    try:
        value = ast.literal_eval(tok_str)
    except (ValueError, SyntaxError):
        return None
    return value if isinstance(value, str) else None


def _violations_in_source(src: str, path_label: str) -> List[Violation]:
    try:
        toks = list(tokenize.generate_tokens(io.StringIO(src).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        # Unparseable file: take no position rather than crashing the hook.
        return []

    sig = [t for t in toks if t.type in _SIG_TYPES]
    out: List[Violation] = []
    for i, t in enumerate(sig):
        if not (t.type == tokenize.NAME and t.string == "row"):
            continue
        line = t.start[0]
        # Pattern A: row . get ( "question_id"
        if (
            i + 4 < len(sig)
            and sig[i + 1].type == tokenize.OP and sig[i + 1].string == "."
            and sig[i + 2].type == tokenize.NAME and sig[i + 2].string == "get"
            and sig[i + 3].type == tokenize.OP and sig[i + 3].string == "("
            and sig[i + 4].type == tokenize.STRING
            and _string_value(sig[i + 4].string) == _TARGET
        ):
            out.append((path_label, line, 'row.get("question_id")'))
            continue
        # Pattern B: row [ "question_id"
        if (
            i + 2 < len(sig)
            and sig[i + 1].type == tokenize.OP and sig[i + 1].string == "["
            and sig[i + 2].type == tokenize.STRING
            and _string_value(sig[i + 2].string) == _TARGET
        ):
            out.append((path_label, line, 'row["question_id"]'))
    return out


def _iter_files(paths: List[str]):
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            for f in sorted(p.rglob("*.py")):
                if not _excluded(f):
                    yield f
        elif p.suffix == ".py" and not _excluded(p):
            yield p


def find_violations(paths: List[str]) -> List[Violation]:
    """Return every (path, lineno, snippet) code violation under ``paths``."""
    out: List[Violation] = []
    for f in _iter_files(paths):
        try:
            src = f.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        out.extend(_violations_in_source(src, str(f)))
    return out


def main(argv: List[str]) -> int:
    violations = find_violations(argv)
    for path, line, snippet in violations:
        print(f"{path}:{line}: {snippet}")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
