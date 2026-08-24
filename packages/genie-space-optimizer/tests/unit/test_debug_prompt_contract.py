from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
DEBUG_PROMPT = REPO_ROOT / "docs" / "debug-prompt.md"

CURRENT_TABLES = {
    "genie_opt_runs",
    "genie_opt_stages",
    "genie_opt_artifacts",
    "genie_opt_iterations",
    "genie_opt_patches",
    "genie_opt_benchmark_mutations",
}

RETIRED_TABLES = {
    "genie_eval_lever_loop_decisions",
    "genie_opt_provenance",
}

RETIRED_COLUMNS = {
    "best_repeatability",
    "human_corrections_json",
    "both_correct_rate",
    "repeatability_pct",
    "applied",
}


def _sql_blocks() -> list[str]:
    text = DEBUG_PROMPT.read_text(encoding="utf-8")
    return re.findall(r"```sql\s*\n(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)


def test_debug_prompt_covers_only_the_current_gso_tables() -> None:
    prompt = DEBUG_PROMPT.read_text(encoding="utf-8").lower()
    sql = "\n".join(_sql_blocks()).lower()

    assert CURRENT_TABLES <= set(re.findall(r"\bgenie_[a-z0-9_]+\b", sql))
    for retired_table in RETIRED_TABLES:
        assert retired_table not in prompt


def test_debug_prompt_sql_does_not_query_retired_columns() -> None:
    sql = "\n".join(_sql_blocks()).lower()
    for retired_column in RETIRED_COLUMNS:
        assert re.search(rf"\b{re.escape(retired_column)}\b", sql) is None


def test_debug_prompt_sql_is_read_only() -> None:
    blocks = _sql_blocks()
    assert blocks

    forbidden = re.compile(
        r"\b(insert|update|delete|merge|alter|create|drop|optimize|vacuum|grant|revoke)\b",
        flags=re.IGNORECASE,
    )
    for block in blocks:
        assert forbidden.search(block) is None
        assert re.match(r"\s*(show|describe|select|with)\b", block, re.IGNORECASE)
