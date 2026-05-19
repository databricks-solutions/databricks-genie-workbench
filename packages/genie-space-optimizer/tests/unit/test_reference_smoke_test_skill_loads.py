"""Plan 2 Task 11 — the reference smoke-test skill pins the layout."""
from __future__ import annotations

import json

from pydantic import BaseModel

from genie_space_optimizer.skills._loader import _SKILL_LOADER


_SKILL_ID = "_reference_smoke_test"


def test_skill_loads_metadata() -> None:
    meta = _SKILL_LOADER.load_metadata(_SKILL_ID)
    assert meta["skill_id"] == _SKILL_ID
    assert meta["llm_call_kind"] == "reasoning"
    assert meta["abstain_supported"] is True
    assert meta["max_tokens"] == 200


def test_skill_loads_prompt_body() -> None:
    body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name="REFERENCE_SMOKE_TEST_PROMPT",
    )
    assert body.strip().startswith("<role>")
    assert "Echo" in body


def test_skill_loads_reasoning_metadata() -> None:
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    assert rsm is not None
    assert rsm.llm_call_kind == "reasoning"
    assert rsm.max_tokens == 200
    assert rsm.abstain_supported is True
    assert rsm.examples_dir == "./examples"
    assert rsm.eval_dir == "./eval"


def test_skill_resolves_output_schema_class() -> None:
    cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    assert issubclass(cls, BaseModel)
    assert cls.__name__ == "ReferenceSmokeTestOutput"


def test_skill_examples_are_loadable() -> None:
    examples = list(_SKILL_LOADER.iter_examples(_SKILL_ID))
    assert len(examples) >= 1
    for name, content in examples:
        assert name.endswith(".json")
        assert isinstance(content, dict)


def test_skill_eval_file_is_valid_jsonl() -> None:
    """Held-out eval cases — Plans 3-7 use these to gate prompt
    changes. The reference skill ships two trivial cases."""
    from pathlib import Path
    eval_path = (
        Path(_SKILL_LOADER._root) / _SKILL_ID / "eval" / "test_cases.jsonl"
    )
    assert eval_path.is_file()
    lines = [
        line for line in eval_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(lines) >= 1
    for line in lines:
        json.loads(line)
