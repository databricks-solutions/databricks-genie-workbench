"""The frozen v1 measure grouping reproduces the keys captured at base (MV-D123).

The dual read finds a dismissal recorded under a measure's v1 key, so the v1
module must rebuild that key from the same occurrences the live scan extracts.
It is tested against the occurrence inputs recorded in the identity-v2 map, not
by re-extracting the SQL, because the extractor fixes for R4 and R6 change what
extraction returns for two of the captured statements.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from genie_space_optimizer.optimization.mv_fingerprint import (
    MeasureRef,
    extract_measures,
)

from genie_space_optimizer.optimization import mv_identity_v1

BASELINE_PATH = Path(__file__).parent / "data" / "mv_identity_baseline_3e71d66f.json"
_MODULE_PATH = Path(mv_identity_v1.__file__)
_ALLOWED_IMPORTS = frozenset(
    {
        ("genie_space_optimizer.optimization.mv_state", "mv_candidate_fingerprint"),
        ("genie_space_optimizer.optimization.mv_fingerprint", "MeasureRef"),
    }
)


@pytest.fixture(scope="module")
def baseline() -> dict:
    return json.loads(BASELINE_PATH.read_text())


def _recorded_occurrences(baseline: dict) -> tuple[dict[int, MeasureRef], dict[int, str]]:
    """Every occurrence of the captured corpus, built from its recorded inputs and
    numbered in extraction order: statements in corpus order, each repeat, each
    measure."""
    occurrences: dict[int, MeasureRef] = {}
    statement_of: dict[int, str] = {}
    for name in baseline["corpus_order"]:
        statement = baseline["statements"][name]
        for _ in range(statement["repeats"]):
            for recorded in statement["v1_occurrence_inputs"]:
                index = len(occurrences)
                occurrences[index] = MeasureRef(
                    canonical_expr=recorded["canonical_expr"],
                    fingerprint=recorded["expr_fingerprint"],
                    aggregate="",
                    source_columns=tuple(recorded["source_columns"]),
                    source_tables=tuple(recorded["spelled_source_tables"]),
                    has_unresolved_columns=recorded["has_unresolved_columns"],
                )
                statement_of[index] = name
    return occurrences, statement_of


def _captured_key_of(baseline: dict) -> dict[str, str]:
    captured: dict[str, str] = {}
    for row in baseline["scan"]:
        for name in row["from_statements"]:
            assert name not in captured, name
            captured[name] = row["candidate_fingerprint"]
    assert set(captured) == set(baseline["statements"])
    return captured


def test_v1_reproduces_every_captured_key(baseline):
    occurrences, statement_of = _recorded_occurrences(baseline)
    captured = _captured_key_of(baseline)

    keys = mv_identity_v1.v1_member_keys(baseline["space_id"], occurrences)

    assert set(keys) == set(occurrences)
    for index, key in keys.items():
        assert key == captured[statement_of[index]], statement_of[index]


def test_v1_groups_live_extracted_measures_like_the_recorded_ones(baseline):
    """The live extractor's output is the same occurrence type. The controls are
    the statements identity v2 leaves alone, so their live extraction still
    rebuilds the captured key."""
    captured = _captured_key_of(baseline)
    occurrences: dict[int, MeasureRef] = {}
    statement_of: dict[int, str] = {}
    for name in baseline["corpus_order"]:
        statement = baseline["statements"][name]
        if statement["role"] != "control":
            continue
        for _ in range(statement["repeats"]):
            for measure in extract_measures(statement["sql"]):
                statement_of[len(occurrences)] = name
                occurrences[len(occurrences)] = measure

    keys = mv_identity_v1.v1_member_keys(baseline["space_id"], occurrences)

    assert {statement_of[index] for index in keys} == {
        name for name, s in baseline["statements"].items() if s["role"] == "control"
    }
    for index, key in keys.items():
        assert key == captured[statement_of[index]], statement_of[index]


def test_v1_puts_two_catalogs_on_one_leaf_in_one_bucket_over_both_names():
    east = MeasureRef("sum(weight)", "fp_weight", "SUM", source_tables=("east.ops.shipments",))
    west = MeasureRef("sum(weight)", "fp_weight", "SUM", source_tables=("west.ops.shipments",))

    keys = mv_identity_v1.v1_member_keys("space", {"a": east, "b": west})

    assert keys["a"] == keys["b"]


def test_v1_keeps_a_table_less_occurrence_out_of_a_split_measure():
    orders = MeasureRef("sum(tax)", "fp_tax", "SUM", source_tables=("main.sales.orders",))
    refunds = MeasureRef("sum(tax)", "fp_tax", "SUM", source_tables=("main.sales.refunds",))
    bare = MeasureRef("sum(tax)", "fp_tax", "SUM")

    keys = mv_identity_v1.v1_member_keys("space", {1: orders, 2: refunds, 3: bare})

    assert len({keys[1], keys[2], keys[3]}) == 3


def _governed_imports(source: str) -> set[tuple[str, str]]:
    imported: set[tuple[str, str]] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(
                (alias.name, "") for alias in node.names if alias.name.startswith("genie_space_optimizer")
            )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                module = f"genie_space_optimizer.optimization.{module}".rstrip(".")
            if module.startswith("genie_space_optimizer"):
                imported.update((module, alias.name) for alias in node.names)
    return imported


def _top_level_functions(source: str) -> set[str]:
    return {
        node.name for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
    }


def test_v1_imports_no_live_grouping():
    source = _MODULE_PATH.read_text(encoding="utf-8")

    assert _governed_imports(source) == _ALLOWED_IMPORTS
    assert {"table_leaves", "_measure_partitions", "v1_member_keys"} <= _top_level_functions(
        source
    )


def test_the_import_pin_bites_on_the_live_grouping():
    seeded = _MODULE_PATH.read_text(encoding="utf-8") + (
        "\nfrom .mv_fingerprint import _measure_buckets\n"
    )

    assert _governed_imports(seeded) != _ALLOWED_IMPORTS
    assert (
        "genie_space_optimizer.optimization.mv_fingerprint",
        "_measure_buckets",
    ) in _governed_imports(seeded)


def test_v1_is_deleted_after_one_release():
    docstring = ast.get_docstring(ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))) or ""

    assert "M8" in docstring
    assert "MV-D123" in docstring
