"""Plan 11 — back-compat shims for RcaKind and RepairShape enum → free-text."""
from genie_space_optimizer.optimization.rca import (
    RcaKind,
    parse_rca_kind_or_label,
)
from genie_space_optimizer.optimization.repair_intent import (
    RepairShape,
    parse_repair_shape_or_hypothesis,
)


def test_parse_rca_kind_old_enum_string():
    """Old Delta rows store e.g. 'FILTER_LOGIC_MISMATCH' as the enum value."""
    result = parse_rca_kind_or_label("FILTER_LOGIC_MISMATCH")
    assert result == "FILTER_LOGIC_MISMATCH"


def test_parse_rca_kind_free_text():
    """New rows store free-text like 'defensive filter dropped wrong rows'."""
    result = parse_rca_kind_or_label("defensive filter dropped wrong rows")
    assert result == "defensive filter dropped wrong rows"


def test_parse_rca_kind_enum_instance():
    """Enum instance → its string value."""
    result = parse_rca_kind_or_label(RcaKind.UNKNOWN)
    assert isinstance(result, str)


def test_parse_rca_kind_empty():
    assert parse_rca_kind_or_label("") == ""
    assert parse_rca_kind_or_label(None) == ""


def test_parse_repair_shape_old_enum_string():
    result = parse_repair_shape_or_hypothesis("plural_top_n_collapse")
    assert result == "plural_top_n_collapse"


def test_parse_repair_shape_free_text():
    result = parse_repair_shape_or_hypothesis(
        "Replace RANK() with ROW_NUMBER() and LIMIT 10"
    )
    assert result == "Replace RANK() with ROW_NUMBER() and LIMIT 10"


def test_parse_repair_shape_enum_instance():
    shape = RepairShape(list(RepairShape)[0])  # any valid member
    result = parse_repair_shape_or_hypothesis(shape)
    assert isinstance(result, str)
