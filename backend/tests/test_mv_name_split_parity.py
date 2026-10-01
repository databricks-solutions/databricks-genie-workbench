"""MV-D122: the probe's ``_split_table`` and the create path's ``_uc_name_parts``
read a three-part name the same way.

``mv_entitlement`` cannot import ``mv_create`` (``mv_create`` imports it), so the
two backtick-aware loops are separate code. One case list pins them together:
a case added here runs against both.
"""

from __future__ import annotations

import pytest

from backend.services import mv_create, mv_entitlement
from backend.services.mv_entitlement import MvProbeError

NAME_CASES = [
    ("finance.sales.orders", ("finance", "sales", "orders")),
    ("`finance`.`sales`.`orders`", ("finance", "sales", "orders")),
    ("`a.b`.c", None),
    ("`a.b`.c.d", None),
    ("`finance.sales.orders`", None),
    ("main..orders", None),
]


def _split_table(name):
    try:
        return mv_entitlement._split_table(name)
    except MvProbeError:
        return None


@pytest.mark.parametrize(
    "split", [_split_table, mv_create._uc_name_parts], ids=["_split_table", "_uc_name_parts"],
)
@pytest.mark.parametrize(("name", "expected"), NAME_CASES)
def test_both_name_splits_read_a_name_alike(split, name, expected):
    assert split(name) == expected
