from __future__ import annotations


def test_normalize_blame_set_flattens_json_encoded_lists() -> None:
    from genie_space_optimizer.optimization.blame_normalization import (
        normalize_blame_set,
    )

    blame = normalize_blame_set([
        '["time_window = mtd"]',
        "[]",
        "[time_window]",
        ["cy_tot_orders", "cy_cust_count"],
    ])

    assert blame == (
        "time_window = mtd",
        "time_window",
        "cy_tot_orders",
        "cy_cust_count",
    )


def test_normalize_blame_set_drops_empty_and_deduplicates() -> None:
    from genie_space_optimizer.optimization.blame_normalization import (
        normalize_blame_set,
    )

    blame = normalize_blame_set([
        "",
        [],
        "[]",
        "time_window",
        "[time_window]",
        "time_window",
    ])

    assert blame == ("time_window",)


def test_normalize_blame_key_uses_empty_string_for_no_blame() -> None:
    from genie_space_optimizer.optimization.blame_normalization import (
        normalize_blame_key,
    )

    assert normalize_blame_key(["[]", [], ""]) == ""
    assert normalize_blame_key(['["time_window = mtd"]']) == ("time_window = mtd",)
