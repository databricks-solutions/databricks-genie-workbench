"""Tests for the broadened table-naming conventions in
``common/naming.py``.

The optimizer used to recognize only ``mv_<domain>_*`` and ``vw_<domain>_*``
table names. Tier 2 of the de-customer-ization plan broadens this to
cover medallion-style short prefixes (``f_``, ``d_``, ``stg_``, …) AND
adds a schema-fallback qualifier for spaces with multiple distinct
schemas. These tests pin the new behavior so future contributors don't
quietly narrow the vocabulary back to one customer's convention.
"""

from __future__ import annotations


def test_default_qualifier_supports_mv_prefix() -> None:
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert (
        domain_qualifier_from_identifier("cat.sch.mv_orders_fact_lines")
        == "ORDERS"
    )


def test_default_qualifier_supports_vw_prefix() -> None:
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert (
        domain_qualifier_from_identifier("cat.sch.vw_claims_summary")
        == "CLAIMS"
    )


def test_default_qualifier_supports_short_f_prefix() -> None:
    """Short-form medallion prefix ``f_<domain>_*`` is now recognized."""
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert (
        domain_qualifier_from_identifier("cat.sch.f_orders_lines") == "ORDERS"
    )


def test_default_qualifier_supports_short_d_prefix() -> None:
    """Short-form medallion prefix ``d_<domain>_*`` is now recognized."""
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert (
        domain_qualifier_from_identifier("cat.sch.d_customer_segments")
        == "CUSTOMER"
    )


def test_default_qualifier_supports_stg_prefix() -> None:
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert (
        domain_qualifier_from_identifier("cat.sch.stg_payments_raw")
        == "PAYMENTS"
    )


def test_default_qualifier_supports_metric_prefix() -> None:
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert (
        domain_qualifier_from_identifier("cat.sch.metric_claims_monthly")
        == "CLAIMS"
    )


def test_default_qualifier_returns_empty_for_no_recognized_prefix() -> None:
    """Single-segment prefixes like ``fact_`` / ``dim_`` are intentionally
    NOT in the domain-qualifier vocabulary because they don't carry a
    distinguishable domain segment in the leaf name.
    """
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert domain_qualifier_from_identifier("cat.sch.fact_orders") == ""
    assert domain_qualifier_from_identifier("cat.sch.dim_customer") == ""


def test_default_qualifier_returns_empty_for_blank() -> None:
    from genie_space_optimizer.common.naming import (
        domain_qualifier_from_identifier,
    )

    assert domain_qualifier_from_identifier("") == ""
    assert domain_qualifier_from_identifier("   ") == ""


def test_schema_fallback_emits_when_multiple_schemas_present() -> None:
    """When no leaf prefix matches and the space has multiple distinct
    schemas, the schema name itself becomes the qualifier so the SQL
    Expression is disambiguated by source.
    """
    from genie_space_optimizer.common.naming import (
        schema_qualifier_from_identifier,
    )

    assert (
        schema_qualifier_from_identifier(
            "cat.orders.fact_lines", distinct_schemas=2
        )
        == "ORDERS"
    )


def test_schema_fallback_skips_for_single_schema() -> None:
    """Single-schema spaces don't need disambiguation, so no qualifier."""
    from genie_space_optimizer.common.naming import (
        schema_qualifier_from_identifier,
    )

    assert (
        schema_qualifier_from_identifier(
            "cat.orders.fact_lines", distinct_schemas=1
        )
        == ""
    )


def test_schema_fallback_skips_generic_schema_names() -> None:
    """Generic schema names (``default``, ``public``, ``main``) wouldn't
    actually disambiguate, so the fallback declines."""
    from genie_space_optimizer.common.naming import (
        schema_qualifier_from_identifier,
    )

    assert (
        schema_qualifier_from_identifier(
            "cat.public.fact_orders", distinct_schemas=2
        )
        == ""
    )
    assert (
        schema_qualifier_from_identifier(
            "cat.default.fact_orders", distinct_schemas=2
        )
        == ""
    )


def test_schema_fallback_requires_three_part_identifier() -> None:
    """Short identifiers (no schema component) cannot use the fallback."""
    from genie_space_optimizer.common.naming import (
        schema_qualifier_from_identifier,
    )

    assert (
        schema_qualifier_from_identifier(
            "fact_orders", distinct_schemas=5
        )
        == ""
    )


def test_optimizer_qualifier_uses_schema_fallback_when_distinct_schemas_provided() -> None:
    """``_domain_qualifier_from_identifier`` in optimizer.py should fall
    back to the schema name for generic table names when the caller
    provides ``distinct_schemas >= 2``.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _domain_qualifier_from_identifier,
    )

    assert (
        _domain_qualifier_from_identifier(
            "cat.orders.fact_lines", distinct_schemas=2
        )
        == "ORDERS"
    )
    assert (
        _domain_qualifier_from_identifier(
            "cat.orders.fact_lines", distinct_schemas=1
        )
        == ""
    )


def test_optimizer_qualifier_prefers_leaf_match_over_schema_fallback() -> None:
    """When both a leaf-prefix match and a schema match exist, the leaf
    qualifier wins (it's more specific)."""
    from genie_space_optimizer.optimization.optimizer import (
        _domain_qualifier_from_identifier,
    )

    assert (
        _domain_qualifier_from_identifier(
            "cat.payments.mv_orders_lines", distinct_schemas=5
        )
        == "ORDERS"
    )


def test_extra_domain_patterns_env_override(monkeypatch) -> None:
    """``GSO_DOMAIN_TABLE_PATTERNS`` lets power users register custom
    regexes for non-standard table-naming conventions."""
    import importlib

    monkeypatch.setenv(
        "GSO_DOMAIN_TABLE_PATTERNS",
        r"^tbl_(?P<domain>[A-Za-z0-9]+)__",
    )

    from genie_space_optimizer.common import naming

    reloaded = importlib.reload(naming)
    try:
        assert (
            reloaded.domain_qualifier_from_identifier(
                "cat.sch.tbl_orders__fact"
            )
            == "ORDERS"
        )
    finally:
        monkeypatch.delenv("GSO_DOMAIN_TABLE_PATTERNS", raising=False)
        importlib.reload(naming)


def test_extra_domain_patterns_malformed_regex_is_ignored(monkeypatch) -> None:
    """Bad regexes in the env var are skipped, not raised."""
    import importlib

    monkeypatch.setenv(
        "GSO_DOMAIN_TABLE_PATTERNS",
        r"[unbalanced(?P<domain>[A-Za-z]+)",
    )

    from genie_space_optimizer.common import naming

    reloaded = importlib.reload(naming)
    try:
        assert reloaded._EXTRA_DOMAIN_PATTERNS == ()
    finally:
        monkeypatch.delenv("GSO_DOMAIN_TABLE_PATTERNS", raising=False)
        importlib.reload(naming)


def test_extra_domain_patterns_missing_domain_group_is_ignored(monkeypatch) -> None:
    """Patterns without a named ``domain`` group are skipped."""
    import importlib

    monkeypatch.setenv(
        "GSO_DOMAIN_TABLE_PATTERNS",
        r"^tbl_([A-Za-z0-9]+)__",
    )

    from genie_space_optimizer.common import naming

    reloaded = importlib.reload(naming)
    try:
        assert reloaded._EXTRA_DOMAIN_PATTERNS == ()
    finally:
        monkeypatch.delenv("GSO_DOMAIN_TABLE_PATTERNS", raising=False)
        importlib.reload(naming)


def test_leaf_soft_prefixes_includes_short_medallion_forms() -> None:
    """The vocabulary has been broadened beyond ``mv_/vw_/dim_/...``."""
    from genie_space_optimizer.common.naming import LEAF_SOFT_PREFIXES

    for prefix in ("mv_", "vw_", "f_", "d_", "stg_", "metric_"):
        assert prefix in LEAF_SOFT_PREFIXES, (
            f"expected {prefix!r} in LEAF_SOFT_PREFIXES"
        )


def test_leaf_two_seg_prefix_recognizes_short_medallion_forms() -> None:
    from genie_space_optimizer.common.naming import LEAF_TWO_SEG_PREFIX

    for ident, expected_tail in (
        ("mv_orders_dim_date", "dim_date"),
        ("vw_orders_summary", "summary"),
        ("f_orders_lines", "lines"),
        ("stg_orders_raw", "raw"),
        ("metric_orders_monthly", "monthly"),
    ):
        match = LEAF_TWO_SEG_PREFIX.match(ident)
        assert match is not None, f"expected {ident!r} to match"
        assert match.group("tail") == expected_tail
