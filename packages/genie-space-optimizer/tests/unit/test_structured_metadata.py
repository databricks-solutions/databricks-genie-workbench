"""Unit tests for genie_space_optimizer.optimization.structured_metadata."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.structured_metadata import (
    COLUMN_DIMENSION_SECTIONS,
    COLUMN_KEY_SECTIONS,
    COLUMN_MEASURE_SECTIONS,
    ENTITY_TYPE_TEMPLATES,
    LEVER_SECTION_OWNERSHIP,
    SECTION_LABELS,
    TABLE_DESCRIPTION_SECTIONS,
    EntityType,
    LeverOwnershipError,
    _contains_embedded_headers,
    classify_column,
    deduplicate_structured_description,
    entity_type_for_column,
    extract_synonyms_section,
    format_column_for_prompt,
    format_synonyms_section,
    merge_synonyms,
    parse_structured_description,
    render_structured_description,
    sections_for_lever,
    update_section,
    update_sections,
)


# ── Parser ────────────────────────────────────────────────────────────


class TestParseStructuredDescription:
    def test_empty_input(self):
        assert parse_structured_description(None) == {}
        assert parse_structured_description("") == {}
        assert parse_structured_description([]) == {}

    def test_legacy_freetext(self):
        result = parse_structured_description("This is a plain column description")
        assert result == {"_preamble": "This is a plain column description"}

    def test_single_section(self):
        text = "**Purpose:** Tracks booking revenue"
        result = parse_structured_description(text)
        assert result == {"purpose": "Tracks booking revenue"}

    def test_multiple_sections(self):
        text = (
            "**Purpose:** Booking fact table\n"
            "**Best for:** Revenue and booking count queries\n"
            "**Grain:** One row per booking"
        )
        result = parse_structured_description(text)
        assert result["purpose"] == "Booking fact table"
        assert result["best_for"] == "Revenue and booking count queries"
        assert result["grain"] == "One row per booking"

    def test_preamble_preserved(self):
        text = (
            "Legacy description text here\n"
            "**Definition:** The store identifier\n"
            "**Synonyms:** store id, location number"
        )
        result = parse_structured_description(text)
        assert result["_preamble"] == "Legacy description text here"
        assert result["definition"] == "The store identifier"
        assert result["synonyms"] == "store id, location number"

    def test_list_input(self):
        lines = [
            "**Purpose:** Booking table",
            "**Grain:** One row per booking",
        ]
        result = parse_structured_description(lines)
        assert result["purpose"] == "Booking table"
        assert result["grain"] == "One row per booking"

    def test_multiline_section_value(self):
        text = (
            "**Purpose:** This is a table\n"
            "that tracks bookings across\n"
            "multiple destinations."
        )
        result = parse_structured_description(text)
        assert "multiple destinations." in result["purpose"]

    def test_unrecognized_section_goes_to_preamble(self):
        text = "**Custom Header:** Some value"
        result = parse_structured_description(text)
        assert "_preamble" in result
        assert "Custom Header" in result["_preamble"]

    def test_idempotent_round_trip(self):
        original = {
            "purpose": "Revenue tracking",
            "best_for": "Booking analysis",
            "grain": "Per booking",
        }
        rendered = render_structured_description(original, "table")
        text = "\n".join(rendered)
        reparsed = parse_structured_description(text)
        assert reparsed.get("purpose") == original["purpose"]
        assert reparsed.get("best_for") == original["best_for"]
        assert reparsed.get("grain") == original["grain"]


# ── Renderer ──────────────────────────────────────────────────────────


class TestRenderStructuredDescription:
    def test_table_rendering(self):
        sections = {"purpose": "Tracks bookings", "grain": "Per booking"}
        lines = render_structured_description(sections, "table")
        text = "\n".join(lines)
        assert "PURPOSE:" in text
        assert "Tracks bookings" in text
        assert "GRAIN:" in text
        assert "Per booking" in text
        assert "**" not in text

    def test_column_dim_rendering(self):
        sections = {"definition": "Destination city", "synonyms": "city, location"}
        lines = render_structured_description(sections, "column_dim")
        text = "\n".join(lines)
        assert "DEFINITION:" in text
        assert "Destination city" in text
        assert "SYNONYMS:" in text
        assert "city, location" in text
        assert "**" not in text

    def test_preserves_preamble(self):
        sections = {"_preamble": "Legacy text", "definition": "The ID"}
        lines = render_structured_description(sections, "column_key")
        assert lines[0] == "Legacy text"
        text = "\n".join(lines)
        assert "DEFINITION:" in text
        assert "The ID" in text
        assert "**" not in text

    def test_empty_sections_skipped(self):
        sections = {"definition": "The value", "values": ""}
        lines = render_structured_description(sections, "column_dim")
        assert not any("VALUES:" in l for l in lines)

    def test_empty_sections_returns_placeholder(self):
        lines = render_structured_description({}, "table")
        assert lines == [""]

    def test_section_ordering_matches_template(self):
        sections = {
            "synonyms": "store_no, loc_id",
            "definition": "Unique store identifier",
            "values": "S001, S002, S003",
        }
        lines = render_structured_description(sections, "column_dim")
        def_idx = next(i for i, l in enumerate(lines) if "DEFINITION:" in l)
        val_idx = next(i for i, l in enumerate(lines) if "VALUES:" in l)
        syn_idx = next(i for i, l in enumerate(lines) if "SYNONYMS:" in l)
        assert def_idx < val_idx < syn_idx

    def test_blank_line_between_sections(self):
        sections = {"purpose": "Booking table", "grain": "Per booking"}
        lines = render_structured_description(sections, "table")
        text = "\n".join(lines)
        assert "Booking table\n\nGRAIN:" in text

    def test_no_trailing_blank_line(self):
        sections = {"definition": "The value"}
        lines = render_structured_description(sections, "column_dim")
        assert lines[-1] != ""


# ── Updater ───────────────────────────────────────────────────────────


class TestUpdateSection:
    def test_basic_update(self):
        current = "**Definition:** Old definition\n**Synonyms:** term1"
        result = update_section(current, "definition", "New definition", 1, "column_dim")
        text = "\n".join(result)
        assert "New definition" in text
        assert "term1" in text

    def test_lever_ownership_enforced(self):
        with pytest.raises(LeverOwnershipError):
            update_section("**Definition:** test", "aggregation", "SUM", 1, "column_measure")

    def test_lever_2_can_update_aggregation(self):
        current = "**Aggregation:** AVG"
        result = update_section(current, "aggregation", "SUM", 2, "column_measure")
        assert any("SUM" in l for l in result)

    def test_lever_4_can_update_join(self):
        result = update_section("", "join", "FK to dim_product", 4, "column_key")
        assert any("FK to dim_product" in l for l in result)

    def test_synonyms_shared_between_levers_1_and_2(self):
        current = "**Synonyms:** store_id"
        result1 = update_section(current, "synonyms", "loc_number", 1, "column_dim")
        assert any("loc_number" in l for l in result1)
        result2 = update_section(current, "synonyms", "location_id", 2, "column_dim")
        assert any("location_id" in l for l in result2)

    def test_preserves_unmodified_sections(self):
        current = (
            "**Definition:** The amount paid\n"
            "**Aggregation:** SUM\n"
            "**Synonyms:** revenue"
        )
        result = update_section(current, "definition", "Total payment", 1, "column_measure")
        text = "\n".join(result)
        assert "Total payment" in text
        assert "SUM" in text
        assert "revenue" in text


class TestUpdateSections:
    def test_multiple_sections(self):
        current = "**Definition:** old\n**Synonyms:** term1"
        updates = {"definition": "new", "synonyms": "term1, term2"}
        result = update_sections(current, updates, 1, "column_dim")
        text = "\n".join(result)
        assert "new" in text
        assert "term1, term2" in text

    def test_rejects_locked_section(self):
        with pytest.raises(LeverOwnershipError):
            update_sections("", {"aggregation": "SUM"}, 1, "column_measure")


# ── Column Classifier ────────────────────────────────────────────────


class TestClassifyColumn:
    def test_key_columns(self):
        assert classify_column("booking_key", "BIGINT") == "key"
        assert classify_column("user_id", "STRING") == "key"
        assert classify_column("id", "INT") == "key"

    def test_measure_by_prefix(self):
        assert classify_column("sum_revenue", "DOUBLE") == "measure"
        assert classify_column("avg_rating", "FLOAT") == "measure"
        assert classify_column("count_bookings", "INT") == "measure"

    def test_measure_by_numeric_type(self):
        assert classify_column("revenue", "DOUBLE") == "measure"
        assert classify_column("nights", "INT") == "measure"

    def test_dimension_by_string_type(self):
        assert classify_column("city_name", "STRING") == "dimension"
        assert classify_column("status", "STRING") == "dimension"

    def test_metric_view_numeric_is_measure(self):
        assert classify_column("amount", "DECIMAL", is_in_metric_view=True) == "measure"


class TestEntityTypeForColumn:
    def test_key(self):
        assert entity_type_for_column("booking_key", "BIGINT") == "column_key"

    def test_dim(self):
        assert entity_type_for_column("city_name", "STRING") == "column_dim"

    def test_measure(self):
        assert entity_type_for_column("sum_revenue", "DOUBLE") == "column_measure"


# ── Synonyms Helpers ──────────────────────────────────────────────────


class TestSynonymsHelpers:
    def test_extract_synonyms_empty(self):
        assert extract_synonyms_section({}) == []
        assert extract_synonyms_section({"synonyms": ""}) == []

    def test_extract_synonyms_basic(self):
        result = extract_synonyms_section({"synonyms": "store id, location number"})
        assert result == ["store id", "location number"]

    def test_format_synonyms(self):
        assert format_synonyms_section(["store id", "location number"]) == "store id, location number"

    def test_format_synonyms_empty(self):
        assert format_synonyms_section([]) == ""

    def test_merge_synonyms_no_duplicates(self):
        existing = ["store id", "location"]
        proposed = ["store id", "loc number"]
        merged = merge_synonyms(existing, proposed)
        assert "store id" in merged
        assert "location" in merged
        assert "loc number" in merged
        assert merged.count("store id") == 1

    def test_merge_synonyms_case_insensitive(self):
        existing = ["Store ID"]
        proposed = ["store id", "STORE ID"]
        merged = merge_synonyms(existing, proposed)
        assert len(merged) == 1


# ── Sections for Lever ────────────────────────────────────────────────


class TestSectionsForLever:
    def test_lever_1_column_dim(self):
        result = sections_for_lever(1, "column_dim")
        assert "definition" in result
        assert "values" in result
        assert "synonyms" in result

    def test_lever_2_column_measure(self):
        result = sections_for_lever(2, "column_measure")
        assert "aggregation" in result
        assert "grain_note" in result
        assert "synonyms" in result
        assert "definition" in result

    def test_lever_4_column_key(self):
        result = sections_for_lever(4, "column_key")
        assert "join" in result
        assert "definition" not in result

    def test_lever_5_empty(self):
        result = sections_for_lever(5, "table")
        assert result == []


# ── Format Column for Prompt ──────────────────────────────────────────


class TestFormatColumnForPrompt:
    def test_basic_output(self):
        output = format_column_for_prompt(
            "booking_key", "**Definition:** Primary key", None, "BIGINT",
        )
        assert "booking_key" in output
        assert "BIGINT" in output
        assert "key" in output
        assert "Definition" in output

    def test_includes_synonyms(self):
        output = format_column_for_prompt(
            "city_name", None, ["city", "destination"], "STRING",
        )
        assert "city" in output
        assert "destination" in output

    def test_empty_sections_show_placeholder(self):
        output = format_column_for_prompt("amount", None, None, "DOUBLE")
        assert "(empty)" in output

    def test_uc_comment_fallback(self):
        output = format_column_for_prompt(
            "revenue", None, None, "DOUBLE", uc_comment="Total revenue in USD",
        )
        assert "Total revenue in USD" in output


# ── Lever Ownership Validation ────────────────────────────────────────


class TestLeverOwnership:
    def test_all_sections_have_at_least_one_owner(self):
        all_sections = set()
        for secs in ENTITY_TYPE_TEMPLATES.values():
            all_sections.update(secs)
        all_owned = set()
        for owned in LEVER_SECTION_OWNERSHIP.values():
            all_owned.update(owned)
        for section in all_sections:
            assert section in all_owned, f"Section '{section}' has no lever owner"

    def test_lever_5_owns_nothing(self):
        assert LEVER_SECTION_OWNERSHIP[5] == set()

    def test_synonyms_shared_by_levers_1_and_2(self):
        assert "synonyms" in LEVER_SECTION_OWNERSHIP[1]
        assert "synonyms" in LEVER_SECTION_OWNERSHIP[2]

    def test_section_labels_cover_all_template_sections(self):
        all_sections = set()
        for secs in ENTITY_TYPE_TEMPLATES.values():
            all_sections.update(secs)
        for section in all_sections:
            assert section in SECTION_LABELS, f"Section '{section}' missing from SECTION_LABELS"

    def test_lever_0_owns_all_entity_sections(self):
        all_entity_sections = set()
        for secs in ENTITY_TYPE_TEMPLATES.values():
            all_entity_sections.update(secs)
        lever_0 = LEVER_SECTION_OWNERSHIP[0]
        for section in all_entity_sections:
            assert section in lever_0, (
                f"Lever 0 must own all entity sections but is missing '{section}'"
            )

    def test_lever_0_update_sections_on_blank_column(self):
        new_desc = update_sections(
            None,
            {"definition": "Unique order identifier", "join": "Joins to dim_customer.order_id"},
            lever=0,
            entity_type="column_key",
        )
        assert isinstance(new_desc, list)
        joined = "\n".join(new_desc)
        assert "DEFINITION:" in joined
        assert "Unique order identifier" in joined
        assert "JOIN:" in joined
        assert "Joins to dim_customer.order_id" in joined


# ── New-format parsing and backward compat ────────────────────────────


class TestPlainTextParsing:
    def test_parse_new_plaintext_format(self):
        text = "PURPOSE:\nBooking fact table\n\nGRAIN:\nOne row per booking"
        result = parse_structured_description(text)
        assert result["purpose"] == "Booking fact table"
        assert result["grain"] == "One row per booking"

    def test_parse_new_plaintext_column(self):
        text = "DEFINITION:\nUnique store identifier\n\nSYNONYMS:\nstore_no, loc_id"
        result = parse_structured_description(text)
        assert result["definition"] == "Unique store identifier"
        assert result["synonyms"] == "store_no, loc_id"

    def test_parse_legacy_markdown_still_works(self):
        text = "**Purpose:** Legacy table\n**Grain:** Per row"
        result = parse_structured_description(text)
        assert result["purpose"] == "Legacy table"
        assert result["grain"] == "Per row"

    def test_round_trip_new_format_table(self):
        original = {"purpose": "Fact table", "grain": "Per booking"}
        rendered = render_structured_description(original, "table")
        text = "\n".join(rendered)
        reparsed = parse_structured_description(text)
        assert reparsed.get("purpose") == original["purpose"]
        assert reparsed.get("grain") == original["grain"]

    def test_round_trip_new_format_column(self):
        original = {"definition": "The city name", "synonyms": "city, town", "values": "NYC, LA"}
        rendered = render_structured_description(original, "column_dim")
        text = "\n".join(rendered)
        reparsed = parse_structured_description(text)
        assert reparsed.get("definition") == original["definition"]
        assert reparsed.get("synonyms") == original["synonyms"]
        assert reparsed.get("values") == original["values"]

    def test_plaintext_with_preamble(self):
        text = "Legacy free text\n\nDEFINITION:\nNew structured def"
        result = parse_structured_description(text)
        assert result.get("_preamble") == "Legacy free text"
        assert result["definition"] == "New structured def"

    def test_multiline_plaintext_value(self):
        text = "PURPOSE:\nThis is a table\nthat tracks bookings\nacross destinations."
        result = parse_structured_description(text)
        assert "across destinations." in result["purpose"]


class TestNewlineInjection:
    """Tests for the pre-processing that injects newlines before inline ALL-CAPS headers."""

    def test_single_line_multi_section(self):
        text = "PURPOSE: Stores job run data BEST FOR: Analyzing history GRAIN: One row per run"
        result = parse_structured_description(text)
        assert result["purpose"] == "Stores job run data"
        assert result["best_for"] == "Analyzing history"
        assert result["grain"] == "One row per run"

    def test_single_line_with_scd(self):
        text = "PURPOSE: Fact table GRAIN: One row SCD: Type 1"
        result = parse_structured_description(text)
        assert result["purpose"] == "Fact table"
        assert result["grain"] == "One row"
        assert result["scd"] == "Type 1"

    def test_multi_word_label_use_instead_of(self):
        text = "DEFINITION: Old column USE INSTEAD OF: new_column"
        result = parse_structured_description(text)
        assert result["definition"] == "Old column"
        assert result["use_instead_of"] == "new_column"

    def test_multi_word_label_important_filters(self):
        text = "PURPOSE: Main table IMPORTANT FILTERS: status = 'active'"
        result = parse_structured_description(text)
        assert result["purpose"] == "Main table"
        assert result["important_filters"] == "status = 'active'"

    def test_already_separate_lines_unchanged(self):
        text = "PURPOSE:\nStores data\n\nGRAIN:\nOne row per run"
        result = parse_structured_description(text)
        assert result["purpose"] == "Stores data"
        assert result["grain"] == "One row per run"

    def test_no_false_positive_without_colon(self):
        text = "PURPOSE: This is the best for analysis of GRAIN patterns"
        result = parse_structured_description(text)
        assert result["purpose"] == "This is the best for analysis of GRAIN patterns"
        assert "best_for" not in result
        assert "grain" not in result

    def test_dedup_round_trip_single_line(self):
        text = "PURPOSE: First desc PURPOSE: Second desc GRAIN: Per row"
        deduped = deduplicate_structured_description(text)
        result = parse_structured_description(deduped)
        assert result["purpose"] == "Second desc"
        assert result["grain"] == "Per row"

    def test_inline_value_with_preamble(self):
        text = "Legacy text\nPURPOSE: Fact table GRAIN: One row"
        result = parse_structured_description(text)
        assert result.get("_preamble") == "Legacy text"
        assert result["purpose"] == "Fact table"
        assert result["grain"] == "One row"


# ── Embedded Header Validation ────────────────────────────────────────


class TestContainsEmbeddedHeaders:
    def test_clean_value_returns_empty(self):
        assert _contains_embedded_headers("purpose", "Fact table for job runs.") == []

    def test_detects_best_for_in_purpose(self):
        found = _contains_embedded_headers(
            "purpose", "Fact table. BEST FOR: Duration analysis."
        )
        assert "BEST FOR" in found

    def test_detects_multiple_embedded(self):
        found = _contains_embedded_headers(
            "purpose",
            "Stores runs. BEST FOR: Analytics. GRAIN: One row per run. SCD: Type 2",
        )
        assert "BEST FOR" in found
        assert "GRAIN" in found
        assert "SCD" in found

    def test_own_label_not_flagged(self):
        assert _contains_embedded_headers(
            "purpose", "PURPOSE: This is a valid description."
        ) == []

    def test_no_false_positive_without_colon(self):
        assert _contains_embedded_headers(
            "purpose", "This is the best for analysis of GRAIN patterns"
        ) == []

    def test_definition_with_values_header(self):
        found = _contains_embedded_headers(
            "definition", "Boolean flag. VALUES: TRUE, FALSE"
        )
        assert "VALUES" in found

    def test_empty_value(self):
        assert _contains_embedded_headers("purpose", "") == []


class TestEmbeddedHeaderRejection:
    """Test that update_section and update_sections reject embedded headers."""

    def test_update_section_rejects_embedded(self):
        with pytest.raises(LeverOwnershipError, match="embeds other headers"):
            update_section(
                "PURPOSE:\nOld text",
                "purpose",
                "New text. BEST FOR: Analytics. GRAIN: One row",
                lever=1,
                entity_type="table",
            )

    def test_update_section_accepts_clean(self):
        result = update_section(
            "PURPOSE:\nOld text",
            "purpose",
            "New fact table for job runs.",
            lever=1,
            entity_type="table",
        )
        assert any("New fact table" in line for line in result)

    def test_update_sections_filters_embedded(self):
        result = update_sections(
            "PURPOSE:\nOld purpose\nBEST FOR:\nOld best for",
            {
                "purpose": "New purpose. GRAIN: One row per run",
                "best_for": "Clean analytics value",
            },
            lever=1,
            entity_type="table",
        )
        joined = "\n".join(result)
        assert "Clean analytics value" in joined
        assert "GRAIN: One row per run" not in joined

    def test_update_sections_all_rejected_raises(self):
        with pytest.raises(LeverOwnershipError):
            update_sections(
                "PURPOSE:\nOld",
                {"purpose": "Bad. BEST FOR: X. GRAIN: Y"},
                lever=1,
                entity_type="table",
            )
