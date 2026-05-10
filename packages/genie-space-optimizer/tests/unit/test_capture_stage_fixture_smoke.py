from scripts.capture_stage_fixture import _redact


def test_redact_question_text() -> None:
    out = _redact({"question_text": "select * from foo"})
    assert out == {"question_text": "<redacted>"}


def test_redact_preserves_short_strings_in_dbx_ids() -> None:
    out = _redact({"databricks_job_id": "123"})
    assert out == {"databricks_job_id": "123"}


def test_redact_dbx_id_keeps_last_4() -> None:
    out = _redact({"databricks_job_id": "1105451933925748"})
    assert out == {"databricks_job_id": "X" * 12 + "5748"}


def test_redact_recurses_into_list() -> None:
    out = _redact([{"sql_body": "x"}, {"sql_body": "y"}])
    assert out == [{"sql_body": "<redacted>"}, {"sql_body": "<redacted>"}]


def test_redact_passes_unknown_fields_through() -> None:
    out = _redact({"some_int": 7, "nested": {"a": 1}})
    assert out == {"some_int": 7, "nested": {"a": 1}}
