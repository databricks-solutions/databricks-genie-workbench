from types import SimpleNamespace

from backend.services.genie_client import normalize_metric_view_sources


class _TablesClient:
    def __init__(self, table_types: dict[str, str | None]):
        self._table_types = table_types

    def get(self, *, full_name: str):
        table_type = self._table_types.get(full_name)
        if table_type is None:
            raise RuntimeError("not found")
        return SimpleNamespace(table_type=table_type)


def _client(table_types: dict[str, str | None]):
    return SimpleNamespace(tables=_TablesClient(table_types))


def test_normalize_metric_view_sources_uses_uc_table_type():
    space_data = {
        "data_sources": {
            "tables": [
                {"identifier": "cat.sch.orders"},
                {"identifier": "cat.sch.sales_metrics"},
            ],
            "metric_views": [],
        }
    }

    normalize_metric_view_sources(
        space_data,
        client=_client({
            "cat.sch.orders": "MANAGED",
            "cat.sch.sales_metrics": "METRIC_VIEW",
        }),
    )

    assert [t["identifier"] for t in space_data["data_sources"]["tables"]] == ["cat.sch.orders"]
    assert [m["identifier"] for m in space_data["data_sources"]["metric_views"]] == ["cat.sch.sales_metrics"]


def test_normalize_metric_view_sources_respects_uc_non_metric_type():
    space_data = {
        "data_sources": {
            "tables": [{"identifier": "cat.sch.mv_orders"}],
            "metric_views": [],
        }
    }

    normalize_metric_view_sources(
        space_data,
        client=_client({"cat.sch.mv_orders": "MANAGED"}),
    )

    assert [t["identifier"] for t in space_data["data_sources"]["tables"]] == ["cat.sch.mv_orders"]
    assert space_data["data_sources"]["metric_views"] == []


def test_normalize_metric_view_sources_falls_back_to_mv_prefix_when_type_unknown():
    space_data = {
        "data_sources": {
            "tables": [
                {"identifier": "cat.sch.orders"},
                {"identifier": "cat.sch.mv_retail_sales"},
            ]
        }
    }

    normalize_metric_view_sources(space_data)

    assert [t["identifier"] for t in space_data["data_sources"]["tables"]] == ["cat.sch.orders"]
    assert [m["identifier"] for m in space_data["data_sources"]["metric_views"]] == ["cat.sch.mv_retail_sales"]

