from genie_space_optimizer.optimization.optimizer import _lever6_reject_payload


def test_lever6_reject_payload_is_structured() -> None:
    payload = _lever6_reject_payload(
        reason="invalid_identifiers",
        cluster_id="AG3",
        target_table="cat.sch.mv_7now_store_sales",
        detail=["Unknown table: bogus"],
    )

    assert payload == {
        "rejected": True,
        "reject_reason": "invalid_identifiers",
        "cluster_id": "AG3",
        "target_table": "cat.sch.mv_7now_store_sales",
        "detail": ["Unknown table: bogus"],
    }
