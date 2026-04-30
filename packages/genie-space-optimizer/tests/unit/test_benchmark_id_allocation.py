from __future__ import annotations


def test_benchmark_id_allocator_skips_existing_ids() -> None:
    from genie_space_optimizer.optimization.evaluation import _make_benchmark_id_allocator

    allocate = _make_benchmark_id_allocator([
        {"id": "sales_gs_001"},
        {"id": "sales_gs_002"},
        {"id": "sales_019"},
    ])

    assert allocate("sales_gs", 1) == "sales_gs_003"
    assert allocate("sales_gs", 1) == "sales_gs_004"
    assert allocate("sales", 19) == "sales_020"


def test_benchmark_id_allocator_tracks_ids_allocated_in_same_call() -> None:
    from genie_space_optimizer.optimization.evaluation import _make_benchmark_id_allocator

    allocate = _make_benchmark_id_allocator([])

    assert allocate("domain_gf", 1) == "domain_gf_001"
    assert allocate("domain_gf", 1) == "domain_gf_002"
    assert allocate("domain_gf", 2) == "domain_gf_003"
