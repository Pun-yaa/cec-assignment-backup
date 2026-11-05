import json

from temperature_observability.history import HistoryCache, HistorySnapshot


def snapshot(version=1):
    return HistorySnapshot(
        version,
        None,
        [
            {"timestamp": 2, "temperature": 27, "out_of_range": True},
            {"timestamp": 2, "temperature": 28, "out_of_range": True},
            {"timestamp": 3, "temperature": 25.5, "out_of_range": False},
            {"timestamp": 4, "temperature": 24, "out_of_range": True},
        ],
    )


def test_snapshot_preserves_equal_timestamp_order_and_inclusive_bounds():
    history = snapshot()
    assert json.loads(history.render(2, 2)) == [
        {"timestamp": 2, "temperature": 27},
        {"timestamp": 2, "temperature": 28},
    ]
    assert json.loads(history.render(2.1, 2.9)) == []
    assert json.loads(history.render(0, 1)) == []
    assert len(json.loads(history.render(outside=True))) == 3
    assert json.loads(history.render(intervals=True)) == [
        {"start_time": 2, "end_time": 3},
        {"start_time": 4, "end_time": None},
    ]
    assert (
        json.loads(HistorySnapshot(2, 5, history.points).render(intervals=True))[-1]["end_time"]
        == 5
    )


def test_cache_versions_and_point_budget_never_return_old_data_for_a_new_version():
    cache = HistoryCache(max_points=8, max_entries=3)
    cache.put("a", snapshot(2))
    cache.put("a", snapshot(1))
    assert cache.get("a", 2).version == 2
    assert cache.get("a", 3) is None
    cache.put("b", snapshot())
    cache.get("a", 2)
    cache.put("c", snapshot())
    assert cache.get("b", 1) is None
    assert cache.points == 8
    cache.put("a", None)
    assert cache.is_large("a") and cache.points == 4


def test_empty_histories_and_large_markers_also_have_an_entry_bound():
    cache = HistoryCache(max_points=8, max_entries=2)
    for i in range(100):
        cache.put(str(i), None if i % 2 else HistorySnapshot(0, None, []))
    assert len(cache.entries) == 2
    assert cache.points == 0
