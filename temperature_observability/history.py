"""Bounded read snapshots for repeated queries over small experiment histories."""

import json
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from dataclasses import dataclass


@dataclass(frozen=True)
class HistorySnapshot:
    version: int
    ended: float | None
    points: list[dict]

    def __post_init__(self):
        object.__setattr__(self, "timestamps", [point["timestamp"] for point in self.points])

    def render(self, start=None, end=None, outside=False, intervals=False):
        if intervals:
            result, active = [], None
            for point in self.points:
                if point["out_of_range"] and active is None:
                    active = {"start_time": point["timestamp"], "end_time": None}
                    result.append(active)
                elif not point["out_of_range"] and active is not None:
                    active["end_time"] = point["timestamp"]
                    active = None
            if active is not None and self.ended is not None:
                active["end_time"] = self.ended
        else:
            left = bisect_left(self.timestamps, start) if start is not None else 0
            right = bisect_right(self.timestamps, end) if end is not None else len(self.points)
            result = [
                {"timestamp": p["timestamp"], "temperature": p["temperature"]}
                for p in self.points[left:right]
                if not outside or p["out_of_range"]
            ]
        return json.dumps(result, separators=(",", ":"))


class HistoryCache:
    """Bound both point storage and entry count; large histories stay in SQL/Redis.

    None marks an experiment exceeding the per-experiment limit. Histories are
    append-only, so repeated narrow queries need not probe its first 513 rows
    again. Evicting a marker only causes another bounded probe, never data loss.
    """

    def __init__(self, max_points=16384, max_entries=256):
        self.max_points, self.max_entries = max_points, max_entries
        self.entries = OrderedDict()
        self.points = 0

    def get(self, experiment, version):
        snapshot = self.entries.get(experiment)
        if experiment in self.entries:
            self.entries.move_to_end(experiment)
        if snapshot is not None and snapshot.version >= version:
            return snapshot
        return None

    def is_large(self, experiment):
        return experiment in self.entries and self.entries[experiment] is None

    def put(self, experiment, snapshot):
        previous = self.entries.get(experiment)
        if previous is not None and snapshot is not None and previous.version > snapshot.version:
            return
        self.points -= len(previous.points) if previous is not None else 0
        self.entries[experiment] = snapshot
        self.entries.move_to_end(experiment)
        self.points += len(snapshot.points) if snapshot is not None else 0
        while self.points > self.max_points or len(self.entries) > self.max_entries:
            _, removed = self.entries.popitem(last=False)
            self.points -= len(removed.points) if removed is not None else 0
