"""Lightweight host and Data measurements alongside an end-to-end load test."""

import asyncio
import os
import time
from pathlib import Path

import httpx
from prometheus_client.parser import text_string_to_metric_families


class Telemetry:
    def __init__(self, data_url):
        self.data_url = data_url
        self.samples = []
        self.previous_cpu = None

    async def sample(self, client, start, queue):
        sample = {"elapsed_seconds": time.monotonic() - start, "queued_events": queue}
        # Linux host data describes the whole shared VM, including the load generator.
        try:
            cpu = await asyncio.to_thread(Path("/proc/stat").read_text)
            ticks = list(map(int, cpu.splitlines()[0].split()[1:9]))
            total, idle, steal = sum(ticks), ticks[3] + ticks[4], ticks[7]
            if self.previous_cpu:
                old_total, old_idle, old_steal = self.previous_cpu
                if total > old_total:
                    sample["host_cpu_utilization"] = 1 - (idle - old_idle) / (total - old_total)
                    sample["host_cpu_steal"] = (steal - old_steal) / (total - old_total)
            self.previous_cpu = total, idle, steal
            fs = os.statvfs("/")
            sample["disk_available_bytes"] = fs.f_bavail * fs.f_frsize
        except OSError:
            pass
        try:
            response = await client.get(self.data_url + "/metrics", timeout=2)
            response.raise_for_status()
            values = {}
            for family in text_string_to_metric_families(response.text):
                for metric in family.samples:
                    if (
                        metric.name.startswith("to_database_")
                        and not metric.name.endswith(("_bucket", "_created"))
                        or metric.name == "to_cache_reads_total"
                    ):
                        labels = ",".join(f"{k}={v}" for k, v in sorted(metric.labels.items()))
                        values[metric.name + ("{" + labels + "}" if labels else "")] = metric.value
            sample["data_metrics"] = values
        except (httpx.HTTPError, ValueError) as exc:
            sample["error"] = type(exc).__name__
        self.samples.append(sample)

    def summary(self):
        cpus = [s["host_cpu_utilization"] for s in self.samples if "host_cpu_utilization" in s]
        steals = [s["host_cpu_steal"] for s in self.samples if "host_cpu_steal" in s]
        measured = [s["data_metrics"] for s in self.samples if "data_metrics" in s]
        database = {}
        if len(measured) > 1:
            before, after = measured[0], measured[-1]
            for key, value in after.items():
                if "_seconds_sum{" in key and key in before:
                    count_key = key.replace("_sum{", "_count{")
                    count = after.get(count_key, 0) - before.get(count_key, 0)
                    if count > 0:
                        database[key.replace("_sum{", "_mean{")] = (value - before[key]) / count
        return {
            "host_cpu_mean": sum(cpus) / len(cpus) if cpus else None,
            "host_cpu_max": max(cpus, default=None),
            "host_cpu_steal_mean": sum(steals) / len(steals) if steals else None,
            "data_operation_mean_seconds": database,
            "samples": len(self.samples),
            "sample_errors": sum("error" in s for s in self.samples),
        }
