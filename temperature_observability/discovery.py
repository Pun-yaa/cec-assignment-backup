import asyncio
import socket
from dataclasses import dataclass

import httpx


@dataclass
class Worker:
    url: str
    active: int
    capacity: int
    reserved: int = 0
    last_selected: int = 0

    @property
    def load(self):
        return max(self.active, self.reserved)


class ExecutorPool:
    """Discover Compose replicas through DNS and reserve the least busy worker."""

    def __init__(self, host: str, port: int, client: httpx.AsyncClient):
        self.host, self.port, self.client = host, port, client
        self.workers: dict[str, Worker] = {}
        self.sequence = 0

    async def refresh(self):
        try:
            addresses = await asyncio.to_thread(
                socket.getaddrinfo, self.host, self.port, socket.AF_INET, socket.SOCK_STREAM
            )
        except OSError:
            self.workers = {}
            return
        urls = sorted({f"http://{address[4][0]}:{self.port}" for address in addresses})

        async def inspect(url):
            try:
                response = await self.client.get(url + "/load", timeout=1)
                response.raise_for_status()
                load = response.json()
                worker = self.workers.get(url, Worker(url, 0, 1))
                worker.active, worker.capacity = load["active"], load["capacity"]
                return worker
            except (httpx.HTTPError, ValueError, KeyError):
                return None

        found = await asyncio.gather(*(inspect(url) for url in urls))
        self.workers = {worker.url: worker for worker in found if worker is not None}

    def reserve(self) -> Worker | None:
        candidates = [w for w in self.workers.values() if w.load < w.capacity]
        if not candidates:
            return None
        worker = min(candidates, key=lambda w: (w.load, w.last_selected, w.url))
        self.sequence += 1
        worker.last_selected = self.sequence
        worker.reserved += 1
        return worker

    @staticmethod
    def release(worker):
        worker.reserved -= 1
