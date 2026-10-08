import asyncio
import logging
import time

import aiohttp
import httpx
from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # Notification URLs contain tokens. Never emit HTTP client access logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


class ServiceClient:
    """Pooled aiohttp transport with HTTPX response/error types at service boundaries.

    The transport avoids HTTPX's per-connection asynchronous lock overhead on this
    small VM. Response objects also make service handlers easy to test in isolation.
    """

    def __init__(self, timeout: float = 10):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout, connect=2),
            connector=aiohttp.TCPConnector(limit=128, keepalive_timeout=30, ttl_dns_cache=2),
            trust_env=False,
            auto_decompress=False,  # HTTPX decodes response bodies using the original headers.
        )

    async def request(self, method, url, **kwargs):
        kwargs.setdefault("allow_redirects", False)
        if "content" in kwargs:
            kwargs["data"] = kwargs.pop("content")
        timeout = kwargs.pop("timeout", None)
        if timeout is not None:
            kwargs["timeout"] = aiohttp.ClientTimeout(total=timeout, connect=min(2, timeout))
        request = httpx.Request(method, url)
        try:
            async with self.session.request(method, url, **kwargs) as response:
                return httpx.Response(
                    response.status,
                    headers=response.headers,
                    content=await response.read(),
                    request=request,
                )
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise httpx.TransportError("upstream request failed", request=request) from exc

    async def get(self, url, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.request("POST", url, **kwargs)

    async def aclose(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()


def http_client(timeout: float = 10):
    return ServiceClient(timeout)


async def cancel_tasks(*tasks: asyncio.Task):
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def supervise(run, name):
    """Restart background loops after an unexpected failure; cancellation still stops them."""
    while True:
        try:
            await run()
        except Exception:
            logging.getLogger(__name__).exception("Restarting %s after unexpected failure", name)
        await asyncio.sleep(1)


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        self.http_duration = Histogram(
            "to_http_request_seconds",
            "HTTP request duration",
            ["route", "method", "status"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
            registry=self.registry,
        )

    def counter(self, name, description, labels=()):
        return Counter(name, description, labels, registry=self.registry)

    def gauge(self, name, description, labels=()):
        return Gauge(name, description, labels, registry=self.registry)

    def histogram(self, name, description):
        return Histogram(
            name,
            description,
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 8, 10, 15, 30, 60),
            registry=self.registry,
        )

    def install(self, app: FastAPI):
        @app.get("/metrics", include_in_schema=False)
        async def metrics():
            return Response(
                generate_latest(self.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
            )

        app.add_middleware(RequestMetricsMiddleware, histogram=self.http_duration)


class RequestMetricsMiddleware:
    """Observe ASGI messages directly, without spawning tasks or buffering responses."""

    def __init__(self, app, histogram):
        self.app, self.histogram = app, histogram

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] == "/metrics":
            return await self.app(scope, receive, send)
        start, status = time.monotonic(), 500

        async def measured_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, measured_send)
        finally:
            path = getattr(scope.get("route"), "path", "unmatched")
            self.histogram.labels(path, scope["method"], str(status)).observe(
                time.monotonic() - start
            )
