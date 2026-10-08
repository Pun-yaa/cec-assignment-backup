import asyncio
import gzip

import httpx
import pytest

from temperature_observability.common import http_client


async def test_service_client_decodes_gzip_once():
    body = gzip.compress(b'{"status":"ok"}')

    async def handle(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Encoding: gzip\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    async with server, http_client() as client:
        port = server.sockets[0].getsockname()[1]
        response = await client.get(f"http://127.0.0.1:{port}")
        assert response.json() == {"status": "ok"}


async def test_service_client_normalizes_connection_errors():
    async with http_client() as client:
        with pytest.raises(httpx.TransportError):
            await client.get("http://127.0.0.1:1", timeout=0.1)
