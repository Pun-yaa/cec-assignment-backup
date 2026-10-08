"""Small readiness probe: avoid importing the HTTP/TLS stack in every container."""

import socket


def main():
    with socket.create_connection(("127.0.0.1", 8000), timeout=2) as connection:
        connection.sendall(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")
        with connection.makefile("rb") as response:
            status = response.readline(256).split()
            if len(status) < 2 or status[1] != b"200":
                raise SystemExit(1)


if __name__ == "__main__":
    main()
