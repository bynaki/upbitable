"""localhost에 띄우는 가짜 Upbit WebSocket 서버."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve

from tests.support.samples import TICKER

PUBLISH_INTERVAL = 0.01
"""구독한 코드마다 메시지를 보내는 간격(초)."""


class FakeUpbit:
    """구독 메시지를 받으면 그 codes마다 ticker를 계속 보낸다.

    새 구독 메시지가 오면 실제 서버처럼 그 연결의 기존 구독을 대체한다.
    """

    def __init__(self) -> None:
        self.connections: list[ServerConnection] = []
        """지금까지 맺어진 연결(닫힌 것 포함)."""
        self.subscriptions: list[tuple[int, list[dict[str, Any]]]] = []
        """(연결 번호, 받은 구독 메시지)."""
        self.closed = 0
        """닫힌 연결 수."""
        self.error: dict[str, str] | None = None
        """정해 두면 다음 구독 메시지에 데이터 대신 이 오류를 보낸다."""
        self._server: Server | None = None

    @property
    def url(self) -> str:
        assert self._server is not None
        host, port = next(iter(self._server.sockets)).getsockname()[:2]
        return f"ws://{host}:{port}"

    @property
    def open_connections(self) -> int:
        return len(self.connections) - self.closed

    def last_codes(self) -> list[str]:
        """마지막 구독 메시지의 codes."""

        return self.subscriptions[-1][1][1]["codes"]

    async def drop_all(self) -> None:
        """열린 연결을 모두 서버 쪽에서 끊는다."""

        for ws in list(self.connections):
            await ws.close()

    async def _handler(self, ws: ServerConnection) -> None:
        index = len(self.connections)
        self.connections.append(ws)
        publisher: asyncio.Task | None = None
        try:
            async for raw in ws:
                payload = json.loads(raw)
                self.subscriptions.append((index, payload))
                if publisher:
                    publisher.cancel()
                if self.error:
                    await ws.send(json.dumps({"error": self.error}))
                    continue
                codes = [code.split(".")[0] for code in payload[1]["codes"]]
                publisher = asyncio.create_task(self._publish(ws, payload[1]["type"], codes))
        except Exception:
            pass
        finally:
            if publisher:
                publisher.cancel()
                with suppress(asyncio.CancelledError):
                    await publisher
            self.closed += 1

    async def _publish(self, ws: ServerConnection, type_: str, codes: list[str]) -> None:
        stream_type = "SNAPSHOT"
        while True:
            for code in codes:
                msg = {**TICKER, "type": type_, "code": code, "stream_type": stream_type}
                await ws.send(json.dumps(msg).encode())
            stream_type = "REALTIME"
            await asyncio.sleep(PUBLISH_INTERVAL)


@asynccontextmanager
async def fake_upbit() -> AsyncIterator[FakeUpbit]:
    fake = FakeUpbit()
    async with serve(fake._handler, "127.0.0.1", 0) as server:
        fake._server = server
        yield fake
