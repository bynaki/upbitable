"""localhost에 띄우는 가짜 Upbit WebSocket 서버."""

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

import jwt
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.http11 import Request, Response

from tests.support.samples import INDICATOR, INDICATOR_SIGNAL, MY_ASSET, MY_ORDER, TICKER

PUBLISH_INTERVAL = 0.01
"""구독한 코드마다 메시지를 보내는 간격(초)."""


class FakeUpbit:
    """구독 메시지를 받으면 그 codes마다 데이터를 계속 보낸다.

    type이 `myOrder`면 myOrder 예제를, `myAsset`이면 codes 없이 myAsset 예제를, `indicator`·
    `indicator_signal`이면 codes × indicators 조합마다 지표 예제를, 그 밖에는 ticker 예제를
    보낸다. 새 구독 메시지가 오면 실제 서버처럼 그 연결의 기존 구독을 대체한다.
    `secrets`를 정해 두면 연결의 `Authorization` 헤더를 HS512 JWT로 검증하고, 실패하면 구독 메시지에
    `INVALID_AUTH`를 보낸다. `rejections`에 넣어 두면 handshake를 그 HTTP 응답으로 하나씩 거절한다.
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
        self.secrets: dict[str, str] | None = None
        """access key → secret key. 정해 두면 인증 헤더를 검증한다."""
        self.tokens: list[dict[str, Any]] = []
        """검증을 통과한 JWT payload(연결 순서)."""
        self.connected_at: list[float] = []
        """연결마다 맺어진 시각(`time.monotonic()`)."""
        self.subscribed_at: list[float] = []
        """구독 메시지마다 받은 시각(`time.monotonic()`)."""
        self.rejections: list[tuple[int, str]] = []
        """(상태 코드, 본문). handshake마다 앞에서 하나씩 꺼내 그 응답으로 거절한다."""
        self.handshakes = 0
        """받은 handshake 수(거절한 것 포함)."""
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

    def _process_request(self, ws: ServerConnection, request: Request) -> Response | None:
        self.handshakes += 1
        if self.rejections:
            status, body = self.rejections.pop(0)
            return ws.respond(status, body)
        return None

    async def _handler(self, ws: ServerConnection) -> None:
        index = len(self.connections)
        self.connections.append(ws)
        self.connected_at.append(time.monotonic())
        auth_error = self._check_auth(ws)
        publisher: asyncio.Task | None = None
        try:
            async for raw in ws:
                payload = json.loads(raw)
                self.subscriptions.append((index, payload))
                self.subscribed_at.append(time.monotonic())
                if publisher:
                    publisher.cancel()
                if error := auth_error or self.error:
                    await ws.send(json.dumps({"error": error}))
                    continue
                codes = [code.split(".")[0] for code in payload[1].get("codes", [""])]
                publisher = asyncio.create_task(self._publish(ws, payload[1], codes))
        except Exception:
            pass
        finally:
            if publisher:
                publisher.cancel()
                with suppress(asyncio.CancelledError):
                    await publisher
            self.closed += 1

    def _check_auth(self, ws: ServerConnection) -> dict[str, str] | None:
        """인증 헤더를 검증한다. 문제가 있으면 보낼 오류를 돌려준다."""

        if self.secrets is None:
            return None
        assert ws.request is not None
        header = ws.request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return {"name": "INVALID_AUTH", "message": "no token"}
        token = header.removeprefix("Bearer ")
        try:
            access_key = jwt.decode(token, options={"verify_signature": False})["access_key"]
            secret = self.secrets[access_key]
            payload = jwt.decode(token, secret, algorithms=["HS512"])
        except jwt.InvalidTokenError, KeyError:
            return {"name": "INVALID_AUTH", "message": "bad token"}
        self.tokens.append(payload)
        return None

    async def _publish(
        self, ws: ServerConnection, type_obj: dict[str, Any], codes: list[str]
    ) -> None:
        type_ = type_obj["type"]
        stream_type = "SNAPSHOT"
        while True:
            for code in codes:
                if type_ in ("indicator", "indicator_signal"):
                    for msg in _indicator_messages(type_obj, code):
                        await ws.send(json.dumps(msg).encode())
                    continue
                if type_ == "myAsset":
                    msg = {**MY_ASSET, "stream_type": stream_type}
                elif type_ == "myOrder":
                    msg = {**MY_ORDER, "code": code, "stream_type": stream_type}
                else:
                    msg = {**TICKER, "type": type_, "code": code, "stream_type": stream_type}
                await ws.send(json.dumps(msg).encode())
            stream_type = "REALTIME"
            await asyncio.sleep(PUBLISH_INTERVAL)


def _indicator_messages(type_obj: dict[str, Any], code: str) -> list[dict[str, Any]]:
    """지표 구독 하나에서 `code`의 지표마다 보낼 메시지. 값은 예제 그대로다."""

    sample = INDICATOR if type_obj["type"] == "indicator" else INDICATOR_SIGNAL
    messages = []
    for ind in type_obj["indicators"]:
        msg = {
            **sample,
            "code": code,
            "timeframe": type_obj["timeframe"],
            "indicator": ind["type"],
            "params": ind["params"],
        }
        if "trigger" in ind:
            msg["trigger"] = ind["trigger"]
            msg["event_params"] = ind.get("event_params", {})
        messages.append(msg)
    return messages


@asynccontextmanager
async def fake_upbit() -> AsyncIterator[FakeUpbit]:
    fake = FakeUpbit()
    async with serve(
        fake._handler, "127.0.0.1", 0, process_request=fake._process_request
    ) as server:
        fake._server = server
        yield fake
