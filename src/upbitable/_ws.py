"""Upbit WebSocket 연결과 구독 메시지 송수신.

binder가 쓰는 내부 모듈이다. 연결 하나를 `WsContext`가 갖고, 소스 generator가 바뀔 때(심볼
합집합이 바뀔 때)마다 같은 연결에 구독 메시지를 다시 보낸다. Upbit는 새 구독 메시지가 그 연결의
기존 구독을 대체하므로 합집합 전체를 보내면 된다.
"""

import asyncio
import json
import time
from collections.abc import AsyncGenerator
from decimal import Decimal
from typing import Any
from uuid import uuid4

from trading_core import SourceRequest
from trading_core.logger import get_logger
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

PUBLIC_URL = "wss://api.upbit.com/websocket/v1"
"""공개 시세(ticker·trade·orderbook·candle) endpoint. 인증이 없다."""

SEND_INTERVAL = 0.2
"""연결·메시지 전송 사이의 최소 간격(초). Upbit 한도는 연결·메시지 각각 초당 5회다."""

RETRY_LIMIT = 5
"""연결이 연속으로 이만큼 실패하면 재연결을 그만두고 예외를 올린다."""

RETRY_BASE_DELAY = 1.0
"""첫 재연결 전 대기(초). 실패할 때마다 두 배로 늘린다."""

RETRY_MAX_DELAY = 30.0
"""재연결 대기의 상한(초)."""

log = get_logger(__name__)

type Message = dict[str, Any]


class UpbitWsError(Exception):
    """서버가 `{"error": {"name", "message"}}`로 알린 오류."""

    def __init__(self, name: str, message: str) -> None:
        super().__init__(f"{name}: {message}")
        self.name = name
        self.message = message


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"JSON으로 바꿀 수 없는 값이다. - {value!r}")


def encode(payload: list[Message]) -> str:
    """구독 메시지를 JSON 문자열로 바꾼다. `Decimal`은 JSON number로 보낸다."""

    return json.dumps(payload, default=_json_default, separators=(",", ":"))


def decode(raw: str | bytes) -> Message:
    """받은 메시지를 dict로 바꾼다. 실수는 `Decimal`로 받아 정밀도를 지킨다.

    서버가 오류를 보냈으면 `UpbitWsError`를 던진다.
    """

    msg = json.loads(raw, parse_float=Decimal)
    if not isinstance(msg, dict):
        raise UpbitWsError("UNEXPECTED_MESSAGE", f"dict가 아닌 메시지다. - {msg!r}")
    if error := msg.get("error"):
        raise UpbitWsError(str(error.get("name", "")), str(error.get("message", "")))
    return msg


class WsContext[Treq: SourceRequest]:
    """스테이지 하나가 쓰는 연결과 요청.

    init 콜백이 만들고 `@x.detached`에서 `aclose()`한다. 소스 generator는 콜백 인자로 요청을
    받지 못하므로 구독 메시지에 필요한 요청 필드를 `req`로 꺼낸다.
    """

    def __init__(self, req: Treq, url: str) -> None:
        self.req = req
        self.url = url
        self.ticket = str(uuid4())
        self._ws: ClientConnection | None = None
        self._last_sent = 0.0

    async def _throttle(self) -> None:
        wait = self._last_sent + SEND_INTERVAL - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_sent = time.monotonic()

    async def subscribe(self, type_obj: Message) -> None:
        """구독 메시지를 보낸다. 연결이 없으면 먼저 연결한다."""

        if self._ws is None:
            await self._throttle()
            self._ws = await connect(self.url)
        payload = [{"ticket": self.ticket}, type_obj, {"format": "DEFAULT"}]
        await self._throttle()
        await self._ws.send(encode(payload))

    async def recv(self) -> Message:
        """메시지 하나를 받는다. `subscribe()`를 먼저 불러야 한다."""

        if self._ws is None:
            raise RuntimeError("연결되지 않았다. `subscribe()`를 먼저 부른다.")
        return decode(await self._ws.recv())

    async def aclose(self) -> None:
        """연결을 닫는다. 다시 `subscribe()`하면 새로 연결한다."""

        ws, self._ws = self._ws, None
        if ws is not None:
            await ws.close()


async def stream_messages(ctx: WsContext, type_obj: Message) -> AsyncGenerator[Message]:
    """`type_obj`를 구독하고 받은 메시지를 끝없이 내보낸다.

    연결이 끊기면 다시 연결해 같은 구독을 보낸다. 메시지를 받으면 실패 횟수를 0으로 되돌리고,
    연속 `RETRY_LIMIT`번 실패하면 마지막 예외를 올린다. 서버의 오류 응답(`UpbitWsError`)은
    재시도하지 않고 올린다. 연결은 닫지 않는다(스테이지가 detach될 때 닫는다).
    """

    failures = 0
    while True:
        try:
            await ctx.subscribe(type_obj)
            while True:
                msg = await ctx.recv()
                failures = 0
                yield msg
        except (WebSocketException, OSError) as exc:
            failures += 1
            await ctx.aclose()
            if failures >= RETRY_LIMIT:
                raise
            delay = min(RETRY_BASE_DELAY * 2 ** (failures - 1), RETRY_MAX_DELAY)
            log.warning(
                "연결이 끊겨 다시 연결한다",
                url=ctx.url,
                failures=failures,
                delay=delay,
                error=repr(exc),
            )
            await asyncio.sleep(delay)
