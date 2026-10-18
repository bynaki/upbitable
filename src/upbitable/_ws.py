"""Upbit WebSocket 연결과 구독 메시지 송수신.

binder가 쓰는 내부 모듈이다. 연결 하나를 `WsContext`가 갖고, 소스 generator가 바뀔 때(심볼
합집합이 바뀔 때)마다 같은 연결에 구독 메시지를 다시 보낸다. Upbit는 새 구독 메시지가 그 연결의
기존 구독을 대체하므로 합집합 전체를 보내면 된다.

Upbit 요청 수 제한은 측정 단위가 다르다. 연결 요청은 인증 없이는 IP, 인증하면 포켓 단위로 세므로
프로세스 전체가 키(`"ip"`, `"pocket:<이름>"`)마다 제한기 하나를 함께 쓴다. 메시지는 연결마다 센다.
"""

import asyncio
import json
import re
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from decimal import Decimal
from typing import Any
from uuid import uuid4

from trading_core import SourceRequest
from trading_core.logger import get_logger
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import InvalidStatus, WebSocketException

from upbitable._limits import RateLimiter, Window

PUBLIC_URL = "wss://api.upbit.com/websocket/v1"
"""공개 시세(ticker·trade·orderbook·candle) endpoint. 인증이 없다."""

PRIVATE_URL = "wss://api.upbit.com/websocket/v1/private"
"""내 주문(myOrder)·내 자산(myAsset) endpoint. JWT 인증이 필요하다."""

INFO_URL = "wss://api.upbit.com/websocket/v1/info"
"""기술적 지표(indicator·indicator_signal) endpoint. JWT 인증이 필요하다(키 권한은 필요 없다)."""

CONNECT_LIMITS: tuple[Window, ...] = ((5, 1.0),)
"""연결 요청 한도(`websocket-connect`). IP 또는 포켓마다 초당 5회."""

MESSAGE_LIMITS: tuple[Window, ...] = ((5, 1.0), (100, 60.0))
"""메시지 전송 한도(`websocket-message`). 연결마다 초당 5회, 분당 100회."""

RETRY_LIMIT = 5
"""연결이 연속으로 이만큼 실패하면 재연결을 그만두고 예외를 올린다."""

RETRY_BASE_DELAY = 1.0
"""첫 재연결 전 대기(초). 실패할 때마다 두 배로 늘린다."""

RETRY_MAX_DELAY = 30.0
"""재연결 대기의 상한(초)."""

log = get_logger(__name__)

_connect_limiters: dict[str, RateLimiter] = {}


def connect_limiter(key: str) -> RateLimiter:
    """연결 한도를 함께 쓰는 단위(`"ip"`, `"pocket:<이름>"`)의 제한기. 처음 부를 때 만든다."""

    limiter = _connect_limiters.get(key)
    if limiter is None:
        limiter = _connect_limiters[key] = RateLimiter(*CONNECT_LIMITS)
    return limiter


type Message = dict[str, Any]


class UpbitWsError(Exception):
    """서버가 `{"error": {"name", "message"}}`로 알린 오류."""

    def __init__(self, name: str, message: str) -> None:
        super().__init__(f"{name}: {message}")
        self.name = name
        self.message = message


RETRYABLE_STATUS = frozenset({429})
"""다시 연결할 handshake 거절 상태 코드(4xx 중). 5xx도 다시 연결한다. 나머지 4xx는 다시 해도 결과가
같고, 418(일시 차단)은 반복하면 차단이 길어지므로 바로 올린다."""


def handshake_error(exc: InvalidStatus) -> UpbitWsError:
    """handshake 거절을 응답 본문의 사유를 담은 `UpbitWsError`로 바꾼다.

    본문은 `{"error": {"name", "message"}}`이고, `message`에 같은 꼴의 오류가 한 번 더 들어 있으면
    안쪽 사유를 쓴다. 실서버는 안쪽 JSON을 이스케이프하지 않고 넣어 본문 전체가 올바른 JSON이
    아니므로(2026-10-17), 그때는 본문에서 마지막 `name`과 JSON이 아닌 마지막 `message`를 찾는다.
    둘 다 안 되면 본문 앞부분을 그대로 담는다.
    """

    status = exc.response.status_code
    body = exc.response.body.decode(errors="replace")
    error = _error_of(body) or _scan_error(body) or (f"HTTP_{status}", body[:300])
    if inner := _error_of(error[1]):
        error = inner
    name, message = error
    return UpbitWsError(name, f"{message} (HTTP {status})")


def _error_of(text: str) -> tuple[str, str] | None:
    try:
        error = json.loads(text)["error"]
        return str(error["name"]), str(error["message"])
    except ValueError, TypeError, KeyError:
        return None


_NAME = re.compile(r'"name"\s*:\s*"([^"]*)"')
_MESSAGE = re.compile(r'"message"\s*:\s*"([^"{][^"]*)"')


def _scan_error(text: str) -> tuple[str, str] | None:
    names, messages = _NAME.findall(text), _MESSAGE.findall(text)
    if not names or not messages:
        return None
    return names[-1], messages[-1]


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

    - `limit_key`: 연결 한도를 함께 쓰는 단위. 공개 연결은 `"ip"`, 인증 연결은 `"pocket:<이름>"`.
    - `headers`: 연결할 때마다 불러 handshake 헤더를 만든다(JWT는 연결마다 새로 만들어야 한다).
    """

    def __init__(
        self,
        req: Treq,
        url: str,
        *,
        limit_key: str = "ip",
        headers: Callable[[], dict[str, str]] | None = None,
    ) -> None:
        self.req = req
        self.url = url
        self.limit_key = limit_key
        self.ticket = str(uuid4())
        self._headers = headers
        self._ws: ClientConnection | None = None
        self._messages = RateLimiter(*MESSAGE_LIMITS)

    async def subscribe(self, type_obj: Message) -> None:
        """구독 메시지를 보낸다. 연결이 없으면 먼저 연결한다."""

        if self._ws is None:
            await connect_limiter(self.limit_key).acquire()
            headers = self._headers() if self._headers else None
            try:
                self._ws = await connect(self.url, additional_headers=headers)
            except InvalidStatus as exc:
                status = exc.response.status_code
                if status < 500 and status not in RETRYABLE_STATUS:
                    raise handshake_error(exc) from exc
                raise
            self._messages = RateLimiter(*MESSAGE_LIMITS)
        payload = [{"ticket": self.ticket}, type_obj, {"format": "DEFAULT"}]
        await self._messages.acquire()
        await self._ws.send(encode(payload))

    async def recv(self) -> Message:
        """메시지 하나를 받는다. `subscribe()`를 먼저 불러야 한다."""

        if self._ws is None:
            raise RuntimeError("연결되지 않았다. `subscribe()`를 먼저 부른다.")
        return decode(await self._ws.recv())

    async def aclose(self) -> None:
        """연결을 닫는다. 다시 `subscribe()`하면 새로 연결한다.

        닫는 동안 남은 메시지를 읽어 버린다. 아무도 읽지 않아 수신 큐가 차면 websockets가 소켓
        읽기를 멈춰 서버의 close 응답도 읽지 못하고, `close()`가 `close_timeout`(10초)만큼 멈춘다.
        """

        ws, self._ws = self._ws, None
        if ws is None:
            return
        drain = asyncio.create_task(_drain(ws))
        try:
            await ws.close()
        finally:
            drain.cancel()
            with suppress(asyncio.CancelledError):
                await drain


async def _drain(ws: ClientConnection) -> None:
    with suppress(Exception):
        async for _ in ws:
            pass


async def stream_messages(ctx: WsContext, type_obj: Message) -> AsyncGenerator[Message]:
    """`type_obj`를 구독하고 받은 메시지를 끝없이 내보낸다.

    연결이 끊기면 다시 연결해 같은 구독을 보낸다. 메시지를 받으면 실패 횟수를 0으로 되돌리고,
    연속 `RETRY_LIMIT`번 실패하면 마지막 예외를 올린다. 서버의 오류 응답과 다시 해도 소용없는
    handshake 거절(`UpbitWsError`)은 재시도하지 않고 올린다. 연결은 닫지 않는다(스테이지가 detach될
    때 닫는다).
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
