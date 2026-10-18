"""Upbit Private WebSocket(내 주문 myOrder·내 자산 myAsset)의 자격 증명·요청·데이터 모델과 binder.

이 모듈을 import하면 binder가 등록된다. 요청에는 API 키 값이 아니라 자격 증명 파일의 **이름**
(`account`)을 넣는다. 키는 init 콜백이 `trading_core.credentials.load_credential()`로 읽는다.

    async with domain.stream(UpbitMyOrderReq(account="upbit-main"), {"KRW-BTC"}) as stream:
        async for data in stream:
            order = cast_model(data, UpbitMyOrder)

- 자격 증명 파일의 항목은 `UpbitCredential` 꼴이다. 키에는 필요한 권한만 준다(myOrder는 주문조회,
  myAsset은 자산조회).
- 연결할 때마다 새 JWT(HS512, `access_key`·`nonce`)를 만들어 `Authorization` 헤더로 보낸다.
- 연결 한도는 포켓 단위로 센다. 같은 포켓의 키는 `pocket`에 같은 이름을 적는다.
- myOrder의 심볼은 페어 코드다. myAsset은 페어 코드를 받지 않고 모든 통화의 잔고를 한 메시지로
  보내므로, 심볼은 `MY_ASSET_SYMBOL`(`"*"`) 하나만 받는다.
- 데이터는 키가 속한 포켓의 것만 온다. 이벤트가 있을 때만 오며, 첫 메시지는 늦을 수 있다.
"""

import base64
import hashlib
import hmac
import json
from collections.abc import Callable
from contextlib import aclosing
from decimal import Decimal
from typing import Any, ClassVar, Self
from uuid import uuid4

from pydantic import BaseModel, SecretStr
from trading_core import DataModel, SourceRequest, SymbolRejected, initialize
from trading_core.credentials import Credential, load_credential

from upbitable._ws import PRIVATE_URL, Message, WsContext, stream_messages
from upbitable.quotation import AskBid, StreamType

MY_ASSET_SYMBOL = "*"
"""myAsset을 구독할 때 쓰는 유일한 심볼. 메시지 하나에 모든 통화의 잔고가 온다."""


# ===== 자격 증명 =====


class UpbitCredential(Credential):
    """Upbit API 키 하나. 자격 증명 파일에서 이름으로 읽는다.

    [upbit-main]
    exchange = "upbit"
    pocket = "main"
    access_key = "..."
    secret_key = "..."
    """

    exchange: ClassVar[str] = "upbit"
    pocket: str
    """키가 속한 포켓의 이름(사용자가 정한다). 인증 연결의 한도를 이 단위로 함께 센다."""
    access_key: SecretStr
    secret_key: SecretStr


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def make_token(cred: UpbitCredential) -> str:
    """연결 한 번에 쓸 JWT(HS512)를 만든다. `nonce`가 매번 달라 같은 토큰이 다시 나오지 않는다."""

    header = {"alg": "HS512", "typ": "JWT"}
    payload = {"access_key": cred.access_key.get_secret_value(), "nonce": str(uuid4())}
    signing_input = ".".join(
        _b64url(json.dumps(part, separators=(",", ":")).encode()) for part in (header, payload)
    )
    secret = cred.secret_key.get_secret_value().encode()
    signature = hmac.new(secret, signing_input.encode("ascii"), hashlib.sha512).digest()
    return f"{signing_input}.{_b64url(signature)}"


# ===== 요청 =====


class UpbitPrivateReq(SourceRequest):
    """Private 요청의 공통 필드. 직접 쓰지 않고 아래 하위 클래스를 쓴다."""

    account: str
    """자격 증명 파일에서 읽을 이름(예: `upbit-main`). 키 값이 아니다."""
    url: str = PRIVATE_URL
    """접속할 endpoint. 테스트에서 가짜 서버를 가리킬 때만 바꾼다."""

    def subscription(self, symbols: set[str]) -> Message:
        """구독 메시지의 type 객체."""

        raise NotImplementedError


class UpbitMyOrderReq(UpbitPrivateReq):
    """내 주문 및 체결(myOrder). 심볼은 페어 코드다."""

    def subscription(self, symbols: set[str]) -> Message:
        return {"type": "myOrder", "codes": sorted(symbols)}


class UpbitMyAssetReq(UpbitPrivateReq):
    """내 자산(myAsset). 심볼은 `MY_ASSET_SYMBOL` 하나다."""

    def subscription(self, symbols: set[str]) -> Message:
        return {"type": "myAsset"}


# ===== 데이터 =====


class UpbitMyOrder(DataModel):
    """내 주문·체결 이벤트. `symbol`은 응답의 `code`다.

    이벤트 종류(`state`)와 주문 종류에 따라 비는 필드가 많아, 식별·상태 필드 말고는 비어도 받는다.
    """

    type: str
    uuid: str
    """주문 식별자."""
    ask_bid: AskBid
    order_type: str
    """`limit`·`price`(시장가 매수)·`market`(시장가 매도)·`best`."""
    state: str
    """`wait`·`watch`·`trade`·`done`·`cancel`·`prevented`."""
    trade_uuid: str | None = None
    """체결 식별자(`state`가 `trade`일 때)."""
    price: Decimal | None = None
    """주문 가격. `state`가 `trade`면 체결 가격."""
    avg_price: Decimal | None = None
    volume: Decimal | None = None
    """주문량. `state`가 `trade`면 체결량."""
    remaining_volume: Decimal | None = None
    executed_volume: Decimal | None = None
    trades_count: int | None = None
    reserved_fee: Decimal | None = None
    remaining_fee: Decimal | None = None
    paid_fee: Decimal | None = None
    locked: Decimal | None = None
    executed_funds: Decimal | None = None
    time_in_force: str | None = None
    """`ioc`·`fok`·`post_only`."""
    trade_fee: Decimal | None = None
    is_maker: bool | None = None
    identifier: str | None = None
    """주문할 때 정한 클라이언트 식별자."""
    smp_type: str | None = None
    """자전거래 체결 방지 타입(`reduce`·`cancel_maker`·`cancel_taker`)."""
    prevented_volume: Decimal | None = None
    prevented_locked: Decimal | None = None
    trade_timestamp: int | None = None
    order_timestamp: int
    timestamp: int
    stream_type: StreamType

    @classmethod
    def from_ws(cls, msg: dict[str, Any]) -> Self:
        """DEFAULT 포맷 응답 하나를 모델로 바꾼다. 모르는 필드는 버린다."""

        return cls.model_validate({**msg, "symbol": msg["code"]})


class AssetUnit(BaseModel):
    """통화 하나의 잔고."""

    currency: str
    balance: Decimal
    """주문 가능 수량."""
    locked: Decimal
    """주문 중 묶인 수량."""


class UpbitMyAsset(DataModel):
    """내 자산 변동. 모든 통화의 잔고가 한 메시지로 오며 `symbol`은 `MY_ASSET_SYMBOL`이다."""

    type: str
    asset_uuid: str
    assets: list[AssetUnit]
    asset_timestamp: int
    timestamp: int
    stream_type: StreamType

    @classmethod
    def from_ws(cls, msg: dict[str, Any]) -> Self:
        """DEFAULT 포맷 응답 하나를 모델로 바꾼다. 모르는 필드는 버린다."""

        return cls.model_validate({**msg, "symbol": MY_ASSET_SYMBOL})


# ===== binder =====


def private_auth(account: str) -> tuple[str, Callable[[], dict[str, str]]]:
    """자격 증명 `account`로 인증 연결에 쓸 (연결 한도 키, 헤더 콜백)을 만든다.

    헤더 콜백은 부를 때마다 새 JWT를 만든다.
    """

    cred = load_credential(account, UpbitCredential)
    return f"pocket:{cred.pocket}", lambda: {"Authorization": f"Bearer {make_token(cred)}"}


def _private_context[Treq: UpbitPrivateReq](req: Treq) -> WsContext[Treq]:
    limit_key, headers = private_auth(req.account)
    return WsContext(req, req.url, limit_key=limit_key, headers=headers)


@initialize
def my_order(req: UpbitMyOrderReq) -> WsContext[UpbitMyOrderReq]:
    return _private_context(req)


@my_order
async def _(ctx: WsContext[UpbitMyOrderReq], symbols: set[str]):
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitMyOrder.from_ws(msg)


@my_order.detached
async def _(ctx: WsContext[UpbitMyOrderReq]):
    await ctx.aclose()


@initialize
def my_asset(req: UpbitMyAssetReq) -> WsContext[UpbitMyAssetReq]:
    return _private_context(req)


@my_asset
async def _(ctx: WsContext[UpbitMyAssetReq], symbols: set[str]):
    if others := symbols - {MY_ASSET_SYMBOL}:
        raise SymbolRejected(others, f"myAsset은 심볼 {MY_ASSET_SYMBOL!r}만 받는다")
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitMyAsset.from_ws(msg)


@my_asset.detached
async def _(ctx: WsContext[UpbitMyAssetReq]):
    await ctx.aclose()
