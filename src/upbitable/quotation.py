"""Upbit 공개 WebSocket 시세(ticker·trade·orderbook·candle)의 요청·데이터 모델과 binder.

이 모듈을 import하면 binder가 등록된다. 소비자는 `Domain`에 요청과 페어 코드 집합을 준다.

    async with domain.stream(UpbitTickerReq(), {"KRW-BTC"}) as stream:
        async for data in stream:
            ticker = cast_model(data, UpbitTicker)

- 요청 필드가 같으면(content_id가 같으면) 소비자들이 연결 하나를 공유한다. 심볼 집합은 구독
  메시지의 `codes`가 되고, 합집합이 바뀌면 같은 연결에 구독을 다시 보낸다.
- 응답은 DEFAULT 포맷을 받는다. 가격·수량·금액은 `Decimal`, ms 타임스탬프는 `int`다.
  `symbol`은 응답의 `code`(예: `KRW-BTC`)다.
- 구독을 다시 보내면 서버가 스냅샷을 다시 보낸다(`only_realtime=False`일 때).
"""

from contextlib import aclosing
from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import BaseModel
from trading_core import DataModel, SourceRequest, initialize

from upbitable._ws import PUBLIC_URL, Message, WsContext, stream_messages

type Change = Literal["RISE", "EVEN", "FALL"]
"""전일 종가 대비 방향. 상승·보합·하락."""

type AskBid = Literal["ASK", "BID"]
"""매도·매수 구분."""

type StreamType = Literal["SNAPSHOT", "REALTIME"]
"""스냅샷인지 실시간인지."""

type CandleUnit = Literal["1s", "1m", "3m", "5m", "10m", "15m", "30m", "60m", "240m"]
"""캔들 단위. 구독 type은 `candle.<unit>`이다."""


# ===== 요청 =====


class UpbitQuoteReq(SourceRequest):
    """공개 시세 요청의 공통 필드. 직접 쓰지 않고 아래 하위 클래스를 쓴다."""

    only_realtime: bool = False
    """참이면 스냅샷 없이 실시간 데이터만 받는다(`is_only_realtime`)."""
    url: str = PUBLIC_URL
    """접속할 endpoint. 테스트에서 가짜 서버를 가리킬 때만 바꾼다."""

    def ws_type(self) -> str:
        """구독 메시지의 `type` 값."""

        raise NotImplementedError

    def ws_codes(self, symbols: set[str]) -> list[str]:
        """구독 메시지의 `codes` 값. 페어 코드를 정렬해 둔다."""

        return sorted(symbols)

    def subscription(self, symbols: set[str]) -> Message:
        """구독 메시지의 type 객체."""

        obj: Message = {"type": self.ws_type(), "codes": self.ws_codes(symbols)}
        if self.only_realtime:
            obj["is_only_realtime"] = True
        return obj


class UpbitTickerReq(UpbitQuoteReq):
    """현재가(ticker)."""

    def ws_type(self) -> str:
        return "ticker"


class UpbitTradeReq(UpbitQuoteReq):
    """체결(trade)."""

    def ws_type(self) -> str:
        return "trade"


class UpbitOrderbookReq(UpbitQuoteReq):
    """호가(orderbook)."""

    size: Literal[1, 5, 15, 30] | None = None
    """호가 쌍 개수. 없으면 서버 기본값(30)이다. codes에 `KRW-BTC.15`처럼 붙여 보낸다."""
    level: Decimal | None = None
    """호가 모아보기 단위(KRW 마켓만). 없으면 기본 호가 단위(0)다."""

    def ws_type(self) -> str:
        return "orderbook"

    def ws_codes(self, symbols: set[str]) -> list[str]:
        codes = super().ws_codes(symbols)
        if self.size is None:
            return codes
        return [f"{code}.{self.size}" for code in codes]

    def subscription(self, symbols: set[str]) -> Message:
        obj = super().subscription(symbols)
        if self.level is not None:
            obj["level"] = self.level
        return obj


class UpbitCandleReq(UpbitQuoteReq):
    """캔들(candle.<unit>)."""

    unit: CandleUnit

    def ws_type(self) -> str:
        return f"candle.{self.unit}"


# ===== 데이터 =====


class UpbitQuoteData(DataModel):
    """공개 시세 데이터의 공통 필드. `symbol`은 응답의 `code`다."""

    type: str
    """데이터 타입(`ticker`, `candle.1m` 등)."""
    timestamp: int
    """타임스탬프(ms)."""
    stream_type: StreamType

    @classmethod
    def from_ws(cls, msg: dict[str, Any]) -> Self:
        """DEFAULT 포맷 응답 하나를 모델로 바꾼다. 모르는 필드는 버린다."""

        return cls.model_validate({**msg, "symbol": msg["code"]})


class UpbitTicker(UpbitQuoteData):
    """현재가. 지원 중단된 `is_trading_suspended`·`market_warning`은 받지 않는다."""

    opening_price: Decimal
    high_price: Decimal
    low_price: Decimal
    trade_price: Decimal
    """현재가."""
    prev_closing_price: Decimal
    change: Change
    change_price: Decimal
    """전일 대비 가격 변동의 절대값."""
    signed_change_price: Decimal
    change_rate: Decimal
    """전일 대비 등락율의 절대값."""
    signed_change_rate: Decimal
    trade_volume: Decimal
    """가장 최근 거래량."""
    acc_trade_volume: Decimal
    """누적 거래량(UTC 0시 기준)."""
    acc_trade_volume_24h: Decimal
    acc_trade_price: Decimal
    """누적 거래대금(UTC 0시 기준)."""
    acc_trade_price_24h: Decimal
    trade_date: str
    """최근 거래 일자(UTC, `yyyyMMdd`)."""
    trade_time: str
    """최근 거래 시각(UTC, `HHmmss`)."""
    trade_timestamp: int
    ask_bid: AskBid
    acc_ask_volume: Decimal
    acc_bid_volume: Decimal
    highest_52_week_price: Decimal
    highest_52_week_date: str
    """`yyyy-MM-dd`."""
    lowest_52_week_price: Decimal
    lowest_52_week_date: str
    """`yyyy-MM-dd`."""
    market_state: str
    """거래 상태(`PREVIEW`·`ACTIVE`·`DELISTED`)."""
    delisting_date: str | None = None
    """거래지원 종료일. 없으면 `None`."""


class UpbitTrade(UpbitQuoteData):
    """체결."""

    trade_price: Decimal
    trade_volume: Decimal
    ask_bid: AskBid
    prev_closing_price: Decimal
    change: Change
    change_price: Decimal
    trade_date: str
    """체결 일자(UTC, `yyyy-MM-dd`)."""
    trade_time: str
    """체결 시각(UTC, `HH:mm:ss`)."""
    trade_timestamp: int
    sequential_id: int
    """체결 번호. 체결마다 고유하다."""
    best_ask_price: Decimal
    best_ask_size: Decimal
    best_bid_price: Decimal
    best_bid_size: Decimal


class OrderbookUnit(BaseModel):
    """호가 한 쌍."""

    ask_price: Decimal
    bid_price: Decimal
    ask_size: Decimal
    bid_size: Decimal


class UpbitOrderbook(UpbitQuoteData):
    """호가."""

    total_ask_size: Decimal
    total_bid_size: Decimal
    orderbook_units: list[OrderbookUnit]
    level: Decimal = Decimal(0)
    """호가 모아보기 단위. 0이면 기본 호가 단위다."""


class UpbitCandle(UpbitQuoteData):
    """캔들. 같은 `candle_date_time_utc`가 여러 번 올 수 있고 마지막 값이 최신이다."""

    candle_date_time_utc: str
    """캔들 기준 시각(UTC, `yyyy-MM-ddTHH:mm:ss`)."""
    candle_date_time_kst: str
    """캔들 기준 시각(KST, `yyyy-MM-ddTHH:mm:ss`)."""
    opening_price: Decimal
    high_price: Decimal
    low_price: Decimal
    trade_price: Decimal
    """종가."""
    candle_acc_trade_volume: Decimal
    candle_acc_trade_price: Decimal


# ===== binder =====


@initialize
def ticker(req: UpbitTickerReq) -> WsContext[UpbitTickerReq]:
    return WsContext(req, req.url)


@ticker
async def _(ctx: WsContext[UpbitTickerReq], symbols: set[str]):
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitTicker.from_ws(msg)


@ticker.detached
async def _(ctx: WsContext[UpbitTickerReq]):
    await ctx.aclose()


@initialize
def trade(req: UpbitTradeReq) -> WsContext[UpbitTradeReq]:
    return WsContext(req, req.url)


@trade
async def _(ctx: WsContext[UpbitTradeReq], symbols: set[str]):
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitTrade.from_ws(msg)


@trade.detached
async def _(ctx: WsContext[UpbitTradeReq]):
    await ctx.aclose()


@initialize
def orderbook(req: UpbitOrderbookReq) -> WsContext[UpbitOrderbookReq]:
    return WsContext(req, req.url)


@orderbook
async def _(ctx: WsContext[UpbitOrderbookReq], symbols: set[str]):
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitOrderbook.from_ws(msg)


@orderbook.detached
async def _(ctx: WsContext[UpbitOrderbookReq]):
    await ctx.aclose()


@initialize
def candle(req: UpbitCandleReq) -> WsContext[UpbitCandleReq]:
    return WsContext(req, req.url)


@candle
async def _(ctx: WsContext[UpbitCandleReq], symbols: set[str]):
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitCandle.from_ws(msg)


@candle.detached
async def _(ctx: WsContext[UpbitCandleReq]):
    await ctx.aclose()
