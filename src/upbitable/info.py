"""Upbit 기술적 지표 WebSocket(지표 값 indicator·조건 이벤트 indicator_signal)의 모델과 binder.

이 모듈을 import하면 binder가 등록된다. 요청에는 자격 증명 이름(`account`), 캔들 단위, 지표 설정을
넣고, 심볼은 KRW 마켓의 페어 코드다.

    req = UpbitIndicatorReq(account="upbit-main", timeframe="5m", indicators=(Rsi(period=14),))
    async with domain.stream(req, {"KRW-BTC"}) as stream:
        async for data in stream:
            ind = cast_model(data, UpbitIndicator)   # ind.indicator == "rsi", ind.value["rsi"]

- Private 연결(`/websocket/v1/info`)이라 JWT가 필요하지만 키에 권한은 필요 없다.
- 지표 설정 모델은 필드 모양과 숫자 타입만 검사한다. 지원하는 값(예: RSI 기간 14·21)은 indicator와
  indicator_signal이 서로 달라 서버가 검사하고, 맞지 않으면 `INVALID_PARAM`으로 스테이지가 실패한다.
- 한 요청의 구독 조합(마켓 수 × 지표 수)은 `MAX_INDICATOR_COMBINATIONS`개까지다. 넘으면 서버가 요청
  전체를 거부하고 연결을 끊으므로, 보내기 전에 넘치는 심볼을 `SymbolRejected`로 거부한다. 이미 구독
  중인 심볼을 먼저 남긴다.
- 스냅샷이 없다. indicator는 구독 뒤 캔들이 처음 마감될 때부터, indicator_signal은 조건이 미충족에서
  충족으로 바뀔 때만 온다.
- 지표 값은 지표마다 필드가 달라 `value`를 `dict[str, Decimal]`로 받는다.
"""

from contextlib import aclosing
from decimal import Decimal
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt
from trading_core import DataModel, SymbolRejected, initialize

from upbitable._ws import INFO_URL, Message, WsContext, stream_messages
from upbitable.private import UpbitPrivateReq, private_auth
from upbitable.quotation import StreamType

MAX_INDICATOR_COMBINATIONS = 100
"""한 요청에 담을 수 있는 구독 조합(마켓 수 × 지표 수)의 상한."""

type Timeframe = Literal["5m", "15m", "60m", "240m", "1d"]
"""지표 계산에 쓰는 캔들 단위."""

type MaType = Literal["SMA", "EMA"]
"""이동평균선 종류."""


# ===== 지표 설정 =====


class IndicatorSpec(BaseModel):
    """지표 하나의 설정. 하위 클래스의 `indicator`가 지표 타입이고 나머지 필드가 계산 파라미터
    (`params`)다.

    숫자 파라미터를 문자열로 주면 거부한다(서버가 JSON number만 받는다).
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    def to_ws(self) -> Message:
        """구독 메시지 `indicators[]`의 항목."""

        params = self.model_dump()
        return {"type": params.pop("indicator"), "params": params}


class Volume(IndicatorSpec):
    """거래량. 값: `volume`, `avg_volume`, `ratio`."""

    indicator: Literal["volume"] = "volume"
    period: int


class Rsi(IndicatorSpec):
    """RSI. 값: `rsi`."""

    indicator: Literal["rsi"] = "rsi"
    period: int


class MovingAverage(IndicatorSpec):
    """이동평균선(단일). 값: `value`."""

    indicator: Literal["moving_average"] = "moving_average"
    type: MaType
    period: int


class MaCrossover(IndicatorSpec):
    """이동평균선(교차). 값: `ma_short`, `ma_long`."""

    indicator: Literal["ma_crossover"] = "ma_crossover"
    type: MaType
    short_period: int
    long_period: int


class BollingerBands(IndicatorSpec):
    """볼린저밴드. 값: `bb_upper`, `bb_middle`, `bb_lower`."""

    indicator: Literal["bollinger_bands"] = "bollinger_bands"
    period: int
    multiplier: float


class Macd(IndicatorSpec):
    """MACD. 값: `macd`, `macd_signal`, `histogram`."""

    indicator: Literal["macd"] = "macd"
    fast: int
    slow: int
    signal_period: int


type Indicator = Annotated[
    Volume | Rsi | MovingAverage | MaCrossover | BollingerBands | Macd,
    Field(discriminator="indicator"),
]
"""지표 설정 하나. dict로 줄 때는 `indicator` 필드로 종류를 고른다."""


class Signal(BaseModel):
    """조건 이벤트 하나: 지표 설정, 트리거, 조건 파라미터.

    `event_params`는 트리거가 요구할 때만 준다(예: RSI `overbought`의 `{"threshold": 70}`).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    indicator: Indicator
    trigger: str
    event_params: dict[str, StrictInt | StrictFloat] = Field(default_factory=dict)

    def to_ws(self) -> Message:
        """구독 메시지 `indicators[]`의 항목."""

        item = {**self.indicator.to_ws(), "trigger": self.trigger}
        if self.event_params:
            item["event_params"] = dict(self.event_params)
        return item


# ===== 요청 =====


class UpbitInfoReq(UpbitPrivateReq):
    """지표 요청의 공통 필드. 직접 쓰지 않고 아래 하위 클래스를 쓴다."""

    url: str = INFO_URL
    timeframe: Timeframe

    @property
    def combinations_per_symbol(self) -> int:
        """마켓 하나가 차지하는 구독 조합 수(지표 수)."""

        raise NotImplementedError


class UpbitIndicatorReq(UpbitInfoReq):
    """지표 값(indicator). 캔들이 마감될 때마다 마켓 × 지표마다 메시지 하나가 온다."""

    indicators: tuple[Indicator, ...] = Field(min_length=1, max_length=MAX_INDICATOR_COMBINATIONS)

    @property
    def combinations_per_symbol(self) -> int:
        return len(self.indicators)

    def subscription(self, symbols: set[str]) -> Message:
        return {
            "type": "indicator",
            "codes": sorted(symbols),
            "timeframe": self.timeframe,
            "indicators": [spec.to_ws() for spec in self.indicators],
        }


class UpbitIndicatorSignalReq(UpbitInfoReq):
    """조건 이벤트(indicator_signal). 조건이 미충족에서 충족으로 바뀔 때 한 번 온다."""

    indicators: tuple[Signal, ...] = Field(min_length=1, max_length=MAX_INDICATOR_COMBINATIONS)

    @property
    def combinations_per_symbol(self) -> int:
        return len(self.indicators)

    def subscription(self, symbols: set[str]) -> Message:
        return {
            "type": "indicator_signal",
            "codes": sorted(symbols),
            "timeframe": self.timeframe,
            "indicators": [signal.to_ws() for signal in self.indicators],
        }


# ===== 데이터 =====


class UpbitIndicator(DataModel):
    """지표 값. `symbol`은 응답의 `code`다.

    같은 요청에 지표가 여럿이면 `indicator`·`params`로 어느 지표의 값인지 구분한다.
    """

    type: str
    timeframe: Timeframe
    indicator: str
    params: dict[str, int | Decimal | str]
    value: dict[str, Decimal]
    """지표마다 필드가 다르다(예: RSI는 `rsi`, MACD는 `macd`·`macd_signal`·`histogram`)."""
    candle_date_time_utc: str
    """지표 계산의 기준이 된 캔들 구간의 시작 시각(UTC)."""
    timestamp: int
    stream_type: StreamType

    @classmethod
    def from_ws(cls, msg: dict[str, Any]) -> Self:
        """DEFAULT 포맷 응답 하나를 모델로 바꾼다. 모르는 필드는 버린다."""

        return cls.model_validate({**msg, "symbol": msg["code"]})


class UpbitIndicatorSignal(UpbitIndicator):
    """조건 이벤트. `value`는 조건이 충족된 시점의 지표 값이다."""

    trigger: str
    event_params: dict[str, int | Decimal]
    """조건 파라미터. 없는 트리거는 빈 dict다."""


# ===== binder =====


class InfoContext[Treq: UpbitInfoReq](WsContext[Treq]):
    """지표 연결. 구독 조합 한도를 지키려고 지금 구독한 심볼을 기억한다."""

    def __init__(self, req: Treq) -> None:
        limit_key, headers = private_auth(req.account)
        super().__init__(req, req.url, limit_key=limit_key, headers=headers)
        self.accepted: set[str] = set()

    def accept(self, symbols: set[str]) -> None:
        """`symbols`가 조합 한도 안이면 기억하고, 넘치면 넘치는 심볼을 `SymbolRejected`로 거부한다.

        이미 구독한 심볼을 먼저 남기고, 새 심볼은 이름순으로 채운다.
        """

        per_symbol = self.req.combinations_per_symbol
        room = MAX_INDICATOR_COMBINATIONS // per_symbol
        ordered = sorted(symbols & self.accepted) + sorted(symbols - self.accepted)
        if rejected := set(ordered[room:]):
            raise SymbolRejected(
                rejected,
                f"구독 조합(마켓 수 × 지표 {per_symbol}개)은 {MAX_INDICATOR_COMBINATIONS}개까지다",
            )
        self.accepted = set(symbols)


@initialize
def indicator(req: UpbitIndicatorReq) -> InfoContext[UpbitIndicatorReq]:
    return InfoContext(req)


@indicator
async def _(ctx: InfoContext[UpbitIndicatorReq], symbols: set[str]):
    ctx.accept(symbols)
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitIndicator.from_ws(msg)


@indicator.detached
async def _(ctx: InfoContext[UpbitIndicatorReq]):
    await ctx.aclose()


@initialize
def indicator_signal(req: UpbitIndicatorSignalReq) -> InfoContext[UpbitIndicatorSignalReq]:
    return InfoContext(req)


@indicator_signal
async def _(ctx: InfoContext[UpbitIndicatorSignalReq], symbols: set[str]):
    ctx.accept(symbols)
    async with aclosing(stream_messages(ctx, ctx.req.subscription(symbols))) as msgs:
        async for msg in msgs:
            yield UpbitIndicatorSignal.from_ws(msg)


@indicator_signal.detached
async def _(ctx: InfoContext[UpbitIndicatorSignalReq]):
    await ctx.aclose()
