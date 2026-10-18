"""기술적 지표(indicator·indicator_signal) — 지표 설정, 구독 메시지, 조합 한도, 모델, 가짜 서버."""

import asyncio
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError
from trading_core import Domain, cast_model

from tests.support.auth import ACCESS, ACCOUNT, SECRET, TIMEOUT, Failures, ignore, write_credentials
from tests.support.fake_upbit import FakeUpbit
from tests.support.samples import INDICATOR, INDICATOR_SIGNAL
from upbitable import (
    INFO_URL,
    MAX_INDICATOR_COMBINATIONS,
    BollingerBands,
    Indicator,
    IndicatorSpec,
    Macd,
    MaCrossover,
    MovingAverage,
    Rsi,
    Signal,
    UpbitIndicator,
    UpbitIndicatorReq,
    UpbitIndicatorSignal,
    UpbitIndicatorSignalReq,
    Volume,
)


@pytest.fixture
def credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    return write_credentials(tmp_path, monkeypatch)


@pytest.fixture
def auth_server(server: FakeUpbit) -> FakeUpbit:
    server.secrets = {ACCESS: SECRET}
    return server


def codes(n: int) -> set[str]:
    return {f"KRW-C{i:03d}" for i in range(n)}


# ----- 지표 설정 -----


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (Volume(period=20), {"type": "volume", "params": {"period": 20}}),
        (Rsi(period=14), {"type": "rsi", "params": {"period": 14}}),
        (
            MovingAverage(type="SMA", period=20),
            {"type": "moving_average", "params": {"type": "SMA", "period": 20}},
        ),
        (
            MaCrossover(type="EMA", short_period=5, long_period=20),
            {
                "type": "ma_crossover",
                "params": {"type": "EMA", "short_period": 5, "long_period": 20},
            },
        ),
        (
            BollingerBands(period=20, multiplier=2.0),
            {"type": "bollinger_bands", "params": {"period": 20, "multiplier": 2.0}},
        ),
        (
            Macd(fast=12, slow=26, signal_period=9),
            {"type": "macd", "params": {"fast": 12, "slow": 26, "signal_period": 9}},
        ),
    ],
)
def test_indicator_spec_to_ws(spec: IndicatorSpec, expected: dict) -> None:
    assert spec.to_ws() == expected


def test_indicator_spec_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        Rsi(period=14, threshold=70)  # type: ignore[call-arg]


def test_indicator_spec_rejects_string_number() -> None:
    """서버는 숫자 파라미터가 문자열이면 거부하므로 미리 막는다."""

    with pytest.raises(ValidationError):
        Rsi(period="14")  # type: ignore[arg-type]


def test_request_accepts_dict_indicators() -> None:
    req = UpbitIndicatorReq.model_validate(
        {"account": ACCOUNT, "timeframe": "5m", "indicators": [{"indicator": "rsi", "period": 14}]}
    )
    assert req.indicators == (Rsi(period=14),)


def test_request_needs_indicators_within_limit() -> None:
    with pytest.raises(ValidationError):
        UpbitIndicatorReq(account=ACCOUNT, timeframe="5m", indicators=())
    too_many = tuple(Rsi(period=14) for _ in range(MAX_INDICATOR_COMBINATIONS + 1))
    with pytest.raises(ValidationError):
        UpbitIndicatorReq(account=ACCOUNT, timeframe="5m", indicators=too_many)


def test_indicators_are_part_of_content_id() -> None:
    def req(*specs: Indicator) -> UpbitIndicatorReq:
        return UpbitIndicatorReq(account=ACCOUNT, timeframe="5m", indicators=specs)

    assert req(Rsi(period=14)).tr_content_id == req(Rsi(period=14)).tr_content_id
    assert req(Rsi(period=14)).tr_content_id != req(Rsi(period=21)).tr_content_id
    assert req(Rsi(period=14)).tr_content_id != req(Volume(period=14)).tr_content_id


def test_default_url_is_info() -> None:
    req = UpbitIndicatorReq(account=ACCOUNT, timeframe="5m", indicators=(Rsi(period=14),))
    assert req.url == INFO_URL
    assert INFO_URL == "wss://api.upbit.com/websocket/v1/info"


# ----- 구독 메시지 -----


def test_indicator_subscription() -> None:
    req = UpbitIndicatorReq(
        account=ACCOUNT, timeframe="15m", indicators=(Rsi(period=14), Volume(period=20))
    )
    assert req.subscription({"KRW-ETH", "KRW-BTC"}) == {
        "type": "indicator",
        "codes": ["KRW-BTC", "KRW-ETH"],
        "timeframe": "15m",
        "indicators": [
            {"type": "rsi", "params": {"period": 14}},
            {"type": "volume", "params": {"period": 20}},
        ],
    }


def test_signal_subscription() -> None:
    req = UpbitIndicatorSignalReq(
        account=ACCOUNT,
        timeframe="5m",
        indicators=(
            Signal(indicator=Rsi(period=14), trigger="overbought", event_params={"threshold": 70}),
            Signal(indicator=Macd(fast=12, slow=26, signal_period=9), trigger="golden_cross"),
        ),
    )
    assert req.subscription({"KRW-BTC"}) == {
        "type": "indicator_signal",
        "codes": ["KRW-BTC"],
        "timeframe": "5m",
        "indicators": [
            {
                "type": "rsi",
                "params": {"period": 14},
                "trigger": "overbought",
                "event_params": {"threshold": 70},
            },
            {
                "type": "macd",
                "params": {"fast": 12, "slow": 26, "signal_period": 9},
                "trigger": "golden_cross",
            },
        ],
    }


def test_signal_keeps_integer_threshold() -> None:
    """threshold는 정수다. float로 바뀌어 `70.0`으로 나가지 않는다."""

    signal = Signal(indicator=Rsi(period=14), trigger="oversold", event_params={"threshold": 30})
    assert type(signal.to_ws()["event_params"]["threshold"]) is int


# ----- 모델 -----


def test_indicator_model() -> None:
    data = UpbitIndicator.from_ws(INDICATOR)

    assert data.symbol == "KRW-BTC"
    assert data.indicator == "macd"
    assert data.params == {"fast": 12, "slow": 26, "signal_period": 9}
    assert data.value["macd"] == Decimal("33554.85769535")
    assert data.candle_date_time_utc == "2026-09-03T04:50:00"


def test_indicator_signal_model() -> None:
    data = UpbitIndicatorSignal.from_ws(INDICATOR_SIGNAL)

    assert data.symbol == "KRW-ARB"
    assert data.trigger == "price_cross_above"
    assert data.event_params == {}
    assert data.value == {"value": Decimal("202.8")}


# ----- 가짜 서버 -----


async def test_indicator_stream_sends_valid_token(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    req = UpbitIndicatorReq(
        account=ACCOUNT, timeframe="5m", indicators=(Rsi(period=14),), url=auth_server.url
    )
    async with asyncio.timeout(TIMEOUT), domain.stream(req, {"KRW-BTC"}) as stream:
        data = await anext(aiter(stream))
    ind = cast_model(data, UpbitIndicator)

    assert ind.symbol == "KRW-BTC"
    assert ind.indicator == "rsi"
    assert auth_server.tokens[0]["access_key"] == ACCESS
    assert auth_server.subscriptions[0][1][1]["type"] == "indicator"


async def test_signal_stream(domain: Domain, auth_server: FakeUpbit, credentials: Path) -> None:
    req = UpbitIndicatorSignalReq(
        account=ACCOUNT,
        timeframe="5m",
        indicators=(
            Signal(indicator=MovingAverage(type="SMA", period=5), trigger="price_cross_above"),
        ),
        url=auth_server.url,
    )
    async with asyncio.timeout(TIMEOUT), domain.stream(req, {"KRW-ARB"}) as stream:
        data = await anext(aiter(stream))
    signal = cast_model(data, UpbitIndicatorSignal)

    assert signal.symbol == "KRW-ARB"
    assert signal.trigger == "price_cross_above"


async def test_rejects_symbols_over_combination_limit(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    """지표 2개 × 마켓 51개 = 102. 100을 넘는 마켓 하나를 거부하고 나머지로 구독한다."""

    failures = Failures()
    req = UpbitIndicatorReq(
        account=ACCOUNT,
        timeframe="5m",
        indicators=(Rsi(period=14), Volume(period=20)),
        url=auth_server.url,
    )
    async with domain.subscribe(req, ignore, failures) as sub:
        await sub.update(codes(51))
        failed = await failures.wait()
        async with asyncio.timeout(TIMEOUT):
            while not auth_server.subscriptions:
                await asyncio.sleep(0.005)

    assert len(failed.symbols) == 1
    assert str(MAX_INDICATOR_COMBINATIONS) in failed.cause
    assert len(auth_server.last_codes()) == 50
    assert not failed.symbols & set(auth_server.last_codes())


async def test_keeps_subscribed_symbols_when_over_limit(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    """한도가 찬 뒤 새로 들어온 마켓을 거부한다.

    이름순으로 앞서도 이미 구독한 마켓을 밀어내지 않는다.
    """

    failures = Failures()
    req = UpbitIndicatorReq(
        account=ACCOUNT,
        timeframe="5m",
        indicators=(Rsi(period=14), Volume(period=20)),
        url=auth_server.url,
    )
    full = codes(50)
    async with domain.subscribe(req, ignore, failures) as sub:
        await sub.update(full)
        async with asyncio.timeout(TIMEOUT):
            while not auth_server.subscriptions:
                await asyncio.sleep(0.005)
        await sub.update(full | {"KRW-AAA"})
        failed = await failures.wait()

    assert failed.symbols == {"KRW-AAA"}
    assert set(auth_server.last_codes()) == full


async def test_unknown_account_fails_the_stage(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    failures = Failures()
    req = UpbitIndicatorReq(
        account="nope", timeframe="5m", indicators=(Rsi(period=14),), url=auth_server.url
    )
    async with domain.subscribe(req, ignore, failures) as sub:
        await sub.update({"KRW-BTC"})
        failed = await failures.wait()

    assert "CredentialError" in failed.cause
    assert auth_server.connections == []
