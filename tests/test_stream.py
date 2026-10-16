"""가짜 서버와 `Domain`으로 binder 전체를 확인한다."""

import asyncio
import time

from trading_core import DataModel, Domain, StageFailed, cast_model

from tests.support.fake_upbit import FakeUpbit
from upbitable import UpbitTicker, UpbitTickerReq, UpbitWsError, _ws

TIMEOUT = 2.0


class Recorder:
    def __init__(self) -> None:
        self.received: list[DataModel] = []

    async def __call__(self, data: DataModel) -> None:
        self.received.append(data)

    @property
    def symbols(self) -> set[str]:
        return {data.symbol for data in self.received}


class Failures:
    def __init__(self) -> None:
        self.received: list[StageFailed] = []

    async def __call__(self, failed: StageFailed) -> None:
        self.received.append(failed)


async def wait_until(predicate, message: str = "") -> None:
    try:
        async with asyncio.timeout(TIMEOUT):
            while not predicate():
                await asyncio.sleep(0.005)
    except TimeoutError as e:
        raise AssertionError(message or "조건이 충족되지 않았다.") from e


async def test_stream_yields_tickers(domain: Domain, server: FakeUpbit) -> None:
    received: list[UpbitTicker] = []
    async with domain.stream(UpbitTickerReq(url=server.url), {"KRW-BTC", "KRW-ETH"}) as stream:
        async for data in stream:
            received.append(cast_model(data, UpbitTicker))
            if len(received) == 6:
                break
    assert {t.symbol for t in received} == {"KRW-BTC", "KRW-ETH"}
    payload = server.subscriptions[0][1]
    assert payload[0]["ticket"]
    assert payload[1] == {"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"]}
    assert payload[2] == {"format": "DEFAULT"}


async def test_equal_requests_share_one_connection(domain: Domain, server: FakeUpbit) -> None:
    a, b = Recorder(), Recorder()
    async with (
        domain.subscribe(UpbitTickerReq(url=server.url), a) as sub_a,
        domain.subscribe(UpbitTickerReq(url=server.url), b) as sub_b,
    ):
        await sub_a.update({"KRW-BTC"})
        await sub_b.update({"KRW-ETH"})
        await wait_until(lambda: a.symbols and b.symbols, "두 소비자가 모두 받지 못했다.")
        assert len(server.connections) == 1
        assert a.symbols == {"KRW-BTC"}
        assert b.symbols == {"KRW-ETH"}


async def test_symbol_change_resubscribes_on_the_same_connection(
    domain: Domain, server: FakeUpbit
) -> None:
    rec = Recorder()
    async with domain.subscribe(UpbitTickerReq(url=server.url), rec) as sub:
        await sub.update({"KRW-BTC"})
        await wait_until(lambda: rec.symbols == {"KRW-BTC"})
        await sub.update({"KRW-BTC", "KRW-XRP"})
        await wait_until(lambda: "KRW-XRP" in rec.symbols, "새 심볼을 받지 못했다.")
        assert len(server.connections) == 1
        assert [index for index, _ in server.subscriptions] == [0, 0]
        assert server.last_codes() == ["KRW-BTC", "KRW-XRP"]


async def test_connection_closes_when_the_last_consumer_leaves(
    domain: Domain, server: FakeUpbit
) -> None:
    rec = Recorder()
    async with domain.subscribe(UpbitTickerReq(url=server.url), rec) as sub:
        await sub.update({"KRW-BTC"})
        await wait_until(lambda: rec.received)
        assert server.open_connections == 1
    await wait_until(lambda: server.open_connections == 0, "연결이 닫히지 않았다.")


async def test_server_error_fails_the_stage(domain: Domain, server: FakeUpbit) -> None:
    server.error = {"name": "INVALID_PARAM", "message": "지원하지 않는 값"}
    failures = Failures()
    async with domain.subscribe(UpbitTickerReq(url=server.url), Recorder(), failures) as sub:
        await sub.update({"KRW-BTC"})
        await wait_until(lambda: failures.received, "실패가 알려지지 않았다.")
    failed = failures.received[0]
    assert failed.symbols == {"KRW-BTC"}
    assert isinstance(failed.__cause__, UpbitWsError)
    assert len(server.subscriptions) == 1  # 오류 응답은 재시도하지 않는다
    await wait_until(lambda: server.open_connections == 0, "실패한 스테이지의 연결이 남았다.")


async def test_dropped_connection_reconnects_and_resubscribes(
    domain: Domain, server: FakeUpbit
) -> None:
    rec = Recorder()
    async with domain.subscribe(UpbitTickerReq(url=server.url), rec) as sub:
        await sub.update({"KRW-BTC"})
        await wait_until(lambda: rec.received)
        await server.drop_all()
        await wait_until(lambda: len(server.connections) == 2, "다시 연결하지 않았다.")
        count = len(rec.received)
        await wait_until(lambda: len(rec.received) > count, "다시 연결한 뒤 데이터가 없다.")
        assert [index for index, _ in server.subscriptions] == [0, 1]
        assert server.last_codes() == ["KRW-BTC"]


async def test_gives_up_after_the_retry_limit(
    domain: Domain, server: FakeUpbit, monkeypatch
) -> None:
    monkeypatch.setattr(_ws, "RETRY_LIMIT", 2)
    failures = Failures()
    url = server.url
    async with domain.subscribe(UpbitTickerReq(url=url), Recorder(), failures) as sub:
        await sub.update({"KRW-BTC"})
        await wait_until(lambda: server.connections)
        server._server.close()  # type: ignore[union-attr]  # 새 연결을 받지 않는다
        await server.drop_all()
        await wait_until(lambda: failures.received, "재시도 한도를 넘겨도 실패하지 않았다.")
    assert failures.received[0].symbols == {"KRW-BTC"}


async def test_close_is_quick_even_if_nobody_reads(server: FakeUpbit) -> None:
    """읽지 않아 수신 큐가 찬 연결도 `close_timeout`을 기다리지 않고 닫힌다."""

    ctx = _ws.WsContext(UpbitTickerReq(url=server.url), server.url)
    await ctx.subscribe({"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"]})
    await asyncio.sleep(0.3)  # 가짜 서버가 0.01초마다 보내 수신 큐(16개)가 찬다
    started = time.monotonic()
    await ctx.aclose()
    assert time.monotonic() - started < 1.0
    await wait_until(lambda: server.open_connections == 0)
