"""요청 수 제한기 — 창 여러 개, 프로세스 공유 연결 한도, 연결마다의 메시지 한도."""

import asyncio

import pytest
from trading_core import Domain

from tests.support.fake_upbit import FakeUpbit
from upbitable import UpbitTickerReq, _ws
from upbitable._limits import RateLimiter


class FakeClock:
    """`sleep`하면 그만큼 시각이 흐르는 시계."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


async def acquire_times(limiter: RateLimiter, clock: FakeClock, count: int) -> list[float]:
    times = []
    for _ in range(count):
        await limiter.acquire()
        times.append(clock.now)
    return times


async def test_window_allows_limit_then_waits() -> None:
    clock = FakeClock()
    limiter = RateLimiter((5, 1.0), clock=clock, sleep=clock.sleep)

    times = await acquire_times(limiter, clock, 11)

    assert times == [0.0] * 5 + [1.0] * 5 + [2.0]


async def test_every_window_is_kept() -> None:
    clock = FakeClock()
    limiter = RateLimiter((2, 1.0), (3, 10.0), clock=clock, sleep=clock.sleep)

    times = await acquire_times(limiter, clock, 5)

    # 초당 2회지만 10초에 3회라 세 번째 다음은 10초 창이 비기를 기다린다.
    assert times == [0.0, 0.0, 1.0, 10.0, 10.0]


async def test_no_window_never_waits() -> None:
    clock = FakeClock()
    limiter = RateLimiter(clock=clock, sleep=clock.sleep)

    await acquire_times(limiter, clock, 100)

    assert clock.sleeps == []


def test_connect_limiter_is_shared_per_key() -> None:
    assert _ws.connect_limiter("ip") is _ws.connect_limiter("ip")
    assert _ws.connect_limiter("pocket:main") is not _ws.connect_limiter("ip")
    assert _ws.connect_limiter("pocket:main") is not _ws.connect_limiter("pocket:sub1")


async def test_stages_share_the_ip_connect_limit(
    domain: Domain, server: FakeUpbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_ws, "CONNECT_LIMITS", ((2, 0.3),))
    # 가짜 서버는 ticker만 보내므로 필드가 다른 ticker 요청 셋(= 스테이지 셋)을 쓴다.
    reqs = [
        UpbitTickerReq(url=server.url),
        UpbitTickerReq(url=server.url, only_realtime=True),
        UpbitTickerReq(url=server.url + "/"),
    ]

    async with asyncio.timeout(2):
        for req in reqs:
            async with domain.stream(req, {"KRW-BTC"}) as stream:
                async for _ in stream:
                    break

    first, _, third = server.connected_at
    assert third - first >= 0.3 - 0.01


async def test_messages_on_one_connection_are_limited(
    domain: Domain, server: FakeUpbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_ws, "MESSAGE_LIMITS", ((1, 0.3),))

    async def ignore(_: object) -> None:
        pass

    async with domain.subscribe(UpbitTickerReq(url=server.url), ignore) as sub:
        await sub.update({"KRW-BTC"})
        async with asyncio.timeout(2):
            while not server.subscriptions:
                await asyncio.sleep(0.01)
            await sub.update({"KRW-BTC", "KRW-ETH"})
            while len(server.subscriptions) < 2:
                await asyncio.sleep(0.01)

    assert len(server.connections) == 1
    first, second = server.subscribed_at
    assert second - first >= 0.3 - 0.01
