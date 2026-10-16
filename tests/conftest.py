"""테스트 공통 픽스처."""

from collections.abc import AsyncIterator

import pytest
from trading_core import Domain

from tests.support.fake_upbit import FakeUpbit, fake_upbit
from upbitable import _ws


@pytest.fixture(autouse=True)
def fast_ws(monkeypatch: pytest.MonkeyPatch) -> None:
    """요청 수 제한을 풀고 재연결 대기를 줄여 테스트를 빠르게 한다.

    연결 제한기는 프로세스 전역이라 테스트마다 비운다. 제한을 시험하는 테스트는 한도를 다시 정한다.
    """

    monkeypatch.setattr(_ws, "CONNECT_LIMITS", ())
    monkeypatch.setattr(_ws, "MESSAGE_LIMITS", ())
    monkeypatch.setattr(_ws, "_connect_limiters", {})
    monkeypatch.setattr(_ws, "RETRY_BASE_DELAY", 0.01)


@pytest.fixture
async def domain() -> AsyncIterator[Domain]:
    d = Domain()
    await d.start()
    try:
        yield d
    finally:
        await d.stop()


@pytest.fixture
async def server() -> AsyncIterator[FakeUpbit]:
    async with fake_upbit() as fake:
        yield fake
