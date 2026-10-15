"""테스트 공통 픽스처."""

from collections.abc import AsyncIterator

import pytest
from trading_core import Domain

from tests.support.fake_upbit import FakeUpbit, fake_upbit
from upbitable import _ws


@pytest.fixture(autouse=True)
def fast_ws(monkeypatch: pytest.MonkeyPatch) -> None:
    """전송 간격과 재연결 대기를 줄여 테스트를 빠르게 한다."""

    monkeypatch.setattr(_ws, "SEND_INTERVAL", 0.0)
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
