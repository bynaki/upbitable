"""요청 수 제한기."""

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable

type Window = tuple[int, float]
"""(최대 횟수, 창 길이(초)). 창 길이 안에서 최대 횟수까지 허용한다."""


class RateLimiter:
    """창 여러 개를 함께 지키는 제한기. 예: 초당 5회 그리고 분당 100회.

    마지막 요청 시각을 창마다 기억하는 미끄러지는 창이다. 락을 쓰지 않는다(한 이벤트 루프 안에서
    대기 뒤 다시 확인하고 자리를 잡는다). 그래서 여러 이벤트 루프가 같은 제한기를 써도 된다.
    """

    def __init__(
        self,
        *windows: Window,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._windows = [(limit, period, deque[float](maxlen=limit)) for limit, period in windows]
        self._clock = clock
        self._sleep = sleep

    async def acquire(self) -> None:
        """모든 창에 자리가 날 때까지 기다린 뒤 한 번을 쓴다."""

        while True:
            now = self._clock()
            wait = 0.0
            for limit, period, stamps in self._windows:
                if len(stamps) == limit:
                    wait = max(wait, stamps[0] + period - now)
            if wait <= 0:
                for _, _, stamps in self._windows:
                    stamps.append(now)
                return
            await self._sleep(wait)
