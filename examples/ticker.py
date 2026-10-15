"""Upbit 실서버에서 KRW-BTC·KRW-ETH 현재가를 몇 건 받아 출력한다.

실행
    uv run examples/ticker.py
"""

import asyncio

from trading_core import Domain, cast_model

from upbitable import UpbitTicker, UpbitTickerReq

COUNT = 6
"""받을 메시지 수."""


async def main() -> None:
    domain = Domain()
    await domain.start()
    try:
        received = 0
        async with domain.stream(UpbitTickerReq(), {"KRW-BTC", "KRW-ETH"}) as stream:
            async for data in stream:
                ticker = cast_model(data, UpbitTicker)
                print(ticker.symbol, ticker.stream_type, ticker.trade_price, ticker.change)
                received += 1
                if received == COUNT:
                    break
    finally:
        await domain.stop()


if __name__ == "__main__":
    asyncio.run(main())
