"""Upbit 실서버에서 KRW-BTC·KRW-ETH 체결을 몇 건 받아 출력한다.

실행
    uv run examples/trade.py
"""

import asyncio

from trading_core import Domain, cast_model

from upbitable import UpbitTrade, UpbitTradeReq

COUNT = 6
"""받을 메시지 수."""


async def main() -> None:
    domain = Domain()
    await domain.start()
    try:
        received = 0
        async with domain.stream(UpbitTradeReq(), {"KRW-BTC", "KRW-ETH"}) as stream:
            async for data in stream:
                trade = cast_model(data, UpbitTrade)
                print(
                    trade.symbol,
                    trade.stream_type,
                    trade.ask_bid,
                    f"{trade.trade_price:.8f}",
                    f"{trade.trade_volume:.8f}",
                )
                received += 1
                if received == COUNT:
                    break
    finally:
        await domain.stop()


if __name__ == "__main__":
    asyncio.run(main())
