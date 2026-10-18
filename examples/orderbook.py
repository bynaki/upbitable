"""Upbit 실서버에서 KRW-BTC 호가를 5단위로 몇 건 받아 최우선 호가를 출력한다.

실행
    uv run examples/orderbook.py
"""

import asyncio

from trading_core import Domain, cast_model

from upbitable import UpbitOrderbook, UpbitOrderbookReq

COUNT = 6
"""받을 메시지 수."""


async def main() -> None:
    domain = Domain()
    await domain.start()
    try:
        received = 0
        async with domain.stream(UpbitOrderbookReq(size=5), {"KRW-BTC"}) as stream:
            async for data in stream:
                book = cast_model(data, UpbitOrderbook)
                best = book.orderbook_units[0]
                print(
                    book.symbol,
                    book.stream_type,
                    f"units={len(book.orderbook_units)}",
                    f"ask={best.ask_price:.8f}({best.ask_size:.8f})",
                    f"bid={best.bid_price:.8f}({best.bid_size:.8f})",
                )
                received += 1
                if received == COUNT:
                    break
    finally:
        await domain.stop()


if __name__ == "__main__":
    asyncio.run(main())
