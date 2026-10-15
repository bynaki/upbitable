"""Upbit 실서버에서 KRW-BTC 1분 캔들을 몇 건 받아 출력한다.

실행
    uv run examples/candle.py
"""

import asyncio

from trading_core import Domain, cast_model

from upbitable import UpbitCandle, UpbitCandleReq

COUNT = 6
"""받을 메시지 수."""


async def main() -> None:
    domain = Domain()
    await domain.start()
    try:
        received = 0
        async with domain.stream(UpbitCandleReq(unit="1m"), {"KRW-BTC"}) as stream:
            async for data in stream:
                candle = cast_model(data, UpbitCandle)
                print(
                    candle.symbol,
                    candle.stream_type,
                    candle.candle_date_time_kst,
                    candle.opening_price,
                    candle.high_price,
                    candle.low_price,
                    candle.trade_price,
                    candle.candle_acc_trade_volume,
                )
                received += 1
                if received == COUNT:
                    break
    finally:
        await domain.stop()


if __name__ == "__main__":
    asyncio.run(main())
