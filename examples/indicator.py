"""Upbit 실서버에서 KRW-BTC 5분봉의 RSI·MACD 값을 기다렸다가 출력한다.

준비
    1. Upbit에서 API 키를 발급하고, 이 컴퓨터의 공인 IP를 허용 IP에 넣는다. 지표는 키에 권한이
       없어도 받을 수 있다.
    2. 자격 증명 파일을 만들고 소유자만 읽게 한다.
           mkdir -p ~/.config/trading
           chmod 700 ~/.config/trading
           $EDITOR ~/.config/trading/credentials.toml
           chmod 600 ~/.config/trading/credentials.toml
       내용:
           [upbit-main]
           exchange = "upbit"
           pocket = "main"
           access_key = "<Access Key>"
           secret_key = "<Secret Key>"

실행
    uv run examples/indicator.py

스냅샷이 없어 5분봉이 처음 마감될 때까지(최대 5분) 기다린다. 마감되면 지표마다 메시지가 하나씩 온다.
"""

import asyncio

from trading_core import Domain, cast_model

from upbitable import Macd, Rsi, UpbitIndicator, UpbitIndicatorReq

ACCOUNT = "upbit-main"
"""자격 증명 파일에서 읽을 이름."""

COUNT = 2
"""받을 메시지 수(지표 2개 × 마켓 1개 = 캔들 하나)."""


async def main() -> None:
    domain = Domain()
    await domain.start()
    try:
        req = UpbitIndicatorReq(
            account=ACCOUNT,
            timeframe="5m",
            indicators=(Rsi(period=14), Macd(fast=12, slow=26, signal_period=9)),
        )
        received = 0
        async with domain.stream(req, {"KRW-BTC"}) as stream:
            print("KRW-BTC 5분봉이 마감되기를 기다린다(최대 5분)...")
            async for data in stream:
                ind = cast_model(data, UpbitIndicator)
                print(ind.symbol, ind.candle_date_time_utc, ind.indicator, ind.params, ind.value)
                received += 1
                if received == COUNT:
                    break
    finally:
        await domain.stop()


if __name__ == "__main__":
    asyncio.run(main())
