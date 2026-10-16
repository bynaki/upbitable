"""Upbit 실서버에서 내 주문·체결 이벤트를 기다렸다가 몇 건 출력한다.

준비
    1. Upbit에서 "주문조회" 권한만 준 API 키를 발급하고, 이 컴퓨터의 공인 IP를 허용 IP에 넣는다.
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
    uv run examples/my_order.py

이벤트는 주문을 내거나 취소하거나 체결될 때만 온다. 기다리는 동안 업비트에서 KRW-BTC 주문을
넣었다 취소하면 이벤트가 찍힌다.
"""

import asyncio

from trading_core import Domain, cast_model

from upbitable import UpbitMyOrder, UpbitMyOrderReq

ACCOUNT = "upbit-main"
"""자격 증명 파일에서 읽을 이름."""

COUNT = 2
"""받을 메시지 수."""


async def main() -> None:
    domain = Domain()
    await domain.start()
    try:
        received = 0
        async with domain.stream(UpbitMyOrderReq(account=ACCOUNT), {"KRW-BTC"}) as stream:
            print("KRW-BTC 주문 이벤트를 기다린다...")
            async for data in stream:
                order = cast_model(data, UpbitMyOrder)
                print(
                    order.symbol,
                    order.state,
                    order.ask_bid,
                    order.order_type,
                    order.price,
                    order.volume,
                    order.uuid,
                )
                received += 1
                if received == COUNT:
                    break
    finally:
        await domain.stop()


if __name__ == "__main__":
    asyncio.run(main())
