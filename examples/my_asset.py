"""Upbit 실서버에서 내 자산 변동을 기다렸다가 한 건 출력한다.

준비는 `my_order.py`와 같다. 키에는 "자산조회" 권한이 있어야 한다.

실행
    uv run examples/my_asset.py

이벤트는 잔고가 바뀔 때만 오고, 처음 구독한 뒤 몇 분은 늦게 올 수 있다. 기다리는 동안 업비트에서
주문을 넣었다 취소하면 묶인 수량(locked)이 바뀌어 이벤트가 온다.
"""

import asyncio

from trading_core import Domain, cast_model

from upbitable import MY_ASSET_SYMBOL, UpbitMyAsset, UpbitMyAssetReq

ACCOUNT = "upbit-main"
"""자격 증명 파일에서 읽을 이름."""


async def main() -> None:
    domain = Domain()
    await domain.start()
    try:
        async with domain.stream(UpbitMyAssetReq(account=ACCOUNT), {MY_ASSET_SYMBOL}) as stream:
            print("자산 변동을 기다린다...")
            async for data in stream:
                asset = cast_model(data, UpbitMyAsset)
                for unit in asset.assets:
                    print(unit.currency, f"balance={unit.balance:f}", f"locked={unit.locked:f}")
                break
    finally:
        await domain.stop()


if __name__ == "__main__":
    asyncio.run(main())
