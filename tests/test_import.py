import trading_core

import upbitable


def test_imports_trading_core() -> None:
    assert trading_core.__version__
    assert "UpbitTickerReq" in upbitable.__all__
