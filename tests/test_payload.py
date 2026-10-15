"""요청 → 구독 메시지."""

import json
from decimal import Decimal

from trading_core import get_model_type

from upbitable import (
    UpbitCandleReq,
    UpbitOrderbookReq,
    UpbitTickerReq,
    UpbitTradeReq,
)
from upbitable._ws import encode


def test_ticker_and_trade_send_sorted_codes() -> None:
    symbols = {"KRW-ETH", "KRW-BTC"}
    assert UpbitTickerReq().subscription(symbols) == {
        "type": "ticker",
        "codes": ["KRW-BTC", "KRW-ETH"],
    }
    assert UpbitTradeReq().subscription(symbols)["type"] == "trade"


def test_only_realtime_sets_is_only_realtime() -> None:
    obj = UpbitTickerReq(only_realtime=True).subscription({"KRW-BTC"})
    assert obj["is_only_realtime"] is True
    assert "is_only_realtime" not in UpbitTickerReq().subscription({"KRW-BTC"})


def test_orderbook_size_suffixes_codes_and_level_is_a_json_number() -> None:
    req = UpbitOrderbookReq(size=15, level=Decimal("10000"))
    obj = req.subscription({"KRW-BTC", "KRW-ETH"})
    assert obj["codes"] == ["KRW-BTC.15", "KRW-ETH.15"]
    sent = json.loads(encode([{"ticket": "t"}, obj, {"format": "DEFAULT"}]))
    assert sent[1]["level"] == 10000
    assert json.loads(encode([{"level": Decimal("0.1")}]))[0]["level"] == 0.1


def test_orderbook_without_size_sends_plain_codes() -> None:
    obj = UpbitOrderbookReq().subscription({"KRW-BTC"})
    assert obj == {"type": "orderbook", "codes": ["KRW-BTC"]}


def test_candle_type_includes_unit() -> None:
    assert UpbitCandleReq(unit="1m").subscription({"KRW-BTC"})["type"] == "candle.1m"


def test_requests_are_registered_as_source() -> None:
    for req in (UpbitTickerReq(), UpbitTradeReq(), UpbitOrderbookReq(), UpbitCandleReq(unit="1s")):
        assert get_model_type(req) == "source"


def test_request_fields_decide_sharing() -> None:
    assert UpbitTickerReq().tr_content_id == UpbitTickerReq().tr_content_id
    assert UpbitTickerReq().tr_content_id != UpbitTickerReq(only_realtime=True).tr_content_id
    assert UpbitCandleReq(unit="1m").tr_content_id != UpbitCandleReq(unit="3m").tr_content_id
