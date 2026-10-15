"""응답 → 데이터 모델. 문서의 수신 예제를 JSON으로 보냈다가 받는다."""

import json
from decimal import Decimal

import pytest

from tests.support.samples import CANDLE, ORDERBOOK, TICKER, TRADE
from upbitable import UpbitCandle, UpbitOrderbook, UpbitTicker, UpbitTrade, UpbitWsError
from upbitable._ws import decode


def roundtrip(msg: dict) -> dict:
    return decode(json.dumps(msg).encode())


def test_ticker_uses_code_as_symbol_and_keeps_decimals() -> None:
    ticker = UpbitTicker.from_ws(roundtrip(TICKER))
    assert ticker.symbol == "KRW-BTC"
    assert ticker.trade_price == Decimal("109868000.0")
    assert ticker.trade_volume == Decimal("0.00009101")
    assert ticker.acc_trade_price_24h == Decimal("198189163442.74924")
    assert ticker.timestamp == 1787729042606
    assert ticker.delisting_date is None
    assert ticker.stream_type == "SNAPSHOT"


def test_unknown_and_deprecated_fields_are_dropped() -> None:
    ticker = UpbitTicker.from_ws(roundtrip({**TICKER, "new_field": 1}))
    dumped = ticker.model_dump()
    assert "new_field" not in dumped
    assert "market_warning" not in dumped
    assert "code" not in dumped


def test_trade() -> None:
    trade = UpbitTrade.from_ws(roundtrip(TRADE))
    assert trade.sequential_id == 17877285547970000
    assert trade.best_ask_price == Decimal(109867000)
    assert trade.ask_bid == "ASK"


def test_orderbook_units() -> None:
    book = UpbitOrderbook.from_ws(roundtrip(ORDERBOOK))
    assert book.level == Decimal(10000)
    assert book.orderbook_units[0].ask_size == Decimal("0.00342134")
    assert len(book.orderbook_units) == 2


def test_candle() -> None:
    candle = UpbitCandle.from_ws(roundtrip(CANDLE))
    assert candle.type == "candle.1m"
    assert candle.candle_acc_trade_price == Decimal("78269528.64757")


def test_error_message_raises() -> None:
    raw = json.dumps({"error": {"name": "NO_CODES", "message": "codes 필드 누락"}})
    with pytest.raises(UpbitWsError) as info:
        decode(raw)
    assert info.value.name == "NO_CODES"
