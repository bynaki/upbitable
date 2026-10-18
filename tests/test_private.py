"""Private WebSocket(myOrder·myAsset) — 자격 증명, JWT, 구독 메시지, 모델, 가짜 서버와의 흐름."""

import asyncio
from decimal import Decimal
from pathlib import Path

import jwt
import pytest
from pydantic import ValidationError
from trading_core import Domain, cast_model

from tests.support.auth import ACCESS, SECRET, TIMEOUT, Failures, ignore, write_credentials
from tests.support.fake_upbit import FakeUpbit
from tests.support.samples import MY_ASSET, MY_ORDER
from upbitable import (
    MY_ASSET_SYMBOL,
    UpbitCredential,
    UpbitMyAsset,
    UpbitMyAssetReq,
    UpbitMyOrder,
    UpbitMyOrderReq,
)
from upbitable.private import make_token


@pytest.fixture
def credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    return write_credentials(tmp_path, monkeypatch)


@pytest.fixture
def auth_server(server: FakeUpbit) -> FakeUpbit:
    server.secrets = {ACCESS: SECRET}
    return server


def cred() -> UpbitCredential:
    return UpbitCredential.model_validate(
        {"exchange": "upbit", "pocket": "main", "access_key": ACCESS, "secret_key": SECRET}
    )


# ----- 자격 증명과 JWT -----


def test_credential_hides_secrets() -> None:
    c = cred()
    assert SECRET not in repr(c)
    assert ACCESS not in repr(c)


def test_credential_rejects_other_exchange() -> None:
    with pytest.raises(ValidationError):
        UpbitCredential.model_validate(
            {"exchange": "bithumb", "pocket": "main", "access_key": "a", "secret_key": "b"}
        )


def test_token_is_hs512_jwt_verifiable_by_pyjwt() -> None:
    token = make_token(cred())

    assert jwt.get_unverified_header(token) == {"alg": "HS512", "typ": "JWT"}
    payload = jwt.decode(token, SECRET, algorithms=["HS512"])
    assert payload["access_key"] == ACCESS
    assert payload["nonce"]


def test_token_nonce_changes_every_time() -> None:
    a = jwt.decode(make_token(cred()), SECRET, algorithms=["HS512"])
    b = jwt.decode(make_token(cred()), SECRET, algorithms=["HS512"])
    assert a["nonce"] != b["nonce"]


# ----- 구독 메시지와 모델 -----


def test_my_order_subscription() -> None:
    req = UpbitMyOrderReq(account="upbit-test")
    assert req.subscription({"KRW-ETH", "KRW-BTC"}) == {
        "type": "myOrder",
        "codes": ["KRW-BTC", "KRW-ETH"],
    }


def test_my_asset_subscription_has_no_codes() -> None:
    assert UpbitMyAssetReq(account="upbit-test").subscription({MY_ASSET_SYMBOL}) == {
        "type": "myAsset"
    }


def test_account_is_part_of_content_id() -> None:
    a = UpbitMyOrderReq(account="upbit-main")
    b = UpbitMyOrderReq(account="upbit-sub1")
    assert a.tr_content_id != b.tr_content_id


def test_my_order_model() -> None:
    order = UpbitMyOrder.from_ws(MY_ORDER)

    assert order.symbol == "KRW-BTC"
    assert order.state == "trade"
    assert order.price == Decimal("0.001453")
    assert order.time_in_force is None
    assert order.is_maker is True


def test_my_order_model_accepts_missing_trade_fields() -> None:
    """체결이 아닌 이벤트(예: 시장가 매도 접수)는 가격·체결 필드가 비어 있다."""

    msg = {
        "type": "myOrder",
        "code": "KRW-BTC",
        "uuid": "u",
        "ask_bid": "ASK",
        "order_type": "market",
        "state": "wait",
        "price": None,
        "order_timestamp": 1,
        "timestamp": 2,
        "stream_type": "REALTIME",
    }
    order = UpbitMyOrder.from_ws(msg)
    assert order.price is None
    assert order.trade_uuid is None


def test_my_asset_model() -> None:
    asset = UpbitMyAsset.from_ws(MY_ASSET)

    assert asset.symbol == MY_ASSET_SYMBOL
    assert asset.assets[0].currency == "KRW"
    assert isinstance(asset.assets[0].balance, Decimal)


# ----- 가짜 서버 -----


async def test_my_order_stream_sends_valid_token(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    req = UpbitMyOrderReq(account="upbit-test", url=auth_server.url)
    async with asyncio.timeout(TIMEOUT), domain.stream(req, {"KRW-BTC"}) as stream:
        data = await anext(aiter(stream))
    order = cast_model(data, UpbitMyOrder)

    assert order.symbol == "KRW-BTC"
    assert auth_server.tokens[0]["access_key"] == ACCESS
    assert auth_server.subscriptions[0][1][1] == {"type": "myOrder", "codes": ["KRW-BTC"]}


async def test_my_asset_stream(domain: Domain, auth_server: FakeUpbit, credentials: Path) -> None:
    req = UpbitMyAssetReq(account="upbit-test", url=auth_server.url)
    async with asyncio.timeout(TIMEOUT), domain.stream(req, {MY_ASSET_SYMBOL}) as stream:
        data = await anext(aiter(stream))
    asset = cast_model(data, UpbitMyAsset)

    assert asset.assets[0].currency == "KRW"
    assert auth_server.subscriptions[0][1][1] == {"type": "myAsset"}


async def test_reconnect_uses_a_fresh_token(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    req = UpbitMyOrderReq(account="upbit-test", url=auth_server.url)
    async with domain.subscribe(req, ignore) as sub:
        await sub.update({"KRW-BTC"})
        async with asyncio.timeout(TIMEOUT):
            while not auth_server.tokens:
                await asyncio.sleep(0.005)
            await auth_server.drop_all()
            while len(auth_server.tokens) < 2:
                await asyncio.sleep(0.005)

    first, second = auth_server.tokens[:2]
    assert first["nonce"] != second["nonce"]


async def test_my_asset_rejects_other_symbols(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    failures = Failures()
    req = UpbitMyAssetReq(account="upbit-test", url=auth_server.url)
    async with domain.subscribe(req, ignore, failures) as sub:
        await sub.update({"KRW"})
        failed = await failures.wait()

    assert failed.symbols == {"KRW"}
    assert MY_ASSET_SYMBOL in failed.cause


async def test_unknown_account_fails_the_stage(
    domain: Domain, auth_server: FakeUpbit, credentials: Path
) -> None:
    failures = Failures()
    req = UpbitMyOrderReq(account="nope", url=auth_server.url)
    async with domain.subscribe(req, ignore, failures) as sub:
        await sub.update({"KRW-BTC"})
        failed = await failures.wait()

    assert "CredentialError" in failed.cause
    assert SECRET not in failed.cause
    assert auth_server.connections == []


async def test_wrong_secret_fails_with_invalid_auth(
    domain: Domain, server: FakeUpbit, credentials: Path
) -> None:
    server.secrets = {ACCESS: "another-secret-" + "y" * 64}
    failures = Failures()
    req = UpbitMyOrderReq(account="upbit-test", url=server.url)
    async with domain.subscribe(req, ignore, failures) as sub:
        await sub.update({"KRW-BTC"})
        failed = await failures.wait()

    assert "INVALID_AUTH" in failed.cause
    assert SECRET not in failed.cause
