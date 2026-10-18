"""Private 테스트가 함께 쓰는 자격 증명과 실패 수집기."""

import asyncio
from pathlib import Path

import pytest
from trading_core import DataModel, StageFailed
from trading_core.credentials import CREDENTIALS_ENV

ACCOUNT = "upbit-test"
ACCESS = "test-access-key"
SECRET = "test-secret-key-that-must-not-leak-" + "x" * 32
"""PyJWT가 짧은 HS512 키에 경고하므로 64바이트를 넘게 둔다."""
TIMEOUT = 2.0


def write_credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """`ACCOUNT` 자격 증명 파일을 만들고 환경변수로 가리킨다."""

    path = tmp_path / "credentials.toml"
    path.write_text(
        f"""
[{ACCOUNT}]
exchange = "upbit"
pocket = "main"
access_key = "{ACCESS}"
secret_key = "{SECRET}"
""",
        encoding="utf-8",
    )
    path.chmod(0o600)
    monkeypatch.setenv(CREDENTIALS_ENV, str(path))
    return path


class Failures:
    """`StageFailed`를 모으는 실패 콜백."""

    def __init__(self) -> None:
        self.received: list[StageFailed] = []

    async def __call__(self, failed: StageFailed) -> None:
        self.received.append(failed)

    async def wait(self) -> StageFailed:
        async with asyncio.timeout(TIMEOUT):
            while not self.received:
                await asyncio.sleep(0.005)
        return self.received[0]


async def ignore(_: DataModel) -> None:
    pass
