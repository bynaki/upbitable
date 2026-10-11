"""trading-core import 확인용 playground."""

import trading_core
from trading_core import Channel, DataModel, Domain

print(f"trading-core {trading_core.__version__}")
print(f"from: {trading_core.__file__}")
print(f"exports: {len(trading_core.__all__)}")

missing = [name for name in trading_core.__all__ if not hasattr(trading_core, name)]
assert not missing, f"missing exports: {missing}"

print(Channel, DataModel, Domain, sep="\n")
print("ok")
