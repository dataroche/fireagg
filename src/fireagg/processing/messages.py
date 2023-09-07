import time
import uuid
from decimal import Decimal

import pydantic


# TODO(will):
# Track the source message that created this message.
# If we are generating
class Message(pydantic.BaseModel):
    id: str = pydantic.Field(default_factory=lambda: uuid.uuid1().hex)


def now_ms():
    return time.time() * 1000.0


class SymbolSpreads(Message):
    connector: str
    symbol_id: int
    timestamp_ms: float
    fetch_timestamp_ms: float

    best_bid: Decimal
    best_ask: Decimal


class SymbolTrade(Message):
    connector: str
    symbol_id: int
    timestamp_ms: float
    fetch_timestamp_ms: float

    price: Decimal
    amount: Decimal

    is_buy: bool


class SymbolWeightAdjust(Message):
    connector: str
    symbol_id: int

    weight: float


class SymbolTrueMidPrice(Message):
    symbol_id: int
    timestamp_ms: float

    true_mid_price: Decimal
    triggering_spread_message_id: str
