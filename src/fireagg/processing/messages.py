import datetime
import uuid
from decimal import Decimal

import pydantic


class Message(pydantic.BaseModel):
    id: str = pydantic.Field(default_factory=lambda: uuid.uuid1().hex)


class SymbolSpreads(Message):
    connector: str
    symbol_id: int
    timestamp: datetime.datetime
    fetch_timestamp: datetime.datetime

    best_bid: Decimal
    best_ask: Decimal


class SymbolTrade(Message):
    connector: str
    symbol_id: int
    timestamp: datetime.datetime
    fetch_timestamp: datetime.datetime

    price: Decimal
    amount: Decimal

    is_buy: bool


class SymbolWeightAdjust(Message):
    connector: str
    symbol_id: int

    weight: float


class SymbolTrueMidPrice(Message):
    symbol_id: int
    timestamp: datetime.datetime

    true_mid_price: Decimal
    triggering_spread_message_id: str
