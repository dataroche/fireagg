import datetime
from decimal import Decimal
import pydantic


class SymbolSpreads(pydantic.BaseModel):
    connector: str
    symbol_id: int
    timestamp: datetime.datetime
    fetch_timestamp: datetime.datetime

    best_bid: Decimal
    best_ask: Decimal


class SymbolTrade(pydantic.BaseModel):
    connector: str
    symbol_id: int
    timestamp: datetime.datetime
    fetch_timestamp: datetime.datetime

    price: Decimal
    amount: Decimal

    is_buy: bool
