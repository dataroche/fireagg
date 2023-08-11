import datetime
from typing import Optional
from decimal import Decimal

import pydantic
from pydapper.commands import CommandsAsync


class SymbolPriceInput(pydantic.BaseModel):
    connector: str
    symbol_id: int
    timestamp: datetime.datetime
    fetch_timestamp: datetime.datetime

    last_price: Optional[Decimal] = None
    best_bid: Optional[Decimal] = None
    mid_price: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None


async def update_prices(
    commands: CommandsAsync, symbol_price_inputs: list[SymbolPriceInput]
):
    await commands.execute_async(
        """
        INSERT INTO history.symbol_prices_history (
            connector,
            symbol_id,
            timestamp,
            last_price,
            best_bid,
            mid_price,
            best_ask,
            update_timestamp,
            fetch_timestamp
        )
        VALUES (
            ?connector?,
            ?symbol_id?,
            ?timestamp?,
            ?last_price?,
            ?best_bid?,
            ?mid_price?,
            ?best_ask?,
            NOW(),
            ?fetch_timestamp?
        );
        """,
        param=[symbol.model_dump() for symbol in symbol_price_inputs],
    )
