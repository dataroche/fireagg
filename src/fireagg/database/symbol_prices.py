import datetime
import pytz
from decimal import Decimal

import pydantic
from pydapper.commands import CommandsAsync


class SymbolPriceInput(pydantic.BaseModel):
    connector: str
    symbol_id: int
    timestamp: datetime.datetime
    fetch_timestamp: datetime.datetime
    price: Decimal


async def update_price(commands: CommandsAsync, symbol_price_input: SymbolPriceInput):
    update_timestamp: datetime.datetime = await commands.execute_scalar_async(
        """
        INSERT INTO history.symbol_prices_history (connector, symbol_id, timestamp, price, update_timestamp, fetch_timestamp)
        VALUES (?connector?, ?symbol_id?, ?timestamp?, ?price?, NOW(), ?fetch_timestamp?);

        INSERT INTO realtime.symbol_prices (connector, symbol_id, timestamp, price, update_timestamp, fetch_timestamp)
        VALUES (?connector?, ?symbol_id?, ?timestamp?, ?price?, NOW(), ?fetch_timestamp?)
        ON CONFLICT (connector, symbol_id)
        DO UPDATE SET
            timestamp=?timestamp?,
            price=?price?,
            update_timestamp=NOW()
        RETURNING update_timestamp;
        """,
        param=symbol_price_input.model_dump(),
    )
    return pytz.UTC.localize(update_timestamp)
