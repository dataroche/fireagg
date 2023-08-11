import asyncio
from decimal import Decimal
from typing import Iterable, Optional

from aiostream import stream

from fireagg.database.symbol_prices import SymbolPriceInput

from fireagg.connectors import create_connector, list_connectors, list_symbol_connectors

from fireagg.processing.core import ProcessingCore


async def seed_connectors(connectors: Optional[list[str]] = None):
    if not connectors:
        connectors = list_connectors()

    connector_instances = [create_connector(name) for name in connectors]

    return await asyncio.gather(
        *[connector.seed_markets() for connector in connector_instances]
    )


async def combine_connectors(symbols: Iterable[str]):
    core = ProcessingCore()
    for symbol in symbols:
        connectors = await list_symbol_connectors(symbol)

        for connector_name in connectors:
            connector = create_connector(connector_name)
            await core.watch_order_book(connector, symbol)

    await core.run()


async def watch_order_book(connector_name: str, symbol: str):
    core = ProcessingCore()
    connector = create_connector(connector_name)
    await core.watch_order_book(connector, symbol)

    await core.run()


class PriceCombinator:
    def __init__(self):
        self.prices: dict[str, Decimal] = {}

    def update(self, price: SymbolPriceInput):
        self.prices[price.connector] = price.price
        prices = list(self.prices.values())
        avg_price = sum(prices) / len(prices)
        print(f"Average: {avg_price}")
