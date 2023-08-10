import asyncio
import math
from decimal import Decimal
from typing import Iterable, Optional

import pypeln
from aiostream import stream

from fireagg.database.symbol_prices import SymbolPriceInput

from fireagg.connectors import create_connector, list_connectors, list_symbol_connectors


async def seed_connectors(connectors: Optional[list[str]] = None):
    if not connectors:
        connectors = list_connectors()

    connector_instances = [create_connector(name) for name in connectors]

    return await asyncio.gather(
        *[connector.seed_markets() for connector in connector_instances]
    )


async def watch_symbol(connector_name: str, symbol: str):
    connector = create_connector(connector_name)
    await connector.watch_market_last_price(symbol)


async def combine_connectors(symbols: Iterable[str]):
    data_streams = []
    for symbol in symbols:
        connectors = await list_symbol_connectors(symbol)

        for connector_name in connectors:
            connector = create_connector(connector_name)
            data_streams.append(connector.iter_watch_market_last_price(symbol))

    combine = stream.merge(*data_streams)
    # combinator = PriceCombinator()
    async with combine.stream() as streamer:
        stage = pypeln.task.each(noop, streamer)
        await stage


def noop(price: SymbolPriceInput):
    pass


class PriceCombinator:
    def __init__(self):
        self.prices: dict[str, Decimal] = {}

    def update(self, price: SymbolPriceInput):
        self.prices[price.connector] = price.price
        prices = list(self.prices.values())
        avg_price = sum(prices) / len(prices)
        print(f"Average: {avg_price}")
