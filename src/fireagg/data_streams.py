import asyncio
from typing import Iterable, Optional

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
            await core.watch_spreads(connector, symbol)
            await core.watch_trades(connector, symbol)

    await core.run()


async def watch_spreads(connector_name: str, symbol: str):
    core = ProcessingCore()
    connector = create_connector(connector_name)
    await core.watch_spreads(connector, symbol)

    await core.run()
