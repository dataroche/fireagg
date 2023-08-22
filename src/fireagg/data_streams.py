import asyncio
import logging
from typing import Iterable, Optional

from fireagg.database import db
from fireagg.connectors import create_connector, list_connectors, list_symbol_connectors

from fireagg.processing.core import ProcessingCore

logger = logging.getLogger(__name__)


async def seed_connectors(connectors: Optional[list[str]] = None):
    async with db.default_pool():
        if not connectors:
            connectors = list_connectors()

        connector_instances = [create_connector(name) for name in connectors]

        connector_batch_size = 4

        for i in range(0, len(connector_instances), connector_batch_size):
            batch = connector_instances[i : i + connector_batch_size]

            logger.info(
                f"Seeding {', '.join(connector.name for connector in batch)}..."
            )
            await asyncio.gather(*[connector.seed_markets() for connector in batch])


GOLD_CONNECTORS = {
    "kucoin",
    "huobi",
    "okx",
    "hitbtc",
    "gateio",
    "ascendex",
    "bitfinex",
    "coinbase",
    "bitstamp",
    "kraken",
    "bybit",
    "binance",
}


async def combine_connectors(
    symbols: Iterable[str], only_connectors: Optional[list[str]] = None
):
    async with db.default_pool():
        core = ProcessingCore()
        for symbol in symbols:
            if only_connectors is None:
                connectors = await list_symbol_connectors(symbol)
            else:
                connectors = only_connectors

            for connector_name in connectors:
                if connector_name not in GOLD_CONNECTORS:
                    continue
                connector = create_connector(connector_name)
                await core.watch_spreads(connector, symbol)
                await core.watch_trades(connector, symbol)

        await core.run()


async def watch_spreads(connector_name: str, symbol: str):
    async with db.default_pool():
        core = ProcessingCore()
        connector = create_connector(connector_name)
        await core.watch_spreads(connector, symbol)

        await core.run()
