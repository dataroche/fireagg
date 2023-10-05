import asyncio
import logging
from typing import Iterable, Optional

from fireagg.database import db, symbols
from fireagg.input_streams import create_connector, list_symbol_connectors

from fireagg.processing.core import ProcessingCore
from fireagg.processing.redis_adapter import RedisStreamsMessageBus, redis_client

logger = logging.getLogger(__name__)

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
    "_benchmark",
}


async def db_benchmark():
    async with db.default_pool():
        core = ProcessingCore()
        await core.consume_streams_to_db()


async def seed_connectors(connectors: Optional[list[str]] = None):
    async with db.default_pool():
        return await _do_seed_connectors(connectors)


async def _do_seed_connectors(connectors: Optional[list[str]] = None):
    if not connectors:
        connectors = list(GOLD_CONNECTORS)

    connector_instances = [create_connector(name) for name in connectors]

    connector_batch_size = 4

    for i in range(0, len(connector_instances), connector_batch_size):
        batch = connector_instances[i : i + connector_batch_size]

        logger.info(f"Seeding {', '.join(connector.name for connector in batch)}...")
        await asyncio.gather(*[connector.seed_markets() for connector in batch])


async def combine_connectors(
    symbols: Iterable[str], only_connectors: Optional[list[str]] = None
):
    async with db.default_pool():
        core = ProcessingCore()
        await core.consume_streams_to_db()
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


async def distributed_core():
    async with db.default_pool():
        await _distributed_bootstrap()

        core = ProcessingCore(bus=get_distributed_bus())
        await core.consume_streams_to_db()
        await core.run()


async def _distributed_bootstrap():
    async with db.connect_async() as commands:
        db_symbols = await symbols.get_all(commands)

    if not db_symbols:
        await _do_seed_connectors()


async def distributed_watch_symbols(
    symbols: Iterable[str], only_connectors: Optional[list[str]] = None
):
    async with db.default_pool():
        core = ProcessingCore(bus=get_distributed_bus())
        for symbol in symbols:
            if only_connectors is None:
                connectors = await list_symbol_connectors(symbol)
            else:
                connectors = only_connectors

            connectors = [c for c in connectors if c in GOLD_CONNECTORS]

            if not connectors:
                raise RuntimeError(f"Symbol {symbol} has no connectors.")

            for connector_name in connectors:
                if connector_name not in GOLD_CONNECTORS:
                    continue
                connector = create_connector(connector_name)
                await core.watch_spreads(connector, symbol)
                await core.watch_trades(connector, symbol)

        await core.run()


def get_distributed_bus():
    return RedisStreamsMessageBus(redis_client())
