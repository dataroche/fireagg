import asyncio
from typing import Optional
import ccxt.async_support as ccxt

from fireagg.connectors import list_connectors, create_connector


async def seed_connectors(connectors: Optional[list[str]] = None):
    if not connectors:
        connectors = list_connectors()

    connector_instances = [create_connector(name) for name in connectors]

    return await asyncio.gather(
        *[connector.seed_markets() for connector in connector_instances]
    )
