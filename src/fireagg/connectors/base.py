import datetime
import logging
import pytz
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import AsyncIterator, NamedTuple

from fireagg.database import db, symbol_prices, symbols


class LastPrice(NamedTuple):
    timestamp_ms: float
    price: Decimal


class Connector(ABC):
    name: str

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"fireagg.connectors.impl.{self.name}")
        self.running = True

    async def seed_markets(self, on_error="ignore"):
        try:
            symbols_input = await self.do_seed_markets()
        except Exception as e:
            if on_error == "ignore":
                err_msg = str(e)
                if len(err_msg) > 200:
                    err_msg = err_msg[:200] + "..."
                self.logger.info(f"Unable to load markets for {self.name}: {err_msg}")
                return
            else:
                raise

        if not symbols_input:
            return

        self.logger.info(f"Loading {len(symbols_input)} symbols for {self.name}")
        async with db.connect_async() as commands:
            await symbols.upsert_many(commands, symbols_input)
            self.logger

    @abstractmethod
    async def do_seed_markets(self) -> list[symbols.ConnectorSymbolInput]:
        raise NotImplementedError()

    async def watch_market_last_price(self, symbol: str):
        async for _ in self.iter_watch_market_last_price(symbol):
            pass

    async def iter_watch_market_last_price(self, symbol: str, on_error="ignore"):
        symbol_mapping = await self.seed_and_get_connector_symbol_mapping(symbol)

        try:
            async for last_price in self.do_watch_market_last_price(
                symbol_mapping.connector_symbol
            ):
                if not last_price.timestamp_ms:
                    continue
                timestamp = datetime.datetime.fromtimestamp(
                    last_price.timestamp_ms / 1000.0, tz=pytz.UTC
                )
                fetch_timestamp = pytz.UTC.localize(datetime.datetime.utcnow())
                symbol_price = symbol_prices.SymbolPriceInput(
                    connector=self.name,
                    symbol_id=symbol_mapping.symbol_id,
                    timestamp=timestamp,
                    fetch_timestamp=fetch_timestamp,
                    price=last_price.price,
                )
                async with db.connect_async() as commands:
                    update_timestamp = await symbol_prices.update_price(
                        commands, symbol_price
                    )
                    fetch_latency = (fetch_timestamp - timestamp).total_seconds()
                    db_update_latency = (update_timestamp - timestamp).total_seconds()
                    self.logger.info(
                        f"Updated {self.name} {symbol}. Latency = {fetch_latency:.3f}s (fetch), {db_update_latency:.3f}s (update)"
                    )
                    yield symbol_price
        except Exception as e:
            if on_error == "ignore":
                err_msg = str(e)
                if len(err_msg) > 200:
                    err_msg = err_msg[:200] + "..."
                self.logger.warning(f"Unable to watch {self.name} {symbol}: {err_msg}")
                return
            else:
                raise

    async def seed_and_get_connector_symbol_mapping(self, symbol: str, _retried=False):
        async with db.connect_async() as commands:
            try:
                return await symbols.get_connector_symbol_mapping(
                    commands, self.name, symbol
                )
            except db.NoResultException:
                if _retried:
                    raise
                await self.seed_markets(on_error="raise")
                return await self.seed_and_get_connector_symbol_mapping(
                    symbol=symbol, _retried=True
                )

    @abstractmethod
    def do_watch_market_last_price(
        self, connector_symbol: str
    ) -> AsyncIterator[LastPrice]:
        raise NotImplementedError()


async def list_symbol_connectors(symbol: str):
    async with db.connect_async() as commands:
        return await symbols.get_symbol_connectors(commands, symbol)
