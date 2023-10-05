import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import AsyncIterator, NamedTuple

from fireagg.input_streams import __name__ as input_streams_name
from fireagg.database import db, symbols


class Trade(NamedTuple):
    timestamp_ms: float
    price: Decimal
    amount: Decimal
    is_buy: bool


class MidPrice(NamedTuple):
    timestamp_ms: float
    best_bid: Decimal
    best_ask: Decimal


class Market(NamedTuple):
    close: Decimal
    volume_24h: float


class Connector(ABC):
    name: str

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{input_streams_name}.impl.{self.name}")
        self.running = True

    async def seed_markets(self, on_error="ignore", skip_if_symbols=True):
        if skip_if_symbols:
            async with db.connect_async() as commands:
                mappings = await symbols.get_connector_symbols(
                    commands, connector=self.name
                )

            if mappings:
                return

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

        await self.update_symbol_mappings(symbols_input)

    async def mark_symbol_mapping(
        self, mapping: symbols.ConnectorSymbolMapping, is_unavailable: bool
    ):
        if is_unavailable:
            self.logger.warning(f"Disabling {self.name} {mapping.symbol}")
        async with db.connect_async() as commands:
            await symbols.mark_connector_symbol_mapping(
                commands,
                connector=mapping.connector,
                symbol_id=mapping.symbol_id,
                is_unavailable=is_unavailable,
            )

    async def update_symbol_mappings(
        self, symbol_mappings: list[symbols.ConnectorSymbolInput]
    ):
        self.logger.info(f"Loading {len(symbol_mappings)} symbols for {self.name}")
        async with db.connect_async() as commands:
            await symbols.upsert_many(commands, symbol_mappings)

    @abstractmethod
    async def do_seed_markets(self) -> list[symbols.ConnectorSymbolInput]:
        raise NotImplementedError()

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

    async def init(self):
        pass

    @abstractmethod
    def do_watch_trades(self, connector_symbol: str) -> AsyncIterator[Trade]:
        raise NotImplementedError()

    @abstractmethod
    def do_watch_spreads(self, connector_symbol: str) -> AsyncIterator[MidPrice]:
        raise NotImplementedError()

    async def do_get_market(self, connector_symbol: str) -> Market:
        raise NotImplementedError()


async def list_symbol_connectors(symbol: str):
    async with db.connect_async() as commands:
        return await symbols.get_symbol_connectors(commands, symbol)
