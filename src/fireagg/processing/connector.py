import asyncio
from contextlib import asynccontextmanager
import datetime
from typing import Generic, Optional, TypeVar
import pytz

from asyncio_multisubscriber_queue import MultisubscriberQueue

from fireagg.database import symbol_prices, symbols

from fireagg.connectors.base import Connector, MidPrice

from .base import Worker
from .messages import SymbolSpreads, SymbolTrade


QueueT = TypeVar("QueueT")


class ConnectorProducer(Worker, Generic[QueueT]):
    producer_name: str

    def __init__(
        self,
        connector: Connector,
        symbol: str,
        queue: MultisubscriberQueue[QueueT],
    ):
        super().__init__()
        self.connector = connector
        self.symbol = symbol
        self.symbol_mapping: Optional[symbols.ConnectorSymbolMapping] = None
        self.queue = queue
        self.is_live = False
        self.running = False

    async def init(self):
        self.symbol_mapping = (
            await self.connector.seed_and_get_connector_symbol_mapping(self.symbol)
        )
        await self.connector.init()

    def is_live_callback(self):
        assert self.symbol_mapping
        self.connector.logger.info(f"{self.symbol_mapping.symbol} is live!")

    def __str__(self):
        return f"{self.__class__.__name__}({self.connector.name=}, {self.producer_name=}, {self.symbol=})"

    async def run(self):
        self.running = True
        while self.running:
            async with self.error_handling() as symbol_mapping:
                await self.run_symbol_mapping(symbol_mapping)
                # If we get here, we're meant to stop.
                self.running = False

    async def run_symbol_mapping(self, symbol_mapping: symbols.ConnectorSymbolMapping):
        raise NotImplementedError()

    @asynccontextmanager
    async def error_handling(self):
        if not self.symbol_mapping:
            raise ValueError("Call init first")

        try:
            yield self.symbol_mapping
        except NotImplementedError:
            # Not a supported connector.
            await self.connector.mark_symbol_mapping(
                self.symbol_mapping, is_unavailable=True
            )
            self.running = False
        except Exception as e:
            self.health_counter -= 1

            err_msg = str(e)
            if len(err_msg) > 200:
                err_msg = err_msg[:200] + "..."

            if self.health_counter <= 0:
                self.connector.logger.error(
                    f"Shutting down {self.producer_name} for {self.connector.name} {self.symbol}: {err_msg}"
                )
                self.running = False
            else:
                self.connector.logger.warning(
                    f"Error in {self.producer_name} for {self.connector.name} {self.symbol}. "
                    f"health={self.health_counter}: {err_msg}"
                )
                await asyncio.sleep(1)


class SymbolTradesProducer(ConnectorProducer[SymbolTrade]):
    producer_name = "trades"

    async def run_symbol_mapping(self, symbol_mapping: symbols.ConnectorSymbolMapping):
        async for trade in self.connector.do_watch_trades(
            symbol_mapping.connector_symbol
        ):
            if not trade.timestamp_ms:
                continue

            self.mark_alive()
            timestamp = datetime.datetime.fromtimestamp(
                trade.timestamp_ms / 1000.0, tz=pytz.UTC
            )
            fetch_timestamp = pytz.UTC.localize(datetime.datetime.utcnow())

            await self.queue.put(
                SymbolTrade(
                    connector=self.connector.name,
                    symbol_id=self.symbol_mapping.symbol_id,
                    timestamp=timestamp,
                    fetch_timestamp=fetch_timestamp,
                    price=trade.price,
                    amount=trade.amount,
                    is_buy=trade.is_buy,
                )
            )


class SymbolSpreadsProducer(ConnectorProducer[SymbolSpreads]):
    producer_name = "spreads"

    async def run_symbol_mapping(self, symbol_mapping: symbols.ConnectorSymbolMapping):
        last: Optional[MidPrice] = None
        async for mid_price in self.connector.do_watch_spreads(
            symbol_mapping.connector_symbol
        ):
            if (
                last
                and last.best_bid == mid_price.best_bid
                and last.best_ask == mid_price.best_ask
            ):
                # Skip no-op updates
                continue

            if not mid_price.timestamp_ms:
                continue
            self.mark_alive()

            timestamp = datetime.datetime.fromtimestamp(
                mid_price.timestamp_ms / 1000.0, tz=pytz.UTC
            )
            fetch_timestamp = pytz.UTC.localize(datetime.datetime.utcnow())

            await self.queue.put(
                SymbolSpreads(
                    connector=self.connector.name,
                    symbol_id=self.symbol_mapping.symbol_id,
                    timestamp=timestamp,
                    fetch_timestamp=fetch_timestamp,
                    best_bid=mid_price.best_bid,
                    best_ask=mid_price.best_ask,
                )
            )
            last = mid_price
