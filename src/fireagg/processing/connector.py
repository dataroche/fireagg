import asyncio
from contextlib import asynccontextmanager
import datetime
from typing import Generic, Optional, TypeVar
import pytz

from fireagg.database import symbols

from fireagg.input_streams.base import Connector, MidPrice

from .base import Worker
from .message_bus import MessageBus
from .messages import SymbolSpreads, SymbolTrade, SymbolWeightAdjust, now_ms


QueueT = TypeVar("QueueT")


class ConnectorProducer(Worker, Generic[QueueT]):
    producer_name: str

    def __init__(
        self,
        connector: Connector,
        symbol: str,
        bus: MessageBus,
        retry_forever: bool = False,
    ):
        super().__init__()
        self.connector = connector
        self.symbol = symbol
        self.symbol_mapping: Optional[symbols.ConnectorSymbolMapping] = None
        self.bus = bus
        self.is_live = False
        self.running = False
        self.retry_forever = retry_forever

        self.weight_task: Optional[asyncio.Task] = None

    async def init(self):
        self.symbol_mapping = (
            await self.connector.seed_and_get_connector_symbol_mapping(self.symbol)
        )
        await self.connector.init()
        await self._adjust_weight()

    async def _adjust_weight(self):
        assert self.symbol_mapping
        ticker = await self.connector.do_get_market(
            self.symbol_mapping.connector_symbol
        )

        await self.bus.weights.put(
            SymbolWeightAdjust(
                connector=self.connector.name,
                symbol_id=self.symbol_mapping.symbol_id,
                weight=float(ticker.volume_24h),
            )
        )

    def is_live_callback(self):
        self.connector.logger.info(f"{self} is live!")

    def __str__(self):
        return f"{self.__class__.__name__}({self.connector.name=}, {self.producer_name=}, {self.symbol=})"

    async def run(self):
        self.running = True
        while self.running:
            async with self.error_handling() as symbol_mapping:
                weight_task = asyncio.create_task(self.run_adjust_weight())
                await self.run_symbol_mapping(symbol_mapping)
                # If we get here, we're meant to stop.
                self.running = False

                weight_task.cancel()

                await self.bus.weights.put(
                    SymbolWeightAdjust(
                        connector=self.connector.name,
                        symbol_id=self.symbol_mapping.symbol_id,
                        weight=0.0,
                    )
                )

    async def run_adjust_weight(self):
        while self.running:
            await asyncio.sleep(500)
            await self._adjust_weight()

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

            if self.health_counter <= 0 and not self.retry_forever:
                self.connector.logger.error(
                    f"Shutting down {self.producer_name} for {self.connector.name} {self.symbol}: {err_msg}"
                )
                self.running = False
            else:
                sleep_s = max(1 - self.health_counter, 5)
                self.connector.logger.warning(
                    f"Error in {self.producer_name} for {self.connector.name} {self.symbol}. "
                    f"health={self.health_counter}, sleep={sleep_s}: {err_msg}"
                )
                await asyncio.sleep(sleep_s)


class SymbolTradesProducer(ConnectorProducer[SymbolTrade]):
    producer_name = "trades"

    async def run_symbol_mapping(self, symbol_mapping: symbols.ConnectorSymbolMapping):
        async for trade in self.connector.do_watch_trades(
            symbol_mapping.connector_symbol
        ):
            if not trade.timestamp_ms:
                continue

            self.mark_alive()

            await self.bus.trades.put(
                SymbolTrade(
                    connector=self.connector.name,
                    symbol_id=self.symbol_mapping.symbol_id,
                    timestamp_ms=trade.timestamp_ms,
                    fetch_timestamp_ms=now_ms(),
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

            await self.bus.spreads.put(
                SymbolSpreads(
                    connector=self.connector.name,
                    symbol_id=self.symbol_mapping.symbol_id,
                    timestamp_ms=mid_price.timestamp_ms,
                    fetch_timestamp_ms=now_ms(),
                    best_bid=mid_price.best_bid,
                    best_ask=mid_price.best_ask,
                )
            )
            last = mid_price
