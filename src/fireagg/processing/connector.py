import asyncio
from contextlib import asynccontextmanager
import datetime
from typing import Optional
import pytz

from fireagg.database import symbol_prices, symbols

from fireagg.connectors.base import Connector, MidPrice

from .base import Worker


class ConnectorProducer(Worker):
    def __init__(
        self,
        connector: Connector,
        symbol: str,
        price_queue: asyncio.Queue[symbol_prices.SymbolPriceInput],
    ):
        self.connector = connector
        self.symbol = symbol
        self.symbol_mapping: Optional[symbols.ConnectorSymbolMapping] = None
        self.price_queue = price_queue
        self.is_live = False

    async def init(self):
        self.symbol_mapping = (
            await self.connector.seed_and_get_connector_symbol_mapping(self.symbol)
        )

    def mark_alive(self):
        assert self.symbol_mapping
        if not self.is_live:
            self.connector.logger.info(f"{self.symbol_mapping.symbol} is live!")
            self.is_live = True

    @asynccontextmanager
    async def error_handling(self):
        if not self.symbol_mapping:
            raise ValueError("Call init first")

        try:
            yield self.symbol_mapping
        except ValueError:
            # Not a supported connector.
            await self.connector.mark_symbol_mapping(self.symbol_mapping, is_error=True)
        except Exception as e:
            err_msg = str(e)
            if len(err_msg) > 200:
                err_msg = err_msg[:200] + "..."

            self.connector.logger.warning(
                f"Unable to watch {self.connector.name} {self.symbol}: {err_msg}"
            )
            return


class ConnectorSymbolPriceInputProducer(ConnectorProducer):
    async def run(self):
        async with self.error_handling() as symbol_mapping:
            async for last_price in self.connector.do_watch_market_last_price(
                symbol_mapping.connector_symbol
            ):
                if not last_price.timestamp_ms:
                    continue
                self.mark_alive()
                timestamp = datetime.datetime.fromtimestamp(
                    last_price.timestamp_ms / 1000.0, tz=pytz.UTC
                )
                fetch_timestamp = pytz.UTC.localize(datetime.datetime.utcnow())

                await self.price_queue.put(
                    symbol_prices.SymbolPriceInput(
                        connector=self.connector.name,
                        symbol_id=self.symbol_mapping.symbol_id,
                        timestamp=timestamp,
                        fetch_timestamp=fetch_timestamp,
                        last_price=last_price.price,
                    )
                )


class ConnectorOrderBookProducer(ConnectorProducer):
    async def run(self):
        last: Optional[MidPrice] = None
        async with self.error_handling() as symbol_mapping:
            async for mid_price in self.connector.do_watch_mid_price(
                symbol_mapping.connector_symbol
            ):
                if (
                    last
                    and last.best_bid == mid_price.best_bid
                    and last.best_ask == mid_price.best_ask
                ):
                    continue

                if not mid_price.timestamp_ms:
                    continue
                self.mark_alive()

                timestamp = datetime.datetime.fromtimestamp(
                    mid_price.timestamp_ms / 1000.0, tz=pytz.UTC
                )
                fetch_timestamp = pytz.UTC.localize(datetime.datetime.utcnow())

                await self.price_queue.put(
                    symbol_prices.SymbolPriceInput(
                        connector=self.connector.name,
                        symbol_id=self.symbol_mapping.symbol_id,
                        timestamp=timestamp,
                        fetch_timestamp=fetch_timestamp,
                        mid_price=(mid_price.best_ask + mid_price.best_bid) / 2,
                        best_bid=mid_price.best_bid,
                        best_ask=mid_price.best_ask,
                    )
                )
                last = mid_price
