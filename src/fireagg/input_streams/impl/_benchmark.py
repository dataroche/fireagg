import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import AsyncIterator, Optional, TypedDict

from fireagg.input_streams.base import Connector, Market, MidPrice, Trade
from fireagg.database import symbols

from fireagg import settings

TRADES_PER_SECOND_TARGET = settings.get().benchmark_trades_per_second_target or 100


class _benchmark(Connector):
    name = "_benchmark"

    async def do_seed_markets(self, skip_if_symbols=True):
        return [
            symbols.ConnectorSymbolInput(
                symbol="seesaw/synthetic",
                connector_symbol="seesaw/synthetic",
                connector=self.name,
                base_asset="seesaw",
                quote_asset="synthetic",
            )
        ]

    async def do_watch_trades(self, connector_symbol: str) -> AsyncIterator[Trade]:
        if connector_symbol == "seesaw/synthetic":
            async for trade in self.synthetic_seesaw(
                Decimal(1), top_price=Decimal("1.5")
            ):
                yield trade
        else:
            raise NotImplementedError(connector_symbol)

    async def synthetic_seesaw(
        self,
        init_price: Decimal,
        top_price: Decimal,
        ticks_per_direction: int = 10000,
    ):
        trades_per_second_target = TRADES_PER_SECOND_TARGET

        ticks_per_direction = ticks_per_direction // 2
        tick_size = (top_price - init_price) / ticks_per_direction
        bottom_price = init_price - (tick_size * ticks_per_direction)

        current_price = init_price
        direction = 1

        target_loop_time = 1 / trades_per_second_target
        sleep_time = target_loop_time
        previous_trade: Optional[Trade] = None

        while self.running:
            trade = _random_trade_at_price(current_price, amount_range=1000)
            yield trade

            if previous_trade:
                delta_s = (trade.timestamp_ms - previous_trade.timestamp_ms) / 1000
                error = target_loop_time - delta_s
                sleep_time = target_loop_time + error

            previous_trade = trade

            current_price += tick_size * direction

            if direction > 0 and current_price > top_price:
                direction = -1
            elif direction < 0 and current_price < bottom_price:
                direction = 1
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    async def do_watch_spreads(self, connector_symbol: str):
        if connector_symbol == "seesaw/synthetic":
            async for trade in self.synthetic_seesaw(
                Decimal(1), top_price=Decimal("1.5")
            ):
                yield MidPrice(
                    timestamp_ms=trade.timestamp_ms,
                    best_bid=trade.price - Decimal("0.001"),
                    best_ask=trade.price + Decimal("0.001"),
                )
        else:
            raise NotImplementedError(connector_symbol)

    async def do_get_market(self, connector_symbol: str) -> Market:
        return Market(close=Decimal(1), volume_24h=10000)


def _random_trade_at_price(price: Decimal, amount_range: float):
    return Trade(
        timestamp_ms=_now_ms(),
        price=price,
        amount=Decimal(random.random() * amount_range),
        is_buy=(random.choice([False, True])),
    )


def _now_ms():
    return time.time() * 1000
