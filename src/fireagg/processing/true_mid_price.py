import asyncio
import datetime
from decimal import Decimal
import logging
from typing import Optional

import numpy as np
import pandas as pd

from .base import Worker
from .queue_adapter import MessageBus
from .messages import SymbolTrueMidPrice, now_ms

logger = logging.getLogger(__name__)


class TrueMidPrice(Worker):
    def __init__(self, bus: MessageBus):
        super().__init__()
        self.bus = bus
        self.symbols: dict[int, SymbolTrueMidPriceProcessor] = {}

    async def run(self):
        self.running = True
        asyncio.create_task(self.run_weights_monitor())
        with self.bus.spreads.queue() as queue:
            logger.info(f"{self} is live!")
            while self.running:
                spread = await queue.get()
                symbol = self.symbols.get(spread.symbol_id)
                if symbol:
                    mid_price = (spread.best_ask + spread.best_bid) / 2
                    true_mid_price = symbol.predict_if_changed(
                        spread.connector, mid_price
                    )
                    if true_mid_price:
                        await self.bus.true_prices.put(
                            SymbolTrueMidPrice(
                                symbol_id=spread.symbol_id,
                                timestamp_ms=now_ms(),
                                true_mid_price=true_mid_price,
                                triggering_spread_message_id=spread.id,
                            )
                        )

    async def run_weights_monitor(self):
        with self.bus.weights.queue() as queue:
            while self.running:
                weight = await queue.get()

                if weight.symbol_id not in self.symbols:
                    self.symbols[weight.symbol_id] = SymbolTrueMidPriceProcessor(
                        weight.symbol_id
                    )

                self.symbols[weight.symbol_id].set_connector_weight(
                    weight.connector, weight.weight
                )


class SymbolTrueMidPriceProcessor:
    def __init__(self, symbol_id: int):
        self.symbol_id = symbol_id
        self.weights = pd.Series()
        self._normalized_weights = pd.Series()
        self.last_mid_prices = pd.Series()
        self.last_true_mid_price: Optional[Decimal] = None

    def set_connector_weight(self, connector: str, weight: float):
        # Store as Decimal for compatibility with the prices
        self.weights[connector] = weight
        try:
            self.last_mid_prices[connector] = self.last_mid_prices[connector]
        except KeyError:
            self.last_mid_prices[connector] = np.NaN

    def predict_if_changed(
        self, connector: str, mid_price: Decimal
    ) -> Optional[Decimal]:
        self.last_mid_prices[connector] = float(mid_price)
        prices = self.last_mid_prices.dropna()
        missing_connectors = prices.index.difference(self.weights.index)
        if not missing_connectors.empty:
            self.weights[missing_connectors] = 0.0

        weights = self.weights[prices.index]

        _normalized_weights = weights / weights.sum()

        if _normalized_weights.empty:
            true_mid_price = Decimal(np.NaN)
        else:
            true_mid_price = Decimal(prices.dot(_normalized_weights))

        if true_mid_price != self.last_true_mid_price:
            self.last_true_mid_price = true_mid_price
            return true_mid_price
