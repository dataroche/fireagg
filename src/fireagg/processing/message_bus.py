from typing import Optional, Protocol

from .kv_adapter import LastValueKVStore

from .queue_adapter import QueueAdapter, AsyncioQueueAdapter
from .messages import (
    SymbolTrade,
    SymbolSpreads,
    SymbolWeightAdjust,
    SymbolTrueMidPrice,
)


class MessageBus(Protocol):
    trades: QueueAdapter[SymbolTrade]
    spreads: QueueAdapter[SymbolSpreads]
    weights: QueueAdapter[SymbolWeightAdjust]
    true_prices: QueueAdapter[SymbolTrueMidPrice]

    last_true_prices: Optional[LastValueKVStore[SymbolTrueMidPrice]] = None

    async def init(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return


class AsyncioMessageBus(MessageBus):
    def __init__(self):
        self.trades = AsyncioQueueAdapter[SymbolTrade]()
        self.spreads = AsyncioQueueAdapter[SymbolSpreads]()
        self.weights = AsyncioQueueAdapter[SymbolWeightAdjust]()
        self.true_prices = AsyncioQueueAdapter[SymbolTrueMidPrice]()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return
