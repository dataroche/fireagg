import asyncio
from contextlib import contextmanager
from typing import Generic, Iterator, TypeVar, Protocol

from asyncio_multisubscriber_queue import MultisubscriberQueue
from .messages import (
    Message,
    SymbolTrade,
    SymbolSpreads,
    SymbolWeightAdjust,
    SymbolTrueMidPrice,
)

T = TypeVar("T", bound=Message)


class QueueAdapter(Protocol, Generic[T]):
    async def put(self, obj: T):
        raise NotImplementedError()

    @contextmanager
    def queue(self) -> Iterator[asyncio.Queue[T]]:
        raise NotImplementedError()


class AsyncioQueueAdapter(MultisubscriberQueue[T], QueueAdapter[T]):
    pass


class MessageBus(Protocol):
    trades: QueueAdapter[SymbolTrade]
    spreads: QueueAdapter[SymbolSpreads]
    weights: QueueAdapter[SymbolWeightAdjust]
    true_prices: QueueAdapter[SymbolTrueMidPrice]

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
