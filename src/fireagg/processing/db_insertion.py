import asyncio
import logging
from typing import Generic, TypeVar

from asyncio_multisubscriber_queue import MultisubscriberQueue

from fireagg.database import db, symbol_prices

from .core import Worker
from .messages import SymbolSpreads, SymbolTrade


logger = logging.getLogger(__name__)

QueueT = TypeVar("QueueT")


class DatabaseStreamQueue(Worker, Generic[QueueT]):
    def __init__(
        self,
        multi_queue: MultisubscriberQueue[QueueT],
        sleep_delay: float = 0.02,
    ):
        self.multi_queue = multi_queue
        self.sleep_delay = sleep_delay

    async def flush(self, records: list[QueueT]):
        raise NotImplementedError()

    async def run(self):
        try:
            with self.multi_queue.queue() as queue:
                while True:
                    records = await self.get_as_much_as_possible(queue)

                    if records:
                        await self.flush(records)

                        for _ in range(len(records)):
                            queue.task_done()
                    else:
                        await asyncio.sleep(self.sleep_delay)
        finally:
            raise RuntimeError("Consumer exited")

    async def get_as_much_as_possible(self, queue: asyncio.Queue):
        prices = []
        while True:
            try:
                prices.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        return prices


class DatabaseStreamTrades(DatabaseStreamQueue[SymbolTrade]):
    async def flush(self, records: list[SymbolTrade]):
        async with db.connect_async() as commands:
            await symbol_prices.insert_symbol_trades(
                commands, trades=[trade.model_dump() for trade in records]
            )


class DatabaseStreamSpreads(DatabaseStreamQueue[SymbolSpreads]):
    async def flush(self, records: list[SymbolSpreads]):
        async with db.connect_async() as commands:
            await symbol_prices.insert_symbol_spreads(
                commands, spreads=[spread.model_dump() for spread in records]
            )
