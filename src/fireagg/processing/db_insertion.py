import asyncio
import logging
from typing import Generic, TypeVar
from pydapper.commands import CommandsAsync

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
        self.throughput_counter = 0
        self.throughput_log_interval = 30

    async def flush(self, commands: CommandsAsync, records: list[QueueT]):
        raise NotImplementedError()

    async def run(self):
        throughput_task = asyncio.create_task(self.run_throughput_monitor())
        try:
            async with await db.create_pool(maxsize=1) as priority_pool:
                # We create our own connection pool to not share the connection with
                # other less important parts of the code.
                with self.multi_queue.queue() as queue:
                    while True:
                        records = await self.get_as_much_as_possible(queue)

                        if records:
                            async with db.connect_async(priority_pool) as commands:
                                await self.flush(commands, records)

                            self.throughput_counter += len(records)

                            for _ in range(len(records)):
                                queue.task_done()
                        else:
                            await asyncio.sleep(self.sleep_delay)
        finally:
            throughput_task.cancel()
            raise RuntimeError("Consumer exited")

    async def run_throughput_monitor(self):
        while True:
            await asyncio.sleep(self.throughput_log_interval)

            if self.throughput_counter:
                logger.info(
                    f"{self.__class__.__name__} processed {self.throughput_counter} records in the last {self.throughput_log_interval}s."
                )
            self.throughput_counter = 0

    async def get_as_much_as_possible(self, queue: asyncio.Queue):
        prices = []
        while True:
            try:
                prices.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        return prices


class DatabaseStreamTrades(DatabaseStreamQueue[SymbolTrade]):
    async def flush(self, commands: CommandsAsync, records: list[SymbolTrade]):
        await symbol_prices.insert_symbol_trades(
            commands, trades=[trade.model_dump() for trade in records]
        )


class DatabaseStreamSpreads(DatabaseStreamQueue[SymbolSpreads]):
    async def flush(self, commands: CommandsAsync, records: list[SymbolSpreads]):
        await symbol_prices.insert_symbol_spreads(
            commands, spreads=[spread.model_dump() for spread in records]
        )
