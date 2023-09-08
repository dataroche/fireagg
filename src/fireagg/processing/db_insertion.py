import asyncio
import platform
from contextlib import contextmanager
import logging
import time
from typing import Generic, TypeVar
from pydapper.commands import CommandsAsync

from fireagg.database import db, symbol_prices

from .core import Worker
from .queue_adapter import QueueAdapter
from .messages import Message, SymbolSpreads, SymbolTrade, SymbolTrueMidPrice

from fireagg.metrics import get_db_inserts_counter


logger = logging.getLogger(__name__)

QueueT = TypeVar("QueueT", bound=Message)


class DatabaseStreamQueue(Worker, Generic[QueueT]):
    name: str

    def __init__(
        self,
        multi_queue: QueueAdapter[QueueT],
        sleep_delay: float = 0.02,
    ):
        super().__init__()
        self.multi_queue = multi_queue
        self.sleep_delay = sleep_delay
        self.local_throughput_counter = 0
        self.local_throughput_log_interval = 5

        self.throughput_counter = get_db_inserts_counter(
            worker=str(self), stream_name=self.name
        )

    async def flush(self, commands: CommandsAsync, records: list[QueueT]):
        raise NotImplementedError()

    async def run(self):
        self.running = True
        throughput_task = asyncio.create_task(self.run_throughput_monitor())
        try:
            async with await db.create_pool(maxsize=1) as priority_pool:
                # We create our own connection pool to not share the connection with
                # other less important parts of the code.
                with self.multi_queue.queue() as queue:
                    logger.info(f"{self} is live!")
                    while self.running:
                        records = await self.get_as_much_as_possible(queue)

                        if records:
                            with warn_if_too_long("flush"):
                                async with db.connect_async(priority_pool) as commands:
                                    await self.flush(commands, records)

                            self.local_throughput_counter += len(records)
                            self.throughput_counter.inc(len(records))

                            for _ in range(len(records)):
                                queue.task_done()
                        else:
                            await asyncio.sleep(self.sleep_delay)
        finally:
            throughput_task.cancel()
            raise RuntimeError("Consumer exited")

    async def run_throughput_monitor(self):
        while self.running:
            await asyncio.sleep(self.local_throughput_log_interval)

            logger.info(
                f"{self.__class__.__name__} processed {self.local_throughput_counter or 'no'} records in the last {self.local_throughput_log_interval}s."
            )
            self.local_throughput_counter = 0

    async def get_as_much_as_possible(self, queue: asyncio.Queue):
        prices = []
        with warn_if_too_long("messages"):
            while True:
                try:
                    prices.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

        return prices


@contextmanager
def warn_if_too_long(for_what: str, too_long: float = 1):
    now = time.monotonic()
    yield
    delta = time.monotonic() - now
    if delta > too_long:
        logger.warning(f"Waited {delta:.2f}s for {for_what}!")


class DatabaseStreamTrades(DatabaseStreamQueue[SymbolTrade]):
    name = "trades"

    async def flush(self, commands: CommandsAsync, records: list[SymbolTrade]):
        await symbol_prices.insert_symbol_trades(
            commands, trades=[trade.model_dump() for trade in records]
        )


class DatabaseStreamSpreads(DatabaseStreamQueue[SymbolSpreads]):
    name = "spreads"

    async def flush(self, commands: CommandsAsync, records: list[SymbolSpreads]):
        await symbol_prices.insert_symbol_spreads(
            commands, spreads=[spread.model_dump() for spread in records]
        )


class DatabaseStreamTrueMidPrice(DatabaseStreamQueue[SymbolTrueMidPrice]):
    name = "mid_prices"

    async def flush(self, commands: CommandsAsync, records: list[SymbolTrueMidPrice]):
        await symbol_prices.insert_symbol_true_mid_price(
            commands, mid_prices=[spread.model_dump() for spread in records]
        )
