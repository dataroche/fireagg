import asyncio
import logging
from typing import Optional
from asyncio_multisubscriber_queue import MultisubscriberQueue

from fireagg.input_streams.base import Connector

from .base import Worker
from .connector import SymbolTradesProducer, SymbolSpreadsProducer
from .db_insertion import (
    DatabaseStreamTrades,
    DatabaseStreamSpreads,
    DatabaseStreamTrueMidPrice,
)
from .message_bus import MessageBus, AsyncioMessageBus
from .redis_adapter import RedisStreamsMessageBus, redis_client
from .true_mid_price import TrueMidPrice

logger = logging.getLogger(__name__)


class ProcessingCore:
    def __init__(self, launch_workers=5, bus: Optional[MessageBus] = None):
        self.worker_queue = asyncio.Queue[Worker]()

        self.active_workers: dict[asyncio.Task, Worker] = {}

        self.bus: MessageBus = bus or AsyncioMessageBus()
        # self.bus = AsyncioMessageBus()
        # self.bus = RedisStreamsMessageBus(redis_client())

        self.launch_workers = launch_workers
        self.is_running = True

    async def watch_trades(self, connector: Connector, symbol: str):
        await self.put_worker(
            SymbolTradesProducer(connector, symbol, bus=self.bus, retry_forever=True)
        )

    async def watch_spreads(self, connector: Connector, symbol: str):
        await self.put_worker(
            SymbolSpreadsProducer(connector, symbol, bus=self.bus, retry_forever=True)
        )

    async def consume_streams_to_db(self):
        await self.put_worker(
            DatabaseStreamTrades(self.bus.trades),
            DatabaseStreamSpreads(self.bus.spreads),
            DatabaseStreamTrueMidPrice(self.bus.true_prices),
            TrueMidPrice(self.bus),
        )

    async def put_worker(self, *workers: Worker):
        for worker in workers:
            await self.worker_queue.put(worker)

    async def run(self):
        launcher_tasks = [
            self._run_worker_launcher_task() for _ in range(self.launch_workers)
        ]

        async with self.bus:
            await asyncio.gather(*launcher_tasks)

    async def _run_worker(self, worker: Worker):
        try:
            return await worker.run()
        except Exception as e:
            logger.error(e)

    async def _run_worker_launcher_task(self):
        while self.is_running:
            worker_to_launch = await self.worker_queue.get()
            logger.info(f"Launching {worker_to_launch}...")
            try:
                await worker_to_launch.init()
            except Exception as e:
                logger.warning(f"Error during init of {worker_to_launch}: {str(e)}")
                continue
            task = asyncio.create_task(self._run_worker(worker_to_launch))
            self.active_workers[task] = worker_to_launch
