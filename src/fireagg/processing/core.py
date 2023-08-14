import asyncio
import logging
from asyncio_multisubscriber_queue import MultisubscriberQueue

from fireagg.connectors.base import Connector

from .base import Worker
from .connector import SymbolTradesProducer, SymbolSpreadsProducer
from .db_insertion import DatabaseStreamTrades, DatabaseStreamSpreads
from .messages import SymbolSpreads, SymbolTrade

logger = logging.getLogger(__name__)


class ProcessingCore:
    def __init__(self, launch_workers=5):
        self.worker_queue = asyncio.Queue[Worker]()

        self.active_workers: dict[asyncio.Task, Worker] = {}

        self.trades = MultisubscriberQueue[SymbolTrade]()
        self.spreads = MultisubscriberQueue[SymbolSpreads]()

        self.trades_consumer = DatabaseStreamTrades(self.trades)
        self.spreads_consumer = DatabaseStreamSpreads(self.spreads)

        self.launch_workers = launch_workers
        self.is_running = True

    async def watch_trades(self, connector: Connector, symbol: str):
        await self.put_worker(
            SymbolTradesProducer(connector, symbol, queue=self.trades)
        )

    async def watch_spreads(self, connector: Connector, symbol: str):
        await self.put_worker(
            SymbolSpreadsProducer(connector, symbol, queue=self.spreads)
        )

    async def put_worker(self, producer: Worker):
        await self.worker_queue.put(producer)

    async def run(self):
        launcher_tasks = [
            self._run_worker_launcher_task() for _ in range(self.launch_workers)
        ]
        processing_tasks = [self.trades_consumer.run(), self.spreads_consumer.run()]

        await asyncio.gather(*processing_tasks, *launcher_tasks)

    async def _run_worker_launcher_task(self):
        while self.is_running:
            worker_to_launch = await self.worker_queue.get()
            logger.info(f"Launching {worker_to_launch}...")
            await worker_to_launch.init()
            task = asyncio.create_task(worker_to_launch.run())
            self.active_workers[task] = worker_to_launch
