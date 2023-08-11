import asyncio
import logging

from fireagg.database import db, symbol_prices

from .core import Worker

logger = logging.getLogger(__name__)


class SymbolPriceInputConsumer(Worker):
    def __init__(
        self,
        queue: asyncio.Queue[symbol_prices.SymbolPriceInput],
        sleep_delay: float = 0.02,
    ):
        self.queue = queue
        self.sleep_delay = sleep_delay

    async def run(self):
        try:
            while True:
                prices = await self.get_as_much_as_possible()

                if prices:
                    async with db.connect_async() as commands:
                        await symbol_prices.update_prices(commands, prices)

                    for _ in range(len(prices)):
                        self.queue.task_done()
                else:
                    await asyncio.sleep(self.sleep_delay)
        finally:
            raise RuntimeError("Consumer exited")

    async def get_as_much_as_possible(self):
        prices = []
        while True:
            try:
                prices.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        return prices
