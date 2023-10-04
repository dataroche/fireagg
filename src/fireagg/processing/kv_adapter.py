import asyncio
import logging
from typing import Callable, Generic, Optional, TypeVar, Type

from .queue_adapter import QueueAdapter
from .messages import (
    Message,
)

T = TypeVar("T", bound=Message)

logger = logging.getLogger(__name__)


class LastValueKVStore(Generic[T]):
    def __init__(
        self,
        name: str,
        queue_type: Type[T],
        queue: QueueAdapter[T],
        key_fn: Callable[[T], str],
    ):
        self.name = name

        self.queue_type = queue_type
        self.queue = queue
        self.key_fn = key_fn

        self.sleep_delay = 0.02

    async def run(self):
        self.running = True
        try:
            with self.queue.queue() as queue:
                logger.info(f"{self} is live!")
                while self.running:
                    last: Optional[T] = await self.get_last(queue)

                    if last:
                        key = self.key_fn(last)
                        await self.set(key, last)
                    else:
                        await asyncio.sleep(self.sleep_delay)
        finally:
            raise RuntimeError("LastValueKVStore exited")

    async def get_last(self, queue: asyncio.Queue):
        data = []
        while True:
            try:
                data.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        return data[-1] if data else None

    async def get(self, key: str) -> Optional[T]:
        raise NotImplementedError()

    async def set(self, key: str, value: T):
        raise NotImplementedError()
