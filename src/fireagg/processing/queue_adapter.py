import asyncio
from contextlib import contextmanager
from typing import Generic, Iterator, TypeVar, Protocol

from asyncio_multisubscriber_queue import MultisubscriberQueue
from .messages import Message

T = TypeVar("T", bound=Message)


class QueueAdapter(Protocol, Generic[T]):
    async def put(self, obj: T):
        raise NotImplementedError()

    @contextmanager
    def queue(self) -> Iterator[asyncio.Queue[T]]:
        raise NotImplementedError()


class AsyncioQueueAdapter(MultisubscriberQueue[T], QueueAdapter[T]):
    pass
