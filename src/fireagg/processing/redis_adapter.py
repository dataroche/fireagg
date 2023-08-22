import asyncio
from contextlib import contextmanager
from typing import Iterator, Type, cast

import redis.asyncio
from redis.typing import FieldT, EncodableT
from asyncio_multisubscriber_queue import MultisubscriberQueue

from .queue_adapter import MessageBus, QueueAdapter, T
from .messages import (
    SymbolTrade,
    SymbolSpreads,
    SymbolWeightAdjust,
    SymbolTrueMidPrice,
)


def redis_client(**kwargs):
    return redis.asyncio.Redis(**kwargs)


class RedisStreamsQueue(QueueAdapter[T]):
    DATA_KEY = b"json"

    def __init__(
        self, client: redis.asyncio.Redis, queue_type: Type[T], stream_key: str
    ):
        self.redis = client
        self.queue_type = queue_type
        self.stream_key = stream_key

        self._output_queue = MultisubscriberQueue[T]()
        self.running = True

    async def put(self, obj: T):
        data = cast(dict[FieldT, EncodableT], {self.DATA_KEY: obj.model_dump_json()})
        await self.redis.xadd(self.stream_key, data)

    @contextmanager
    def queue(self) -> Iterator[asyncio.Queue[T]]:
        with self._output_queue.queue() as queue:
            yield queue

    async def run_reader(self):
        while self.running:
            streams = await self.redis.xread(streams={self.stream_key: "$"}, block=200)
            for stream in streams:
                stream_key, data = stream
                for obj in data:
                    msg_id, data = obj
                    raw_data = data[self.DATA_KEY]
                    await self._output_queue.put(
                        self.queue_type.model_validate_json(raw_data)
                    )


class RedisStreamsMessageBus(MessageBus):
    def __init__(self, client: redis.asyncio.Redis):
        self.redis = client
        self.trades = RedisStreamsQueue(client, SymbolTrade, "symbol_trades")
        self.spreads = RedisStreamsQueue(client, SymbolSpreads, "symbol_spreads")
        self.weights = RedisStreamsQueue(
            client, SymbolWeightAdjust, "connector_weights"
        )
        self.true_prices = RedisStreamsQueue(
            client, SymbolTrueMidPrice, "symbol_true_prices"
        )

        self._queues: list[RedisStreamsQueue] = [
            self.trades,
            self.spreads,
            self.weights,
            self.true_prices,
        ]
        self._tasks = []

    async def init(self):
        await self.redis.ping()

        self._tasks = [asyncio.create_task(q.run_reader()) for q in self._queues]

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, *args):
        for t in self._tasks:
            t.cancel()
        await self.redis.close()
