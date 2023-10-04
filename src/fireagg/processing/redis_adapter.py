import asyncio
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Type, Union, cast
import pydantic

import redis.asyncio
from redis.typing import FieldT, EncodableT
from asyncio_multisubscriber_queue import MultisubscriberQueue

from fireagg import settings

from .message_bus import MessageBus
from .queue_adapter import QueueAdapter, T
from .kv_adapter import LastValueKVStore

from .messages import (
    SymbolTrade,
    SymbolSpreads,
    SymbolWeightAdjust,
    SymbolTrueMidPrice,
)


def redis_client(**kwargs):
    url = settings.get().redis_url
    return redis.asyncio.Redis.from_url(str(url), **kwargs)


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
        await self.redis.xadd(
            self.stream_key, {self.DATA_KEY: redis_encode_pydantic(obj)}
        )

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
                    model = redis_decode_pydantic(self.queue_type, raw_data)
                    await self._output_queue.put(model)


def redis_encode_pydantic(obj: pydantic.BaseModel):
    return cast(EncodableT, obj.model_dump_json())


def redis_decode_pydantic(cls: Type[T], encoded: Any):
    return cls.model_validate_json(encoded)


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

        # self.last_true_prices = RedisKVStore(
        #     client,
        #     name="true_mid_prices",
        #     queue_type=SymbolTrueMidPrice,
        #     queue=self.true_prices,
        #     key_fn=lambda tp: str(tp.symbol_id),
        # )

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
        if self.last_true_prices:
            self._tasks.append(asyncio.create_task(self.last_true_prices.run()))

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, *args):
        for t in self._tasks:
            t.cancel()
        await self.redis.close()


class RedisKVStore(LastValueKVStore[T]):
    def __init__(
        self,
        client: redis.asyncio.Redis,
        name: str,
        queue_type: Type[T],
        queue: QueueAdapter[T],
        key_fn: Callable[[T], str],
    ):
        super().__init__(name=name, queue_type=queue_type, queue=queue, key_fn=key_fn)
        self.redis = client

    def _key_name(self, key: str):
        return f"{self.name}__{key}"

    async def get(self, key: str) -> T:
        redis_key = self._key_name(key)
        value = await self.redis.get(redis_key)
        return value and redis_decode_pydantic(self.queue_type, value)

    async def set(self, key: str, value: T):
        redis_key = self._key_name(key)
        await self.redis.set(redis_key, redis_encode_pydantic(value))
