from contextlib import asynccontextmanager
import os
import asyncio
from typing import Optional
import pydapper
import pydapper.exceptions

import aiopg

from fireagg import settings

DATABASE_URL = os.environ.get("DATABASE_URL")

NoResultException = pydapper.exceptions.NoResultException


async def create_pool(maxsize=10):
    url = settings.get().postgres_database_url
    assert url
    return await aiopg.create_pool(str(url), maxsize=maxsize)


DEFAULT_POOL_LOCK = asyncio.Lock()
DEFAULT_POOL: Optional[aiopg.Pool] = None


@asynccontextmanager
async def default_pool():
    global DEFAULT_POOL
    try:
        yield
    finally:
        async with DEFAULT_POOL_LOCK:
            if DEFAULT_POOL:
                DEFAULT_POOL.close()
                await DEFAULT_POOL.wait_closed()
                DEFAULT_POOL = None


@asynccontextmanager
async def connect_async(pool: Optional[aiopg.Pool] = None):
    global DEFAULT_POOL

    async with DEFAULT_POOL_LOCK:
        if not DEFAULT_POOL:
            DEFAULT_POOL = await create_pool()

    if not pool:
        pool = DEFAULT_POOL

    async with pool.acquire() as conn:
        async with pydapper.using_async(conn) as commands:
            yield commands


def connect():
    # postgresql://
    url = settings.get().postgres_database_url
    assert url
    return pydapper.connect(str(url))
