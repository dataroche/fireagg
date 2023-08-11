from contextlib import asynccontextmanager
import os
from typing import Optional
import pydapper
import pydapper.exceptions

import aiopg

DATABASE_URL = os.environ.get("DATABASE_URL")
POSTGRES_DATABASE_URL = os.environ.get("POSTGRES_DATABASE_URL")

NoResultException = pydapper.exceptions.NoResultException


async def create_pool(maxsize=10):
    return await aiopg.create_pool(DATABASE_URL, maxsize=maxsize)


DEFAULT_POOL: Optional[aiopg.Pool] = None


@asynccontextmanager
async def default_pool():
    try:
        yield
    finally:
        if DEFAULT_POOL:
            DEFAULT_POOL.close()
            await DEFAULT_POOL.wait_closed()


@asynccontextmanager
async def connect_async(pool: Optional[aiopg.Pool] = None):
    global DEFAULT_POOL
    assert DATABASE_URL

    if not DEFAULT_POOL:
        DEFAULT_POOL = await create_pool()

    if not pool:
        pool = DEFAULT_POOL

    async with pool.acquire() as conn:
        async with pydapper.using_async(conn) as commands:
            yield commands


def connect():
    # postgresql://
    assert POSTGRES_DATABASE_URL
    return pydapper.connect(POSTGRES_DATABASE_URL)
