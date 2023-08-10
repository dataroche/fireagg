from contextlib import asynccontextmanager
import os
import pydapper
import pydapper.exceptions

import aiopg
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")
POSTGRES_DATABASE_URL = os.environ.get("POSTGRES_DATABASE_URL")

NoResultException = pydapper.exceptions.NoResultException


@asynccontextmanager
async def create_pool():
    raise RuntimeError("TODO(will): Pool not working!")
    async with aiopg.create_pool(DATABASE_URL) as pool:
        yield pool


def connect_async():
    # postgresql+aiopg://
    assert DATABASE_URL
    return pydapper.connect_async(DATABASE_URL)


def connect():
    # postgresql://
    assert POSTGRES_DATABASE_URL
    return pydapper.connect(POSTGRES_DATABASE_URL)
