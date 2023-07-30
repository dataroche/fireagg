from contextlib import asynccontextmanager
import os
import pydapper

import aiopg

DATABASE_URL = os.environ["DATABASE_URL"]


@asynccontextmanager
async def create_pool():
    raise RuntimeError("TODO(will): Pool not working!")
    async with aiopg.create_pool(DATABASE_URL) as pool:
        yield pool


def connect_async():
    return pydapper.connect_async(DATABASE_URL)
