import asyncio
import aiohttp


async def run(ws):
    async for msg in ws:
        print("Message received from server:", msg)


async def main():
    async with aiohttp.ClientSession().ws_connect(
        "wss://ws.coincap.io/trades/kucoin"
    ) as ws:
        await run(ws)


if __name__ == "__main__":
    asyncio.run(main())
