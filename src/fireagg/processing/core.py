import asyncio

from fireagg.connectors.base import Connector
from fireagg.database.symbol_prices import SymbolPriceInput

from .base import Worker
from .connector import ConnectorSymbolPriceInputProducer, ConnectorOrderBookProducer
from .db_insertion import SymbolPriceInputConsumer


class ProcessingCore:
    def __init__(self):
        self.producers: list[Worker] = []

        self.price_queue = asyncio.Queue[SymbolPriceInput]()

    async def watch_symbol(self, connector: Connector, symbol: str):
        producer = ConnectorSymbolPriceInputProducer(
            connector, symbol, price_queue=self.price_queue
        )
        await producer.init()
        self.producers.append(producer)

    async def watch_order_book(self, connector: Connector, symbol: str):
        producer = ConnectorOrderBookProducer(
            connector, symbol, price_queue=self.price_queue
        )
        await producer.init()
        self.producers.append(producer)

    async def run(self):
        if not self.producers:
            raise ValueError("No producers.")
        price_consumer = SymbolPriceInputConsumer(self.price_queue)

        await asyncio.gather(price_consumer.run(), *(p.run() for p in self.producers))
