import asyncio
from asyncio_multisubscriber_queue import MultisubscriberQueue

from fireagg.connectors.base import Connector

from .base import Worker
from .connector import SymbolTradesProducer, SymbolSpreadsProducer
from .db_insertion import DatabaseStreamTrades, DatabaseStreamSpreads
from .messages import SymbolSpreads, SymbolTrade


class ProcessingCore:
    def __init__(self):
        self.producers: list[Worker] = []
        self.trades = MultisubscriberQueue[SymbolTrade]()
        self.spreads = MultisubscriberQueue[SymbolSpreads]()

        self.trades_consumer = DatabaseStreamTrades(self.trades)
        self.spreads_consumer = DatabaseStreamSpreads(self.spreads)

    async def watch_trades(self, connector: Connector, symbol: str):
        await self._add_producer(
            SymbolTradesProducer(connector, symbol, queue=self.trades)
        )

    async def watch_spreads(self, connector: Connector, symbol: str):
        await self._add_producer(
            SymbolSpreadsProducer(connector, symbol, queue=self.spreads)
        )

    async def _add_producer(self, producer: Worker):
        await producer.init()
        self.producers.append(producer)

    async def run(self):
        if not self.producers:
            raise ValueError("No producers.")

        await asyncio.gather(
            self.trades_consumer.run(),
            self.spreads_consumer.run(),
            *(p.run() for p in self.producers)
        )
