from abc import ABC, abstractmethod
import logging

from fireagg.database import db, symbols


class BaseConnector(ABC):
    name: str

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"fireagg.connectors.impl.{self.name}")

    async def seed_markets(self):
        symbols_input = await self.do_seed_markets()
        if not symbols_input:
            return

        self.logger.info(f"Loading {len(symbols_input)} symbols for {self.name}")
        async with db.connect_async() as commands:
            await symbols.upsert_many(commands, symbols_input)
            self.logger

    @abstractmethod
    async def do_seed_markets(self) -> list[symbols.ConnectorSymbolInput]:
        raise NotImplementedError()
