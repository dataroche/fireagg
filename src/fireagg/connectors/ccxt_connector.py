import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
import time
from typing import AsyncIterator

import ccxt.async_support
import ccxt.pro
from ccxt.async_support.base.exchange import Exchange
from ccxt.base.errors import AuthenticationError, NotSupported, RequestTimeout

from fireagg.connectors.base import Connector, Trade, MidPrice
from fireagg.database import symbols


def list_ccxt_connector_names():
    exclude = {
        "coinbase",
        "kuna",
        "tidex",
        "okcoin",
    }
    return [exch for exch in ccxt.exchanges if exch not in exclude]


LOWEST_ORDER_BOOK_BY_EXCHANGE = {"binance": 5, "binanceus": 5, "kucoin": 20, "bybit": 1, "kucoinfutures": 20}


class CCXTConnector(Connector):
    symbols_rewrite: dict[str, str] = {"BTC/USD:BTC": "BTC/USD"}

    async def do_seed_markets(self):
        markets = await self._load_markets()
        return [
            symbols.ConnectorSymbolInput(
                symbol=self.symbols_rewrite.get(data["symbol"], data["symbol"]),
                connector_symbol=data["id"],
                connector=self.name,
                base_asset=data["base"],
                quote_asset=data["quote"],
            )
            for data in markets.values()
        ]

    async def _load_markets(self):
        markets = {}
        exchange: Exchange = getattr(ccxt.async_support, self.name)()
        try:
            markets = await exchange.load_markets()
        finally:
            await exchange.close()
        return markets

    async def do_watch_trades(self, connector_symbol: str) -> AsyncIterator[Trade]:
        async with self._exchange() as exchange:
            # 5 minutes in the past.
            timestamp_deadline = (time.time() - 300) * 1000
            while self.running:
                trades: list[dict] = await exchange.watch_trades(connector_symbol)
                for trade in trades:
                    timestamp_ms = trade["timestamp"]
                    if timestamp_ms < timestamp_deadline:
                        # Some exchanges return very old trades for some reason.
                        continue
                    yield Trade(
                        timestamp_ms=timestamp_ms,
                        price=Decimal(trade["price"]),
                        amount=Decimal(trade["amount"]),
                        is_buy=(trade["side"] == "buy"),
                    )

    async def do_watch_spreads(self, connector_symbol: str):
        limit = LOWEST_ORDER_BOOK_BY_EXCHANGE.get(self.name, 25)
        retries = 3
        async with self._exchange() as exchange:
            while self.running:
                try:
                    book = await exchange.watch_order_book(
                        connector_symbol, limit=limit
                    )
                    best_bid = Decimal(book["bids"][0][0])
                    best_ask = Decimal(book["asks"][0][0])

                    yield MidPrice(
                        timestamp_ms=book["timestamp"],
                        best_bid=best_bid,
                        best_ask=best_ask,
                    )
                except (asyncio.TimeoutError, RequestTimeout):
                    if retries > 0:
                        self.logger.info(f"Timeout with {self.name}: Retrying...")
                        retries = retries - 1
                        await asyncio.sleep(5)
                    else:
                        raise

    @asynccontextmanager
    async def _exchange(self):
        try:
            exchange: Exchange = getattr(ccxt.pro, self.name)()
        except AttributeError:
            raise NotImplementedError(
                f"Unable to watch connector {self.name} because of it's not supported by CCXT Pro (no pro exchange named like that)."
            )

        try:
            yield exchange
        except AuthenticationError:
            raise NotImplementedError(
                f"Unable to watch connector {self.name} because of authentication error."
            )
        except NotSupported:
            raise NotImplementedError(
                f"Unable to watch connector {self.name} because it's not supported by CCXT."
            )
        except Exception as e:
            raise RuntimeError(
                f"Runtime error while watching {self.name}: {str(e)}"
            ) from e
        finally:
            await exchange.close()
