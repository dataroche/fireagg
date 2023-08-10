from decimal import Decimal
from typing import AsyncIterator
import ccxt.async_support
from ccxt.async_support.base.exchange import Exchange
import ccxt.pro
from ccxt.base.errors import AuthenticationError, NotSupported

from fireagg.connectors.base import Connector, LastPrice
from fireagg.database import symbols


def list_ccxt_connector_names():
    exclude = {
        "coinbase",
        "kuna",
        "tidex",
        "okcoin",
    }
    return [exch for exch in ccxt.exchanges if exch not in exclude]


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

    async def do_watch_market_last_price(
        self, connector_symbol: str
    ) -> AsyncIterator[LastPrice]:
        try:
            exchange: Exchange = getattr(ccxt.pro, self.name)()
        except AttributeError:
            raise RuntimeError(
                f"Unable to watch connector {self.name} because of it's not supported by CCXT Pro (no pro exchange named like that)."
            )

        try:
            while self.running:
                ticker = await exchange.watch_ticker(connector_symbol)
                yield LastPrice(
                    timestamp_ms=ticker["timestamp"], price=Decimal(ticker["last"])
                )
        except AuthenticationError:
            raise RuntimeError(
                f"Unable to watch connector {self.name} because of authentication error."
            )
        except NotSupported:
            raise RuntimeError(
                f"Unable to watch connector {self.name} because it's not supported by CCXT."
            )
        except Exception as e:
            raise RuntimeError(
                f"Runtime error while watching {self.name} {connector_symbol}"
            ) from e
        finally:
            await exchange.close()
