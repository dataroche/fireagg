import ccxt.async_support as ccxt

from fireagg.database import symbols

from fireagg.connectors.base import BaseConnector


def list_ccxt_connector_names():
    exclude = {
        "coinbase",  # Require apikey
        "kuna",
        "tidex",
        "okcoin",
    }
    return [exch for exch in ccxt.exchanges if exch not in exclude]


class CCXTConnector(BaseConnector):
    async def do_seed_markets(self):
        markets = await self._load_markets()
        return [
            symbols.ConnectorSymbolInput(
                # Some binance symbols contain trailing stuff like BTC/USD:BTC
                symbol=data["symbol"].split(":")[0],
                connector_symbol=data["id"],
                connector=self.name,
                base_asset=data["base"],
                quote_asset=data["quote"],
            )
            for data in markets.values()
        ]

    async def _load_markets(self):
        markets = {}
        exchange: ccxt.Exchange = getattr(ccxt, self.name)()
        try:
            markets = await exchange.load_markets()
        except Exception as e:
            err_msg = str(e)
            if len(err_msg) > 200:
                err_msg = err_msg[:200] + "..."
            self.logger.info(f"Unable to load markets for {self.name}: {err_msg}")
        finally:
            await exchange.close()
        return markets
