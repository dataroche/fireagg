import pydantic
from pydapper.commands import CommandsAsync


class SymbolInput(pydantic.BaseModel):
    symbol: str
    base_asset: str
    quote_asset: str


class Symbol(SymbolInput):
    id: int


class ConnectorSymbolInput(SymbolInput):
    connector_symbol: str
    connector: str


async def get_all(commands: CommandsAsync):
    return await commands.query_async(
        "SELECT id, symbol, base_asset, quote_asset FROM realtime.symbols",
        model=Symbol,
    )


async def upsert_many(commands: CommandsAsync, symbols: list[ConnectorSymbolInput]):
    if not symbols:
        return

    await commands.execute_async(
        """
        INSERT INTO realtime.symbols (symbol, base_asset, quote_asset)
        VALUES (?symbol?, ?base_asset?, ?quote_asset?)
        ON CONFLICT (symbol) DO NOTHING
        """,
        param=[
            symbol.model_dump(include={"symbol", "base_asset", "quote_asset"})
            for symbol in symbols
        ],
    )

    all_symbols = await get_all(commands)
    symbols_to_id = {symbol.symbol: symbol.id for symbol in all_symbols}

    symbols_map = [
        {
            "symbol_id": symbols_to_id[symbol.symbol],
            "connector_symbol": symbol.connector_symbol,
            "connector": symbol.connector,
        }
        for symbol in symbols
    ]

    await commands.execute_async(
        """
        INSERT INTO realtime.symbols_map (symbol_id, connector, connector_symbol)
        VALUES (?symbol_id?, ?connector?, ?connector_symbol?)
        ON CONFLICT (symbol_id, connector) DO UPDATE SET connector_symbol=?connector_symbol?
        """,
        param=symbols_map,
    )
