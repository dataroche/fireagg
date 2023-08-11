from typing import Optional
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


class ConnectorSymbolMapping(SymbolInput):
    symbol_id: int
    connector: str
    connector_symbol: str
    is_error: Optional[bool]


async def get_all(commands: CommandsAsync):
    return await commands.query_async(
        "SELECT id, symbol, base_asset, quote_asset FROM realtime.symbols",
        model=Symbol,
    )


async def get_connector_symbol_mapping(
    commands: CommandsAsync, connector: str, symbol: str
):
    return await commands.query_single_async(
        """
        SELECT
            symbol_id,
            connector,
            connector_symbol,
            symbol,
            base_asset,
            quote_asset,
            is_error
        FROM realtime.symbols_map
        JOIN realtime.symbols ON symbol_id = id
        WHERE connector = ?connector? AND symbol = ?symbol?
        """,
        model=ConnectorSymbolMapping,
        param={"connector": connector, "symbol": symbol},
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


async def get_symbol_connectors(commands: CommandsAsync, symbol: str):
    connectors = await commands.query_async(
        """
        SELECT connector
        FROM realtime.symbols_map
        JOIN realtime.symbols ON id = symbol_id
        WHERE symbol = ?symbol? AND (is_error IS NULL or not is_error)
        """,
        param={"symbol": symbol},
    )

    return [data["connector"] for data in connectors]


async def mark_connector_symbol_mapping(
    commands: CommandsAsync,
    connector: str,
    symbol_id: int,
    is_error: Optional[bool] = None,
):
    await commands.execute_async(
        """
        UPDATE realtime.symbols_map
        SET is_error=?is_error?
        WHERE connector = ?connector? AND symbol_id = ?symbol_id?
        """,
        param={"connector": connector, "symbol_id": symbol_id, "is_error": is_error},
    )
