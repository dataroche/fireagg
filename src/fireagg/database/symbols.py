from typing import Optional
import pydantic
from pydapper.commands import CommandsAsync
from pydapper.exceptions import NoResultException


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
    is_unavailable: Optional[bool]


async def get_all(commands: CommandsAsync):
    return await commands.query_async(
        "SELECT id, symbol, base_asset, quote_asset FROM symbols",
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
            is_unavailable
        FROM symbols_map
        JOIN symbols ON symbol_id = id
        WHERE connector = ?connector? AND symbol = ?symbol?
        """,
        model=ConnectorSymbolMapping,
        param={"connector": connector, "symbol": symbol},
    )


async def get_symbol(commands: CommandsAsync, symbol: str):
    try:
        return await commands.query_single_async(
            """
            SELECT
                *
            FROM symbols
            WHERE symbol = ?symbol?
            """,
            model=Symbol,
            param={"symbol": symbol},
        )
    except NoResultException:
        return None


async def upsert_many(commands: CommandsAsync, symbols: list[ConnectorSymbolInput]):
    if not symbols:
        return

    await commands.execute_async(
        """
        INSERT INTO symbols (symbol, base_asset, quote_asset)
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
        INSERT INTO symbols_map (symbol_id, connector, connector_symbol)
        VALUES (?symbol_id?, ?connector?, ?connector_symbol?)
        ON CONFLICT (symbol_id, connector) DO UPDATE SET connector_symbol=?connector_symbol?
        """,
        param=symbols_map,
    )


async def get_symbol_connectors(commands: CommandsAsync, symbol: str):
    connectors = await commands.query_async(
        """
        SELECT connector
        FROM symbols_map
        JOIN symbols ON id = symbol_id
        WHERE symbol = ?symbol? AND (is_unavailable IS NULL or not is_unavailable)
        """,
        param={"symbol": symbol},
    )

    return [data["connector"] for data in connectors]


async def get_connector_symbols(commands: CommandsAsync, connector: str):
    connectors = await commands.query_async(
        """
        SELECT symbol
        FROM symbols_map
        JOIN symbols ON id = symbol_id
        WHERE connector = ?connector? AND (is_unavailable IS NULL or not is_unavailable)
        """,
        param={"connector": connector},
    )

    return [data["symbol"] for data in connectors]


async def mark_connector_symbol_mapping(
    commands: CommandsAsync,
    connector: str,
    symbol_id: int,
    is_unavailable: Optional[bool] = None,
):
    await commands.execute_async(
        """
        UPDATE symbols_map
        SET is_unavailable=?is_unavailable?
        WHERE connector = ?connector? AND symbol_id = ?symbol_id?
        """,
        param={
            "connector": connector,
            "symbol_id": symbol_id,
            "is_unavailable": is_unavailable,
        },
    )
