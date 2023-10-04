from pydapper.commands import CommandsAsync
from pydapper.types import ListParamType
from pydapper.exceptions import NoResultException


async def insert_symbol_trades(commands: CommandsAsync, trades: ListParamType):
    await commands.execute_async(
        """
        INSERT INTO symbol_trades_stream (
            connector,
            symbol_id,
            timestamp,
            price,
            amount,
            is_buy,
            update_timestamp,
            fetch_timestamp
        )
        VALUES (
            ?connector?,
            ?symbol_id?,
            TO_TIMESTAMP(?timestamp_ms? / 1000.0),
            ?price?,
            ?amount?,
            ?is_buy?,
            NOW(),
            TO_TIMESTAMP(?fetch_timestamp_ms? / 1000.0)
        );
        """,
        param=trades,
    )


async def insert_symbol_spreads(commands: CommandsAsync, spreads: ListParamType):
    await commands.execute_async(
        """
        INSERT INTO symbol_spreads_stream (
            connector,
            symbol_id,
            timestamp,
            best_bid,
            best_ask,
            update_timestamp,
            fetch_timestamp
        )
        VALUES (
            ?connector?,
            ?symbol_id?,
            TO_TIMESTAMP(?timestamp_ms? / 1000.0),
            ?best_bid?,
            ?best_ask?,
            NOW(),
            TO_TIMESTAMP(?fetch_timestamp_ms? / 1000.0)
        );
        """,
        param=spreads,
    )


async def insert_symbol_true_mid_price(
    commands: CommandsAsync, mid_prices: ListParamType
):
    await commands.execute_async(
        """
        INSERT INTO symbol_true_mid_price_stream (
            symbol_id,
            timestamp,
            true_mid_price,
            update_timestamp
        )
        VALUES (
            ?symbol_id?,
            TO_TIMESTAMP(?timestamp_ms? / 1000.0),
            ?true_mid_price?,
            NOW()
        );
        """,
        param=mid_prices,
    )


async def get_last_symbol_true_mid_price(
    commands: CommandsAsync,
    symbol_id: int,
):
    try:
        data = await commands.query_first_async(
            """
            SELECT *
            FROM symbol_true_mid_price_stream
            WHERE symbol_id = ?symbol_id?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            param={"symbol_id": symbol_id},
        )
        return data
    except NoResultException:
        return None
