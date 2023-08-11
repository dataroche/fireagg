from pydapper.commands import CommandsAsync
from pydapper.types import ListParamType


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
            ?timestamp?,
            ?price?,
            ?amount?,
            ?is_buy?,
            NOW(),
            ?fetch_timestamp?
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
            ?timestamp?,
            ?best_bid?,
            ?best_ask?,
            NOW(),
            ?fetch_timestamp?
        );
        """,
        param=spreads,
    )
