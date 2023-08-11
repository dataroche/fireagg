import asyncio
from typing import Optional
import typer
import dotenv

from fireagg import data_streams, settings

cli = typer.Typer()


@cli.command()
def seed_markets(connector: Optional[str] = None):
    asyncio.run(data_streams.seed_connectors([connector] if connector else None))


@cli.command()
def watch_symbol(symbol: str, connector: str = "kraken"):
    asyncio.run(data_streams.watch_order_book(connector, symbol=symbol))


@cli.command()
def combine_connectors(symbols: list[str]):
    asyncio.run(data_streams.combine_connectors(symbols))


if __name__ == "__main__":
    dotenv.load_dotenv()
    settings.setup_logging()
    cli()
