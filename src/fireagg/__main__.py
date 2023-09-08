import asyncio
from typing import Optional
import typer
import dotenv

from fireagg import data_streams, settings, metrics

cli = typer.Typer()
distributed = typer.Typer()
cli.add_typer(distributed, name="distributed")


@cli.command()
def seed_markets(connector: Optional[str] = None):
    asyncio.run(data_streams.seed_connectors([connector] if connector else None))


@cli.command()
def watch_symbol(symbol: str, connector: str = "kraken"):
    asyncio.run(data_streams.watch_spreads(connector, symbol=symbol))


@cli.command()
def combine_connectors(symbols: list[str], only_connector: Optional[str] = None):
    asyncio.run(
        data_streams.combine_connectors(
            symbols, only_connectors=only_connector and [only_connector] or None
        ),
    )


@distributed.command()
def core():
    asyncio.run(
        data_streams.distributed_core(),
    )


@distributed.command()
def symbols(symbols: list[str], only_connector: Optional[str] = None):
    asyncio.run(
        data_streams.distributed_watch_symbols(
            symbols, only_connectors=only_connector and [only_connector] or None
        ),
    )


def run():
    dotenv.load_dotenv()
    settings_obj = settings.get()
    settings.setup_logging()

    if settings_obj.enable_metrics_exporter:
        metrics.run_metrics_server(settings_obj.metrics_exporter_port)

    cli()


if __name__ == "__main__":
    run()
