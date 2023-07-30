import asyncio
import typer

from fireagg import data_streams, settings

cli = typer.Typer()


@cli.command()
def seed_markets():
    asyncio.run(data_streams.seed_connectors())


if __name__ == "__main__":
    settings.setup_logging()
    cli()
