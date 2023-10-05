import importlib

from fireagg.input_streams import __name__ as input_streams_name
from fireagg.input_streams.base import Connector
from fireagg.input_streams.ccxt_connector import (
    CCXTConnector,
    list_ccxt_connector_names,
)


def list_connectors():
    return list_ccxt_connector_names()


def create_connector(name: str) -> Connector:
    try:
        possible_module_name = f"{input_streams_name}.impl.{name}"
        connector_module = importlib.import_module(possible_module_name)
    except ImportError:
        # Fallback to CCXT connector.
        return CCXTConnector(name)

    try:
        connector_class = getattr(connector_module, name)
        if not issubclass(connector_class, Connector):
            raise ValueError("Not a Connector.")
    except (AttributeError, ValueError):
        raise ValueError(
            f"Module {possible_module_name} requires a class named {name} which extends {Connector}"
        )

    return connector_class(name)
