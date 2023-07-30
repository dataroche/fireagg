import importlib

from fireagg.connectors.base import BaseConnector
from fireagg.connectors.ccxt_connector import CCXTConnector, list_ccxt_connector_names


def list_connectors():
    return list_ccxt_connector_names()


def create_connector(name: str) -> BaseConnector:
    try:
        possible_module_name = f"fireagg.connectors.impl.{name}"
        connector_module = importlib.import_module(possible_module_name)
    except ImportError:
        # Fallback to CCXT connector.
        return CCXTConnector(name)

    try:
        connector_class = getattr(connector_module, name)
        if not issubclass(connector_class, BaseConnector):
            raise ValueError("Not a BaseConnector.")
    except (AttributeError, ValueError):
        raise ValueError(
            f"Module {possible_module_name} requires a class named {name} which extends {BaseConnector}"
        )

    return connector_class(name)
