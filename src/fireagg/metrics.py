import platform
import logging
from prometheus_client import start_http_server, Counter


logger = logging.getLogger(__name__)


def run_metrics_server(port: int):
    logger.info(f"Starting metrics server on port {port}")
    start_http_server(port=port)


db_inserts_counter = Counter(
    "db_inserts",
    documentation="Database inserts counter",
    labelnames=["worker", "stream_name", "instance"],
)


def get_db_inserts_counter(**labels):
    return db_inserts_counter.labels(instance=platform.node(), **labels)
