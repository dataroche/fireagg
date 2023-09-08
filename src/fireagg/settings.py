from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

from pydantic import (
    PostgresDsn,
    RedisDsn,
)

import logging


def setup_logging():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)


class FireAggSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: PostgresDsn
    redis_url: RedisDsn

    cryptowatch_pub_key: Optional[str] = None
    cryptowatch_private_key: Optional[str] = None

    benchmark_trades_per_second_target: Optional[int] = None

    enable_metrics_exporter: bool = False
    metrics_exporter_port: int = 9000


def get():
    return FireAggSettings()
