# -*- coding: utf-8 -*-
"""落地方式工厂。"""

from __future__ import annotations

import logging

from ..config import AppConfig, ConfigError
from .base import MultiSink, Sink, WriteResult
from .csv_sink import CsvSink
from .mysql_sink import MySQLSink

__all__ = ["Sink", "MultiSink", "WriteResult", "CsvSink", "MySQLSink", "build_sink"]


def build_sink(cfg: AppConfig, mode: str | None = None,
               log: logging.Logger | None = None) -> Sink:
    """按配置构建落地目标。

    ``mode``：``csv``（默认）/ ``mysql`` / ``both``；为空时取 ``cfg.run.sink``。
    """
    mode = (mode or cfg.run.sink or "csv").lower()
    if mode == "csv":
        return CsvSink(cfg, log)
    if mode == "mysql":
        _require_mysql(cfg)
        return MySQLSink(cfg, log)
    if mode == "both":
        _require_mysql(cfg)
        return MultiSink([CsvSink(cfg, log), MySQLSink(cfg, log)])
    raise ConfigError(f"不支持的落地方式：{mode!r}（可选 csv / mysql / both）")


def _require_mysql(cfg: AppConfig) -> None:
    if not cfg.mysql.enabled:
        raise ConfigError(
            "需要写入 MySQL，但 mysql.enabled=false。"
            "请在 config/config.yaml 中配置数据库并设置 mysql.enabled: true"
            "（或设置环境变量 CTRIP__MYSQL__ENABLED=true）"
        )
