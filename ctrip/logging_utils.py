# -*- coding: utf-8 -*-
"""统一日志：控制台 + 按账号分文件，替换原来满屏 print。

日志布局（多账号隔离的一部分）::

    logs/
    ├── main.log              # 主调度日志
    ├── A001/collect.log      # A001 的采集与登录日志
    └── A002/collect.log
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_CONSOLE_FMT = "%(asctime)s %(levelname)-7s [%(name)s] %(message)s"
_FILE_FMT = "%(asctime)s %(levelname)-7s [%(name)s] %(funcName)s:%(lineno)d %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_configured: set[str] = set()


def _level(value: str | int) -> int:
    if isinstance(value, int):
        return value
    return getattr(logging, str(value).upper(), logging.INFO)


def setup_root(level: str | int = "INFO", log_dir: Path | None = None) -> logging.Logger:
    """初始化根 logger（控制台 + main.log）。重复调用安全。"""
    root = logging.getLogger("ctrip")
    root.setLevel(_level(level))
    root.propagate = False

    if "console" not in _configured:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_CONSOLE_FMT, _DATE_FMT))
        handler.setLevel(_level(level))
        root.addHandler(handler)
        _configured.add("console")

    if log_dir is not None and "main_file" not in _configured:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "main.log", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(_FILE_FMT, _DATE_FMT))
        file_handler.setLevel(logging.DEBUG)
        root.addHandler(file_handler)
        _configured.add("main_file")

    return root


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"ctrip.{name}")


def account_logger(account_id: str, log_dir: Path, level: str | int = "INFO") -> logging.Logger:
    """返回某账号专属 logger，日志同时写入该账号目录，实现日志层面的账号隔离。"""
    logger = logging.getLogger(f"ctrip.acct.{account_id}")
    logger.setLevel(_level(level))
    key = f"acct:{account_id}"
    if key not in _configured:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_dir / "collect.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter(_FILE_FMT, _DATE_FMT))
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        _configured.add(key)
    return logger
