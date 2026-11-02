# -*- coding: utf-8 -*-
"""时区解析：带降级保护，避免因缺少时区数据库直接崩溃。

背景
----
Linux 自带 ``/usr/share/zoneinfo``，而 **Windows 没有系统时区数据库**，
Python 的 ``zoneinfo`` 在 Windows 上必须依赖 ``tzdata`` 包才能解析
``Asia/Shanghai`` 这类 IANA 时区名，否则抛 ``ZoneInfoNotFoundError``。

``tzdata`` 已在 ``pyproject.toml`` 中按平台声明为依赖，但仍保留本模块的
降级逻辑：采集任务往往跑在定时调度里，因为一个时区数据包缺失就整批失败
是不可接受的。降级为固定偏移后，采集依然能正常完成——本项目的时区仅用于
「今天/昨天/上个月是哪天」这类日期推导，不涉及历史夏令时换算，
固定偏移与真实时区在此场景下结果完全一致。
"""

from __future__ import annotations

import logging
from datetime import timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger("ctrip.timeutil")

# 常见时区的固定偏移兜底表。中国自 1991 年后不再使用夏令时，
# 因此 Asia/Shanghai 用固定 +08:00 兜底不会产生偏差。
_FALLBACK_OFFSETS: dict[str, int] = {
    "Asia/Shanghai": 8,
    "Asia/Chongqing": 8,
    "Asia/Hong_Kong": 8,
    "Asia/Macau": 8,
    "Asia/Taipei": 8,
    "Asia/Singapore": 8,
    "Asia/Tokyo": 9,
    "Asia/Seoul": 9,
    "UTC": 0,
}

_warned: set[str] = set()


def get_timezone(name: str, *, log: logging.Logger | None = None) -> tzinfo:
    """解析时区名，失败时降级为固定偏移。

    :param name: IANA 时区名，如 ``Asia/Shanghai``
    :return: ``ZoneInfo`` 或降级后的 ``timezone``（固定偏移）
    """
    log = log or logger
    tz_name = (name or "Asia/Shanghai").strip() or "Asia/Shanghai"

    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, KeyError, ValueError) as exc:
        offset_hours = _FALLBACK_OFFSETS.get(tz_name)
        if offset_hours is None:
            # 未知时区且无时区库可用，只能退到 UTC+8（本项目面向国内业务）
            offset_hours = 8
            hint = (
                f"时区 {tz_name!r} 无法解析且不在兜底表中，已降级为 UTC+08:00。"
            )
        else:
            hint = f"时区 {tz_name!r} 无法解析，已降级为固定偏移 UTC{offset_hours:+03d}:00。"

        # 同一时区只告警一次，避免多账号并发时刷满日志
        if tz_name not in _warned:
            _warned.add(tz_name)
            log.warning(
                "%s原因：%s。"
                "如需精确时区支持，请安装时区数据库：pip install tzdata"
                "（Windows 必需，Linux 通常已自带系统时区库）",
                hint, exc,
            )
        return timezone(timedelta(hours=offset_hours), name=tz_name)


def describe_timezone(name: str) -> str:
    """返回时区的可读描述，用于 ``show-config`` / ``doctor`` 展示。"""
    try:
        ZoneInfo((name or "").strip() or "Asia/Shanghai")
        return f"{name}（时区数据库可用）"
    except (ZoneInfoNotFoundError, KeyError, ValueError):
        offset = _FALLBACK_OFFSETS.get(name, 8)
        return f"{name}（时区数据库缺失，降级为 UTC{offset:+03d}:00）"
