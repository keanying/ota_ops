# -*- coding: utf-8 -*-
"""携程商家后台（VBooking）多账号采集工具包。

模块职责
--------
* ``config``      配置加载（YAML + 环境变量 + ${ENV} 占位符）
* ``schema``      **表结构单一维护点**：字段、类型、注释、唯一键、上游字段映射
* ``session``     Cookie -> requests.Session（修复域名处理与请求头）
* ``api``         携程接口调用（重试 + 业务状态校验 + 登录态失效识别）
* ``auth``        Playwright 自动登录、登录态存储、账号指纹校验
* ``accounts``    账号来源（管理 API 优先，本地配置回落）
* ``sinks``       落地层（CSV 默认 / MySQL 可选 / both）
* ``collectors``  三个业务采集器
* ``pipeline``    多账号调度编排
* ``cli``         命令行入口
"""

__version__ = "1.0.0"
