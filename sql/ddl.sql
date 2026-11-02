-- 本文件由 ctrip/schema.py 自动生成，请勿手工修改；
-- 修改字段请编辑 ctrip/schema.py 后执行: python -m ctrip.cli show-ddl --write

CREATE TABLE IF NOT EXISTS `ads_sup_ota_ctrip_trade_di` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `account_id` VARCHAR(50) NOT NULL COMMENT '账号标识（多账号隔离键）',
  `scenic_id` VARCHAR(50) NULL COMMENT '景区ID（如 PFTSCA01018088，账号级绑定）',
  `transaction_date` DATE NOT NULL COMMENT '交易日期',
  `signing_code` VARCHAR(20) NOT NULL COMMENT '签约类型编码',
  `signing_type` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '签约类型名称',
  `biz_type` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '业务类型',
  `date_type` VARCHAR(20) NOT NULL COMMENT '日期类型（使用日期/预订日期）',
  `period_type` VARCHAR(10) NOT NULL DEFAULT '1d' COMMENT '周期类型（1d:昨日 7d:近7日 30d:近30日）',
  `turnover` DECIMAL(18,2) NULL COMMENT '成交销售额（元）',
  `turnover_mom` DECIMAL(12,6) NULL COMMENT '成交销售额环比',
  `order_count` BIGINT NULL COMMENT '订单数（笔）',
  `order_count_mom` DECIMAL(12,6) NULL COMMENT '订单数环比',
  `sold_ticket_cnt` BIGINT NULL COMMENT '成交票量（张）',
  `sold_ticket_mom` DECIMAL(12,6) NULL COMMENT '成交票量环比',
  `validated_ticket_volume` BIGINT NULL COMMENT '核验票量（张）',
  `validated_ticket_mom` DECIMAL(12,6) NULL COMMENT '核验票量环比',
  `refund_rate` DECIMAL(12,6) NULL COMMENT '退订率',
  `refund_rate_mom` DECIMAL(12,6) NULL COMMENT '退订率环比',
  `deal_resource_count` BIGINT NULL COMMENT '成交资源数（去重资源ID数）',
  `deal_resource_count_mom` DECIMAL(12,6) NULL COMMENT '成交资源数环比',
  `collect_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '采集时间',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ads_sup_ota_ctrip_trade_di` (`account_id`, `transaction_date`, `period_type`, `signing_code`, `biz_type`, `date_type`),
  KEY `idx_kylin_date` (`transaction_date`),
  KEY `idx_kylin_account_date` (`account_id`, `transaction_date`),
  KEY `idx_kylin_scenic_date` (`scenic_id`, `transaction_date`),
  KEY `idx_kylin_period` (`account_id`, `period_type`, `transaction_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='携程交易卡片每日数据';

CREATE TABLE IF NOT EXISTS `ads_sup_ota_ctrip_destination_rating_di` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `account_id` VARCHAR(50) NOT NULL COMMENT '账号标识（多账号隔离键）',
  `scenic_id` VARCHAR(50) NULL COMMENT '景区ID（如 PFTSCA01018088，账号级绑定）',
  `evaluation_month` VARCHAR(7) NOT NULL COMMENT '考核月份（YYYY-MM）',
  `region_id` VARCHAR(20) NOT NULL DEFAULT 'ALL' COMMENT '目的地ID（账号级汇总时为 ALL）',
  `region_name` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '目的地名称',
  `region_type` VARCHAR(10) NULL COMMENT '目的地类型',
  `analysis_text` MEDIUMTEXT NULL COMMENT 'AI 分析原文',
  `analysis_text_len` INT NULL COMMENT '分析原文字符数',
  `collect_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '采集时间',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctrip_destination_rating` (`account_id`, `evaluation_month`, `region_id`),
  KEY `idx_rating_month` (`evaluation_month`),
  KEY `idx_rating_scenic` (`scenic_id`, `evaluation_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='携程目的地评级';

CREATE TABLE IF NOT EXISTS `ads_sup_ota_ctrip_score_di` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `account_id` VARCHAR(50) NOT NULL COMMENT '账号标识（多账号隔离键）',
  `scenic_id` VARCHAR(50) NULL COMMENT '景区ID（如 PFTSCA01018088，账号级绑定）',
  `evaluation_month` VARCHAR(7) NOT NULL COMMENT '月份（YYYY-MM）',
  `region_id` VARCHAR(20) NOT NULL COMMENT '目的地ID',
  `region_name` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '目的地名称',
  `region_type` VARCHAR(10) NULL COMMENT '目的地类型',
  `overall_service_score` DECIMAL(8,2) NULL COMMENT '综合服务分',
  `ocr_score` DECIMAL(8,2) NULL COMMENT '订单确认率评分',
  `fdr_score` DECIMAL(8,2) NULL COMMENT '履约缺陷率评分',
  `manual_intervention_rate_score` DECIMAL(8,2) NULL COMMENT '客服人工介入率评分',
  `timely_response_20s_score` DECIMAL(8,2) NULL COMMENT '20秒回复及时率评分',
  `im_neg_rate_score` DECIMAL(8,2) NULL COMMENT 'IM差评率评分',
  `collect_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '采集时间',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ads_sup_ota_ctrip_score_di` (`account_id`, `evaluation_month`, `region_id`),
  KEY `idx_score_month` (`evaluation_month`),
  KEY `idx_score_region` (`region_id`),
  KEY `idx_score_scenic` (`scenic_id`, `evaluation_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='携程供应商评级雷达图数据';

CREATE TABLE IF NOT EXISTS `ads_service_ota_ctrip_account` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `account_id` VARCHAR(50) NOT NULL COMMENT '账号标识（多账号隔离键）',
  `scenic_id` VARCHAR(50) NULL COMMENT '景区ID（如 PFTSCA01018088，账号级绑定）',
  `account_name` VARCHAR(100) NOT NULL DEFAULT '' COMMENT '账号备注名',
  `login_name` VARCHAR(100) NOT NULL DEFAULT '' COMMENT '登录用户名',
  `vendor_id` VARCHAR(50) NULL COMMENT '携程侧供应商ID（身份指纹）',
  `status` VARCHAR(20) NOT NULL DEFAULT 'active' COMMENT '账号状态（active/disabled）',
  `last_login_at` DATETIME NULL COMMENT '最近登录成功时间',
  `remark` VARCHAR(255) NULL COMMENT '备注',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctrip_account` (`account_id`),
  KEY `idx_account_status` (`status`),
  KEY `idx_account_scenic` (`scenic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='携程采集账号维表（不存储密码）';

CREATE TABLE IF NOT EXISTS `ads_service_ota_ctrip_run_log` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `account_id` VARCHAR(50) NOT NULL COMMENT '账号标识（多账号隔离键）',
  `scenic_id` VARCHAR(50) NULL COMMENT '景区ID（如 PFTSCA01018088，账号级绑定）',
  `job` VARCHAR(30) NOT NULL COMMENT '任务名（kylin/rating/score）',
  `run_key` VARCHAR(60) NOT NULL DEFAULT '' COMMENT '批次标识（日期或月份区间）',
  `started_at` DATETIME NULL COMMENT '开始时间',
  `finished_at` DATETIME NULL COMMENT '结束时间',
  `duration_ms` INT NULL COMMENT '耗时（毫秒）',
  `status` VARCHAR(20) NOT NULL DEFAULT '' COMMENT '状态（success/partial/failed）',
  `rows_fetched` INT NULL COMMENT '采集条数',
  `rows_written` INT NULL COMMENT '写入条数',
  `sink` VARCHAR(30) NULL COMMENT '落地方式（csv/mysql/both）',
  `error` VARCHAR(1000) NULL COMMENT '错误摘要',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctrip_collect_run_log` (`account_id`, `job`, `run_key`),
  KEY `idx_log_started` (`started_at`),
  KEY `idx_log_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='采集运行审计日志（按账号+任务+批次）';

