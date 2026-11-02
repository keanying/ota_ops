-- =============================================================================
-- 迁移脚本：旧表结构 -> 新 DDL（数仓命名规范 + 英文业务字段名 + 审计时间字段）
--
-- 适用场景：库里已经存在早期版本的表（ctrip_kylin_daily 等旧表名）时执行本脚本。
--           全新部署**无需**本脚本，直接 `python -m ctrip.cli init-db` 即可。
--
-- 变更内容：
--   1. 表名改为数仓规范（ads_sup_ota_* 业务表、ads_service_ota_* 服务表）；
--   2. 业务字段改为英文业务命名（如 record_date -> transaction_date）；
--   3. 新增 create_time / update_time，由数据库自动维护；
--   4. collected_at 改名为 collect_time（仍由程序写入，语义不变）；
--   5. 交易表补充唯一键，避免重跑累积重复行；
--   6. 评分表唯一键索引名加 uk_ 前缀，避免与表名同名；
--   7. 交易表新增 period_type（1d/7d/30d 周期口径，入唯一键）
--      与 deal_resource_count / deal_resource_count_mom（成交资源数）。
--
-- 执行前务必备份：mysqldump -u<user> -p <db> > backup_$(date +%F).sql
-- 表结构的唯一维护点是 ctrip/schema.py，本脚本仅为存量库的一次性补丁。
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 第一步：重命名表
-- -----------------------------------------------------------------------------
RENAME TABLE `ctrip_kylin_daily`         TO `ads_sup_ota_ctrip_trade_di`;
RENAME TABLE `ctrip_destination_rating`  TO `ads_sup_ota_ctrip_destination_rating_di`;
RENAME TABLE `ctrip_supplier_rate_score` TO `ads_sup_ota_ctrip_score_di`;
RENAME TABLE `ctrip_account`             TO `ads_service_ota_ctrip_account`;
RENAME TABLE `ctrip_collect_run_log`     TO `ads_service_ota_ctrip_run_log`;

-- -----------------------------------------------------------------------------
-- 第二步：交易表字段重命名 + 类型调整 + 补唯一键
-- 说明：计数类字段由 INT 提升为 BIGINT，避免长周期累计后溢出。
-- -----------------------------------------------------------------------------
ALTER TABLE `ads_sup_ota_ctrip_trade_di`
  CHANGE `record_date`          `transaction_date`        DATE           NOT NULL COMMENT '交易日期',
  CHANGE `vbk_bu_type`          `biz_type`                VARCHAR(50)    NOT NULL DEFAULT '' COMMENT '业务类型',
  CHANGE `income`               `turnover`                DECIMAL(18,2)  NULL COMMENT '成交销售额（元）',
  CHANGE `income_mom_rate`      `turnover_mom`            DECIMAL(12,6)  NULL COMMENT '成交销售额环比',
  CHANGE `order_count`          `order_count`             BIGINT         NULL COMMENT '订单数（笔）',
  CHANGE `order_count_mom_rate` `order_count_mom`         DECIMAL(12,6)  NULL COMMENT '订单数环比',
  CHANGE `ticket_qty`           `sold_ticket_cnt`         BIGINT         NULL COMMENT '成交票量（张）',
  CHANGE `ticket_qty_mom_rate`  `sold_ticket_mom`         DECIMAL(12,6)  NULL COMMENT '成交票量环比',
  CHANGE `verify_qty`           `validated_ticket_volume` BIGINT         NULL COMMENT '核验票量（张）',
  CHANGE `verify_qty_mom_rate`  `validated_ticket_mom`    DECIMAL(12,6)  NULL COMMENT '核验票量环比',
  CHANGE `refund_rate_mom_rate` `refund_rate_mom`         DECIMAL(12,6)  NULL COMMENT '退订率环比',
  CHANGE `collected_at`         `collect_time`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '采集时间';

-- 新增周期口径与成交资源数。
--
-- period_type 为何必须进唯一键：同一个 transaction_date 下
--   1d/7d/30d 是三条互不相干的记录，不进唯一键会互相覆盖，
--   最后只剩下最后写入的那一条。
-- 存量数据都是日粒度采集结果，故默认值给 '1d'。
ALTER TABLE `ads_sup_ota_ctrip_trade_di`
  ADD COLUMN `period_type` VARCHAR(10) NOT NULL DEFAULT '1d'
      COMMENT '周期类型（1d:昨日 7d:近7日 30d:近30日）' AFTER `date_type`,
  ADD COLUMN `deal_resource_count` BIGINT NULL
      COMMENT '成交资源数（去重资源ID数）' AFTER `refund_rate_mom`,
  ADD COLUMN `deal_resource_count_mom` DECIMAL(12,6) NULL
      COMMENT '成交资源数环比' AFTER `deal_resource_count`;

-- 补唯一键。若存在历史重复数据，此语句会失败；
-- 先用下方"重复数据体检"查出并清理后再执行。
ALTER TABLE `ads_sup_ota_ctrip_trade_di`
  ADD UNIQUE KEY `uk_ads_sup_ota_ctrip_trade_di`
    (`account_id`, `transaction_date`, `period_type`,
     `signing_code`, `biz_type`, `date_type`);

-- 索引跟随字段改名重建（idx_kylin_period 为本次新增，
-- 支持按周期口径查询，以及覆盖写入时按 period_type 定向删除）
ALTER TABLE `ads_sup_ota_ctrip_trade_di`
  DROP KEY `idx_kylin_date`,
  DROP KEY `idx_kylin_account_date`,
  DROP KEY `idx_kylin_scenic_date`,
  ADD KEY `idx_kylin_date` (`transaction_date`),
  ADD KEY `idx_kylin_account_date` (`account_id`, `transaction_date`),
  ADD KEY `idx_kylin_scenic_date` (`scenic_id`, `transaction_date`),
  ADD KEY `idx_kylin_period` (`account_id`, `period_type`, `transaction_date`);

-- -----------------------------------------------------------------------------
-- 第三步：目的地评级表字段重命名
-- -----------------------------------------------------------------------------
ALTER TABLE `ads_sup_ota_ctrip_destination_rating_di`
  CHANGE `stat_month`     `evaluation_month`  VARCHAR(7)  NOT NULL COMMENT '考核月份（YYYY-MM）',
  CHANGE `message`        `analysis_text`     MEDIUMTEXT  NULL COMMENT 'AI 分析原文',
  CHANGE `message_length` `analysis_text_len` INT         NULL COMMENT '分析原文字符数',
  CHANGE `collected_at`   `collect_time`      DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '采集时间';

ALTER TABLE `ads_sup_ota_ctrip_destination_rating_di`
  DROP KEY `idx_rating_month`,
  DROP KEY `idx_rating_scenic`,
  ADD KEY `idx_rating_month` (`evaluation_month`),
  ADD KEY `idx_rating_scenic` (`scenic_id`, `evaluation_month`);

-- -----------------------------------------------------------------------------
-- 第四步：供应商评级表字段重命名 + 唯一键索引改名
-- -----------------------------------------------------------------------------
ALTER TABLE `ads_sup_ota_ctrip_score_di`
  CHANGE `stat_month`              `evaluation_month`               VARCHAR(7)    NOT NULL COMMENT '月份（YYYY-MM）',
  CHANGE `service_score`           `overall_service_score`          DECIMAL(8,2)  NULL COMMENT '综合服务分',
  CHANGE `order_confirm_rate`      `ocr_score`                      DECIMAL(8,2)  NULL COMMENT '订单确认率评分',
  CHANGE `fulfillment_defect_rate` `fdr_score`                      DECIMAL(8,2)  NULL COMMENT '履约缺陷率评分',
  CHANGE `cs_intervention_rate`    `manual_intervention_rate_score` DECIMAL(8,2)  NULL COMMENT '客服人工介入率评分',
  CHANGE `reply_timeliness_rate`   `timely_response_20s_score`      DECIMAL(8,2)  NULL COMMENT '20秒回复及时率评分',
  CHANGE `im_negative_rate`        `im_neg_rate_score`              DECIMAL(8,2)  NULL COMMENT 'IM差评率评分',
  CHANGE `collected_at`            `collect_time`                   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '采集时间';

ALTER TABLE `ads_sup_ota_ctrip_score_di`
  DROP KEY `uk_ctrip_supplier_rate_score`,
  ADD UNIQUE KEY `uk_ads_sup_ota_ctrip_score_di`
    (`account_id`, `evaluation_month`, `region_id`),
  DROP KEY `idx_score_month`,
  DROP KEY `idx_score_scenic`,
  ADD KEY `idx_score_month` (`evaluation_month`),
  ADD KEY `idx_score_scenic` (`scenic_id`, `evaluation_month`);

-- -----------------------------------------------------------------------------
-- 第五步：五张表统一新增 create_time / update_time（数据库自动维护）
-- 说明：存量行的 create_time 会填为本次执行时刻，无法还原真实入库时间；
--       若需要精确的历史入库时间，可用旧表的 created_at 回填（见末尾可选语句）。
-- -----------------------------------------------------------------------------
ALTER TABLE `ads_sup_ota_ctrip_trade_di`
  ADD COLUMN `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  ADD COLUMN `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

ALTER TABLE `ads_sup_ota_ctrip_destination_rating_di`
  ADD COLUMN `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  ADD COLUMN `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

ALTER TABLE `ads_sup_ota_ctrip_score_di`
  ADD COLUMN `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  ADD COLUMN `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

ALTER TABLE `ads_service_ota_ctrip_account`
  ADD COLUMN `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  ADD COLUMN `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

ALTER TABLE `ads_service_ota_ctrip_run_log`
  ADD COLUMN `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  ADD COLUMN `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

-- -----------------------------------------------------------------------------
-- 可选一：若旧表带有 created_at / updated_at，用它们回填真实时间后再删除旧列
-- -----------------------------------------------------------------------------
-- UPDATE `ads_sup_ota_ctrip_trade_di`
--   SET `create_time` = `created_at`, `update_time` = `updated_at`;
-- ALTER TABLE `ads_sup_ota_ctrip_trade_di` DROP COLUMN `created_at`, DROP COLUMN `updated_at`;

-- -----------------------------------------------------------------------------
-- 可选二：补 scenic_id（仅当存量表还没有该字段时执行）
-- -----------------------------------------------------------------------------
-- ALTER TABLE `ads_sup_ota_ctrip_trade_di`
--   ADD COLUMN `scenic_id` VARCHAR(50) NULL
--       COMMENT '景区ID（如 PFTSCA01018088，账号级绑定）' AFTER `account_id`,
--   ADD KEY `idx_kylin_scenic_date` (`scenic_id`, `transaction_date`);

-- 以账号维表为准统一回填三张业务表（推荐：绑定关系只维护一处）
-- UPDATE `ads_sup_ota_ctrip_trade_di` t
--   JOIN `ads_service_ota_ctrip_account` a ON a.`account_id` = t.`account_id`
--   SET t.`scenic_id` = a.`scenic_id`
--   WHERE t.`scenic_id` IS NULL AND a.`scenic_id` IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 重复数据体检：补唯一键前先确认存量数据是否已有重复
-- 若返回结果非空，需要先决定保留哪一行（通常保留 id 最大者）再加唯一键
-- -----------------------------------------------------------------------------
-- SELECT `account_id`, `transaction_date`, `period_type`,
--        `signing_code`, `biz_type`, `date_type`,
--        COUNT(*) AS dup_cnt, GROUP_CONCAT(`id`) AS ids
--   FROM `ads_sup_ota_ctrip_trade_di`
--   GROUP BY 1, 2, 3, 4, 5, 6
--   HAVING dup_cnt > 1;

-- 清理重复（保留每组 id 最大的一行）——执行前务必先备份
-- DELETE t FROM `ads_sup_ota_ctrip_trade_di` t
--   JOIN (
--     SELECT MAX(`id`) AS keep_id
--       FROM `ads_sup_ota_ctrip_trade_di`
--       GROUP BY `account_id`, `transaction_date`, `period_type`,
--                `signing_code`, `biz_type`, `date_type`
--   ) k ON t.`id` < k.keep_id
--   JOIN `ads_sup_ota_ctrip_trade_di` s ON s.`id` = k.keep_id
--   WHERE t.`account_id` = s.`account_id`
--     AND t.`transaction_date` = s.`transaction_date`
--     AND t.`period_type` = s.`period_type`
--     AND t.`signing_code` = s.`signing_code`
--     AND t.`biz_type` = s.`biz_type`
--     AND t.`date_type` = s.`date_type`;

-- -----------------------------------------------------------------------------
-- 迁移完成后的校验
-- -----------------------------------------------------------------------------
-- 1) 结构对照：python -m ctrip.cli show-ddl 输出与实际表结构比对
-- 2) 自检：python tools/verify_schema.py
-- 3) 幂等回归：连续执行两次 collect，确认总行数不增长
