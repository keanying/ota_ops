# 携程商家后台多账号数据采集工具

本项目重构了原有的四个独立脚本（`scraper.py`、`fetch_daily_kylin.py`、`fetch_destination_analysis.py`、`fetch_supplier_rate_score.py`），把自动登录、数据采集、数据落地整合为一套可用于定时任务的工程化程序。核心设计目标有三个：**多账号从底层开始隔离**、**默认落 CSV 且可无痛切换 MySQL**、**表结构集中在单一文件维护**。

---

## 一、快速开始

`.env` 会被程序**自动加载**，无需任何手动导入动作。这一点对 Windows 尤其重要：PowerShell 与 CMD 里并不存在 `source` 命令，若按 Linux 习惯去执行 `set -a && source .env` 会直接失败，环境变量不会生效，程序随即报「账号缺少登录用户名」。

### Windows（PowerShell）

```powershell
# 1. 安装依赖
uv venv .venv
.\.venv\Scripts\Activate.ps1
uv pip install -e .
playwright install chromium

# 2. 准备配置（注意是 Copy-Item，不是 cp）
Copy-Item config\config.example.yaml config\config.yaml
Copy-Item .env.example .env
notepad .env                              # 填入账号密码、MySQL 连接信息

# 3. 环境体检（强烈建议先跑这一步）
python -m ctrip.cli doctor

# 4. 一条命令跑完整条链路（体检 → 建表 → 登录 → 采集 → 写入）
python -m ctrip.cli run --start-date 2025-01-01 --start-month 2025-01 --sink both
```

### Linux / macOS

```bash
# 1. 安装依赖（依赖与数据库初始化配置统一在 pyproject.toml 中声明）
uv venv .venv && source .venv/bin/activate
uv pip install -e .
playwright install chromium

# 2. 准备配置
cp config/config.example.yaml config/config.yaml
cp .env.example .env && vim .env          # 填入账号密码、MySQL 连接信息

# 3. 环境体检
python -m ctrip.cli doctor

# 4. 一条命令跑完整条链路
python -m ctrip.cli run --start-date 2025-01-01 --start-month 2025-01 --sink both
```

### 统一入口 `run`：一条命令跑完整条链路

`run` 把整条链路串成一次调用，是**日常定时任务与首次建仓的推荐入口**：

1. **环境体检** —— 凭据、浏览器、时区、MySQL 连通性，有阻塞问题立即停下并给出修复命令；
2. **自动建表** —— 表不存在则建（`CREATE TABLE IF NOT EXISTS`，对存量表无副作用）；
3. **登录** —— 逐账号确保登录态：有效则复用，失效自动重登，登录后校验账号指纹防串号；
4. **采集** —— 按指定区间与批次采三类数据，中途登录态失效会自动重登并重跑当前任务；
5. **写入** —— 落 CSV / MySQL，并输出含景区 ID、采集量、写入量、清理量的汇总报告。

```powershell
# 默认：T+1（昨天）与上月，落地方式取配置
python -m ctrip.cli run

# 首次建仓：日粒度从 2025-01-01 起按 30 天一批，月粒度从 2025-01 起
python -m ctrip.cli run --start-date 2025-01-01 --start-month 2025-01 --sink both

# 让浏览器登录后常驻后台，后续运行直接复用（不再触发验证）
python -m ctrip.cli run --keep-alive

# 首次需要人工过滑块时，显示浏览器窗口
python -m ctrip.cli run --keep-alive --no-headless
```

退出码：`0` 全部成功、`1` 部分成功、`2` 全部失败或体检未通过，可直接用于定时任务告警。

若只想执行单个环节，`login`、`collect`、`init-db` 等子命令依然保留，参数与 `run` 一致。

### 让浏览器常驻，避免反复触发验证

浏览器 profile 保存在项目内 `browser/<账号ID>/profile/`，**同一账号始终复用同一套设备指纹与本地存储**，因此不会每次登录都被当成陌生设备。再配合常驻模式，浏览器进程在后台持续存活，连启动都省掉：

```powershell
python -m ctrip.cli browser --start      # 拉起常驻浏览器（后台存活）
python -m ctrip.cli browser --status     # 查看 profile 与常驻状态
python -m ctrip.cli login                # 自动复用已打开的浏览器
python -m ctrip.cli browser --stop       # 停止常驻（profile 保留，指纹不丢）
```

启用常驻需要在 `config/config.yaml` 的 `ctrip` 段配置 `cdp_port`（多账号自动递增端口）。详见第七章。

### 指定采集的时间起点

不传时间参数时，**日粒度默认采到昨天（T+1）、月粒度默认采上个月**，适合日常定时任务。要从指定时间开始则加起点参数，长区间会**自动按 30 天一批**切分：

```bash
# 日常增量：交易采到昨天，两个评级采上个月
python -m ctrip.cli collect --job all --sink both
ads_scenic_customer_insight_source_ranking_mo
# 从 2025-01-01 采到昨天（交易，自动分批）
python -m ctrip.cli collect --job kylin --start-date 2026-09-16 --sink both

# 从 2025-01 采到上个月（两个评级）
python -m ctrip.cli collect --job rating --job score --start-month 2026-08 --sink both

# 三类数据均从 2025 年初开始
python -m ctrip.cli collect --job all --start-date 2025-01-01 --start-month 2025-01 --sink both
```

更多参数与调用案例见「六、命令与配置」。重复采集同一天不会产生重复数据：默认会先清理该日期（或该月份）的旧数据再写入。

### 要采两年历史数据

日常增量用上面的 `collect`；首次建仓补历史用回溯脚本，先探测上游到底给多少数据，再正式跑：

```bash
python tools/backfill.py --probe --probe-from 2025-01   # 探测（只读，不落库）
python tools/backfill.py --start 2025-01 --sink both    # 正式回溯
```

详见「七、历史数据回溯」。

### 遇到问题先跑体检

`python -m ctrip.cli doctor` 会逐项检查并直接给出修复命令：

```
[1/6] .env 文件        → 是否存在、是否已加载、BOM/CRLF 情况
[2/6] 配置文件        → 实际生效的文件路径、账号源
[3/6] 账号凭据        → 逐个账号的用户名/密码/景区ID 是否就位
[4/6] MySQL 连接      → 能否连通、五张表是否已建
[5/6] Playwright     → Chromium 是否已安装（并区分完整版/无头精简版）
[6/6] 时区数据库      → 能否解析 Asia/Shanghai（Windows 需 tzdata）
```

---

## 二、原代码存在的问题与修复对照

下表是逐条核对后的问题清单，左列为原实现的行为，右列为本项目的处理方式。

| 环节 | 原实现的问题 | 本项目的修复 |
|---|---|---|
| 凭据管理 | `ACCOUNT` / `PASSWORD` 明文硬编码在 `scraper.py` | 配置文件用 `${ENV_VAR}` 占位符，密码只从环境变量或账号管理 API 读取，源码零明文 |
| 登录态 | 固定单文件 `auth/ctrip_auth.json`，无法多账号 | 每账号独立目录 `auth/{account_id}/storage_state.json` + `meta.json`，文件权限 0600 |
| 元素定位 | `is_visible(timeout=3000)` —— 该方法并不接受 `timeout` 参数 | 改用 `wait_for(state="visible", timeout=...)` |
| 登录按钮 | 硬编码 `name="登 录"`（中间带空格），一旦文案变化即失效 | 正则 `^登\s*录$` 等多候选匹配，并保留回车提交兜底 |
| 协议勾选 | 抓第一个 `input[type=checkbox]` 并 `check()`，携程自定义样式下常常无效 | 先 `check(force=True)`，失败则点击关联 `label` |
| 弹窗处理 | 「暂不修改」弹窗在等待跳转**之后**才处理，弹窗会挡住跳转判定导致误判超时 | 弹窗清理放入等待跳转的轮询循环内，每轮都尝试关闭 |
| 成功判定 | 只看 URL 是否含 `dashboard` | 双重判定：URL 到达后台 **且** 真实业务接口 `getVendorDstList` 返回 Success |
| 运行环境 | 强制 `headless=False`，服务器无图形界面无法运行 | 默认无头并注入反自动化检测脚本；识别到滑块/验证码时明确提示改用 `--no-headless` |
| 交互阻塞 | `input("是否重新登录？")` 会卡住定时任务 | 全部改为命令行参数（`--force`），无任何交互式输入 |
| 失败排障 | 失败时无任何现场信息 | 自动落盘全页截图与 HTML 到 `logs/{account_id}/login/` |
| Cookie 注入 | `fetch_daily_kylin.py` 用 `domain.lstrip(".")` 去掉前导点，`.ctrip.com` 的 Cookie 不会发往 `m.ctrip.com`；另两个脚本又把 `vbooking.ctrip.com` 强行改写成 `.ctrip.com`，把站点专属 Cookie 提升为父域 Cookie | 统一构造标准 `http.cookiejar.Cookie`，原样保留 domain/path 及 `domain_initial_dot`，由 requests 按域正确匹配 |
| 请求头 | 三个脚本头部不一致，kylin 缺 UA / Origin / Referer | 统一 Session 构建，补齐 UA、Origin、Referer、`appname` |
| 参数解析 | 手搓 `sys.argv`，靠「以 `/` 开头」「长度为 7」判断参数含义；`fetch_destination_analysis.py` 的 `positional[-1].startswith("/")` 会把月份误判为输出路径 | 统一 `argparse` 子命令，参数语义明确、自带校验与帮助 |
| 会话校验 | 用「HTML 长度 > 5000 字节」判断登录态 | 状态码 + 重定向目标 + 真实接口返回三重判据 |
| 业务失败 | `Ack != Success` 时直接 `return []`，数据静默丢失且无告警 | 业务失败纳入重试，识别鉴权类错误抛 `SessionExpired`，超限抛异常并记录 |
| 列名合法性 | 指标名 `ttd_arfd_ord_cnt\|ttd_pay_ord_cnt\|/` 直接作为 CSV 列名，无法作为数据库列名 | schema 层做 `source → name` 映射，统一为 `refund_rate` 等合法列名 |
| CSV 格式 | kylin 写「中文行 + 英文行」双表头，文件无法被 pandas / `LOAD DATA` 直接读取；supplier 用中文表头；destination 输出 JSON | 三者统一为单行英文表头（可选追加中文说明行），列名与数据库完全一致 |
| 重复累积 | kylin 以追加模式写 CSV，重复运行数据不断累积 | 默认按批次覆盖写，写入前按唯一键去重；MySQL 侧用 UPSERT |
| 日志 | 全部 `print`，无级别、无留档 | 标准 logging，控制台 + `logs/main.log` + 每账号 `logs/{account_id}/collect.log` |
| 退出码 | 无规范 | 0 全部成功、1 部分成功、2 全部失败、130 用户中断，便于定时任务告警 |
| 时区 | `date.today()` 无时区控制，服务器 UTC 会少采一天 | 统一 `zoneinfo` 时区（默认 `Asia/Shanghai`） |
| 落库能力 | 完全没有，文档提到的 `ctrip_data_loader.py` 实际不存在 | 完整实现 CSV / MySQL / both 三种落地，参数化 UPSERT |
| SQL 安全 | `scenic_supplier.py` 用字符串拼接生成 INSERT，存在注入与转义隐患 | 全程参数化 `executemany`，不做任何 SQL 字符串拼接 |
| 多账号 | 数据无 `account_id`，无法区分来源 | `account_id` 是所有表的第一个字段且位于唯一键首位 |
| 景区关联 | 数据不带景区 ID，无法与内部景区维表关联 | 账号级绑定 `scenic_id`，采集时统一注入每一行 |
| 指标缺失 | 「成交资源数」从未被请求过——它不在原脚本的 `METRICS` 里，而且即使加进去也拉不到，因为它的 `queryId`、接口、聚合方式与其他五项全不相同 | 单独发一轮 `queryId=354` + `getKylinDataWithSublineV2` + `aggregateType=distinct` 请求，再按日期合并到同一行；它失败不会拖累主指标 |
| 周期口径 | 只能采逐日明细，页面上的「近 7 日 / 近 30 日」无法落库 | 新增 `period_type` 与 `--period`：`1d` 逐日、`7d`/`30d` 当前窗口聚合（默认只采 `1d`，不浪费请求） |
| 资源数只有首日 | （本轮发现）资源数走的聚合接口固定 `needAggregate: true`，传入区间只返回起始日一个值，导致自定义区间下只有首日有数、其余天全为 NULL | 日口径下逐日单发资源数请求（`startdate == enddate`、`value=1`）；主指标请求方式保持不变 |
| 自检失真 | （本轮发现）模拟接口与假客户端在 `value <= 1` 时逐日返回，恰好掩盖了上面这个 bug，致使自检全绿但真实环境下缺数 | 两者都改为严格只返回起始日一个键，并补上「逐日请求无遗漏」的断言 |
| 当月回溯 | （本轮发现）回溯分片取月末为截止日，而月末晚于今天会被区间校验拒绝，整个当月直接 failed，本月已结算的几天一条也采不到 | 当月分片自动截断到昨天并标记 `partial`，未来月份标为 `not_ready` 而不计失败 |
| 回溯汇总误导 | 断点续跑时已采过的分片会跳过，但汇总只报「本次采集 0 行」，看不出到底是没采到还是早已采好 | 汇总区分「本次新采 / 历史已采 / 区间累计」，并直接给出核对 SQL 与 `--force` 提示 |
| 重试浪费 | 超出上游历史窗口的空分片每次重跑都会重试，大量时间耗在必然为空的月份上 | 新增 `--max-empty-retry`（默认 3），连续多次为空则判定窗口外不再重试，采到数据则计数归零 |
| 覆盖范围 | （本轮发现）引入周期口径后，清理条件仅按「账号 + 日期」则会误删同日其他口径；默认 30 天区间下 `30d` 窗口起始日必然落入 `1d` 区间，一重跑就丢数据 | schema 新增 `partition_extra` 维度，清理范围同步限定 `period_type`（CSV 与 MySQL 两侧一致） |
| 时间区间 | 只能传「往前 N 天」和单个月份，无法指定开始时间；长区间也不分批 | 日粒度支持 `--start-date` 并按 `--batch-days`（默认 30 天）自动切批；月粒度支持 `--start-month`/`--end-month` |
| 重采语义 | 同一天重复采集时旧数据不会被清掉，上游下线的维度会永久沾在库里 | 默认 `overwrite`：同事务内先按「账号 + 日期/月份」删除旧数据再写入 |
| 中途失效 | 登录态在采集中途过期后只能整份失败重跑 | 识别到 `SessionExpired` 后自动重登、重验指纹并重跑当前任务 |
| 异常层级 | （本项目自身引入的缺陷）`SessionExpired` 继承自 `ApiError`，被采集器「单片失败不中断」的 `except ApiError` 吸走，降级为普通警告，致使自动重登永不触发 | 三个采集器共 6 处显式前置 `except SessionExpired: raise`，让全局故障穿透容错网直达调度层 |
| 浏览器指纹 | 每次登录都新开临时浏览器（`launch` + `new_context`），设备指纹、localStorage、IndexedDB 全部丢弃，携程每次都当成陌生设备，反复触发验证 | 改用项目内持久化 profile（`launch_persistent_context`），每账号一个目录，设备指纹与本地存储跳过重启保留 |
| 浏览器启动 | 每次运行都要重新拉起浏览器，耗时且增加被风控识别的机会 | 常驻模式：以独立进程拉起浏览器并监听 CDP 端口，后续运行通过 `connect_over_cdp` 直接复用，进程持续存活 |
| 并发保护 | （本轮发现）headless 下 Chromium **不写 SingletonLock**，仅靠锁文件判断时无头场景（定时任务常态）完全没有保护，两进程同写一个 profile 会损坏 Cookie 数据库 | `session.json` 记录 `holder_pid` 并在会话结束时清除，并发判据改为三道：常驻在线、holder 进程存活、锁文件指向活进程 |
| 浏览器构建 | （本轮发现）Playwright 默认的 `chrome-headless-shell` 不写 `Preferences` / `Login Data` / `History`，持久化不完整，且与常驻模式二进制不同导致指纹不一致 | 强制 `channel="chromium"` 使用完整构建，启动失败时降级并告警 |
| 常驻指纹 | （本轮发现）裸 Chromium 常驻时 UA 带 `HeadlessChrome`、时区为 UTC、屏幕 800x600，常驻模式反而更易被风控识别，与目标背道而驰 | spawn 命令行补齐 `--user-agent` / `--window-size` / `--lang` / `--accept-lang` / `--screen-info`，并以 `TZ` 环境变量传递时区；连接后校验 UA 与时区并告警 |
| 端口归属 | （本轮发现）CDP 端口被无关浏览器占用时，`connect_over_cdp` 会静默连到陌生浏览器——既无登录态，又会用陌生指纹发请求 | 复用前用 `session.json` 的 pid 与 `/json/version` 的 `webSocketDebuggerUrl` 双重校验归属，不匹配则回退 profile 模式并提示换端口；spawn 前端口已占用则拒绝启动 |
| 停止时序 | （本轮发现）`stop_keep_alive` 在子进程与锁文件释放前就返回，紧接着的下一次运行会报「profile 被占用」 | 等待占用状态真正释放，并自动清理死进程遗留的锁文件 |
| 运行入口 | 登录、建表、采集需要分多次命令手动串起来 | 新增 `run` 统一入口，体检 → 建表 → 登录 → 采集 → 写入 一次连贯完成，退出码可直接用于告警 |
| 退出噪声 | （本轮发现）Playwright 断开连接后遗留协程在进程退出时向 stderr 打印 `Task was destroyed but it is pending`，用户会误以为采集出错 | 精确过滤这几行无害噪声，已断言验证真实错误与非零退出码不受影响 |

---

## 三、景区 ID（scenic_id）绑定

采集时会为每一行数据绑定景区 ID（形如 `PFTSCA01018088`），便于与内部景区维表关联。绑定粒度为 **账号级**：一个采集账号对应一个景区，在账号配置里声明一次即可，各采集器无需关心该字段。

```yaml
accounts:
  - account_id: A001
    name: 主账号
    account: "${CTRIP_A001_ACCOUNT}"
    password: "${CTRIP_A001_PASSWORD}"
    scenic_id: PFTSCA01018088    # 景区ID，账号级绑定
    status: active
```

除 YAML 之外，还支持三种注入方式：环境变量占位符 `scenic_id: "${CTRIP_A001_SCENIC_ID}"`；内联账号列表 `CTRIP_ACCOUNTS="A001:user1:pwd1:PFTSCA01018088"`；账号管理 API 下发 `scenic_id`（也兼容 `scenicId`）。若 API 未下发该字段，会保留本地配置中已有的绑定，不会被空值洗掉。

字段落地时有几个关键设计选择。`scenic_id` 位于 `account_id` 之后的第二列，**不纳入唯一键**：账号与景区是一对一关系，`account_id` 已能唯一定位记录；若景区 ID 进了唯一键，日后修正绑定关系时会产生重复行而不是原地更新。相应地，它**参与 UPSERT 更新**，所以改完配置重跑一次即可自动回填历史数据的景区 ID。三张业务表均含该字段，账号维表与采集审计表也一并加上（前者作为绑定关系的权威源，后者便于按景区监控采集状况）。

安全与容错上做了三点约束。景区 ID 会经白名单校验（`^[A-Za-z0-9_-]{1,50}$`），尝试传入 `PFT'; DROP TABLE--` 之类的值会直接被拒绝；未绑定时写入 `NULL` 而非空串，并在日志与 `show-config` 中明确告警；命令行提供 `--scenic-id` 作临时覆盖，但**仅允许在只处理单个账号时使用**，否则会把多个账号的数据错标成同一个景区：

```bash
python -m ctrip.cli collect --job all --account-id A002 --scenic-id PFTSCA01029177
```

已有旧表需要补字段时，执行一次 `sql/migration_to_new_schema.sql` 即可，里面同时附了以账号维表为准回填历史数据的 `UPDATE ... JOIN` 语句（详见第十节的迁移说明）。

---

## 四、多账号数据隔离设计

这是本次重构的重点。隔离不是「在表上补一个字段」，而是贯穿六个层次的约束。

| 层次 | 隔离手段 |
|---|---|
| 凭据层 | 每账号一条配置记录，密码经 `${ENV}` 注入；账号列表可由外部账号管理 API 下发；数据库只存**不含密码**的账号维表 |
| 登录态层 | `auth/{account_id}/storage_state.json`，账号之间物理分离，不存在共享 Cookie 文件 |
| 浏览器层 | `browser/{account_id}/profile/` 每账号一个独立设备指纹，常驻端口按账号递增，进程与本地存储互不干扰 |
| 运行时层 | 每账号独占 `requests.Session`、独立 `logger`、独立 Sink 句柄；并发模式下每线程独立 MySQL 连接 |
| 文件层 | `data/output/{account_id}/{表名}_{批次}.csv`，且 CSV 内部首列仍是 `account_id`，多文件合并后依然可区分来源 |
| 数据库层 | 所有业务表首字段为 `account_id NOT NULL`，唯一键以 `account_id` 开头，UPSERT 天然按账号隔离；另可选按账号分物理表 |

在这些结构性约束之外，还有四道主动防线。

第一道是 **schema 层的强制校验**。`TableSchema.__post_init__` 会拒绝任何「唯一键未以 `account_id` 开头」的表定义，从源头上排除写出会跨账号覆盖的 SQL 的可能：

```python
if "account_id" in names and self.unique_key and self.unique_key[0] != "account_id":
    raise ValueError(f"表 {self.name}: 唯一键必须以 account_id 开头，确保账号间数据隔离")
```

第二道是 **`account_id` 白名单校验**。由于 `account_id` 会参与拼接目录名与表名，加载配置时即用 `^[A-Za-z0-9_-]{1,50}$` 校验，杜绝路径穿越。

第三道是 **禁止匿名默认账号**。原方案文档中设计了 `ctrip_auth_default.json` 作为向后兼容路径，本项目刻意不提供：一旦允许「不指定账号也能采集」，就必然出现来源不明的数据混入。未配置账号时程序直接报错退出。

第四道，也是最关键的一道，是 **账号身份指纹校验**。多账号场景下最危险的故障是「Cookie 串号」——某个账号的登录态被错误地放到了另一个账号的目录下，程序会毫不知情地把 A 账号的数据打上 B 账号的标签写入数据库。本项目在每次采集前，用该账号在携程侧可见的目的地列表作为身份指纹，与历史记录比对；若两者毫无交集，立即拒绝本次采集：

```
2026-08-13 02:08:53 ERROR [ctrip.pipeline] [A002] 账号指纹不一致！
历史目的地 ['200011', '200012']，当前 ['100031', '100032', '100033']，无任何交集。
很可能是登录态串号，已拒绝本次采集以保护账号数据隔离；
如确实是账号权限变更，请删除 auth/A002/meta.json 中的 fingerprint 后重跑
```

> 实现上有一个容易踩的坑：如果登录态校验环节顺手刷新了指纹，那么串号后的新身份会立刻成为「基准」，比对将永远通过。因此指纹的写入只发生在 `check_fingerprint` 中，且身份吻合时取历史与当前的并集作为新基准（以适应目的地正常增减），而非直接覆盖。这个缺陷在验证阶段被实测发现并修复。

最后，**容错也是隔离的一部分**。单个账号的登录失败、接口异常、落库报错都只影响该账号自身，其余账号继续执行，最终汇总为一张报告；每个「账号 × 任务」都会在 `ads_service_ota_ctrip_run_log` 留下一条审计记录。

> 审计记录这里也有一个隐蔽的坑。原本用 `"-"` 作为 `run_key` 的占位符（登录失败时还未产生真正的批次标识），但 `"-"` 正好在脏数据清洗的空值占位符白名单里，会被归一为 `NULL`，进而因唯一键为空被行校验拦下——结果是「登录失败」「串号拦截」这些**最需要被记录的异常**反而静默丢失。已改为 `"NA"` 并实测确认入库。

---

## 五、表结构集中维护

所有表结构定义在 **`ctrip/schema.py` 这一个文件**中。修改字段只需改这里，四类产物会自动同步：建表 DDL、CSV 表头与列顺序、MySQL UPSERT 语句、上游 API 字段映射与类型清洗。

字段定义的写法如下，注释即 CSV 中文表头与数据库列注释，`source` 是上游接口的原始字段名，`cast` 决定清洗方式：

```python
Field("turnover", "DECIMAL(18,2)", "成交销售额（元）",
      source="ttd_suc_nocoef_income", cast="decimal"),
Field("refund_rate", "DECIMAL(12,6)", "退订率",
      source="ttd_arfd_ord_cnt|ttd_pay_ord_cnt|/", cast="decimal"),
```

`source` 这一层映射解决了原代码的一个硬伤：携程的退订率指标名里带 `|` 和 `/`，既不能作为数据库列名，也让 CSV 难以处理。现在上游叫什么、落库叫什么互相解耦，将来携程改了指标名，只改 `source` 一处即可，数据库列名和历史数据都不受影响。

`cast` 提供了统一的脏数据清洗，实测覆盖以下情况：

| 输入 | 清洗结果 | 说明 |
|---|---|---|
| `"1,234.56"` | `Decimal("1234.56")` | 千分位 |
| `"12笔"` | `12` | 带单位 |
| `"12.5%"` | `Decimal("0.125")` | 百分号自动折算 |
| `"2026/08/05"` | `date(2026, 8, 5)` | 日期格式归一 |
| `""` / `"-"` / `"--"` / `"null"` | `None` | 空值占位符 |
| `50.0` | `50` | 浮点转整数 |

### 改字段的完整流程

```bash
vim ctrip/schema.py                        # 1. 增删改字段
python -m ctrip.cli show-schema            # 2. 确认字段与上游映射
python -m ctrip.cli show-ddl --write       # 3. 重新生成 sql/ddl.sql
python -m ctrip.cli init-db                # 4. 新增表会自动创建
python tools/verify_schema.py              # 5. 自检
```

对**已存在的表新增字段**，MySQL 的 `CREATE TABLE IF NOT EXISTS` 不会自动加列，需要手工执行一条 `ALTER TABLE`（`show-ddl` 的输出可以直接对照列定义抄写）。这是有意的保守设计——自动改动生产表结构风险过高。

### 五张表

| 表名 | 用途 | 唯一键 |
|---|---|---|
| `ads_sup_ota_ctrip_trade_di` | 交易卡片数据 | `(account_id, transaction_date, period_type, signing_code, biz_type, date_type)` |
| `ads_sup_ota_ctrip_destination_rating_di` | 目的地评级 | `(account_id, evaluation_month, region_id)` |
| `ads_sup_ota_ctrip_score_di` | 供应商评级雷达图 | `(account_id, evaluation_month, region_id)` |
| `ads_service_ota_ctrip_account` | 账号维表（**不含密码**） | `(account_id)` |
| `ads_service_ota_ctrip_run_log` | 采集运行审计日志 | `(account_id, job, run_key)` |

五张表的前两列统一为 `account_id` 与 `scenic_id`，前者进唯一键首位、后者不进唯一键；末尾统一为 `collect_time`（采集时间，由程序写入）、`create_time` 与 `update_time`（由数据库自动维护）。

### 三类时间字段的职责划分

表里有三个时间字段，容易混淆，因此在 schema 层做了严格区分：

| 字段 | 语义 | 写入方 | 进 CSV |
|---|---|---|---|
| `collect_time` | 本次采集发生的时刻（业务含义） | 程序显式写入 | 是 |
| `create_time` | 该行首次入库时间 | 数据库默认值 | 否 |
| `update_time` | 该行最近一次被写入的时间 | 数据库自动维护 | 否 |

`create_time` 与 `update_time` 在 `schema.py` 里标记为 `db_managed=True`，因此只出现在建表 DDL 里，不进 INSERT 列清单、不进 CSV 表头、也不参与行构造——程序永远不会覆盖它们，时间戳语义由数据库完全接管。它们与 CSV 无关也是有意的：CSV 是采集产物快照，不应当携带数据库的入库元信息。

需要注意的是，`update_time` 在 DDL 里虽然已带 `ON UPDATE CURRENT_TIMESTAMP`，但 MySQL 仅在行内容**真的发生变化**时才触发它。因此 UPSERT 语句里额外显式加了 `update_time = CURRENT_TIMESTAMP`，保证「数值没变但确实重跑过一次」也能从表里看出来。

### 字段命名对照（上游接口 → 落库列名）

携程接口的原始字段名既不可读也不合法，因此全部在 schema 层做了映射。以交易表为例：

| 上游字段（`source`） | 落库列名 | 中文含义 |
|---|---|---|
| `date_start` | `transaction_date` | 交易日期 |
| `code` | `signing_code` | 签约类型编码 |
| `vbk_bu_type` | `biz_type` | 业务类型 |
| `ttd_suc_nocoef_income` | `turnover` | 成交销售额 |
| `ttd_suc_ord_cnt` | `order_count` | 订单数 |
| `ttd_suc_nocoef_qty` | `sold_ticket_cnt` | 成交票量 |
| `ttd_chk_qty` | `validated_ticket_volume` | 核验票量 |
| `ttd_arfd_ord_cnt\|ttd_pay_ord_cnt\|/` | `refund_rate` | 退订率 |
| `suc_resourceid` | `deal_resource_count` | 成交资源数 |
| 上述各项 + `_MOMRate` | 对应列名 + `_mom` | 环比 |

其中「成交资源数」与其他五项**不是同一个请求**，三处都不同，缺一不可：

| | 其他五项指标 | 成交资源数 |
|---|---|---|
| `queryId` | 353 | **354** |
| 接口 | `getKylinTrendLineData` | **`getKylinDataWithSublineV2`** |
| 聚合方式 | 无 | **`aggregateType: "distinct"`**（资源 ID 去重计数）|

这也是早期版本采不到这个指标的原因——原脚本的 `METRICS` 里根本没有它，而即使把指标名塞进原来的 `metricList`，因为 queryId 与聚合方式不对也仍然会返回空值。现在资源数单独发请求再合并到同一行上；它拿不到时**不会让主指标整批失败**，只留 NULL 并告警。

还有一个更隐蔽的约束：**日口径下资源数必须逐日单发请求**。`getKylinDataWithSublineV2` 固定 `needAggregate: true`，传入一个区间时会把整段聚合成**一个**值，返回的 `valueDateMap` 只有区间起始日一个键。如果按主指标的做法一次传 30 天，结果就是**只有首日有资源数，其余 29 天全是 NULL**。而资源数是去重计数，天粒度的值无法由区间聚合值拆分得出，因此逐日发请求是唯一途径（真实拓包也证实页面在日口径下发的就是 `startdate == enddate`、`calculate.value = 1` 的单日请求）。

这里的分工要看清：**主指标的请求方式完全未变**，仍然是 `getKylinTrendLineData` 一次拿回整个区间的每日明细；只有资源数这一项是逐日补充的。以 30 天区间、一个签约组合、一种日期类型为例，一共发 1 + 30 = 31 次请求。单天失败只会让那一天的资源数留 NULL，不影响主指标、也不影响其他天，重跑可回填；失败日期会汇总成一条告警而不是刷 30 行日志。`7d`/`30d` 口径下它仍是区间单请求，与窗口一致。

评分表同理：`serviceScore` → `overall_service_score`、`orderConfirmationRate` → `ocr_score`、`fulfillmentDefectRate` → `fdr_score`、`customerServiceManualInterventionRate` → `manual_intervention_rate_score`、`20secondsResponseTimelinessRate` → `timely_response_20s_score`、`IMNegativeReviewRate` → `im_neg_rate_score`。注意 `20secondsResponseTimelinessRate` 以数字开头，本身就不是合法的 SQL 标识符，这也是必须做映射层的原因之一。

这层映射的价值在于：将来携程改了接口字段名，只需改 `source`，数据库列名与历史数据完全不受影响；反之你要调整落库列名（就像这次），也不需要去碰任何采集逻辑。

### 与原方案文档的差异

业务表有两处调整。一是 `ads_sup_ota_ctrip_score_di` 补上了 `region_id`：原脚本只写目的地中文名，一旦携程改了名称，唯一键就会失效并产生重复数据。二是 `ads_sup_ota_ctrip_destination_rating_di` 的唯一键加入了 `region_id`（账号级汇总时取 `ALL`），这样将来需要逐目的地采集分析文本时无需再改表结构。此外新增了账号维表与审计日志表——多账号运行时，「哪个账号哪天没采到」是必须能被快速查出来的。

另有一处是根据你提供的 DDL 主动补充的：`ads_sup_ota_ctrip_trade_di` 原本只有三个普通索引、没有唯一键。没有唯一键会让 `INSERT ... ON DUPLICATE KEY UPDATE` 退化成纯 `INSERT`，同一天的数据重跑就会不断累积重复行（这正是原 `fetch_daily_kylin.py` 追加写 CSV 的老问题）。而交易数据往上游回补很常见，重跑几乎是必然，因此补上了 `(account_id, transaction_date, period_type, signing_code, biz_type, date_type)` 作为唯一键。同时把 `ads_sup_ota_ctrip_score_di` 与表名重名的唯一键索引改为 `uk_` 前缀，避免索引名与表名同名带来的运维歧义。

唯一键里的 `period_type` 是引入周期口径时一并加的，而且**必需**：同一个 `transaction_date` 下 `1d`/`7d`/`30d` 是三条互不相干的记录，不进唯一键就会互相覆盖、最后只剩一条。这与 `scenic_id` 的情况恰好相反（后者与账号一对一，不影响唯一性，所以不进）。

---

## 六、命令与配置

### 命令一览

| 命令 | 说明 |
|---|---|
| `run` | **统一入口**：体检 → 建表 → 登录 → 采集 → 写入 一次连贯完成，参数与 `collect` 完全一致 |
| `browser` | 持久化浏览器管理：`--status` / `--start` / `--stop` / `--reset` / `--clear-locks` |
| `login` | 登录并保存各账号独立登录态，支持 `--force` 强制重登、`--no-headless` 人工过验证码、`--keep-alive` 登录后浏览器常驻 |
| `check` | 校验各账号登录态是否有效 |
| `collect` | 采集并落地，内置「登录态检测 → 无效重登 → 采集」完整流程，参数见下表 |
| `init-db` | 按 schema 建库建表，支持 `--seed-accounts` 同步账号维表 |
| `show-ddl` | 输出建表语句，`--write` 同时写入 `sql/ddl.sql` |
| `show-schema` | 查看各表字段、注释与上游字段映射 |
| `show-config` | 查看生效配置（密码脱敏显示） |
| `doctor` | 环境体检：.env、配置文件、账号凭据、MySQL、浏览器、时区，并给出修复命令 |

`run` 与 `collect` 的关系：`run` 是 `collect` 的超集，额外干了体检与建表两件事。日常定时任务用 `run` 即可；已确信环境就绪、只想纯采集时用 `collect`。可用 `--no-check` / `--no-init-db` 关掉 `run` 的前两步。

另有一个独立脚本 `tools/backfill.py` 用于一次性回溯历史数据（详见「七、历史数据回溯」）。

### collect 的时间区间参数

三个任务的时间粒度不同，因此参数也分为两组。**不传任何时间参数时，日粒度默认采到昨天（T+1）、月粒度默认采上个月**，直接 `collect --job all` 即为日常增量采集。

日粒度（仅影响交易卡片 `kylin`）：

| 参数 | 说明 |
|---|---|
| `--start-date` | 开始日期，如 `2025-01-01`；指定后自动分批采集 |
| `--end-date` | 截止日期，默认昨天（T+1 口径） |
| `--days` | 从截止日往前推 N 天，与 `--start-date` 二选一 |
| `--batch-days` | 单批天数，默认 **30 天**，长区间自动切成多批 |
| `--period` | 周期口径，可选 `1d` / `7d` / `30d` / `all`，可重复指定；**默认只采 `1d`** |

周期口径（`period_type`）对应携程页面上的日期选择，三种口径的采集方式与行数完全不同：

| 口径 | 区间 | 采集方式 | 产出行数 | 默认 |
|---|---|---|---|---|
| `1d` | 你指定区间内的**每一天** | TrendLine 逐日明细 | 每天一行，随区间长度增长 | **开启** |
| `7d` | 截止日往前 7 天（**仅当前窗口**） | Subline 区间聚合 | 固定 1 行 | 关闭 |
| `30d` | 截止日往前 30 天（**仅当前窗口**） | Subline 区间聚合 | 固定 1 行 | 关闭 |

**`7d` / `30d` 默认不采集。** 两个原因：一是它们是「当前快照」，日常增量采集时每跑一次就多发一轮请求；二是近 7 日 / 近 30 日完全可以由 `1d` 明细在下游 SQL 里聚合得出，没必要占用上游请求配额。需要时用 `--period 7d,30d`（临时）或在 `config.yaml` 的 `collect.periods` 里打开（长期）。

> `7d` / `30d` 也**不随 `--start-date` 做历史滚动**，因为回溯两年按日滚动会额外产生上千次请求，而携程页面本身也只展示当前窗口。

聚合口径的 `transaction_date` 存区间**起始日**（与携程返回的 key 一致）。例如「`transaction_date=2026-08-06` + `period_type=7d`」表示「从 08-06 起算 7 天的聚合值」。

切换口径**不会误删其他口径的数据**：覆盖写入的清理范围同时限定 `period_type`，因此已有 `7d`/`30d` 历史行的情况下，按默认（仅 `1d`）重跑也不会把它们删掉。

月粒度（影响目的地评级 `rating` 与供应商评级 `score`）：

| 参数 | 说明 |
|---|---|
| `--start-month` | 起始月，如 `2025-01` |
| `--end-month` | 结束月，默认上个月 |
| `--month` | 只采单个月份 |
| `--months` | 指定多个非连续月份，逗号分隔 |

日期与月份均兼容 `2025-01-01`、`20250101`、`2025/01/01` 三种写法。区间参数在登录**之前**就会校验，写错立即报错并退出码 2，不会白白拉起浏览器。

### 调用案例

```bash
# 【日常增量】三类数据一起采：交易到昨天、评级采上月
python -m ctrip.cli collect --job all --sink both

# 【日粒度指定起始】从 2025-01-01 采到昨天，自动按 30 天一批切分
python -m ctrip.cli collect --job kylin --start-date 2025-01-01 --sink both

# 【日粒度指定区间】只采 2025 年一季度（90 天 → 3 批）
python -m ctrip.cli collect --job kylin --start-date 2025-01-01 --end-date 2025-03-31

# 【改批次大小】上游超时时缩小到 15 天一批
python -m ctrip.cli collect --job kylin --start-date 2025-01-01 --batch-days 15

# 【最近 7 天】用长度而非起点指定
python -m ctrip.cli collect --job kylin --days 7 --account-id A001

# 【开启近7日/近30日】默认不采，需要时显式指定
python -m ctrip.cli collect --job kylin --period 7d,30d --sink both

# 【三种口径全采】all 等价于 1d + 7d + 30d
python -m ctrip.cli collect --job kylin --days 7 --period all --sink both

# 【月粒度指定起始月】从 2025-01 采到上个月
python -m ctrip.cli collect --job rating --job score --start-month 2025-01 --sink both

# 【月粒度指定区间】只采 2025-01 到 2025-04
python -m ctrip.cli collect --job score --start-month 2025-01 --end-month 2025-04

# 【补单个月】重采 2025-02
python -m ctrip.cli collect --job score --month 2025-02

# 【非连续月份】只补这两个月
python -m ctrip.cli collect --job score --months 2025-03,2025-07

# 【日+月同时指定】三类数据均从 2025 年初开始
python -m ctrip.cli collect --job all --start-date 2025-01-01 --start-month 2025-01 --sink both

# 【多账号并发】
python -m ctrip.cli collect --account-ids A001,A002 --job all --workers 2

# 【不删旧数据】改为仅按唯一键更新
python -m ctrip.cli collect --job all --write-mode upsert

# 【临时指定景区ID】仅单账号时允许
python -m ctrip.cli collect --job all --account-id A001 --scenic-id PFTSCA01018088
```

### 采集前的登录流程（无需先跑 `login`）

`collect` 已经内置完整的登录链路，正常情况下直接跑 `collect` 即可，`login` 命令仅用于首次手工过验证码或主动排查：

1. 若已有登录态文件，先看是否超过 `cookie_ttl_hours`（默认 12 小时）；
2. 未过期则**实打实调接口校验**（首页返回 200 且未被重定向到登录页，且 `getVendorDstList` 能返回 Success），而不是只看文件在不在；
3. 校验通过则复用，否则自动重登（带 `login_retry` 次尝试）；
4. 登录后做账号指纹校验，确认拿到的确实是该账号的登录态；
5. **采集中途登录态失效**（长区间回溯很常见）会被识别并自动重登，重建会话、重验指纹后重跑当前任务，不需要人工重跑。

重登只尝试一次（账号被风控时反复登录只会加重封锁），且重登事件会记入任务警告与审计表，事后可查。

### 持久化浏览器与常驻模式

原实现每次登录都 `launch` 一个全新浏览器再 `new_context`，进程一退就什么都不剩。这意味着设备指纹、localStorage、IndexedDB、信任标记全部丢弃，携程自然把每次登录都当成陌生设备，于是反复触发验证。现在改为**两层复用**。

第一层是**持久化 profile**（默认开启，无需额外配置）。浏览器用户目录落在项目内，改用 `launch_persistent_context` 启动，关掉浏览器也不丢指纹：

```
browser/
  A001/
    profile/          # Chromium 用户目录（Cookies / Local Storage / 设备指纹）
    session.json      # 常驻进程 pid、CDP 端口、占用标记
  A002/
    profile/
    session.json
```

第二层是**常驻模式**（需配置 `cdp_port`）。以独立进程拉起浏览器并监听 CDP 调试端口，本次运行结束后浏览器**继续挂在后台**，下次运行直接 `connect_over_cdp` 接上去，连启动开销都省掉：

```yaml
ctrip:
  persistent_browser: true    # 持久化 profile（默认 true）
  browser_dir: browser        # profile 根目录
  cdp_port: 9222              # 常驻端口；0 表示不启用常驻
```

多账号时端口**按账号自动递增**（A001 用 9222、A002 用 9223……），进程与 profile 互不干扰，与多账号隔离的整体设计一致。

会话打开时按四级逐层回退，任一层不可用都不会直接失败：

| 优先级 | 模式 | 触发条件 |
|---|---|---|
| 1 | `cdp` | 常驻浏览器已在运行且**归属校验通过** → 直接复用 |
| 2 | `cdp`（新拉起） | 传了 `--keep-alive` 且端口空闲 → 拉起常驻进程再接入 |
| 3 | `profile` | 持久化 profile 模式（默认路径） |
| 4 | `ephemeral` | 持久化被关闭时的临时 profile，会告警提醒可能触发验证 |

常用命令：

```powershell
python -m ctrip.cli browser --status        # profile 大小、端口、常驻是否在线
python -m ctrip.cli browser --start        # 拉起常驻浏览器
python -m ctrip.cli browser --stop         # 停止常驻（profile 保留）
python -m ctrip.cli browser --clear-locks  # 清理强杀/断电后残留的锁文件
python -m ctrip.cli browser --reset --account-id A001   # 删除 profile（下次登录相当于新设备）
```

几个实现上的关键取舍，都是实测踩到坑后才确定的：

**强制使用完整 Chromium 构建**。Playwright 默认的 `chrome-headless-shell` 在持久化上是残缺的，不会写 `Preferences` / `Login Data` / `History` / `Service Worker`，而且它与常驻模式用的二进制不同——两种模式交替跑就等于在两台设备之间切换。现在统一 `channel="chromium"`。

**常驻进程需要手动补齐伪装参数**。常驻模式下浏览器不经 Playwright 启动，拿不到 `user_agent` / `viewport` / `timezone_id` 这些 context 参数，实测 UA 会带 `HeadlessChrome`、时区变 UTC、屏幕只有 800x600——常驻反而更容易被风控盯上。现在在 spawn 命令行里逐项补齐，并在连接后校验 UA 与时区，不一致就告警。

**同一 profile 绝不允许两个进程同时打开**，否则 Cookie 数据库会损坏。但实测发现 headless 下 Chromium **根本不写 `SingletonLock`**，只靠锁文件判断的话，无头场景（也就是定时任务的常态）并发保护形同虚设。因此改用三道判据：常驻是否在线、`session.json` 里的 `holder_pid` 是否存活、锁文件是否指向活进程。被占用时抛 `BrowserBusyError` 并给出处理命令。

**复用前先确认端口背后那个浏览器确实是自己的**。这一条是验证时真出事才加上的：测试机上本来就有一个监听 9222 的无关 chromium，`connect_over_cdp` 二话不说就连上了——它既没有携程登录态，还会用陌生指纹发请求，而且非常难查。现在以 `session.json` 的 pid 加 `/json/version` 返回的 `webSocketDebuggerUrl` 双重校验归属，不匹配就回退 profile 模式并提示换端口。

### 配置优先级

配置来源从低到高为：`config/config.example.yaml` → `config/config.yaml` → `${ENV_VAR}` 占位符展开 → `CTRIP__<段>__<键>` 环境变量覆盖 → 命令行参数。这样同一份代码可以在开发、测试、生产之间切换而不改任何文件：

```bash
export CTRIP__RUN__SINK=both
export CTRIP__MYSQL__ENABLED=true
export CTRIP__MYSQL__HOST=10.0.0.5
export CTRIP__RUN__WORKERS=2
```

容器与 CI 场景也可以完全不落文件，用 `CTRIP_ACCOUNTS="A001:user1:pwd1,A002:user2:pwd2"` 内联账号列表。

### .env 自动加载

项目根目录的 `.env` 在每次加载配置时自动读入，无需 `source`。解析器自行实现而未使用 python-dotenv，为的是精确处理几个在 Windows 上很容易踩到的坑：

| 情形 | 处理方式 |
|---|---|
| UTF-8 BOM（记事本保存） | 自动去除，不会让首行变量名变成 `\ufeffMYSQL_HOST` |
| CRLF 换行 | 自动去除行尾 `\r`，密码不会多一个不可见字符 |
| `export VAR=x` 写法 | 兼容（从 Linux 拷过来的配置很常见） |
| 成对引号 | 剥除首尾引号，但不伤害密码中间的引号 |
| 密码含 `:` `@` 等符号 | 原样保留 |
| 环境变量已存在 | 真实环境变量优先，不被 `.env` 覆盖，便于 CI 临时覆盖 |

### 账号来源

由 `ctrip.account_source` 选择，共四种：

| 取值 | 行为 | 适用场景 |
|---|---|---|
| `config`（默认） | 读 `config.yaml` 的 `accounts` + `.env` | 账号数量不多，配置即管理 |
| `mysql` | 从 `ads_service_ota_ctrip_account` 读取 | 账号统一存库、由其他系统维护 |
| `api` | 从 `ctrip.account_api_url` 拉取 | 已有账号管理微服务 |
| `auto` | 依次尝试 api → mysql → config | 多环境混用、逐步迁移 |

任何外部源取不到数据时都会**自动回落到配置文件**而不是直接失败。合并时同 ID 以外部源为准，但对 `password`、`account`、`scenic_id` 做了**空值不覆盖**保护，不会把 `.env` 里已配置好的值洗成空。

使用数据库账号源时，密码列名由 `ctrip.account_password_column` 指定（默认 `password`）：

```yaml
mysql:
  enabled: true                    # 数据库账号源必须开启
ctrip:
  account_source: mysql
  account_password_column: password
```

这里有一个需要注意的设计：本工具同步写入的账号维表**刻意不包含密码列**（避免采集程序把密码写进数仓）。若你自行在该表加了 `password` 列，程序会读取它但**永不写入也不覆盖**它（UPSERT 仅更新自己列出的列）。若表中无该列，也不报错，而是降级为「账号元信息读库、密码读 `.env`」并给出告警。

### 两种入库策略

`mysql.table_strategy` 默认为 `shared`：所有账号写同一批表，靠唯一键隔离，便于跨账号汇总分析，推荐使用。若合规上要求物理隔离，可切换为 `per_account`，程序会为每个账号建立独立物理表（`ads_sup_ota_ctrip_trade_di_a001` 等），而账号维表与审计日志表始终共享。

### 写入模式：覆盖还是更新

`run.write_mode` 默认为 `overwrite`，即「同一日期（或同一月份）已有数据先清理再写入」；可通过 `--write-mode upsert` 或配置改为仅按唯一键更新。

两者的差别不在幂等性（两种模式都不会产生重复行），而在对**上游已消失的维度**的处理：

| 场景 | `upsert` | `overwrite`（默认） |
|---|---|---|
| 数值变化 | 原地更新 | 删旧写新，结果相同 |
| 重复跑同一天 | 不产生重复行 | 不产生重复行 |
| 某个签约类型/目的地下线后 | **旧行永久沾在库里**，下游汇总偏高 | 旧行被清除，与上游保持一致 |
| 写入开销 | 略低 | 多一次 DELETE |

清理范围严格限定为「当前账号 + 本次实际采到的日期/月份」，具体为 `WHERE account_id = ? AND 分区列 IN (...)`。这个设计有三个要点：

第一，**删除与写入在同一事务内**。若先提交删除再写入，中间崩溃就会造成数据凭空消失；现在任何环节失败都会整体回滚，库里要么是旧数据要么是新数据，不会出现空窗口。

第二，**采到 0 行时不删任何数据**。上游临时故障返回空结果是常有的事，如果那时先清空再写入，一次上游抽风就会把已有历史数据删掉。因此清理范围取自**本批实际采到的数据**而非请求参数。

第三，**跨账号绝不误删**。`account_id` 始终作为删除条件的第一项，A001 重采不会碰到 A002 同一天的数据。

CSV 侧同样实现了分区覆盖：重写时只剔除「同账号 + 同日期/月份」的旧行，其余月份保持不动，因此可以只重跑某一个月而不影响已有全量文件。控制台会明确打印清理行数与分区个数，报告表也新增了「清理」列。

---

## 七、历史数据回溯

日常增量用 `collect` 就好。首次建仓、要把两年历史数据一次性补齐时，用 `tools/backfill.py`。

### 为何不直接 `collect --days 730`

可以跑，但有四个现实问题，回溯脚本逐一处理：

| 问题 | 直接跑的后果 | 回溯脚本的做法 |
|---|---|---|
| 上游历史窗口未知 | 若携程只给 1 年，你会得到一堆空结果却不知边界在哪 | `--probe` 先探测最早可用月份 |
| 风控 | 数百次请求连续打出，容易被限 | 分片间隔 + 随机抖动，连续失败自动中止 |
| 中断成本 | 跑到一半断了要从头来 | 每个分片落盘进度，重跑自动跳过已完成 |
| 失败定位 | 哪个月失败埋在几千行日志里 | 结束时输出分片级汇总表 |

### 三步走完

```bash
# 1. 先看计划（不发任何请求）
python tools/backfill.py --start 2025-01 --dry-run

# 2. 探测上游到底给多少历史数据（只读，不落库）
python tools/backfill.py --probe --probe-from 2025-01

# 3. 按探测到的边界正式回溯
python tools/backfill.py --start 2025-01 --sink both
```

探测采用**从远到近逐月试探**而不是二分：上游的可用区间未必连续，某个月恰好没业务数据也会返回空，二分法遇到这种空洞会误判边界。逐月试探多花几次请求，但结果可靠，而且找到第一个有数据的月份就停。

### 三张表的数据粒度差异

不传 `--job` 时默认三张表全采（等价于 `--job all`）。但三张表的粒度相差很大，**行数悬殊是正常的，不代表后两张表漏采**：

| 目标表 | 粒度 | 一个月的量级 |
|---|---|---|
| `ads_sup_ota_ctrip_trade_di` | 日 × 签约组合 × 日期类型 × 周期口径 | 上百行 |
| `ads_sup_ota_ctrip_score_di` | 目的地 × 月 | 每个目的地 1 行 |
| `ads_sup_ota_ctrip_destination_rating_di` | 账号 × 月 | **仅 1 行** |

第三张表容易引起误解：它是携程返回的**账号级整体分析文本**（`region_id='ALL'`），一个账号一个月就是一条，所以回溯 20 个月也只有 20 行。这是接口本身的返回粒度，不是采集不全。

因此回溯汇总里会按目标表分别列出行数，计划预览也会直接打印将要写入的表名：

```
目标表：ads_sup_ota_ctrip_trade_di、ads_sup_ota_ctrip_destination_rating_di、ads_sup_ota_ctrip_score_di

按目标表（三张表粒度不同，行数相差大是正常的）：
  ads_sup_ota_ctrip_destination_rating_di         1 行（本次 1 + 历史 0，1 分片）
  ads_sup_ota_ctrip_score_di                      3 行（本次 3 + 历史 0，1 分片）
  ads_sup_ota_ctrip_trade_di                     78 行（本次 78 + 历史 0，1 分片）
```

某张表若真的一行都没有，该行末尾会标注「← 无数据」，此时再看上方的分片状态判断是空数据还是失败。表名从 `ctrip/schema.py` 读取，改表名后这里自动跟随。

### 分片的七种状态

| 状态 | 含义 | 重跑时 |
|---|---|---|
| `success` | 采到数据并已落地 | 跳过（`--force` 可强制重跑） |
| `skipped` | 历史已成功，本次未重复请求 | 仍跳过（报告会列出历史行数） |
| `empty` | 接口正常但无数据 | 会重试（达到上限后转 `skipped_empty`） |
| `skipped_empty` | 连续多次为空，已判定超出历史窗口 | 不再重试（`--max-empty-retry 0` 可恢复） |
| `not_ready` | 区间尚在未来，无已结算数据 | 不执行，也不计失败 |
| `failed` | 该分片采集出错 | 会重试 |
| `blocked` | 账号登录失败，该账号全部分片未执行 | 会重试 |

几个区分的用意：`empty` 刻意不算「已完成」——上游当时可能还没出数据，过几天重跑说不定就有了；但连续 `--max-empty-retry` 次（默认 3）都是空，基本可以判定超出携程的历史窗口，再每次重试只是白耗请求、抬高风控概率，所以转为 `skipped_empty`（一旦某次采到数据，计数立即归零）。`blocked` 与 `failed` 分开，是因为登录失败不是分片自身的问题，它不写进度文件，否则修好凭据后会误导你以为这些月份已经试过。`not_ready` 则避开了一个真实踩过的坑：未来月份（含当月末尾）本来就没数据，把它当失败会让汇总里堆满假错误。

**当月分片会自动截断到昨天**并标记为部分区间（报告里提示「下月初补一次」）。早期版本取月末为截止日，而月末晚于今天会被区间校验直接拒绝，导致整个当月 failed、本月已结算的几天一条也采不到。注意这意味着当月分片在下个月需要重跑一次才能补齐整月，重跑时加 `--force` 即可（因为它已被记为 success）。

### 常用参数

| 参数 | 说明 |
|---|---|
| `--start` / `--end` | 回溯区间，`YYYY-MM`，`--end` 默认当前月 |
| `--job` | `kylin` / `rating` / `score` / `all`，可重复 |
| `--interval` / `--jitter` | 分片间隔与随机抖动（默认 3s + 0~2s） |
| `--stop-after-failures` | 同账号连续失败多少次后中止（默认 5，0 不限） |
| `--force` | 忽略进度记录，全部重跑 |
| `--max-empty-retry` | 连续空多少次后不再重试（默认 3，0 表示总是重试） |
| `--write-mode` | `overwrite`（默认）/ `upsert` |
| `--reset-progress` | 清空进度文件后重新开始 |
| `--dry-run` | 只打印分片计划 |

中断后直接**重跑相同命令**即可继续，不需要额外参数（`--resume` 已是默认行为）。进度文件位于 `data/backfill_progress.json`。

### 与定时采集的关系

回溯与日常采集写的是同一批表、同一套唯一键，因此两者可以重叠执行而不会产生重复数据。建议顺序是：先跑一次回溯把历史补齐，再挂上定时任务跑增量。

---

## 八、部署运行验证结果

### 环境

沙箱内实际部署了 MySQL 8.0.46，创建了三个测试账号（A001、A002 各为 active，A003 为 disabled 用于验证过滤），并用 `tools/mock_ctrip_server.py` 按真实接口结构模拟携程后台——它会依据 Cookie 中的账号标识为不同账号返回**不同的签约类型、不同的目的地、不同量级的数据**，专门用于验证隔离逻辑是否真的生效。

### 真实登录页验证

针对真实的携程 vbooking 登录页做了无头登录实测，日志显示账号填入、密码填入、协议勾选、登录按钮点击（正则 `^登\s*录$` 命中）四步全部成功，最终因使用的是虚构账号而停留在登录页，程序自动落盘了截图与 HTML。截图确认账号框已填入 `vbk_778001`、密码框已填、协议复选框呈勾选状态、登录按钮可见——即失败原因是凭据错误而非选择器失效。换成真实凭据即可完成登录。

为验证景区绑定，三个账号分别绑定了不同的景区 ID：`A001 → PFTSCA01018088`、`A002 → PFTSCA01029177`、`A003 → PFTSCA01033260`。

```
account_id  状态        登录名                 密码      景区ID(scenic_id)       备注
A001        active    vbk_569202          已配置     PFTSCA01018088        主账号
A002        active    vbk_778001          已配置     PFTSCA01029177        第二账号
A003        disabled  vbk_990002          已配置     PFTSCA01033260        第三账号（停用示例）
```

### 建表结果

```
MySQL 8.0.46 @ mysql://ctrip@127.0.0.1:3306/ctrip_data
已初始化 5 张表（策略：shared）：
  - ads_sup_ota_ctrip_trade_di
  - ads_sup_ota_ctrip_destination_rating_di
  - ads_sup_ota_ctrip_score_di
  - ads_service_ota_ctrip_account
  - ads_service_ota_ctrip_run_log
已写入账号维表 3 条（不含密码）
```

### 采集与双写结果

```
==============================================================================
采集汇总（落地方式：both）
==============================================================================
账号      景区ID              任务      状态           采集    写入    跳过  目标
------------------------------------------------------------------------------
A001    PFTSCA01018088    kylin   success     180   360     0  .../A001/ads_sup_ota_ctrip_trade_di_...csv
A001    PFTSCA01018088    rating  success       1     2     0  .../A001/ads_sup_ota_ctrip_destination_rating_di_...csv
A001    PFTSCA01018088    score   success       3     6     0  .../A001/ads_sup_ota_ctrip_score_di_...csv
A002    PFTSCA01029177    kylin   success     120   240     0  .../A002/ads_sup_ota_ctrip_trade_di_...csv
A002    PFTSCA01029177    rating  success       1     2     0  .../A002/ads_sup_ota_ctrip_destination_rating_di_...csv
A002    PFTSCA01029177    score   success       2     4     0  .../A002/ads_sup_ota_ctrip_score_di_...csv
------------------------------------------------------------------------------
合计：采集 307 行，写入 614 行，账号 2 个
==============================================================================
```

写入行数是采集行数的两倍，因为 `both` 模式同时写了 CSV 和 MySQL；`disabled` 状态的 A003 已按预期被自动跳过。

### 数据库回验

```
+------------+----------------+--------+
| account_id | scenic_id      | 行数   |
+------------+----------------+--------+
| A001       | PFTSCA01018088 |    180 |
| A002       | PFTSCA01029177 |    120 |
+------------+----------------+--------+

+------------+----------------+------------------+-------------+-----------------------+
| account_id | scenic_id      | evaluation_month | region_name | overall_service_score |
+------------+----------------+------------------+-------------+-----------------------+
| A001       | PFTSCA01018088 | 2026-07          | 吉林        |                  3.30 |
| A001       | PFTSCA01018088 | 2026-07          | 长白山      |                  2.50 |
| A001       | PFTSCA01018088 | 2026-07          | 延边        |                  3.40 |
| A002       | PFTSCA01029177 | 2026-07          | 三亚        |                  4.60 |
| A002       | PFTSCA01029177 | 2026-07          | 海口        |                  4.20 |
+------------+----------------+------------------+-------------+-----------------------+
```

两个账号的数据量、签约类型、目的地范围各自独立，A001 的三个目的地（吉林、长白山、延边）与 A002 的两个目的地（三亚、海口）没有任何交叉污染，且每行都正确带上了各自的景区 ID。三张业务表、账号维表、采集审计表均已验证 `scenic_id` 正确落库；CSV 中该列也位于第二列：

```
account_id,scenic_id,transaction_date,signing_code,signing_type,biz_type,date_type,turnover,...,collect_time
A001,PFTSCA01018088,2026-07-14,1,跟团游,跟团游,使用日期,7226.75,...,2026-08-13 11:51:37
```

CSV 表头使用与数据库完全一致的英文列名，且**不包含** `create_time` 与 `update_time`；连 CSV 文件名也随表名自动变化（如 `ads_sup_ota_ctrip_trade_di_20260714_20260812.csv`），无需在代码里另行维护文件名。

### 幂等性验证

连续执行三次相同的采集任务后：

```
+-----------+--------------+---------------------+---------------------+
| 总行数    | 唯一键数     | 最早创建            | 最后更新            |
+-----------+--------------+---------------------+---------------------+
|       300 |          300 | 2026-08-13 03:51:37 | 2026-08-13 03:52:05 |
+-----------+--------------+---------------------+---------------------+
```

总行数与唯一键去重后的行数完全相等，说明 UPSERT 生效、没有产生任何重复数据。这是原实现（追加写 CSV）无法做到的，也是为交易表补上唯一键的直接收益。

同一个查询里还可以看到两类审计时间的行为差异：`create_time` 始终停留在首次入库的 03:51:37，而 `update_time` 随重跑刷新到 03:52:05。这意味着你可以用 `create_time` 追溯数据首次落库的时间，用 `update_time` 判断数据是否在最近一轮采集中被触及过。

### 串号防护验证

刻意把 A001 的登录态文件复制到 A002 的目录，再以 A002 身份采集，程序拒绝执行并给出明确原因；数据库中 A002 的记录仍然只有三亚与海口，未被 A001 的吉林等数据污染。

### 景区绑定相关验证

四项验证均符合预期。用 `--scenic-id PFTSCA01099999` 修正 A002 的绑定并重跑后，历史 28 行数据的景区 ID 被原地回填为新值而未产生重复行，证实了「不进唯一键 + 参与 UPSERT」这个设计选择的价值；多账号时使用 `--scenic-id` 被拒绝并提示配合 `--account-id`；传入 `PFT'; DROP TABLE--` 被白名单校验拦下；未配置景区的账号会输出告警且字段写入 `NULL`。另外在模拟的旧版存量库上实测了 `sql/migration_add_scenic_id.sql`，字段添加与「以账号维表为准回填历史数据」都正常完成。

### 按账号分表验证

切换 `table_strategy=per_account` 后自动生成 11 张表（3 类业务表 × 3 个账号 + 2 张共享表），数据分别落入 `ads_sup_ota_ctrip_score_di_a001` 与 `ads_sup_ota_ctrip_score_di_a002`，而账号维表与审计日志表保持共享未被分表。

### 容错验证

将接口地址指向不可用端口，两个账号均返回 `partial` 状态并在报告中列出具体原因，进程未崩溃、未中断，符合「单账号失败不阻塞整体」的预期。

### 历史回溯验证

为验证回溯能力，给模拟接口加了一个可配置的历史窗口（`MOCK_HISTORY_START=2025-06-01`，早于此日期返回空），用以复现「上游只保留部分历史」的真实情形。

探测准确报出三类任务的最早可用月份均为 2025-06，并提示从更早月份开始会白跑。随后执行 2025-01 至今的完整回溯（两个账号 × 三类任务 × 20 个月 = 120 个分片）：

```
分片状态：empty=30  success=90
合计：采集 4675 行，写入 9350 行
```

30 个 `empty` 正好对应 2025-01 至 2025-05 这五个超出窗口的月份 × 三类任务 × 两个账号，与预期完全吻合。数据库回验：

```
+-------+------------------+--------+------------+------------+----------+
| 账号  | 景区ID           | 总行数 | 最早日期   | 最晚日期   | 覆盖月数 |
+-------+------------------+--------+------------+------------+----------+
| A001  | PFTSCA01018088   |   2742 | 2025-06-01 | 2026-08-31 |       15 |
| A002  | PFTSCA01029177   |   1828 | 2025-06-01 | 2026-08-31 |       15 |
+-------+------------------+--------+------------+------------+----------+
```

三张业务表的总行数与唯一键去重数完全相等（交易 4570/4570、评级 30/30、评分 75/75），说明跳跃式回溯同样保持幂等。此外验证了三个关键行为：重跑相同命令时 `success` 分片全部 `skipped` 而 `empty` 会重试；接口不可达时登录只尝试一次（截图仅 1 组）而非每个分片重复拉起浏览器，全部分片正确标为 `blocked` 且退出码为 1；进度文件损坏时安全降级为空而不是崩溃。

### 时间区间与分批验证

指定 `--start-date 2025-01-01 --end-date 2025-03-31` 后，90 天被自动切为 3 批（每批 30 天），日志逐批打印起止与行数：

```
交易卡片采集区间 2025-01-01 ~ 2025-03-31（共 90 天），拆为 3 批（每批最多 30 天）
批次 [1/3] 2025-01-01~2025-01-30（30 天） | 使用日期 | 景点玩乐/门票 → 30 行
批次 [2/3] 2025-01-31~2025-03-01（30 天） | 使用日期 | 景点玩乐/门票 → 30 行
批次 [3/3] 2025-03-02~2025-03-31（30 天） | 使用日期 | 景点玩乐/门票 → 30 行
交易卡片共采集 540 行（批次 18/18 成功）
```

分批天数之和恰好等于区间总天数，相邻批次首尾相接不差一天（这是手写切片最容易出错的地方）；90 个日期分区全部正确落库。月粒度使用 `--start-month 2025-01 --end-month 2025-04` 后也准确展开为四个月。

参数校验均在登录前拦下，退出码 2：`--start-date` 与 `--days` 同时使用、截止日期晚于今天、起始月晚于结束月、非法日期格式四类情形都给出了可操作的中文提示而非堆栈。

### 覆盖写入验证

默认跑一次后再重跑，报告表显示清理与写入行数一致，且总行数与唯一键去重数相等（300/300、2/2、5/5），无重复行。

三个关键场景的实测：

**一、跨账号不误删**。先把 A001 在 2025-01-05 的六行篡改为 `99999.99`，并给 A002 同一天插入一行 `777.77`，然后只重采 A001：

```
重采前：A001 被篡改行=6，A002 标记行仍存在
重采后：A001 被篡改行=0（已恢复为真实值），A002 的 777.77 完好无损
```

**二、清除上游已下线的维度**（`upsert` 做不到的事）。手动插入一行 `signing_code=99` 的「已下线业务」数据：

```
插入后：7 行（含幽灵行）
upsert   重采：7 行 → 幽灵行仍在
overwrite 重采：6 行 → 幽灵行已清除
```

**三、CSV 分区覆盖**。五个月的全量文件（15 行）下只重采 2025-02，其余四个月的行保持不动，总行数仍为 15 行。

### 中途登录态失效与自动重登验证

给模拟接口加了 `MOCK_EXPIRE_AFTER`（第 N 次业务调用后返回 401，但登录态校验接口仍正常）来复现真实场景。由于真实登录需要拉起浏览器无法在自动化测试中跑通，`tools/test_relogin.py` 用桩函数替换了登录环节，保留其余全部真实链路：

```
[OK] 至少发生一次自动重登（登录调用 2 次：['force=False', 'force=True']）
[OK] 重登使用 force_relogin=True（强制重新登录而非复用旧态）
[OK] 任务 kylin / rating / score 最终均成功
[OK] 报告中包含重登说明：「中途登录态失效，已自动重登并重试：
     已自动重登并通过指纹校验（账号指纹一致（共同目的地 3 个））」
[OK] 重登后数据完整采集（共 184 行，与无失效时一致）
```

正是这项验证暴露了上文提到的 `SessionExpired` 被吸走的缺陷：修复前，401 只会变成一条警告、任务标为 `partial`，自动重登完全不会被触发。

### 持久化浏览器验证

`tools/test_browser.py` 专项验证浏览器复用与指纹稳定性，47 项断言全部通过：

```
[OK] 首次会话建立持久化 profile（browser/A001/profile/）
[OK] profile 包含 Cookies / Preferences / Local Storage / History
[OK] 两次会话设备指纹完全一致（UA、屏幕、时区、语言、硬件并发数）
[OK] localStorage 写入后跳过浏览器重启仍可读取
[OK] profile 模式与常驻 CDP 模式指纹一致（两种模式交替不会被当成不同设备）
[OK] 并发占用检测：另一进程正在使用时抛 BrowserBusyError
[OK] 会话结束后占用标记自动释放，不影响下一次运行
[OK] 端口被陌生浏览器占用时拒绝复用并回退 profile 模式
[OK] 退出噪声过滤已安装且不吞真实错误
```

跨进程的常驻复用用两次独立运行验证：

```
第一次  [A001] 正在启动常驻浏览器（pid 28451，CDP 端口 9500）…
        [A001] 常驻浏览器已就绪，后续运行将直接复用
第二次  [A001] 常驻浏览器已在运行，无需重开（pid 28451，启动于 09:29:12）
        [A001] 浏览器仍在后台：常驻浏览器在线（pid 28451）
```

**两次 pid 完全一致**，证明浏览器进程未重启、设备指纹未变。`browser --stop` 后 profile 仍保留在磁盘上，再次启动复用同一套指纹。

### 统一入口 `run` 端到端验证

命令：`run --start-date 2026-06-01 --end-date 2026-07-15 --start-month 2026-05 --sink both`，四个步骤逐一执行后的汇总：

```
账号      景区ID              任务      状态           采集    写入    清理    跳过
A001    PFTSCA01018088    kylin   success     270   540    24     0
A001    PFTSCA01018088    rating  success       3     6     2     0
A001    PFTSCA01018088    score   success       9    18     6     0
A002    PFTSCA01029177    kylin   success     180   360    16     0
A002    PFTSCA01029177    rating  success       3     6     2     0
A002    PFTSCA01029177    score   success       6    12     4     0
合计：采集 471 行，写入 942 行，清理旧数据 54 行，账号 2 个
运行结果：全部成功
```

同区间重跑时清理 228 行、写入 228 行，总行数不增长，覆盖写入在 `run` 入口下同样生效。

### 周期口径与成交资源数验证

默认行为与显式开启各跑一轮（两账号、3 天区间）：

```
默认（不传 --period）  采集口径 1d          采集 30 行，双写 60 行
--period all         采集口径 1d/7d/30d   采集 50 行，双写 100 行
--period 7d          采集口径 7d           采集 4 行
```

落库后按口径统计：`1d` 30 行、`7d` 10 行、`30d` 10 行。`1d` 行数随区间增长而 `7d`/`30d` 固定不变，符合快照语义。所有行的 `deal_resource_count` 与 `deal_resource_count_mom` 均已写入（各 30/30）。连跑三次后总行数 50 = 唯一键数 50，幂等无重复。

**覆盖范围隔离**：表里已有三种口径共 50 行时，按默认（仅 `1d`）重跑，结果仍为 `1d` 30 / `7d` 10 / `30d` 10，`7d`/`30d` 未被误删。这一项必须验证：默认 `kylin_days=30` 时，`30d` 窗口的起始日必然落在 `1d` 区间内，若清理条件不限定 `period_type`，就会连带删除同日的其他口径。

请求形态用假客户端逐条拦下来断言，而不只看最终有无数值——因为资源数采不到的根因恰好就在请求形态：已验证它走 `queryId=354`、带 `aggregateType=distinct`、使用 `getKylinDataWithSublineV2` 而非 TrendLine、且未被混入主指标 `metricList`（混入则采不到）；主指标仍为 5 项且 `queryId=353` 不变。只采 `7d`/`30d` 时完全不调 TrendLine，默认只采 `1d` 时也不发窗口聚合主指标请求，两边都不浪费请求。

### 资源数逐日覆盖验证

这一项单独验证，因为它属于「看似采到了、实际只有首日有数」的隐蔽失效。取 10 天区间（`2026-08-01 ~ 08-10`，单账号）实际落库：

```
总行数 60 | 有资源数 60 | 有环比 60 | 覆盖天数 10
逐日值   4, 3, 0, 5, 0, 2, 3, 6, 6, 0
```

数值确实随日期变化而不是只有首日有值，环比同样 60/60 全部写入。复跑后总行数 60 = 唯一键数 60，幂等正常。

假客户端层面则直接校验请求本身：日口径下每一个资源数请求都满足 `startdate == enddate`，请求数恰为「天数 × 日期类型数」，且请求日期集合与区间内每一天完全对应（无遗漏）；`7d`/`30d` 下它仍是 `startdate != enddate` 的区间单请求，起止与窗口一致，没有被误改成逐日。

还有一个让自检本身不再失真的修正：模拟接口与假客户端原本在 `calculate.value <= 1` 时会逐日返回数据，而真实接口无论如何都只返回起始日一个键。这个差异恰好把 bug 掩盖掉了，使得自检全绿却在真实环境缺数。两者现已改为严格只返回一个键，以后任何人把逐日请求改回区间请求都会立刻被断言拦住。

### 当月回溯与断点续跑验证

以 2026-08-14 为「今天」回溯 `2026-07 ~ 2026-08`（含当月）：

```
A001  kylin  2026-07  success(成功)   186  186   -
A001  kylin  2026-08  success(成功)    78   78   -  当月只采到昨日，下月初补一次
本次新采：264 行，写入 264 行
```

落库后 2026-07 覆盖 07-01~07-31 共 31 天 186 行，2026-08 覆盖 08-01~08-13 共 13 天 78 行，两者的资源数分别是 186/186 与 78/78 全量写入。修复前这个命令下 2026-08 整片 failed，本月一条数据也拿不到。

重跑相同命令，汇总不再只报「0 行」而是把来源说清楚：

```
A001  kylin  2026-07  skipped(已采过)   0  0  186  上次成功 2026-08-14
A001  kylin  2026-08  skipped(已采过)   0  0   78  上次成功 2026-08-14
历史已采：264 行（之前已成功入库，本次跳过未重复请求）
区间累计：264 行
```

另外验证了两个边界：未来月份（`2026-09`、`2026-10`）在计划预览中标为「尚无已结算数据 / 无需采集」而不会当作失败；把某个分片标为连续 3 次空后，它被判为 `skipped_empty(窗口外)` 不再重试，并提示可用 `--force` 或 `--max-empty-retry 0` 强制重试。

### schema 自检

六个自检脚本共 342 项断言全部通过：`tools/verify_schema.py` 122 项（表结构、分区列、区间解析与切片、周期口径约束）、`tools/test_migration.py` 59 项（存量库迁移）、`tools/test_backfill.py` 67 项（回溯逻辑，含闰年月末、分片天数之和恰好覆盖区间无重叠、当月截断、空分片计数与归零、七种分片状态渲染、按目标表拆分行数）、`tools/test_browser.py` 47 项（持久化浏览器与指纹稳定性）、`tools/test_kylin_period.py` 40 项（周期口径与资源数请求形态，含逐日覆盖无遗漏）、`tools/test_relogin.py` 7 项（中途重登）。

其中 `python tools/verify_schema.py` 覆盖 122 项断言，包括含 `|` `/` 的指标名映射、六类脏数据清洗、唯一键约束校验、非法列名拦截、CSV/DDL/UPSERT 三处列一致、新增字段自动贯通四类产物、`per_account` 分表 DDL 生成，以及 `scenic_id` 的完整约束（五张表均含该字段、紧跟 `account_id`、不在唯一键、参与 UPSERT 回填、未绑定时为 `NULL`）。

本轮适配新 DDL 时补充了两类断言：一是五张表的表名与唯一键索引名逐一对照；二是 `create_time` / `update_time` 的行为约束（只进 DDL、不进写入列清单、不进 CSV、不参与行构造，且 `update_time` 带 `ON UPDATE CURRENT_TIMESTAMP` 并在 UPSERT 中被显式刷新）。另外用一个脚本将生成的 DDL 与你提供的 DDL 做了逐列程序化比对，确认五张表的**列名、列顺序、列类型完全一致**，仅余上述两处经你确认的索引差异。

---

## 九、定时任务部署

### Linux（crontab）

`.env` 由程序自动加载，cron 里不再需要 `set -a && . ./.env`，只要 `cd` 到项目目录即可：

推荐直接用 `run` 统一入口，一条 cron 即涵盖体检、建表、登录、采集、写入：

```bash
# 日常增量：每天 02:00 跑完整链路（交易近 3 天 + 评级上月）
0 2 * * * cd /opt/ctrip_collector && \
  .venv/bin/python -m ctrip.cli run --days 3 --sink both >> logs/cron_run.log 2>&1

# 常驻浏览器：开机自启，让后续采集直接复用同一浏览器与设备指纹
@reboot cd /opt/ctrip_collector && \
  .venv/bin/python -m ctrip.cli browser --start >> logs/cron_browser.log 2>&1
```

若希望三类数据分开调度，也可以继续用 `collect` 子命令：

```bash
# 交易卡片：每天 02:00 采集近 3 天（覆盖上游数据回补）
0 2 * * * cd /opt/ctrip_collector && \
  .venv/bin/python -m ctrip.cli collect --job kylin --days 3 --sink both >> logs/cron_kylin.log 2>&1

# 目的地评级分析：每月 2 日 03:00 采集上月
0 3 2 * * cd /opt/ctrip_collector && \
  .venv/bin/python -m ctrip.cli collect --job rating --sink both >> logs/cron_rating.log 2>&1

# 供应商评级：每月 2 日 04:00 采集上月
0 4 2 * * cd /opt/ctrip_collector && \
  .venv/bin/python -m ctrip.cli collect --job score --sink both >> logs/cron_score.log 2>&1

# 登录态保活：每 6 小时校验一次，失效自动重登
0 */6 * * * cd /opt/ctrip_collector && \
  .venv/bin/python -m ctrip.cli login >> logs/cron_login.log 2>&1
```

### Windows（任务计划程序）

先建一个 `run_collect.bat`（重点是 `cd /d` 切到项目目录，否则找不到 `.env` 与 `config.yaml`）：

```bat
@echo off
cd /d C:\path\to\ctrip_collector
call .venv\Scripts\activate.bat
python -m ctrip.cli run --days 3 --sink both >> logs\task_run.log 2>&1
```

上面用的是 `run` 统一入口；若只想跑单个任务，把最后一行换成 `collect --job kylin --days 3 --sink both` 即可。

再注册为每日任务（管理员 PowerShell）：

```powershell
schtasks /create /tn "CtripKylin" /tr "C:\path\to\ctrip_collector\run_collect.bat" /sc daily /st 02:00
```

进程退出码为 0（全部成功）、1（部分成功）、2（全部失败）、130（用户中断），可直接接入告警。需要更细粒度的监控时，查询审计表即可：

```sql
SELECT account_id, job, status, rows_written, error
FROM ads_service_ota_ctrip_run_log
WHERE started_at >= CURDATE() AND status <> 'success';
```

### 关于验证码

携程后台在异地登录或频繁登录时可能触发滑块或短信验证。无头模式下程序会识别并明确报错，提示改用 `--no-headless` 在有图形界面的环境完成一次人工登录；登录态保存后即可长期复用，保活任务会在过期前自动续期。若服务器无图形界面，可用 `xvfb-run -a python -m ctrip.cli login --no-headless` 配合 VNC 处理。

降低触发频率的关键是**不要让浏览器每次都像新设备**。建议首次就把常驻开起来：先 `browser --start` 拉起常驻浏览器，再用 `login --no-headless` 人工过一次验证，之后所有运行都复用同一个浏览器进程与同一套设备指纹。需要注意的是 `browser --reset` 会删除 profile，下次登录相当于换了一台新设备，极可能重新触发验证，因此仅在 profile 损坏时使用。

---

## 十、目录结构

```
ctrip_collector/
├── pyproject.toml              # 依赖与数据库初始化配置（uv 管理）
├── .env.example                # 敏感信息模板
├── run.py                      # 便捷入口，等价于 python -m ctrip.cli
├── otaops.sh                   # 服务器上管理控制台：start / stop / restart / status / check / logs
├── config/
│   ├── config.example.yaml     # 配置模板（可提交）
│   └── config.yaml             # 实际配置（已 gitignore）
├── ctrip/
│   ├── schema.py               # ★ 表结构单一维护点
│   ├── config.py               # 配置加载（.env 自动加载 + YAML + 环境变量 + 占位符）
│   ├── session.py              # Cookie → Session（修复域名与请求头）
│   ├── api.py                  # 接口调用（重试 + 业务状态校验）
│   ├── auth.py                 # 自动登录、登录态存储、指纹校验
│   ├── browser.py              # ★ 持久化浏览器 profile、常驻会话、并发与归属校验
│   ├── accounts.py             # 账号来源（config / mysql / api / auto）
│   ├── pipeline.py             # 多账号调度编排与报告
│   ├── cli.py                  # 命令行入口
│   ├── logging_utils.py        # 统一日志
│   ├── timeutil.py              # 时区获取（缺 tzdata 时降级为 UTC+8）
│   ├── daterange.py             # 日期/月份区间解析与分批切片
│   ├── sinks/                  # 落地层：base / csv_sink / mysql_sink
│   └── collectors/             # 采集器：kylin_daily / destination_rating / supplier_rate_score
├── tools/
│   ├── backfill.py             # ★ 历史数据回溯（窗口探测 + 断点续跑 + 限流）
│   ├── verify_schema.py        # 表结构与区间自检（122 项断言）
│   ├── test_migration.py       # 迁移脚本回归测试（59 项断言）
│   ├── test_backfill.py        # 回溯逻辑自检（67 项断言）
│   ├── test_browser.py         # 持久化浏览器与指纹稳定性自检（47 项断言）
│   ├── test_kylin_period.py    # 周期口径与成交资源数请求形态自检（40 项断言）
│   ├── test_relogin.py         # 中途登录态失效自动重登验证（7 项断言）
│   ├── mock_ctrip_server.py    # 接口模拟服务（仅本地验证）
│   └── make_mock_cookies.py    # 生成模拟登录态（仅本地验证）
├── sql/
│   ├── ddl.sql                 # 由 schema.py 自动生成
│   └── migration_to_new_schema.sql  # 旧表结构 -> 新 DDL 的一次性迁移脚本
├── data/
│   ├── output/{account_id}/     # CSV 输出，按账号分目录
│   └── backfill_progress.json  # 回溯进度（断点续跑用）
├── auth/{account_id}/          # 登录态，按账号分目录
├── browser/{account_id}/       # 浏览器 profile 与常驻会话信息，按账号分目录
│   ├── profile/                 # Chromium 用户目录（设备指纹与本地存储）
│   └── session.json             # 常驻 pid、CDP 端口、占用标记
└── logs/{account_id}/          # 日志，按账号分目录
```

---

## 十一、存量库迁移

如果库里已经跑过早期版本（表名为 `ctrip_kylin_daily` 等），执行一次 `sql/migration_to_new_schema.sql` 即可平滑升级到新 DDL：

```bash
mysqldump -u<user> -p <db> > backup_$(date +%F).sql     # 先备份
mysql -u<user> -p <db> < sql/migration_to_new_schema.sql
python -m ctrip.cli show-ddl                            # 对照结构
python tools/verify_schema.py                           # 自检
```

脚本分五步完成：重命名五张表、重命名业务字段（并将计数类字段从 `INT` 提升为 `BIGINT`）、为交易表补唯一键、重建跟随字段改名的索引、为五张表新增 `create_time` 与 `update_time`。注释部分还预置了四组可选语句：用旧表的 `created_at` / `updated_at` 回填真实时间、补 `scenic_id`、以账号维表为准回填景区绑定，以及重复数据体检与清理。

**最需要注意的是补唯一键这一步**。存量库若因之前没有唯一键而已经累积了重复行，`ADD UNIQUE KEY` 会直接失败。脚本末尾提供了体检查询与「保留每组 `id` 最大一行」的清理语句，建议先体检、确认影响面后再清理，而不是直接执行删除。

迁移脚本本身也做了回归测试。`python tools/test_migration.py` 会自动构造一个含存量数据的旧版模拟库、执行迁移、然后用 59 项断言逐一校验结果，最后自动清理测试库。断言覆盖表重命名、字段重命名、旧字段消失、历史行数与数值未丢失、计数字段已提升为 `BIGINT`、唯一键与索引正确、`update_time` 自动维护生效，以及**迁移后五张表的列与 `schema.py` 生成的结构逐列完全一致**。其中一项断言会实际插入一条重复数据验证唯一键真的生效（行数不增、数值原地更新为 8888.88），而不只是检查索引名存在。

---

## 十二、常见问题

### 报「账号 A001 缺少登录用户名」

绝大多数情况下不是配置写错了，而是 **`.env` 没有被加载**。直接跑 `python -m ctrip.cli doctor` 就能看到具体卡在哪一环。三种典型原因：

| 现象 | 原因 | 处理 |
|---|---|---|
| 报错提示「未找到 .env 文件」 | 只有 `.env.example`，没复制成 `.env` | Windows: `Copy-Item .env.example .env` |
| 报错提示「.env 已找到但 `CTRIP_A001_ACCOUNT` 为空」 | 变量未填写或等号两边有空格 | 按提示补全变量值 |
| 在项目目录之外执行命令 | 相对路径找不到配置 | `cd` 到项目根目录再运行 |

早期版本的 README 曾给出 `set -a && source .env && set +a`，这是 Linux/macOS 的写法，PowerShell 与 CMD 不支持 `source`，因此在 Windows 上执行会默默失败。现在 `.env` 已由程序自动加载，三个平台都无需任何手动动作。

### 报 `ZoneInfoNotFoundError: 'No time zone found with key Asia/Shanghai'`

**Windows 不自带系统时区数据库**（Linux 有 `/usr/share/zoneinfo`），Python 的 `zoneinfo` 在 Windows 上必须依赖 `tzdata` 包。一条命令解决：

```powershell
uv pip install tzdata
```

该依赖已按平台写进 `pyproject.toml`（`tzdata>=2024.1; sys_platform == 'win32'`），重新 `uv pip install -e .` 也会自动装上。同时程序现在带降级保护：即使缺包也不会崩溃，而是回退到固定偏移 UTC+08:00 并告警一次。之所以可以这么降级而不影响数据正确性：本项目的时区只用于推导「今天/昨天/上个月是哪天」，不涉及历史夏令时换算，而中国自 1991 年后不再使用夏令时，固定 +08:00 与真实时区结果完全一致。

### 为什么表里只有 `period_type=1d` 的数据

这是默认行为。`7d` / `30d` 默认不采集，因为它们是「当前快照」，每跑一次都会额外发一轮请求，而这两个口径完全可以由 `1d` 明细在下游聚合得出。两种开启方式：

```powershell
# 临时：本次运行生效
python -m ctrip.cli collect --job kylin --period 7d,30d --sink both

# 长期：改 config.yaml，把 collect.periods 下的 7d / 30d 取消注释
```

切换口径**不会弄丢已有数据**：覆盖写入的清理范围同时限定 `period_type`，因此已有 `7d`/`30d` 历史行的情况下，按默认（仅 `1d`）重跑也不会把它们删掉。

### `deal_resource_count` 为空

先看日志里有没有「成交资源数采集失败」告警。这个指标走**独立请求**（`queryId=354`、`getKylinDataWithSublineV2`、`aggregateType=distinct`），与其他五项不同源，所以存在「主指标正常、它单独失败」的情况。这时该列留 `NULL` 而不会拖累整批数据，重跑即可回填。

若日志显示携程返回 `Ack != Success`，则可能是上游改了指标代码（当前为 `suc_resourceid`）。重新抓包确认后修改 `ctrip/collectors/kylin_daily.py` 里的 `RESOURCE_METRIC` 一处即可。

### 只有区间首日有 `deal_resource_count`，其余天全是空

这是 1.8.0 之前的缺陷，升级后重跑对应区间即可回填。根因是 `getKylinDataWithSublineV2` 固定 `needAggregate: true`，传入区间时会把整段聚合成**一个**值、返回的 `valueDateMap` 只有区间起始日一个键，而早期版本像主指标一样一次传整个区间。现在日口径下改为逐日单发请求（`startdate == enddate`）。

逐日发请求会增加请求量（以 30 天、一个签约组合、一种日期类型为例，从 1 次变为 1 + 30 = 31 次），但这是无法避开的：资源数是去重计数，天粒度的值无法由区间聚合值拆分得出。回溯长区间时建议保留默认的请求间隔。

### 回溯报「本次新采 0 行」是不是没采到

不一定。看汇总里的**历史已采**与**区间累计**两行：如果分片状态是 `skipped(已采过)` 且历史已采有值，说明数据之前已经入库，本次属于正常的断点续跑（`--resume` 是默认行为）。汇总末尾会直接给出核对 SQL，跑一下就能确认库里的日期跨度。确实需要重采时加 `--force`。

若看到大量 `skipped_empty(窗口外)`，说明这些月份连续多次都返回空，已被判定超出携程的历史保留期——这是上游限制，不是程序问题，不必再重试。

### 密码想统一存在数据库而不是 `.env`

开启数据库账号源即可（详见「六、命令与配置 — 账号来源」）：

```yaml
mysql:
  enabled: true
ctrip:
  account_source: mysql
```

自行在账号表加 `password` 列后，`.env` 里就只需保留 MySQL 连接信息。若你不想把密码存进数据库，也可以只把账号元信息（含 `scenic_id` 绑定）放库里，密码仍由 `.env` 提供——这是自动降级行为，无需额外配置。

### 密码含特殊字符

`.env` 里直接写原文即可，`ZYLXs@12301` 这类包含 `@` `:` `#` 的密码都能正确解析，不需要转义。只有当密码首尾刚好是成对引号时会被当作引号剥除，这种情况建议改用数据库账号源。

### 报「--start-date 与 --days 不能同时使用」

两者都在描述区间起点，同时传入时无法判断你想要哪个，因此直接报错而不默默取其一（静默取舍会采到一个与预期不同的区间，而你很难发现）。要指定起点用 `--start-date`，要指定长度用 `--days`。

### 一次采两年会不会被风控

`collect --start-date` 会把长区间切成 30 天一批，批次间有 `collect.request_interval` 控制的间隔。但两年这种量级建议用 `tools/backfill.py`，它额外具备上游窗口探测、断点续跑、随机抖动与连续失败中断保护（详见「七、历史数据回溯」）。

### 重采会不会把其他数据删掉

不会。清理条件固定为 `account_id = 当前账号 AND 日期/月份 IN (本次实际采到的值)`，且与写入处于同一事务。三条保证：不跨账号、不碰本次区间以外的日期、采到 0 行时不删任何数据（避免上游临时故障造成历史数据被清空）。若希望保留上游已下线的历史维度，改用 `--write-mode upsert`。

### 报「profile 正被其他进程使用」

同一个浏览器 profile 不能被两个进程同时打开，否则 Cookie 数据库会损坏，因此程序会直接拒绝而不是冒险并行。先用 `browser --status` 看占用情况：若确实有常驻浏览器在跑，直接复用或 `browser --stop`；若是上次强杀、断电遗留的假占用，执行 `browser --clear-locks` 清理。

### 常驻浏览器会不会一直占内存

会。一个无头 Chromium 常驻大约占 150–300 MB，多账号会成倍增长。若采集频率低（如每天一次），建议不传 `--keep-alive`，只用持久化 profile 就已能保留设备指纹；常驻主要适合高频采集或滑块频繁的账号。不需要时 `browser --stop` 即可释放，profile 仍保留。

### 报「CDP 端口已被其他浏览器占用」

说明那个端口背后的浏览器不是本工具启动的——很可能是你自己开的调试浏览器或其他自动化程序。程序不会连上去（那个浏览器没有携程登录态，且会用陌生指纹发请求），而是回退到 profile 模式继续工作。把 `cdp_port` 改成其他空闲端口即可。

### 密码曾在聊天/日志中外泄

立即在携程后台与 MySQL 侧修改密码，再更新 `.env`。本工具的日志与 `show-config` 输出已对密码脱敏（仅显示「已配置/缺失」），不会写入明文。

---

## 十三、后续扩展建议

登录态目前存本地文件。多机部署时可把 `CookieStore` 换成 Redis 或数据库实现——它已经是接口化的（`exists` / `load_meta` / `save_meta` / `is_fresh` / `delete`），替换实现不影响采集器与调度层。

新增采集任务只需在 `ctrip/collectors/` 下继承 `BaseCollector` 并在 `COLLECTORS` 注册；新增落地方式只需实现 `Sink` 接口并在 `sinks/__init__.py` 的工厂里注册。

关于字段变更，后续只需记住一条：**改 `ctrip/schema.py` 这一处**。本次适配你新 DDL 的完整工作量就是一个例子：五张表改名、二十多个字段改名、新增两个审计字段，采集器与落地层一共只动了两行业务代码（`collect_time` 与 `analysis_text_len` 两个字段名），其余建表 DDL、CSV 表头与文件名、UPSERT 语句、上游字段映射与类型清洗全部自动跟随。

---

**版本**：1.8.1

变更记录：

| 版本 | 说明 |
|---|---|
| 1.8.1 | 回溯汇总按**目标表**分别列出行数，计划预览直接打印将写入的三张表表名。起因是三张表粒度悬殊——交易表一个月上百行，而目的地分析表是账号级分析文本、每账号每月只有一行——只报一个总行数容易让人误判后两张表漏采。表名从 schema 读取，改表名自动跟随 |
| 1.8.0 | 修正**资源数逐日采集**：该聚合接口固定 `needAggregate: true`，传入区间只返回起始日一个值，原实现导致自定义区间下只有首日有数、其余天全为 NULL；现日口径下逐日单发请求（`startdate == enddate`、`value=1`），主指标请求方式保持不变。同时修复回溯的三个问题：当月分片取月末为截止日会被区间校验拒绝而整月失败（改为截断到昨天并标记 `partial`）、汇总只报本次行数无法区分「没采到」与「早已采好」（改为区分本次新采/历史已采/区间累计并给出核对 SQL）、超窗空分片每次重跑都白试（新增 `--max-empty-retry`）。另修正模拟接口与假客户端的聚合语义失真——它们原本在 `value <= 1` 时逐日返回，恰好掩盖了上述 bug 使自检全绿 |
| 1.7.0 | 补齐**成交资源数** `deal_resource_count` 及环比（走独立的 `queryId=354` + `getKylinDataWithSublineV2` + `aggregateType=distinct` 请求，与其他五项不同源）；新增 `period_type` 周期字段与 `--period` 参数（`1d` 逐日明细、`7d`/`30d` 当前窗口快照，**默认只采 `1d`**）；`period_type` 纳入交易表唯一键，且覆盖写入的清理范围同步限定该列，避免切换口径时误删其他口径的历史数据 |
| 1.6.0 | 浏览器改为项目内**持久化 profile**（`browser/{account_id}/profile/`，设备指纹与本地存储跳过重启保留，降低验证触发率）；新增**常驻模式**（CDP 端口复用，浏览器挂在后台跳进程复用）与 `browser` 管理命令；新增 `run` 统一入口（体检 → 建表 → 登录 → 采集 → 写入 一次连贯完成）；修复无头 Chromium 不写 `SingletonLock` 导致并发保护失效、`chrome-headless-shell` 持久化不完整、常驻进程指纹不一致、CDP 端口被陌生浏览器占用时静默误连、停止常驻后锁文件残留五个问题 |
| 1.5.0 | 日粒度新增 `--start-date` / `--end-date` / `--batch-days`（默认 30 天一批自动切分），月粒度新增 `--start-month` / `--end-month`；落库默认改为 `overwrite`（同日期/月份旧数据先清理再写入，同事务、不跨账号、采到 0 行不删）；采集中途登录态失效自动重登并重跑当前任务；修复 `SessionExpired` 被采集器 `except ApiError` 吸走导致自动重登永不触发的缺陷 |
| 1.4.0 | 修复 Windows 缺 `tzdata` 导致的时区报错（并增加 UTC+8 降级保护）；新增 `tools/backfill.py` 历史数据回溯（上游窗口探测、按月分片、断点续跑、限流与连续失败保护）；`doctor` 扩至 6 项 |
| 1.3.0 | `.env` 自动加载（修复 Windows 下报「缺少登录用户名」）；新增 `doctor` 体检命令；新增 `account_source` 支持从 MySQL 账号表读取账号密码；密码纳入必填校验；报错信息改为可操作的诊断提示 |
| 1.2.0 | 适配新版 MySQL DDL：数仓命名表名、英文业务字段名、`create_time` / `update_time` 审计字段 |
| 1.1.0 | 新增 `scenic_id` 景区 ID 账号级绑定 |
| 1.0.0 | 重构原四个脚本：多账号隔离、CSV/MySQL 双通道、表结构单一维护点 |

python -m webconsole