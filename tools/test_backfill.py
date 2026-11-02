#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""回溯脚本的逻辑自检。

不依赖真实接口，用替身对象覆盖三类关键逻辑：月份切片、进度断点、
以及失败处理（连续失败中止、登录失败标记 blocked）。

运行::

    python tools/test_backfill.py
"""

from __future__ import annotations

import sys
import tempfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ctrip.config import load_dotenv  # noqa: E402

load_dotenv()

from ctrip.schema import (  # noqa: E402
    DESTINATION_RATING, KYLIN_DAILY, SUPPLIER_RATE_SCORE,
)
from tools.backfill import (  # noqa: E402
    Progress, SliceReport, _table_of, build_slices, month_bounds, month_range,
    parse_month, render_report,
)

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((PASS if cond else FAIL, name, detail))


def main() -> int:
    print("=" * 90)
    print("回溯脚本逻辑自检")
    print("=" * 90)

    # ---------------- 月份工具 ----------------
    check("parse_month 支持 YYYY-MM", parse_month("2025-01") == (2025, 1))
    check("parse_month 支持 YYYYMM", parse_month("202512") == (2025, 12))
    try:
        parse_month("2025/13")
        check("parse_month 拒绝非法输入", False, "未抛异常")
    except ValueError:
        check("parse_month 拒绝非法输入", True)

    months = month_range("2025-01", "2026-08")
    check("月份区间长度正确（2025-01~2026-08 共 20 个月）", len(months) == 20,
          f"实际 {len(months)}")
    check("月份区间跨年正确", months[11] == "2025-12" and months[12] == "2026-01",
          f"{months[11]} / {months[12]}")
    check("月份区间升序", months == sorted(months))

    try:
        month_range("2026-05", "2025-01")
        check("起始晚于结束时报错", False, "未抛异常")
    except ValueError:
        check("起始晚于结束时报错", True)

    # 闰年与月末
    check("2 月末日（平年）", month_bounds("2025-02")[1] == date(2025, 2, 28))
    check("2 月末日（闰年）", month_bounds("2024-02")[1] == date(2024, 2, 29))
    check("12 月末日跨年不越界", month_bounds("2025-12")[1] == date(2025, 12, 31))
    check("月首日正确", month_bounds("2025-07")[0] == date(2025, 7, 1))

    # ---------------- 分片 ----------------
    class _Cfg:
        pass

    kylin_slices = build_slices("kylin", months, _Cfg())
    check("kylin 分片数等于月份数", len(kylin_slices) == 20, f"{len(kylin_slices)}")
    feb = next(s for s in kylin_slices if s.key == "2025-02")
    check("kylin 分片起点为月首日",
          feb.kwargs["start_date"] == date(2025, 2, 1), str(feb.kwargs["start_date"]))
    check("kylin 分片截止日为月末（非闰年 2 月为 28 日）",
          feb.kwargs["end_date"] == date(2025, 2, 28), str(feb.kwargs["end_date"]))
    check("kylin 分片不再传 days（避免与起点冲突）",
          "days" not in feb.kwargs, str(sorted(feb.kwargs)))
    # 天数之和应等于区间总天数，确保既不漏也不重
    total_days = sum((s.kwargs["end_date"] - s.kwargs["start_date"]).days + 1
                     for s in kylin_slices)
    expected_days = (month_bounds(months[-1])[1] - month_bounds(months[0])[0]).days + 1
    check("kylin 分片天数之和覆盖完整区间且无重叠",
          total_days == expected_days, f"{total_days} vs {expected_days}")
    # 相邻月分片首尾相接：跨月时最容易差一天
    ordered = sorted(kylin_slices, key=lambda s: s.kwargs["start_date"])
    check("kylin 相邻月分片首尾相接",
          all((ordered[i + 1].kwargs["start_date"]
               - ordered[i].kwargs["end_date"]).days == 1
              for i in range(len(ordered) - 1)))

    for job in ("rating", "score"):
        sl = build_slices(job, ["2025-06"], _Cfg())[0]
        check(f"{job} 分片按月传参", sl.kwargs.get("month") == "2025-06",
              str(sl.kwargs))

    try:
        build_slices("unknown", ["2025-01"], _Cfg())
        check("未知任务名报错", False, "未抛异常")
    except ValueError:
        check("未知任务名报错", True)

    # ---------------- 进度断点 ----------------
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "progress.json"
        prog = Progress.load(path)
        check("进度文件不存在时初始为空", prog.done == {})

        prog.mark("A001", "kylin", "2025-06", "success", 180)
        prog.mark("A001", "kylin", "2025-07", "failed", 0, "接口超时")
        prog.mark("A001", "kylin", "2025-08", "empty", 0)
        check("进度文件已落盘", path.exists())

        check("success 分片判定为已完成", prog.is_done("A001", "kylin", "2025-06"))
        check("failed 分片不算已完成", not prog.is_done("A001", "kylin", "2025-07"))
        # empty 需要重试：上游当时可能还没出数据，下次跑也许就有了
        check("empty 分片不算已完成（允许重试）",
              not prog.is_done("A001", "kylin", "2025-08"))
        check("其他账号同名分片互不影响",
              not prog.is_done("A002", "kylin", "2025-06"))
        check("其他任务同名分片互不影响",
              not prog.is_done("A001", "score", "2025-06"))

        reloaded = Progress.load(path)
        check("重新加载后进度保持", reloaded.is_done("A001", "kylin", "2025-06"))
        check("重新加载后失败信息保留",
              reloaded.done["A001|kylin|2025-07"]["error"] == "接口超时")

        reloaded.reset()
        check("reset 后进度清空且文件删除",
              not reloaded.done and not path.exists())

        # 损坏的进度文件不应导致崩溃
        path.write_text("{ 这不是合法 JSON", encoding="utf-8")
        broken = Progress.load(path)
        check("进度文件损坏时安全降级为空", broken.done == {})

    # ---------------- 报告渲染 ----------------
    reports = [
        SliceReport("A001", "kylin", "2025-06", "success", 180, 360),
        SliceReport("A001", "kylin", "2025-05", "empty", 0, 0),
        SliceReport("A001", "score", "2025-07", "failed", 0, 0, "接口 500"),
        SliceReport("A002", "kylin", "2025-06", "blocked", 0, 0, "登录失败：验证码"),
    ]
    text = render_report(reports, ["2025-05", "2025-06", "2025-07"])
    check("报告含本次新采行", "本次新采：180 行，写入 360 行" in text)
    check("报告区分四种状态并带中文说明",
          all(s in text for s in ("success(成功)=1", "empty(无数据)=1",
                                  "failed(失败)=1", "blocked(未执行)=1")))
    check("报告单列登录失败账号", "因登录失败而未执行的账号：A002" in text)
    check("报告列出失败分片明细", "[A001/score/2025-07]" in text)
    check("报告提示空数据分片", "空数据分片 1 个" in text)

    # ---------------- 当月分片截断 ----------------
    # 原本当月分片取月末为截止日，而月末晚于今天会被区间校验直接
    # 拒绝，导致整个当月 failed、本月已结算的几天也一条都采不到。
    fake_today = date(2026, 8, 14)
    cur = build_slices("kylin", ["2026-07", "2026-08", "2026-09"],
                       _Cfg(), fake_today)
    by_key = {s.key: s for s in cur}
    check("往月分片仍到月末（不受截断影响）",
          by_key["2026-07"].kwargs["end_date"] == date(2026, 7, 31),
          str(by_key["2026-07"].kwargs["end_date"]))
    check("当月分片截断到昨天而非月末",
          by_key["2026-08"].kwargs["end_date"] == date(2026, 8, 13),
          str(by_key["2026-08"].kwargs["end_date"]))
    check("当月分片起点仍为月首日",
          by_key["2026-08"].kwargs["start_date"] == date(2026, 8, 1))
    check("当月分片标记 partial（提醒下月补采）",
          by_key["2026-08"].partial is True)
    check("往月分片不标 partial", by_key["2026-07"].partial is False)
    check("未来月份标为无需采集而非失败",
          bool(by_key["2026-09"].skip_reason)
          and "start_date" not in by_key["2026-09"].kwargs,
          by_key["2026-09"].skip_reason[:30])
    # 今天正好是 1 号：本月尚无已结算数据，应跳过而不是报错
    first_day = build_slices("kylin", ["2026-08"], _Cfg(), date(2026, 8, 1))
    check("月初第一天跳过当月（无已结算数据）",
          bool(first_day[0].skip_reason), first_day[0].label)
    # 不传 today 时保持原行为，避免影响其他调用方
    check("未传 today 时不做截断",
          build_slices("kylin", ["2026-08"], _Cfg())[0]
          .kwargs["end_date"] == date(2026, 8, 31))

    # ---------------- 连续空分片不再白跑 ----------------
    with tempfile.TemporaryDirectory() as tmp:
        prog2 = Progress.load(Path(tmp) / "p.json")
        for i in range(3):
            prog2.mark("A001", "kylin", "2025-01", "empty", 0)
            check(f"empty 第 {i + 1} 次后计数为 {i + 1}",
                  prog2.empty_streak("A001", "kylin", "2025-01") == i + 1)
        check("连续 3 次空即判定超出历史窗口",
              prog2.should_skip_empty("A001", "kylin", "2025-01", 3))
        check("阀值设 0 时总是重试",
              not prog2.should_skip_empty("A001", "kylin", "2025-01", 0))
        check("阀值高于计数时仍重试",
              not prog2.should_skip_empty("A001", "kylin", "2025-01", 5))
        # 一旦采到数据计数必须归零，否则下次会被误跳
        prog2.mark("A001", "kylin", "2025-01", "success", 30)
        check("采到数据后空计数归零",
              prog2.empty_streak("A001", "kylin", "2025-01") == 0)
        check("成功后不再被当作窗口外",
              not prog2.should_skip_empty("A001", "kylin", "2025-01", 3))
        check("历史行数可从进度文件读回",
              prog2.entry_of("A001", "kylin", "2025-01").get("rows") == 30)

    # ---------------- 报告区分本次新采与历史已采 ----------------
    hist = [
        SliceReport("A001", "kylin", "2025-01", "skipped",
                    history_rows=186, history_at="2026-08-14 01:00:00"),
        SliceReport("A001", "kylin", "2025-02", "skipped",
                    history_rows=168, history_at="2026-08-14 01:05:00"),
    ]
    htext = render_report(hist, ["2025-01", "2025-02"])
    check("全跳过时仍报历史已采行数", "历史已采：354 行" in htext)
    check("全跳过时说明属正常断点续跑", "本次无新增采集" in htext)
    check("全跳过时给出核对 SQL", "SELECT MIN(transaction_date)" in htext)
    check("全跳过时提示 --force 强制重采", "--force" in htext)

    ptext = render_report(
        [SliceReport("A001", "kylin", "2026-08", "success", 78, 78,
                     partial=True)], ["2026-08"])
    check("部分区间分片在报告中单列", "部分区间分片 1 个" in ptext)

    ntext = render_report(
        [SliceReport("A001", "kylin", "2026-09", "not_ready",
                     error="2026-09 尚无已结算数据")], ["2026-09"])
    check("尚无数据分片不计作失败",
          "尚无已结算数据的分片 1 个" in ntext and "失败分片" not in ntext)

    etext = render_report(
        [SliceReport("A001", "kylin", "2025-01", "skipped_empty")],
        ["2025-01"])
    check("窗口外分片单独提示并给出重试方式",
          "超出历史窗口的分片 1 个" in etext
          and "--max-empty-retry 0" in etext)

    # ---------------- 按目标表拆分行数 ----------------
    # 三张表粒度相差很大（目的地分析表每账号每月只一行），只报一个总行数
    # 会让人误以为后两张表没采到，所以报告必需把每张表分开列出。
    ttext = render_report([
        SliceReport("A001", "kylin", "2026-08", "success", 78, 156),
        SliceReport("A001", "rating", "2026-08", "success", 1, 2),
        SliceReport("A001", "score", "2026-08", "success", 3, 6),
    ], ["2026-08"])
    check("汇总列出三张目标表的真实表名",
          all(t in ttext for t in (KYLIN_DAILY.name, DESTINATION_RATING.name,
                                   SUPPLIER_RATE_SCORE.name)))
    check("每张表行数分开统计而不是合并成一个数",
          f"{KYLIN_DAILY.name:<42}{78:>7} 行" in ttext
          and f"{DESTINATION_RATING.name:<42}{1:>7} 行" in ttext
          and f"{SUPPLIER_RATE_SCORE.name:<42}{3:>7} 行" in ttext)
    check("报告里的表名从 schema 取而非写死",
          _table_of("kylin") == KYLIN_DAILY.name
          and _table_of("rating") == DESTINATION_RATING.name
          and _table_of("score") == SUPPLIER_RATE_SCORE.name)
    check("未知任务名不报错而退回任务名",
          _table_of("nonexistent") == "nonexistent")
    # 历史已采也得计入每张表的累计，否则断点续跑时每张表都显示 0 行
    tbtext = render_report([
        SliceReport("A001", "rating", "2026-07", "skipped",
                    history_rows=5, history_at="2026-08-14 10:00:00"),
    ], ["2026-07"])
    check("历史已采行数计入每张表累计",
          f"{DESTINATION_RATING.name:<42}{5:>7} 行" in tbtext
          and "本次 0 + 历史 5" in tbtext)
    # 完全无数据的表要给出提示，与「本来就只有一行」区分开
    tztext = render_report(
        [SliceReport("A001", "rating", "2026-07", "empty")], ["2026-07"])
    check("无数据的表给出排查提示", "← 无数据" in tztext)
    # not_ready 本来就未执行，不应出现在目标表汇总里
    tntext = render_report(
        [SliceReport("A001", "rating", "2026-12", "not_ready")], ["2026-12"])
    check("not_ready 分片不计入目标表汇总", "按目标表" not in tntext)

    # ---------------- 汇总 ----------------
    print()
    width = max(len(n) for _, n, _ in results) + 2
    for status, name, detail in results:
        line = f"[{status}] {name:<{width}}"
        if detail:
            line += f" -- {detail}"
        print(line)
    failed = sum(1 for s, _, _ in results if s == FAIL)
    print("-" * 90)
    print(f"共 {len(results)} 项，失败 {failed} 项")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
