"""WhaleFlow CLI entry point."""

from datetime import date

import click

from whaleflow.utils.logging import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


@click.group()
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="顯示詳細 log（預設只顯示警告）")
@click.pass_context
def cli(ctx: click.Context, verbose: bool):
    """WhaleFlow -- 台灣集保分佈大戶追蹤選股系統"""
    import logging
    if not verbose:
        logging.getLogger().setLevel(logging.WARNING)


# ── init ──────────────────────────────────────────────────────────────────────

@cli.command()
def init():
    """初始化資料庫（建立所有資料表）。"""
    from whaleflow.db.migrations import init_db
    init_db()
    click.echo("資料庫初始化完成。")


# ── fetch group ───────────────────────────────────────────────────────────────

@cli.group()
def fetch():
    """資料抓取指令群組。"""


@fetch.command("stocks")
def fetch_stocks():
    """同步全市場股票清單（並標記 ETF）。"""
    from whaleflow.db.engine import get_session
    from whaleflow.services.stock_service import sync_stock_list_sync
    with get_session() as session:
        count = sync_stock_list_sync(session)
    click.echo(f"已同步 {count} 檔股票。")


@fetch.command("institutional")
@click.option("--date", "target_date", default=None,
              help="交易日期 YYYY-MM-DD（預設：最近一個週五）")
@click.option("--week", is_flag=True, default=False,
              help="抓取整週（週一到週五）資料")
def fetch_institutional(target_date: str | None, week: bool):
    """抓取三大法人買賣超（TWSE + TPEx 官方 API）。"""
    import asyncio
    from whaleflow.db.engine import get_session
    from whaleflow.services.institutional_service import (
        fetch_and_store_institutional, fetch_week_institutional,
    )
    from whaleflow.utils.date_utils import get_last_friday, str_to_date

    d = str_to_date(target_date) if target_date else get_last_friday()

    with get_session() as session:
        if week:
            click.echo(f"抓取整週三大法人資料（週末={d}）...")
            count = asyncio.run(fetch_week_institutional(session, d))
        else:
            click.echo(f"抓取三大法人資料 date={d} ...")
            count = asyncio.run(fetch_and_store_institutional(session, d))
    click.echo(f"已儲存 {count} 筆三大法人記錄。")


@fetch.command("tdcc")
@click.option("--date", "target_date", default=None,
              help="目標日期 YYYY-MM-DD（預設：最近一個週五）")
@click.option("--filter/--no-filter", "use_filter", default=True,
              help="先用三大法人預篩（預設開啟）")
@click.option("--delay", default=1.0, show_default=True,
              help="每筆請求間隔秒數")
def fetch_tdcc(target_date: str | None, use_filter: bool, delay: float):
    """爬取 TDCC 集保分佈資料。

    預設流程：先用本週三大法人資料預篩股票，再只爬有法人買進的個股。
    若 DB 中沒有本週法人資料，自動先抓法人資料。
    """
    import asyncio
    from whaleflow.db.engine import get_session
    from whaleflow.services.institutional_service import (
        get_tdcc_candidates, fetch_week_institutional,
    )
    from whaleflow.services.tdcc_service import fetch_and_store_tdcc
    from whaleflow.utils.date_utils import get_last_friday, str_to_date, normalize_to_week

    d = str_to_date(target_date) if target_date else get_last_friday()
    week_end = normalize_to_week(d)

    with get_session() as session:
        stock_ids = None

        if use_filter:
            candidates = get_tdcc_candidates(session, week_end)

            if not candidates:
                click.echo(f"DB 中無本週法人資料，先抓三大法人（week={week_end}）...")
                asyncio.run(fetch_week_institutional(session, week_end))
                session.commit()
                candidates = get_tdcc_candidates(session, week_end)

            if candidates:
                stock_ids = candidates
                click.echo(f"預篩完成：{len(stock_ids)} 檔（週三大法人合計淨買超 > 0）")
            else:
                click.echo("警告：預篩後無符合條件的股票，改為抓全市場。", err=True)

        n_target = len(stock_ids) if stock_ids else "全市場"
        click.echo(f"開始爬取 TDCC 集保分佈 date={week_end}，共 {n_target} 檔 ...")
        count = asyncio.run(
            fetch_and_store_tdcc(session, week_end, stock_ids=stock_ids, scraper_delay=delay)
        )

    click.echo(f"完成。已儲存 {count} 檔集保分佈資料。")


@fetch.command("all")
@click.option("--date", "target_date", default=None,
              help="目標日期 YYYY-MM-DD（預設：最近一個週五）")
@click.option("--delay", default=1.0, show_default=True,
              help="TDCC 每筆請求間隔秒數")
def fetch_all(target_date: str | None, delay: float):
    """完整週流程：股票清單 → 三大法人 → TDCC（含預篩）。"""
    import asyncio
    from whaleflow.db.engine import get_session
    from whaleflow.services.stock_service import sync_stock_list_sync
    from whaleflow.services.institutional_service import (
        fetch_week_institutional, get_tdcc_candidates,
    )
    from whaleflow.services.tdcc_service import fetch_and_store_tdcc
    from whaleflow.utils.date_utils import get_last_friday, str_to_date, normalize_to_week

    d = str_to_date(target_date) if target_date else get_last_friday()
    week_end = normalize_to_week(d)

    with get_session() as session:
        click.echo("① 同步股票清單...")
        n_stocks = sync_stock_list_sync(session)
        click.echo(f"   → {n_stocks} 檔")

        click.echo(f"② 抓取三大法人（週={week_end}）...")
        n_inst = asyncio.run(fetch_week_institutional(session, week_end))
        session.commit()
        click.echo(f"   → {n_inst} 筆")

        click.echo("③ 預篩 TDCC 候選股票...")
        candidates = get_tdcc_candidates(session, week_end)
        click.echo(f"   → {len(candidates)} 檔符合條件")

        click.echo(f"④ 爬取 TDCC 集保分佈（delay={delay}s）...")
        n_tdcc = asyncio.run(
            fetch_and_store_tdcc(session, week_end, stock_ids=candidates, scraper_delay=delay)
        )
        click.echo(f"   → {n_tdcc} 筆集保資料")

    click.echo(f"\n完整流程完成。本週 TDCC 抓取了 {n_tdcc} 檔股票。")


# ── backfill ──────────────────────────────────────────────────────────────────

@fetch.command("tdcc-api")
def fetch_tdcc_api():
    """用 TDCC OpenAPI 快速抓取最新週集保分佈（一次取得全市場，無需爬蟲）。

    比 fetch tdcc 快很多，但只能取得最新一週資料。
    適合每週例行更新使用。
    """
    import asyncio
    from whaleflow.db.engine import get_session
    from whaleflow.services.tdcc_service import fetch_and_store_tdcc_via_api

    with get_session() as session:
        week_date, count = asyncio.run(fetch_and_store_tdcc_via_api(session))

    if count:
        click.echo(f"已儲存 {count} 筆集保資料（週：{week_date}）。")
    else:
        click.echo("無資料或 API 暫時無法使用。")


@fetch.command("backfill")
@click.option("--weeks", default=4, show_default=True,
              help="補抓最近幾週的資料（從最新週往前算）")
@click.option("--delay", default=1.0, show_default=True,
              help="TDCC 每筆請求間隔秒數")
@click.option("--no-inst", is_flag=True, default=False,
              help="跳過三大法人抓取（已有資料時可加速）")
@click.option("--no-filter", "no_filter", is_flag=True, default=False,
              help="跳過法人預篩，抓全市場所有股票（較慢，每週約 40~50 分鐘）")
def fetch_backfill(weeks: int, delay: float, no_inst: bool, no_filter: bool):
    """補抓歷史 TDCC 集保分佈資料（含三大法人預篩）。

    從 TDCC 取得可用週清單，對最近 N 週依序執行：
    三大法人 → 預篩 → TDCC 集保。
    已有的資料會 upsert（不重複計算）。

    加 --no-filter 可抓全市場（不做法人預篩），資料更完整但耗時較長。
    """
    import asyncio
    from whaleflow.db.engine import get_session
    from whaleflow.fetchers.tdcc_scraper import TdccScraper
    from whaleflow.services.institutional_service import (
        fetch_week_institutional, get_tdcc_candidates,
    )
    from whaleflow.services.tdcc_service import fetch_and_store_tdcc
    from whaleflow.utils.date_utils import normalize_to_week
    from datetime import date as date_type

    async def get_available_weeks() -> list[date_type]:
        async with TdccScraper() as s:
            raw = await s.get_available_dates()
        dates = []
        for d in raw:
            try:
                dates.append(date_type(int(d[:4]), int(d[4:6]), int(d[6:])))
            except ValueError:
                continue
        return sorted(set(dates), reverse=True)

    available = asyncio.run(get_available_weeks())
    targets = available[:weeks]

    click.echo(f"TDCC 可用週數：{len(available)}，本次補抓最近 {len(targets)} 週")
    click.echo(f"範圍：{targets[-1]} ～ {targets[0]}\n")

    total_inst = 0
    total_tdcc = 0

    with get_session() as session:
        for i, week_end in enumerate(reversed(targets), 1):  # 舊→新
            click.echo(f"[{i}/{len(targets)}] 週 {week_end}")

            if not no_inst:
                n = asyncio.run(fetch_week_institutional(session, week_end))
                session.commit()
                total_inst += n
                click.echo(f"  三大法人：{n} 筆")

            if no_filter:
                click.echo("  預篩：略過（全市場模式）")
                stock_ids_to_fetch = None  # fetch_and_store_tdcc 會自動載入全市場
            else:
                candidates = get_tdcc_candidates(session, week_end)
                if not candidates:
                    click.echo("  預篩：無符合條件股票，跳過")
                    continue
                click.echo(f"  預篩：{len(candidates)} 檔")
                stock_ids_to_fetch = candidates

            n_tdcc = asyncio.run(
                fetch_and_store_tdcc(session, week_end, stock_ids=stock_ids_to_fetch, scraper_delay=delay)
            )
            total_tdcc += n_tdcc
            click.echo(f"  TDCC：{n_tdcc} 筆\n")

    click.echo(f"補抓完成。三大法人共 {total_inst} 筆，TDCC 共 {total_tdcc} 筆。")


# ── scan ──────────────────────────────────────────────────────────────────────

@cli.group()
def scan():
    """選股掃描指令群組。"""


@scan.command("weekly")
@click.option("--date", "target_date", default=None,
              help="目標週五 YYYY-MM-DD（預設：最近一個週五）")
@click.option("--top", default=30, show_default=True,
              help="顯示前 N 名")
@click.option("--price-filter/--no-price-filter", "use_price_filter", default=True,
              help="排除週漲幅 > 20% 的股票（預設開啟）")
@click.option("--save/--no-save", default=False,
              help="儲存結果為 CSV（reports/scan_YYYYMMDD.csv）")
def scan_weekly(target_date: str | None, top: int, use_price_filter: bool, save: bool):
    """執行週集保趨勢掃描，輸出大戶持續增加的候選股。

    需要目標週及前三週的 TDCC 資料（共 4 週）。
    若 DB 中資料不足，請先執行 fetch backfill --weeks 4。
    """
    import asyncio
    import tomllib
    from pathlib import Path
    from datetime import timedelta
    from whaleflow.db.engine import get_session
    from whaleflow.strategy.tdcc_trend import scan_weekly as _scan
    from whaleflow.utils.date_utils import get_last_friday, str_to_date, normalize_to_week

    # 讀取設定檔
    config_path = Path("scan_config.toml")
    cfg: dict = {}
    if config_path.exists():
        with config_path.open("rb") as f:
            cfg = tomllib.load(f)
    else:
        click.echo("提示：找不到 scan_config.toml，使用預設參數。", err=True)

    noise_cfg   = cfg.get("noise", {})
    cond_cfg    = cfg.get("conditions", {})
    price_cfg   = cfg.get("price_filter", {})
    scoring_cfg = cfg.get("scoring", {})

    min_holder_delta = int(noise_cfg.get("min_holder_delta", 3))
    scores = {
        "A": float(cond_cfg.get("condition_a_score", 3.0)),
        "B": float(cond_cfg.get("condition_b_score", 2.0)),
        "C": float(cond_cfg.get("condition_c_score", 2.5)),
        "_a_cfg": {
            "min_pct_delta": float(cond_cfg.get("condition_a_min_pct_delta", 0.0)),
            "min_holder_delta": int(cond_cfg.get("condition_a_min_holder_delta", min_holder_delta)),
            "lookback_weeks": int(cond_cfg.get("condition_a_lookback_weeks", 4)),
        },
        "_b_cfg": {
            "min_holder_delta": int(cond_cfg.get("condition_b_min_holder_delta", min_holder_delta)),
            "consecutive_weeks": int(cond_cfg.get("condition_b_consecutive_weeks", 2)),
            "min_share_delta": int(cond_cfg.get("condition_b_min_share_delta", 0)),
        },
        "_c_cfg": {
            "min_holder_delta": int(cond_cfg.get("condition_c_min_holder_delta", min_holder_delta)),
            "min_share_delta": int(cond_cfg.get("condition_c_min_share_delta", 0)),
        },
    }
    enabled = {
        "A": bool(cond_cfg.get("condition_a_enabled", True)),
        "B": bool(cond_cfg.get("condition_b_enabled", True)),
        "C": bool(cond_cfg.get("condition_c_enabled", True)),
    }
    max_weekly_gain = float(price_cfg.get("max_weekly_gain", 0.20))
    price_filter_on = bool(price_cfg.get("enabled", True)) and use_price_filter

    liquidity_cfg = cfg.get("liquidity_filter", {})
    min_avg_daily_volume = int(liquidity_cfg.get("min_avg_daily_volume", 1000))
    liquidity_filter_on = bool(liquidity_cfg.get("enabled", False))

    from sqlalchemy import select, func
    from whaleflow.db.models import TdccDistribution

    d = str_to_date(target_date) if target_date else get_last_friday()
    week_t0 = normalize_to_week(d)

    # 從 DB 取最近 4 個實際存在的週日期（≤ week_t0），避免 TDCC 日期非標準週五的問題
    with get_session() as session:
        available = session.execute(
            select(TdccDistribution.week_date)
            .where(TdccDistribution.week_date <= week_t0)
            .distinct()
            .order_by(TdccDistribution.week_date.desc())
            .limit(4)
        ).scalars().all()
    weeks = sorted(available)

    if len(weeks) < 2:
        click.echo("DB 中 TDCC 資料不足（需要至少 2 週），請先執行 fetch tdcc-api。")
        return

    click.echo(f"掃描週：{' → '.join(str(w) for w in weeks)}")
    click.echo(f"設定：雜訊門檻={min_holder_delta}人  漲幅上限={max_weekly_gain*100:.0f}%  "
               f"條件={''.join(k for k,v in enabled.items() if v)}\n")

    with get_session() as session:
        signals = _scan(session, weeks,
                        min_holder_delta=min_holder_delta,
                        scores=scores, enabled=enabled)

    # 總分過濾
    min_total_score = float(scoring_cfg.get("min_total_score", 0.0))
    if min_total_score > 0 and signals:
        before = len(signals)
        signals = [s for s in signals if s.score >= min_total_score]
        click.echo(
            f"總分過濾（≥{min_total_score}分）：{before} → {len(signals)} 檔"
            f"（排除 {before - len(signals)} 檔）"
        )

    if not signals:
        click.echo("本週無符合條件的股票。（請確認 DB 有 4 週 TDCC 資料）")
        return

    # ── 股價抓取、存入 DB、漲幅過濾 ─────────────────────────────────────────
    prices_t: dict[str, float] = {}
    prices_t1: dict[str, float] = {}

    if price_filter_on or liquidity_filter_on:
        try:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert
            from whaleflow.fetchers.price import fetch_closing_prices
            from whaleflow.strategy.price_filter import filter_by_weekly_gain, filter_by_liquidity
            from whaleflow.db.models import WeeklyPrice

            # 流動性過濾需要所有週的成交量；價格過濾只需要最近 2 週
            weeks_to_fetch = weeks if liquidity_filter_on else weeks[-2:]
            click.echo(f"抓取 {len(weeks_to_fetch)} 週股價／成交量資料...")

            async def _fetch_prices():
                return await asyncio.gather(
                    *[fetch_closing_prices(w) for w in weeks_to_fetch]
                )

            fetch_results = asyncio.run(_fetch_prices())
            # fetch_results[i] = (prices_dict, volumes_dict) for weeks_to_fetch[i]

            prices_t  = fetch_results[-1][0]   # 最新週收盤價
            prices_t1 = fetch_results[-2][0]   # 上一週收盤價
            all_volumes = [r[1] for r in fetch_results]  # 各週成交量

            # 存入 DB（upsert price + volume）
            with get_session() as session:
                for trade_date, (price_map, vol_map) in zip(weeks_to_fetch, fetch_results):
                    if not price_map:
                        continue
                    records = [
                        {
                            "stock_id": sid,
                            "trade_date": trade_date,
                            "close_price": price,
                            "volume": vol_map.get(sid),
                        }
                        for sid, price in price_map.items()
                    ]
                    stmt = sqlite_insert(WeeklyPrice).values(records)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["stock_id", "trade_date"],
                        set_={
                            "close_price": stmt.excluded.close_price,
                            "volume": stmt.excluded.volume,
                        },
                    )
                    session.execute(stmt)
                    session.commit()

            if price_filter_on:
                before = len(signals)
                signals = filter_by_weekly_gain(signals, prices_t, prices_t1,
                                                max_gain=max_weekly_gain)
                click.echo(
                    f"漲幅過濾（≤{max_weekly_gain*100:.0f}%）：{before} → {len(signals)} 檔"
                    f"（排除 {before - len(signals)} 檔）"
                )

            if liquidity_filter_on:
                before = len(signals)
                signals = filter_by_liquidity(signals, all_volumes,
                                              min_avg_daily_volume=min_avg_daily_volume)
                click.echo(
                    f"流動性過濾（均量≥{min_avg_daily_volume}張）：{before} → {len(signals)} 檔"
                    f"（排除 {before - len(signals)} 檔）"
                )

            click.echo()
        except Exception as e:
            click.echo(f"警告：股價／流動性過濾失敗（{e}），顯示未過濾結果。", err=True)

    if not signals:
        click.echo("價格過濾後無剩餘股票。")
        return

    # 取得股票名稱
    from datetime import timedelta
    from sqlalchemy import select, func
    from whaleflow.db.models import Stock, InstitutionalTrading
    stock_ids = [s.stock_id for s in signals]
    week_start = weeks[-1] - timedelta(days=4)  # 週一
    with get_session() as session:
        name_rows = session.execute(
            select(Stock.stock_id, Stock.stock_name).where(Stock.stock_id.in_(stock_ids))
        ).all()
        inst_rows = session.execute(
            select(
                InstitutionalTrading.stock_id,
                func.sum(InstitutionalTrading.total_net).label("week_net"),
            )
            .where(InstitutionalTrading.stock_id.in_(stock_ids))
            .where(InstitutionalTrading.trade_date >= week_start)
            .where(InstitutionalTrading.trade_date <= weeks[-1])
            .group_by(InstitutionalTrading.stock_id)
        ).all()
    name_map = {r[0]: r[1] for r in name_rows}
    inst_map = {r[0]: r[1] for r in inst_rows}  # total_net 單位：股

    def _gain_str(stock_id: str) -> str:
        p0 = prices_t.get(stock_id)
        p1 = prices_t1.get(stock_id)
        if p0 and p1 and p1 > 0:
            g = (p0 - p1) / p1 * 100
            return f"{g:+.1f}%"
        return "N/A"

    def _inst_str(stock_id: str) -> str:
        net = inst_map.get(stock_id)
        if net is None:
            return "N/A"
        lots = net // 1000  # 股 → 張
        if abs(lots) >= 1000:
            return f"{lots/1000:+.1f}千張"
        return f"{lots:+d}張"

    # 評分標準說明
    max_score = sum(scores[k] for k in ("A", "B", "C") if enabled.get(k, True))
    score_a = scores["A"]
    score_b = scores["B"]
    score_c = scores["C"]
    lookback_a = cond_cfg.get("condition_a_lookback_weeks", 4)
    min_hd_a = cond_cfg.get("condition_a_min_holder_delta", min_holder_delta)
    consec_b = cond_cfg.get("condition_b_consecutive_weeks", 2)
    min_hd_b = cond_cfg.get("condition_b_min_holder_delta", min_holder_delta)
    min_hd_c = cond_cfg.get("condition_c_min_holder_delta", min_holder_delta)
    click.echo(f"【評分標準】滿足任一條件即入選，多條件分數累加（最高 {max_score:.1f} 分）")
    click.echo(f"  條件A（{score_a:.1f}分）400張以上持股比例，T 週 > T-{lookback_a-1} 週（{lookback_a}週淨增加，人數需增加 >{min_hd_a} 人）")
    click.echo(f"  條件B（{score_b:.1f}分）800張以上持股張數，連續{consec_b}週增加（每週人數變動需 >{min_hd_b} 人）")
    click.echo(f"  條件C（{score_c:.1f}分）1000張以上人數與張數，本週同時增加（人數增加需 >{min_hd_c} 人）")
    click.echo(f"  ※ 已排除近一週漲幅 > {max_weekly_gain*100:.0f}% 的個股")
    click.echo()
    click.echo("【欄位說明】")
    click.echo("  週漲跌幅：T-1週五 → T週五收盤價變化")
    click.echo("  法人淨買：本週（週一～週五）三大法人合計淨買超張數，正數=買超 負數=賣超")
    click.echo()
    click.echo(f"{'排名':<4} {'代號':<7} {'名稱':<10} {'週漲跌幅':>8} {'法人淨買':>9}  {'分數':<5} {'條件':<6} 說明")
    click.echo("-" * 95)
    for rank, sig in enumerate(signals[:top], 1):
        conds = "+".join(sig.conditions)
        name = name_map.get(sig.stock_id, "")
        click.echo(
            f"{rank:<4} {sig.stock_id:<7} {name:<10} {_gain_str(sig.stock_id):>8} {_inst_str(sig.stock_id):>9}  "
            f"{sig.score:<5.1f} {conds:<6} {sig.detail}"
        )

    click.echo(f"\n共 {len(signals)} 檔符合，顯示前 {min(top, len(signals))} 名。")

    # ── 儲存 CSV ────────────────────────────────────────────────────────────
    if save:
        import csv
        from pathlib import Path
        from sqlalchemy import select
        from whaleflow.db.engine import get_session
        from whaleflow.db.models import Stock

        # 取得股票名稱
        stock_ids = [s.stock_id for s in signals]
        with get_session() as session:
            rows = session.execute(
                select(Stock.stock_id, Stock.stock_name)
                .where(Stock.stock_id.in_(stock_ids))
            ).all()
        name_map = {r[0]: r[1] for r in rows}

        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        out_path = reports_dir / f"scan_{weeks[-1].strftime('%Y%m%d')}.csv"

        with out_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            # 頁首：評分標準說明
            writer.writerow(["# WhaleFlow 集保趨勢選股報告", f"掃描日期：{weeks[-1]}"])
            writer.writerow(["# 評分標準：滿足以下任一條件即入選"])
            writer.writerow(["# 條件A（3分）", "400張以上持股比例，連續4週每週都在增加（含雜訊過濾：每週人數變動需 > 3人）"])
            writer.writerow(["# 條件B（2分）", "800張以上持股股數，連續2週增加（含雜訊過濾：每週人數變動需 > 3人）"])
            writer.writerow(["# 條件C（2.5分）", "1000張以上持股人數與股數，同時在本週增加（人數增加需 > 3人）"])
            writer.writerow(["# 多條件同時滿足則分數累加，最高7.5分"])
            writer.writerow(["# 股價過濾：排除近一週漲幅 > 20% 的個股"])
            writer.writerow([])
            # 欄位標題
            writer.writerow(["排名", "股票代號", "股票名稱", "週漲跌幅", "法人淨買", "分數", "條件", "說明", "掃描週"])
            for rank, sig in enumerate(signals, 1):
                writer.writerow([
                    rank,
                    sig.stock_id,
                    name_map.get(sig.stock_id, ""),
                    _gain_str(sig.stock_id),
                    _inst_str(sig.stock_id),
                    sig.score,
                    "+".join(sig.conditions),
                    sig.detail,
                    str(sig.week_date),
                ])

        click.echo(f"\n報告已儲存：{out_path}")


# ── status ────────────────────────────────────────────────────────────────────

@cli.command()
def status():
    """顯示資料庫目前統計。"""
    from sqlalchemy import func, select
    from whaleflow.db.engine import get_session
    from whaleflow.db.models import FetchLog, InstitutionalTrading, Stock, TdccDistribution

    with get_session() as session:
        total_stocks = session.scalar(select(func.count()).select_from(Stock))
        etf_count = session.scalar(
            select(func.count()).select_from(Stock).where(Stock.is_etf == True)  # noqa: E712
        )
        tdcc_count = session.scalar(select(func.count()).select_from(TdccDistribution))
        latest_tdcc = session.scalar(select(func.max(TdccDistribution.week_date)))
        inst_count = session.scalar(select(func.count()).select_from(InstitutionalTrading))
        latest_inst = session.scalar(select(func.max(InstitutionalTrading.trade_date)))

    click.echo(f"股票數量：{total_stocks or 0}（ETF {etf_count or 0} 檔）")
    click.echo(f"集保記錄：{tdcc_count or 0} 筆，最新週 {latest_tdcc or '無'}")
    click.echo(f"三大法人：{inst_count or 0} 筆，最新日 {latest_inst or '無'}")


@cli.command()
@click.option("--port", default=8501, show_default=True, help="Streamlit 監聽埠")
def dashboard(port: int):
    """啟動 Streamlit 視覺化儀表板。"""
    import subprocess
    import sys
    from pathlib import Path

    app_path = Path(__file__).parent / "dashboard" / "app.py"
    click.echo(f"啟動 Dashboard：http://localhost:{port}")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(app_path), "--server.port", str(port)],
        check=False,
    )


if __name__ == "__main__":
    cli()
