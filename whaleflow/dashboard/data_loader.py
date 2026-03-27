"""資料查詢層：所有 Dashboard 用的 DB 查詢函數。

所有函數使用 st.cache_data 快取，直接讀 DB（不呼叫外部 API）。
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import func, select

from whaleflow.dashboard.config_reader import ScanConfig
from whaleflow.db.engine import get_session
from whaleflow.db.models import (
    InstitutionalTrading,
    Stock,
    TdccDistribution,
    WeeklyPrice,
)
from whaleflow.strategy.price_filter import filter_by_liquidity, filter_by_weekly_gain
from whaleflow.strategy.tdcc_trend import StockSignal, scan_weekly


# ── 週清單 ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_available_weeks() -> list[date]:
    """取得 DB 中所有可用的 TDCC 週日期（降序）。"""
    with get_session() as session:
        rows = session.execute(
            select(TdccDistribution.week_date)
            .distinct()
            .order_by(TdccDistribution.week_date.desc())
        ).scalars().all()
    return list(rows)


def get_scan_weeks(target_week: date, n: int = 4) -> list[date]:
    """回傳 target_week 及其前 n-1 週的 DB 實際週日期（升序）。"""
    with get_session() as session:
        rows = session.execute(
            select(TdccDistribution.week_date)
            .where(TdccDistribution.week_date <= target_week)
            .distinct()
            .order_by(TdccDistribution.week_date.desc())
            .limit(n)
        ).scalars().all()
    return sorted(rows)


# ── 掃描 ───────────────────────────────────────────────────────────────────────

def run_scan(target_week: date, cfg: ScanConfig) -> list[StockSignal]:
    """
    執行 TDCC 趨勢掃描 + 總分過濾 + 價格過濾 + 流動性過濾。

    讀取 DB 中的 weekly_price 資料（不呼叫外部 API）。
    若 DB 缺少價格資料，跳過對應過濾步驟。
    """
    weeks = get_scan_weeks(target_week)
    if len(weeks) < 2:
        return []

    with get_session() as session:
        signals = scan_weekly(
            session, weeks,
            min_holder_delta=cfg.min_holder_delta,
            scores=cfg.scores,
            enabled=cfg.enabled,
        )

    # 總分過濾
    if cfg.min_total_score > 0:
        signals = [s for s in signals if s.score >= cfg.min_total_score]

    if not signals:
        return []

    # 讀 DB 中的收盤價和成交量
    stock_ids = tuple(s.stock_id for s in signals)
    week_dates = tuple(weeks)
    prices_t, prices_t1, volumes_by_week = _load_prices_volumes(stock_ids, week_dates)

    # 週漲幅過濾
    if cfg.price_filter_enabled and prices_t and prices_t1:
        signals = filter_by_weekly_gain(signals, prices_t, prices_t1,
                                        max_gain=cfg.max_weekly_gain)

    # 流動性過濾
    if cfg.liquidity_filter_enabled and volumes_by_week:
        signals = filter_by_liquidity(signals, volumes_by_week,
                                      min_avg_daily_volume=cfg.min_avg_daily_volume)

    return signals


def _load_prices_volumes(
    stock_ids: tuple[str, ...],
    week_dates: tuple[date, ...],
) -> tuple[dict[str, float], dict[str, float], list[dict[str, int]]]:
    """從 DB 讀取收盤價（最新2週）與成交量（所有週）。"""
    if not week_dates:
        return {}, {}, []

    with get_session() as session:
        rows = session.execute(
            select(WeeklyPrice.stock_id, WeeklyPrice.trade_date,
                   WeeklyPrice.close_price, WeeklyPrice.volume)
            .where(WeeklyPrice.stock_id.in_(list(stock_ids)))
            .where(WeeklyPrice.trade_date.in_(list(week_dates)))
        ).all()

    # 按週彙整
    prices_by_week: dict[date, dict[str, float]] = {}
    volumes_by_week: dict[date, dict[str, int]] = {}
    for stock_id, trade_date, close_price, volume in rows:
        if close_price:
            prices_by_week.setdefault(trade_date, {})[stock_id] = close_price
        if volume:
            volumes_by_week.setdefault(trade_date, {})[stock_id] = volume

    prices_t  = prices_by_week.get(week_dates[-1], {})
    prices_t1 = prices_by_week.get(week_dates[-2], {}) if len(week_dates) >= 2 else {}
    vols_list = [volumes_by_week.get(w, {}) for w in week_dates]

    return prices_t, prices_t1, vols_list


# ── 輔助資料 ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_all_stocks() -> list[tuple[str, str]]:
    """回傳 [(stock_id, stock_name), ...] 供下拉選單使用。"""
    with get_session() as session:
        rows = session.execute(
            select(Stock.stock_id, Stock.stock_name)
            .where(Stock.is_active == True)  # noqa: E712
            .where(Stock.is_etf == False)    # noqa: E712
            .order_by(Stock.stock_id)
        ).all()
    return [(r[0], r[1]) for r in rows]


@st.cache_data(ttl=3600)
def get_stock_info(stock_ids: tuple[str, ...]) -> dict[str, dict]:
    """回傳 {stock_id: {name, market, industry}} 基本資訊。"""
    with get_session() as session:
        rows = session.execute(
            select(Stock.stock_id, Stock.stock_name, Stock.market, Stock.industry)
            .where(Stock.stock_id.in_(list(stock_ids)))
        ).all()
    return {
        r[0]: {"name": r[1], "market": r[2], "industry": r[3] or "—"}
        for r in rows
    }


@st.cache_data(ttl=300)
def get_institutional_weekly(
    stock_ids: tuple[str, ...],
    week_end: date,
) -> dict[str, int]:
    """回傳本週三大法人合計淨買超（張）。"""
    week_start = week_end - timedelta(days=4)
    with get_session() as session:
        rows = session.execute(
            select(
                InstitutionalTrading.stock_id,
                func.sum(InstitutionalTrading.total_net).label("week_net"),
            )
            .where(InstitutionalTrading.stock_id.in_(list(stock_ids)))
            .where(InstitutionalTrading.trade_date >= week_start)
            .where(InstitutionalTrading.trade_date <= week_end)
            .group_by(InstitutionalTrading.stock_id)
        ).all()
    return {r[0]: (r[1] or 0) // 1000 for r in rows}  # 股 → 張


@st.cache_data(ttl=300)
def get_weekly_price_range(
    stock_ids: tuple[str, ...],
    week_date: date,
) -> tuple[dict[str, float], dict[str, float], dict[str, int]]:
    """回傳指定週及前一週的收盤價，以及該週成交量。"""
    # 取最近 2 個週日期（≤ week_date）
    with get_session() as session:
        available = session.execute(
            select(WeeklyPrice.trade_date)
            .where(WeeklyPrice.stock_id.in_(list(stock_ids)))
            .where(WeeklyPrice.trade_date <= week_date)
            .distinct()
            .order_by(WeeklyPrice.trade_date.desc())
            .limit(2)
        ).scalars().all()
        available = sorted(available)

        if not available:
            return {}, {}, {}

        rows = session.execute(
            select(WeeklyPrice.stock_id, WeeklyPrice.trade_date,
                   WeeklyPrice.close_price, WeeklyPrice.volume)
            .where(WeeklyPrice.stock_id.in_(list(stock_ids)))
            .where(WeeklyPrice.trade_date.in_(available))
        ).all()

    prices_map: dict[date, dict[str, float]] = {}
    volumes: dict[str, int] = {}
    for sid, td, price, vol in rows:
        if price:
            prices_map.setdefault(td, {})[sid] = price
        if vol and td == available[-1]:
            volumes[sid] = vol

    prices_t  = prices_map.get(available[-1], {})
    prices_t1 = prices_map.get(available[0], {}) if len(available) >= 2 else {}
    return prices_t, prices_t1, volumes


# ── 個股趨勢 ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def get_tdcc_history(stock_id: str, limit_weeks: int = 52) -> pd.DataFrame:
    """回傳單支股票的 TDCC 集保分佈歷史資料（最近 N 週），供趨勢圖使用。"""
    with get_session() as session:
        rows = session.execute(
            select(TdccDistribution)
            .where(TdccDistribution.stock_id == stock_id)
            .order_by(TdccDistribution.week_date.desc())
            .limit(limit_weeks)
        ).scalars().all()

    if not rows:
        return pd.DataFrame()

    data = [
        {
            "week_date":      r.week_date,
            "holders_400":    r.holders_above_400 or 0,
            "shares_400":     (r.shares_above_400 or 0) / 1000,   # 千張
            "pct_400":        r.pct_above_400 or 0.0,
            "holders_800":    r.holders_above_800 or 0,
            "shares_800":     (r.shares_above_800 or 0) / 1000,
            "pct_800":        r.pct_above_800 or 0.0,
            "holders_1000":   r.holders_above_1000 or 0,
            "shares_1000":    (r.shares_above_1000 or 0) / 1000,
            "pct_1000":       r.pct_above_1000 or 0.0,
        }
        for r in rows
    ]
    df = pd.DataFrame(data).sort_values("week_date").reset_index(drop=True)
    df["week_date"] = pd.to_datetime(df["week_date"])
    return df


@st.cache_data(ttl=600)
def get_price_history(stock_id: str, limit_weeks: int = 52) -> pd.DataFrame:
    """回傳單支股票的歷史週收盤價與成交量。"""
    with get_session() as session:
        rows = session.execute(
            select(WeeklyPrice)
            .where(WeeklyPrice.stock_id == stock_id)
            .order_by(WeeklyPrice.trade_date.desc())
            .limit(limit_weeks)
        ).scalars().all()

    if not rows:
        return pd.DataFrame()

    data = [
        {
            "trade_date":  r.trade_date,
            "close_price": r.close_price,
            "volume":      r.volume or 0,
        }
        for r in rows
    ]
    df = pd.DataFrame(data).sort_values("trade_date").reset_index(drop=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


# ── 掃描結果 DataFrame ─────────────────────────────────────────────────────────

def build_scan_dataframe(
    signals: list[StockSignal],
    week_end: date,
) -> pd.DataFrame:
    """將掃描結果組裝成可顯示的 DataFrame，補充股名/漲幅/法人/成交量欄位。"""
    if not signals:
        return pd.DataFrame()

    stock_ids = tuple(s.stock_id for s in signals)
    info_map  = get_stock_info(stock_ids)
    inst_map  = get_institutional_weekly(stock_ids, week_end)
    prices_t, prices_t1, volumes = get_weekly_price_range(stock_ids, week_end)

    rows = []
    for rank, sig in enumerate(signals, 1):
        info  = info_map.get(sig.stock_id, {})
        p0    = prices_t.get(sig.stock_id)
        p1    = prices_t1.get(sig.stock_id)
        gain  = round((p0 - p1) / p1 * 100, 1) if p0 and p1 and p1 > 0 else None
        inst  = inst_map.get(sig.stock_id)
        vol   = volumes.get(sig.stock_id)

        rows.append({
            "排名":    rank,
            "代號":    sig.stock_id,
            "名稱":    info.get("name", ""),
            "產業":    info.get("industry", "—"),
            "分數":    sig.score,
            "條件":    "+".join(sig.conditions),
            "週漲跌%": gain,
            "法人淨買(張)": inst,
            "成交量(張)":   vol,
            "說明":    sig.detail,
            "掃描週":  str(sig.week_date),
        })

    return pd.DataFrame(rows)
