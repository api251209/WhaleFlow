"""Fetch and store 三大法人買賣超 from TWSE + TPEx official APIs."""

import asyncio
import re
from datetime import date, timedelta

_EQUITY_ID_RE = re.compile(r"^\d{4,5}$")

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from whaleflow.db.models import InstitutionalTrading, Stock
from whaleflow.fetchers.twse import fetch_twse_institutional
from whaleflow.fetchers.tpex import fetch_tpex_institutional
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)


def _upsert(session: Session, records: list[dict]) -> None:
    if not records:
        return
    stmt = insert(InstitutionalTrading).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["stock_id", "trade_date"],
        set_={
            "foreign_buy": stmt.excluded.foreign_buy,
            "foreign_sell": stmt.excluded.foreign_sell,
            "foreign_net": stmt.excluded.foreign_net,
            "investment_trust_buy": stmt.excluded.investment_trust_buy,
            "investment_trust_sell": stmt.excluded.investment_trust_sell,
            "investment_trust_net": stmt.excluded.investment_trust_net,
            "dealer_buy": stmt.excluded.dealer_buy,
            "dealer_sell": stmt.excluded.dealer_sell,
            "dealer_net": stmt.excluded.dealer_net,
            "total_net": stmt.excluded.total_net,
        },
    )
    session.execute(stmt)


async def fetch_and_store_institutional(
    session: Session,
    trade_date: date,
) -> int:
    """
    Fetch institutional trading for both TWSE and TPEx for a given date.
    Merges both sources and upserts into institutional_trading table.

    Returns:
        Total number of records upserted.
    """
    logger.info("Fetching institutional trading for %s ...", trade_date)

    twse_records, tpex_records = await asyncio.gather(
        fetch_twse_institutional(trade_date),
        fetch_tpex_institutional(trade_date),
    )

    all_records = twse_records + tpex_records
    if not all_records:
        logger.warning("No institutional data for %s (holiday or non-trading day?)", trade_date)
        return 0

    _upsert(session, all_records)
    logger.info(
        "Stored %d institutional records for %s (TWSE=%d, TPEx=%d)",
        len(all_records), trade_date, len(twse_records), len(tpex_records),
    )
    return len(all_records)


async def fetch_week_institutional(
    session: Session,
    week_end: date,
) -> int:
    """
    Fetch institutional trading for the 5 trading days of the given week.
    week_end should be a Friday. Skips weekends and returns total records stored.
    """
    monday = week_end - timedelta(days=4)
    total = 0
    for i in range(5):  # Mon–Fri
        d = monday + timedelta(days=i)
        count = await fetch_and_store_institutional(session, d)
        total += count
    return total


def fetch_and_store_institutional_sync(session: Session, trade_date: date) -> int:
    return asyncio.run(fetch_and_store_institutional(session, trade_date))


def get_tdcc_candidates(
    session: Session,
    week_end: date,
) -> list[str]:
    """
    Return stock IDs suitable for TDCC scraping, based on this week's
    institutional trading activity.

    Filter criterion:
      Weekly 三大法人合計淨買超 > 0

    Args:
        session: DB session.
        week_end: The Friday of the target week.

    Returns:
        Sorted list of stock_id strings that pass the filter.
    """
    from sqlalchemy import func, select
    from datetime import timedelta

    week_start = week_end - timedelta(days=4)  # Monday

    stmt = (
        select(InstitutionalTrading.stock_id)
        .join(Stock, Stock.stock_id == InstitutionalTrading.stock_id)
        .where(Stock.is_etf == False)   # noqa: E712
        .where(Stock.is_active == True)  # noqa: E712
        .where(InstitutionalTrading.trade_date >= week_start)
        .where(InstitutionalTrading.trade_date <= week_end)
        .group_by(InstitutionalTrading.stock_id)
        .having(func.sum(InstitutionalTrading.total_net) > 0)
    )

    raw = [row[0] for row in session.execute(stmt).all()]
    candidates = sorted(sid for sid in raw if _EQUITY_ID_RE.match(sid))
    logger.info(
        "TDCC candidates for week %s–%s: %d stocks (non-ETF equity, weekly net_buy > 0)",
        week_start, week_end, len(candidates),
    )
    return candidates
