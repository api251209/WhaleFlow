"""Fetch and store TDCC distribution data using the TDCC website scraper."""

import asyncio
from datetime import date

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from whaleflow.db.models import Stock, TdccDistribution
from whaleflow.fetchers.tdcc_scraper import TdccScraper
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)


def _upsert_batch(session: Session, records: list[dict]) -> None:
    if not records:
        return
    stmt = insert(TdccDistribution).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["stock_id", "week_date"],
        set_={
            "holders_above_400": stmt.excluded.holders_above_400,
            "shares_above_400": stmt.excluded.shares_above_400,
            "pct_above_400": stmt.excluded.pct_above_400,
            "holders_above_600": stmt.excluded.holders_above_600,
            "shares_above_600": stmt.excluded.shares_above_600,
            "pct_above_600": stmt.excluded.pct_above_600,
            "holders_above_800": stmt.excluded.holders_above_800,
            "shares_above_800": stmt.excluded.shares_above_800,
            "pct_above_800": stmt.excluded.pct_above_800,
            "holders_above_1000": stmt.excluded.holders_above_1000,
            "shares_above_1000": stmt.excluded.shares_above_1000,
            "pct_above_1000": stmt.excluded.pct_above_1000,
            "total_holders": stmt.excluded.total_holders,
            "total_shares": stmt.excluded.total_shares,
        },
    )
    session.execute(stmt)


async def fetch_and_store_tdcc(
    session: Session,
    target_date: date,
    stock_ids: list[str] | None = None,
    scraper_delay: float = 1.0,
    batch_size: int = 50,
) -> int:
    """
    Scrape TDCC distribution for the given stocks on target_date.

    Args:
        session: DB session.
        target_date: Any date; normalized to the Friday of that week.
        stock_ids: Stocks to scrape. If None, loads active non-ETF stocks from DB.
        scraper_delay: Seconds between requests (default 1.0 to be polite).
        batch_size: DB commit interval.

    Returns:
        Number of records upserted.
    """
    from sqlalchemy import select

    week_date = target_date  # caller is responsible for passing the correct date

    if stock_ids is None:
        stock_ids = [
            row[0]
            for row in session.execute(
                select(Stock.stock_id)
                .where(Stock.is_active == True)  # noqa: E712
                .where(Stock.is_etf == False)  # noqa: E712
            ).all()
        ]

    total = len(stock_ids)
    logger.info(
        "TDCC scrape: %d stocks for week_date=%s (delay=%.1fs)",
        total, week_date, scraper_delay,
    )

    upserted = 0
    batch: list[dict] = []

    async with TdccScraper(delay=scraper_delay) as scraper:
        for i, stock_id in enumerate(stock_ids, 1):
            try:
                data = await scraper.fetch_stock(stock_id, week_date)
            except Exception as e:
                logger.warning("TDCC fetch error for %s: %s, skipping", stock_id, e)
                data = None

            if data:
                batch.append({"week_date": week_date, **data})

            if len(batch) >= batch_size or (i == total and batch):
                _upsert_batch(session, batch)
                session.commit()
                upserted += len(batch)
                logger.info("Progress: %d/%d stocks | %d saved", i, total, upserted)
                batch = []

    logger.info("TDCC done. Upserted %d records for %s.", upserted, week_date)
    return upserted


def fetch_and_store_tdcc_sync(
    session: Session,
    target_date: date,
    stock_ids: list[str] | None = None,
    scraper_delay: float = 1.0,
) -> int:
    return asyncio.run(fetch_and_store_tdcc(session, target_date, stock_ids, scraper_delay))


async def fetch_and_store_tdcc_via_api(session: Session) -> tuple[date | None, int]:
    """
    Fetch the latest week's TDCC distribution via the TDCC OpenAPI (fast path).

    One HTTP request returns all equity stocks at once — no scraping needed.

    Returns:
        (week_date, upserted_count)
    """
    from whaleflow.fetchers.tdcc_api import fetch_tdcc_api

    week_date, records = await fetch_tdcc_api()
    if not records:
        return week_date, 0

    _upsert_batch(session, records)
    session.commit()
    logger.info("TDCC API: upserted %d records for %s", len(records), week_date)
    return week_date, len(records)


async def get_available_dates() -> list[str]:
    """Return the list of dates available on TDCC (YYYYMMDD strings)."""
    async with TdccScraper() as scraper:
        return await scraper.get_available_dates()
