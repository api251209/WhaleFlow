"""Sync stock master list from FinMind TaiwanStockInfo, with ETF / low-volume filtering."""

import asyncio
import re

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from whaleflow.db.models import Stock
from whaleflow.fetchers.finmind import FinMindFetcher
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

# ── ETF detection ─────────────────────────────────────────────────────────────
# 1. Stock IDs that match 00xxx pattern (e.g. 0050, 00878, 006208)
_ETF_ID_RE = re.compile(r"^00\d{2,}")

# 2. Industry categories that indicate ETF / non-equity instruments
_ETF_INDUSTRIES = {
    "ETF",
    "受益憑證",
    "指數股票型基金",
    "債券ETF",
    "槓桿及反向",
    "期貨信託",
}

# 3. Name keywords that strongly indicate ETF
_ETF_NAME_KEYWORDS = ["ETF", "指數", "債券", "期貨信託"]


def is_etf(stock_id: str, stock_name: str, industry: str) -> bool:
    """Return True if this security appears to be an ETF or non-equity instrument."""
    if _ETF_ID_RE.match(stock_id):
        return True
    if industry and any(kw in industry for kw in _ETF_INDUSTRIES):
        return True
    if any(kw in stock_name for kw in _ETF_NAME_KEYWORDS):
        return True
    return False


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_stock(row: dict) -> dict | None:
    stock_id = str(row.get("stock_id", "")).strip()
    name = str(row.get("stock_name", "")).strip()
    market = str(row.get("type", "")).strip()

    if not stock_id or not name:
        return None

    market_upper = market.upper()
    if market_upper in ("TWSE", "上市", "TSE"):
        market_label = "TWSE"
    elif market_upper in ("TPEX", "上櫃", "OTC"):
        market_label = "TPEx"
    else:
        market_label = market or "UNKNOWN"

    industry = str(row.get("industry_category", "") or "").strip() or None
    etf = is_etf(stock_id, name, industry or "")

    return {
        "stock_id": stock_id,
        "stock_name": name,
        "market": market_label,
        "industry": industry,
        "is_active": True,
        "is_etf": etf,
    }


# ── Service ───────────────────────────────────────────────────────────────────

async def sync_stock_list(session: Session, fetcher: FinMindFetcher | None = None) -> int:
    """
    Fetch full stock list from FinMind and upsert into stocks table.
    Automatically marks ETFs via is_etf flag.

    Returns:
        Number of stocks upserted.
    """
    fetcher = fetcher or FinMindFetcher()
    logger.info("Fetching stock list from FinMind TaiwanStockInfo ...")

    rows = await fetcher.fetch("TaiwanStockInfo", {})
    if not rows:
        logger.warning("TaiwanStockInfo returned no data.")
        return 0

    records = [r for row in rows if (r := _parse_stock(row)) is not None]
    etf_count = sum(1 for r in records if r["is_etf"])
    logger.info("Parsed %d stocks (%d ETFs, %d equities).", len(records), etf_count, len(records) - etf_count)

    if not records:
        return 0

    stmt = insert(Stock).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["stock_id"],
        set_={
            "stock_name": stmt.excluded.stock_name,
            "market": stmt.excluded.market,
            "industry": stmt.excluded.industry,
            "is_active": stmt.excluded.is_active,
            "is_etf": stmt.excluded.is_etf,
        },
    )
    session.execute(stmt)
    logger.info("Upserted %d stocks.", len(records))
    return len(records)


def get_tradeable_stock_ids(
    session: Session,
    include_etf: bool = False,
    markets: list[str] | None = None,
) -> list[str]:
    """
    Return active stock IDs filtered by ETF status and market.

    Args:
        include_etf: Include ETF securities (default False).
        markets: Filter by market list, e.g. ['TWSE', 'TPEx']. None = all.

    Returns:
        Sorted list of stock_id strings.
    """
    from sqlalchemy import select

    q = select(Stock.stock_id).where(Stock.is_active == True)  # noqa: E712
    if not include_etf:
        q = q.where(Stock.is_etf == False)  # noqa: E712
    if markets:
        q = q.where(Stock.market.in_(markets))

    return [row[0] for row in session.execute(q.order_by(Stock.stock_id)).all()]


def sync_stock_list_sync(session: Session, fetcher: FinMindFetcher | None = None) -> int:
    return asyncio.run(sync_stock_list(session, fetcher))
