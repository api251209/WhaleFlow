"""
TDCC OpenAPI fetcher for 集保戶股權分散表.

API endpoint: GET https://openapi-t.tdcc.com.tw/v1/opendata/1-5
- Returns ALL stocks' distribution data for the latest available week.
- No authentication required.
- One request returns ~67,000 rows (all stocks × 17 bracket levels).

Level mapping (持股分級):
  12 → 400,001-600,000 shares
  13 → 600,001-800,000 shares
  14 → 800,001-1,000,000 shares
  15 → 1,000,001以上 shares
  17 → 合計 (total)
"""

import re
from datetime import date

import httpx

from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

_API_URL = "https://openapi-t.tdcc.com.tw/v1/opendata/1-5"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# Bracket levels that define our thresholds (cumulative from this level up)
_THRESHOLD_LEVELS = {
    "400": {12, 13, 14, 15},
    "600": {13, 14, 15},
    "800": {14, 15},
    "1000": {15},
}
_TOTAL_LEVEL = 17
_EQUITY_RE = re.compile(r"^\d{4,5}$")


def _parse_date(raw: str) -> date | None:
    """Parse YYYYMMDD string (possibly with BOM) into date."""
    s = raw.strip().lstrip("\ufeff")
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, IndexError):
        return None


def _accumulate_api_rows(rows_by_level: dict[int, dict]) -> dict:
    """
    Compute cumulative holders/shares/pct for 400/600/800/1000 thresholds.

    Args:
        rows_by_level: {level_int: {"persons": int, "shares": int, "pct": float}}

    Returns:
        Dict with holders_above_X / shares_above_X / pct_above_X fields.
    """
    total = rows_by_level.get(_TOTAL_LEVEL, {})
    result: dict = {
        "total_holders": total.get("persons", 0),
        "total_shares": total.get("shares", 0),
    }
    for threshold, levels in _THRESHOLD_LEVELS.items():
        h = sum(rows_by_level.get(lv, {}).get("persons", 0) for lv in levels)
        s = sum(rows_by_level.get(lv, {}).get("shares", 0) for lv in levels)
        p = sum(rows_by_level.get(lv, {}).get("pct", 0.0) for lv in levels)
        result[f"holders_above_{threshold}"] = h
        result[f"shares_above_{threshold}"] = s
        result[f"pct_above_{threshold}"] = round(p, 4)
    return result


async def fetch_tdcc_api() -> tuple[date | None, list[dict]]:
    """
    Fetch the latest week's TDCC distribution for all equity stocks.

    Returns:
        (week_date, records) where records are ready for TdccDistribution upsert.
        week_date is None if data cannot be parsed.
    """
    logger.info("Fetching TDCC OpenAPI distribution data ...")

    async with httpx.AsyncClient(timeout=60.0, headers=_HEADERS) as client:
        r = await client.get(_API_URL)
        r.raise_for_status()
        raw_rows = r.json()

    logger.info("TDCC OpenAPI: received %d raw rows", len(raw_rows))

    # Determine week_date from the data
    week_date: date | None = None
    for row in raw_rows[:5]:
        date_str = row.get("\ufeff資料日期") or row.get("資料日期", "")
        week_date = _parse_date(date_str)
        if week_date:
            break

    if not week_date:
        logger.error("TDCC OpenAPI: could not determine week_date")
        return None, []

    # Group by stock_id → {level: row_data}
    by_stock: dict[str, dict[int, dict]] = {}
    for row in raw_rows:
        stock_id = row.get("證券代號", "").strip()
        if not _EQUITY_RE.match(stock_id):
            continue  # skip ETFs, bonds, warrants, etc.
        try:
            level = int(row["持股分級"])
            persons = int(str(row.get("人數", "0")).replace(",", "") or 0)
            shares = int(str(row.get("股數", "0")).replace(",", "") or 0)
            pct = float(str(row.get("占集保庫存數比例%", "0")).replace(",", "") or 0.0)
        except (ValueError, KeyError):
            continue

        if stock_id not in by_stock:
            by_stock[stock_id] = {}
        by_stock[stock_id][level] = {"persons": persons, "shares": shares, "pct": pct}

    # Build records
    records: list[dict] = []
    for stock_id, levels in by_stock.items():
        accumulated = _accumulate_api_rows(levels)
        records.append({
            "stock_id": stock_id,
            "week_date": week_date,
            **accumulated,
        })

    logger.info(
        "TDCC OpenAPI: parsed %d equity stocks for week_date=%s",
        len(records), week_date,
    )
    return week_date, records
