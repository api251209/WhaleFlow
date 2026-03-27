"""
Fetch daily closing prices and volumes for all equity stocks (TWSE + TPEx).

Used by the price momentum filter and liquidity filter.
"""

import asyncio
import re
from datetime import date

import httpx

from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}
_TWSE_MI_INDEX = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
_TPEX_QUOTES = (
    "https://www.tpex.org.tw/web/stock/aftertrading/"
    "otc_quotes_no1430/stk_wn1430_result.php"
)
_EQUITY_RE = re.compile(r"^\d{4,5}$")


def _parse_price(s: str) -> float | None:
    s = str(s).strip().replace(",", "")
    if not s or s in ("--", "-", "X", "除息", "除權", "除權息"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_volume(s) -> int | None:
    """Parse volume string to integer (shares). Returns None on failure."""
    s = str(s).strip().replace(",", "")
    if not s or s in ("--", "-", "X"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


async def fetch_twse_closing(trade_date: date) -> tuple[dict[str, float], dict[str, int]]:
    """
    Fetch closing prices and volumes for all TWSE-listed stocks on trade_date.

    Returns:
        (prices, volumes) — prices: {stock_id: close_price},
                            volumes: {stock_id: volume_in_lots (張)}.
        Both dicts are empty on API error or non-trading day.
    """
    date_str = trade_date.strftime("%Y%m%d")
    async with httpx.AsyncClient(timeout=30.0, headers=_HEADERS) as client:
        try:
            r = await client.get(
                _TWSE_MI_INDEX,
                params={"response": "json", "date": date_str, "type": "ALL"},
            )
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPError as e:
            logger.warning("TWSE MI_INDEX fetch error for %s: %s", trade_date, e)
            return {}, {}

    if data.get("stat") != "OK":
        logger.warning(
            "TWSE MI_INDEX: stat=%s for %s (likely non-trading day)",
            data.get("stat"), trade_date,
        )
        return {}, {}

    prices: dict[str, float] = {}
    volumes: dict[str, int] = {}
    for table in data.get("tables", []):
        fields = table.get("fields", [])
        if "證券代號" not in fields or "收盤價" not in fields:
            continue
        id_idx = fields.index("證券代號")
        close_idx = fields.index("收盤價")
        vol_idx = fields.index("成交股數") if "成交股數" in fields else None
        for row in table.get("data", []):
            if len(row) <= max(id_idx, close_idx):
                continue
            stock_id = str(row[id_idx]).strip()
            if not _EQUITY_RE.match(stock_id):
                continue
            price = _parse_price(row[close_idx])
            if price and price > 0:
                prices[stock_id] = price
            if vol_idx is not None and len(row) > vol_idx:
                shares = _parse_volume(row[vol_idx])
                if shares and shares > 0:
                    volumes[stock_id] = shares // 1000  # shares → lots

    logger.info("TWSE closing: %d stocks / %d volumes for %s", len(prices), len(volumes), trade_date)
    return prices, volumes


async def fetch_tpex_closing(trade_date: date) -> tuple[dict[str, float], dict[str, int]]:
    """
    Fetch closing prices and volumes for all TPEx-listed stocks on trade_date.

    Returns:
        (prices, volumes) — prices: {stock_id: close_price},
                            volumes: {stock_id: volume_in_lots (張)}.
        Both dicts are empty on API error or non-trading day.
    """
    roc_year = trade_date.year - 1911
    date_str = f"{roc_year}/{trade_date.month:02d}/{trade_date.day:02d}"

    async with httpx.AsyncClient(timeout=30.0, headers=_HEADERS) as client:
        try:
            r = await client.get(
                _TPEX_QUOTES,
                params={"l": "zh-tw", "d": date_str, "se": "AL"},
            )
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPError as e:
            logger.warning("TPEx quotes fetch error for %s: %s", trade_date, e)
            return {}, {}

    prices: dict[str, float] = {}
    volumes: dict[str, int] = {}

    tables = data.get("tables", [])
    if tables:
        table = tables[0]
        fields = table.get("fields", [])
        rows = table.get("data", [])
        # Determine column indices from fields if available
        id_col = next((i for i, f in enumerate(fields) if "代號" in f), 0)
        close_col = next((i for i, f in enumerate(fields) if "收盤" in f), 2)
        vol_col = next(
            (i for i, f in enumerate(fields) if "成交股數" in f or "成交量" in f),
            8,  # fallback index
        )
    else:
        rows = data.get("aaData", [])
        id_col, close_col, vol_col = 0, 2, 8

    for row in rows:
        if not row or len(row) <= max(id_col, close_col):
            continue
        stock_id = str(row[id_col]).strip()
        if not _EQUITY_RE.match(stock_id):
            continue
        price = _parse_price(row[close_col])
        if price and price > 0:
            prices[stock_id] = price
        if len(row) > vol_col:
            shares = _parse_volume(row[vol_col])
            if shares and shares > 0:
                volumes[stock_id] = shares // 1000  # shares → lots

    logger.info("TPEx closing: %d stocks / %d volumes for %s", len(prices), len(volumes), trade_date)
    return prices, volumes


async def fetch_closing_prices(trade_date: date) -> tuple[dict[str, float], dict[str, int]]:
    """
    Fetch closing prices and volumes for all equity stocks (TWSE + TPEx) on trade_date.

    Returns:
        (prices, volumes) — merged from both markets.
    """
    (twse_p, twse_v), (tpex_p, tpex_v) = await asyncio.gather(
        fetch_twse_closing(trade_date),
        fetch_tpex_closing(trade_date),
    )
    combined_prices = {**twse_p, **tpex_p}
    combined_volumes = {**twse_v, **tpex_v}
    logger.info(
        "Combined: %d prices / %d volumes for %s (TWSE=%d, TPEx=%d)",
        len(combined_prices), len(combined_volumes), trade_date, len(twse_p), len(tpex_p),
    )
    return combined_prices, combined_volumes
