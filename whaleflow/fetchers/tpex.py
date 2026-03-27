"""
TPEx 三大法人買賣超 fetcher.

Dual-track strategy:
  Primary:  New OpenAPI (today's data, no date param)
            https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading
  Fallback: Legacy API (supports specific date in ROC calendar)
            https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php

The OpenAPI returns today's data. For historical dates, use the legacy API.
"""

from datetime import date

import httpx

from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

_OPENAPI_URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
_LEGACY_URL = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.tpex.org.tw/",
}

# OpenAPI field names (English, slightly inconsistent spacing in source)
_OPENAPI_FIELD_MAP = {
    "SecuritiesCompanyCode": "stock_id",
    "ForeignInvestorsIncludeMainlandAreaInvestors-TotalBuy": "foreign_buy",
    "ForeignInvestorsInclude MainlandAreaInvestors-Difference": "foreign_net",
    " Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Total Sell": "foreign_sell",
    "SecuritiesInvestmentTrustCompanies-TotalBuy": "investment_trust_buy",
    "SecuritiesInvestmentTrustCompanies-TotalSell": "investment_trust_sell",
    "SecuritiesInvestmentTrustCompanies-Difference": "investment_trust_net",
    "Dealers-TotalBuy": "dealer_buy",
    "Dealers -TotalSell": "dealer_sell",
    "Dealers-Difference": "dealer_net",
    "TotalDifference": "total_net",
}


def _parse_num(s) -> int:
    if s is None:
        return 0
    return int(str(s).replace(",", "").strip()) if str(s).strip() else 0


def _parse_openapi_row(row: dict, trade_date: date) -> dict | None:
    stock_id = str(row.get("SecuritiesCompanyCode", "")).strip()
    if not stock_id or not stock_id[0].isdigit():
        return None

    # foreign_sell field has a leading space in the key
    foreign_sell_key = next(
        (k for k in row if "Total Sell" in k and "Foreign" in k), ""
    )
    foreign_net_key = next(
        (k for k in row if "Difference" in k and "MainlandArea" in k and "Include" in k), ""
    )

    return {
        "stock_id": stock_id,
        "trade_date": trade_date,
        "foreign_buy": _parse_num(row.get("ForeignInvestorsIncludeMainlandAreaInvestors-TotalBuy")),
        "foreign_sell": _parse_num(row.get(foreign_sell_key)),
        "foreign_net": _parse_num(row.get(foreign_net_key)),
        "investment_trust_buy": _parse_num(row.get("SecuritiesInvestmentTrustCompanies-TotalBuy")),
        "investment_trust_sell": _parse_num(row.get("SecuritiesInvestmentTrustCompanies-TotalSell")),
        "investment_trust_net": _parse_num(row.get("SecuritiesInvestmentTrustCompanies-Difference")),
        "dealer_buy": _parse_num(row.get("Dealers-TotalBuy")),
        "dealer_sell": _parse_num(row.get("Dealers -TotalSell")),
        "dealer_net": _parse_num(row.get("Dealers-Difference")),
        "total_net": _parse_num(row.get("TotalDifference")),
    }


def _ad_to_roc(d: date) -> str:
    """Convert AD date to ROC calendar string 'YYY/MM/DD'."""
    roc_year = d.year - 1911
    return f"{roc_year}/{d.month:02d}/{d.day:02d}"


async def _fetch_openapi(trade_date: date) -> list[dict]:
    """Fetch from TPEx OpenAPI (returns today's data only)."""
    async with httpx.AsyncClient(timeout=30.0, headers=_HEADERS) as client:
        r = await client.get(_OPENAPI_URL)
        r.raise_for_status()
        rows = r.json()

    if not rows:
        return []

    # OpenAPI date field: '1150325' → ROC year 115, month 03, day 25
    first_date_str = rows[0].get("Date", "") if rows else ""
    if first_date_str:
        try:
            roc_year = int(first_date_str[:3])
            month = int(first_date_str[3:5])
            day = int(first_date_str[5:7])
            api_date = date(roc_year + 1911, month, day)
            if api_date != trade_date:
                logger.debug(
                    "TPEx OpenAPI date=%s != requested=%s, skipping", api_date, trade_date
                )
                return []
        except (ValueError, IndexError):
            pass

    records = [rec for row in rows if (rec := _parse_openapi_row(row, trade_date))]
    return records


async def _fetch_legacy(trade_date: date) -> list[dict]:
    """Fetch from TPEx legacy API using ROC date format."""
    roc_date = _ad_to_roc(trade_date)
    params = {"l": "zh-tw", "d": roc_date, "se": "EW", "o": "json"}

    async with httpx.AsyncClient(timeout=30.0, headers=_HEADERS) as client:
        r = await client.get(_LEGACY_URL, params=params)
        r.raise_for_status()
        data = r.json()

    # API v2 format: {"tables": [{"data": [...]}]}
    tables = data.get("tables", [])
    rows = tables[0].get("data", []) if tables else []

    # Fallback: old format with aaData
    if not rows:
        rows = data.get("aaData", [])

    if not rows:
        return []

    # New 24-column format:
    # 0:代號 1:名稱
    # 2:外資買 3:外資賣 4:外資淨
    # 5:外資自營買 6:外資自營賣 7:外資自營淨
    # 8:投信買 9:投信賣 10:投信淨
    # 11:自營(自行)買 12:自營(自行)賣 13:自營(自行)淨
    # 14:自營(避險)買 15:自營(避險)賣 16:自營(避險)淨
    # 17:自營合計買 18:自營合計賣 19:自營合計淨
    # 20:三大合計買 21:三大合計賣 22:三大合計淨
    # 23:三大法人買賣超合計
    records = []
    for row in rows:
        if not isinstance(row, list):
            continue
        stock_id = str(row[0]).strip()
        if not stock_id or not stock_id[0].isdigit():
            continue
        if len(row) >= 24:
            # New format
            records.append({
                "stock_id": stock_id,
                "trade_date": trade_date,
                "foreign_buy": _parse_num(row[2]),
                "foreign_sell": _parse_num(row[3]),
                "foreign_net": _parse_num(row[4]),
                "investment_trust_buy": _parse_num(row[8]),
                "investment_trust_sell": _parse_num(row[9]),
                "investment_trust_net": _parse_num(row[10]),
                "dealer_buy": _parse_num(row[17]),
                "dealer_sell": _parse_num(row[18]),
                "dealer_net": _parse_num(row[19]),
                "total_net": _parse_num(row[23]),
            })
        elif len(row) >= 12:
            # Old 12-column format (fallback)
            records.append({
                "stock_id": stock_id,
                "trade_date": trade_date,
                "foreign_buy": _parse_num(row[2]),
                "foreign_sell": _parse_num(row[3]),
                "foreign_net": _parse_num(row[4]),
                "investment_trust_buy": _parse_num(row[5]),
                "investment_trust_sell": _parse_num(row[6]),
                "investment_trust_net": _parse_num(row[7]),
                "dealer_buy": _parse_num(row[8]),
                "dealer_sell": _parse_num(row[9]),
                "dealer_net": _parse_num(row[10]),
                "total_net": _parse_num(row[11]),
            })
    return records


async def fetch_tpex_institutional(trade_date: date) -> list[dict]:
    """
    Fetch all TPEx listed stocks' institutional trading for a given date.
    Tries OpenAPI first; falls back to legacy API.

    Returns:
        List of records for institutional_trading table.
    """
    logger.info("Fetching TPEx institutional for date=%s ...", trade_date)

    records = await _fetch_openapi(trade_date)
    if records:
        logger.info("TPEx OpenAPI: %d records for %s", len(records), trade_date)
        return records

    logger.info("TPEx OpenAPI empty, trying legacy API ...")
    records = await _fetch_legacy(trade_date)
    logger.info("TPEx legacy: %d records for %s", len(records), trade_date)
    return records
