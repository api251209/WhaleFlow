"""
TWSE 三大法人買賣超 fetcher.

Source: 台灣證券交易所 T86
URL: https://www.twse.com.tw/rwd/zh/fund/T86?date=YYYYMMDD&selectType=ALL
Returns: All listed stocks for a given trading date in one call.
"""

from datetime import date

import httpx

from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.twse.com.tw/",
}

# Field index mapping from T86 response
# ['證券代號','證券名稱','外陸資買進','外陸資賣出','外陸資買賣超',
#  '外資自營商買進','外資自營商賣出','外資自營商買賣超',
#  '投信買進','投信賣出','投信買賣超',
#  '自營商買賣超','自營商買進(自行)','自營商賣出(自行)','自營商買賣超(自行)',
#  '自營商買進(避險)','自營商賣出(避險)','自營商買賣超(避險)',
#  '三大法人買賣超股數']
_IDX = {
    "stock_id": 0,
    "stock_name": 1,
    "foreign_buy": 2,
    "foreign_sell": 3,
    "foreign_net": 4,
    "investment_trust_buy": 8,
    "investment_trust_sell": 9,
    "investment_trust_net": 10,
    "dealer_net": 11,
    "dealer_buy": 12,
    "dealer_sell": 13,
    "total_net": 18,
}


def _parse_num(s: str) -> int:
    return int(s.replace(",", "").strip()) if s.strip() else 0


def _parse_row(row: list[str], trade_date: date) -> dict | None:
    if len(row) <= 18:
        return None
    stock_id = row[_IDX["stock_id"]].strip()
    if not stock_id or not stock_id[0].isdigit():
        return None
    return {
        "stock_id": stock_id,
        "trade_date": trade_date,
        "foreign_buy": _parse_num(row[_IDX["foreign_buy"]]),
        "foreign_sell": _parse_num(row[_IDX["foreign_sell"]]),
        "foreign_net": _parse_num(row[_IDX["foreign_net"]]),
        "investment_trust_buy": _parse_num(row[_IDX["investment_trust_buy"]]),
        "investment_trust_sell": _parse_num(row[_IDX["investment_trust_sell"]]),
        "investment_trust_net": _parse_num(row[_IDX["investment_trust_net"]]),
        "dealer_buy": _parse_num(row[_IDX["dealer_buy"]]),
        "dealer_sell": _parse_num(row[_IDX["dealer_sell"]]),
        "dealer_net": _parse_num(row[_IDX["dealer_net"]]),
        "total_net": _parse_num(row[_IDX["total_net"]]),
    }


async def fetch_twse_institutional(trade_date: date) -> list[dict]:
    """
    Fetch all TWSE listed stocks' institutional trading for a given date.

    Returns:
        List of records ready for insertion into institutional_trading table.
        Empty list if no data (holiday / non-trading day).
    """
    date_str = trade_date.strftime("%Y%m%d")
    logger.info("Fetching TWSE T86 institutional for date=%s ...", date_str)

    async with httpx.AsyncClient(timeout=30.0, headers=_HEADERS) as client:
        r = await client.get(_URL, params={"date": date_str, "selectType": "ALL"})
        r.raise_for_status()
        data = r.json()

    if data.get("stat") != "OK":
        logger.warning("TWSE T86 returned stat=%s for %s", data.get("stat"), date_str)
        return []

    raw_rows = data.get("data", [])
    records = [rec for row in raw_rows if (rec := _parse_row(row, trade_date))]
    logger.info("TWSE: parsed %d institutional records for %s", len(records), date_str)
    return records
